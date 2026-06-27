# REPORT — End-to-End Coding-Agent Evaluation Pipeline

Turns the ad-hoc `scripts/` into a configurable, reproducible **Airflow + MLflow**
pipeline that runs `mini-swe-agent` on SWE-bench instances and evaluates the
produced patches with the SWE-bench harness.

- **Author:** arielmit
- **VM:** Nebius `Ariel-HW3` (8 vCPU / 32 GB, Ubuntu 24.04, CPU-only) — inference via Nebius Token Factory
- **Model:** `nebius/moonshotai/Kimi-K2.6`
- **Workflow:** `prepare_run → run_agent → run_eval → summarize_and_log`

---

## 1. Architecture

```
Airflow DAG: evaluate_agent
┌──────────────┐   ┌───────────────┐   ┌──────────────┐   ┌─────────────────────┐
│ prepare_run  │──▶│  run_agent    │──▶│  run_eval    │──▶│ summarize_and_log   │
│ params →     │   │ mini-swe-agent│   │ SWE-bench    │   │ metrics.json +      │
│ config.json  │   │ → preds.json  │   │ harness      │   │ manifest.json +     │
│              │   │ + trajectories│   │ → logs+report│   │ → MLflow            │
└──────────────┘   └───────────────┘   └──────────────┘   └─────────────────────┘
        │                  │                   │                     │
        └──────────────── runs/<run-id>/ (single source of truth) ──┘
```

**Separation of concerns** (deliberate, to avoid dependency conflicts):

- **Airflow** runs in its own `uv tool` environment and only *orchestrates*.
- **Agent / eval / MLflow** run in the project **`.venv`** via subprocess, so the
  heavy `mini-swe-agent`, `swebench`, and `mlflow` deps never collide with Airflow.
- All pipeline logic lives in the importable, testable **`pipeline/`** package;
  the DAG is a thin wiring layer.

| File | Responsibility |
|---|---|
| `dags/evaluate_agent.py` | The DAG: params, task graph, wiring |
| `pipeline/runconfig.py` | `build_run_config`, `prepare_run_dir`, dataset mapping, defaults |
| `pipeline/steps.py` | `run_agent_batch`, `run_swebench_eval` (+ testable command builders) |
| `pipeline/metrics.py` | `collect_metrics`, `write_metrics`, `write_manifest` |
| `pipeline/tracking.py` | `log_mlflow_run` (also runnable as a CLI in the venv) |

---

## 2. How to trigger a run

### Prerequisites (on the VM)
```bash
uv sync                                   # installs mini-swe-agent, swebench, mlflow
echo "NEBIUS_API_KEY=..." > ~/.config/mini-swe-agent/.env   # picked up by the agent
# reference repo (provides the agent's swebench.yaml config)
git clone https://github.com/SWE-agent/mini-swe-agent.git   # sibling of this repo
```

### Start the stack
**Option A — docker-compose (production-style, recommended):**
```bash
docker compose up -d --build      # postgres + minio + mlflow server + airflow
# Airflow :8080 · MLflow :5000 · MinIO console :9001
docker compose exec airflow cat /root/airflow/simple_auth_manager_passwords.json.generated
```

**Option B — standalone (dev):**
```bash
bash run-airflow-standalone.sh            # serves on :8080 (admin / admin)
ssh ariel-hw3                             # SSH alias forwards 8080 to localhost
# browse http://localhost:8080
```

### Trigger from the UI
`DAGs → evaluate_agent → Trigger DAG w/ config`, then set parameters:

| Param | Required | Default | Notes |
|---|---|---|---|
| `subset` | ✅ | `verified` | `lite` / `verified` / `full` / `multimodal` |
| `split` | ✅ | `test` | dataset split |
| `workers` | ✅ | `4` | parallel agent + eval workers |
| `model` | | `nebius/moonshotai/Kimi-K2.6` | any litellm model name |
| `task_slice` | | `0:3` | instance slice; blank = all |
| `run_id` | | _(auto timestamp)_ | identifies the run folder |
| `cost_limit` | | `2.0` | per-instance $ cap |
| `step_limit` | | `75` | per-instance step cap (keeps wandering runs bounded) |

**No experiment value is hard-coded** — defaults live only in `pipeline/runconfig.py`.

### Trigger from the CLI
```bash
uv tool run apache-airflow dags trigger evaluate_agent \
  --conf '{"subset":"verified","split":"test","workers":1,"task_slice":"0:1","step_limit":40,"run_id":"e2e-test-01"}'
```

---

## 3. Artifact layout

Every run produces a self-contained folder. Hand someone `runs/<run-id>/` and
they can reconstruct the whole experiment.

```
runs/<run-id>/
├── config.json        # full normalized run config (params + derived + provenance)
├── run-agent/
│   ├── preds.json                      # SWE-bench predictions (instance → patch)
│   ├── <instance>/<instance>.traj.json # agent trajectory
│   └── minisweagent.log
├── run-eval/
│   ├── <model_slug>.<run-id>.json      # SWE-bench summary report (counts)
│   └── logs/run_evaluation/<run-id>/<model_slug>/<instance>/
│       ├── report.json     # per-instance FAIL_TO_PASS / PASS_TO_PASS results
│       ├── patch.diff      # the applied model patch
│       ├── test_output.txt # raw test output
│       └── run_instance.log
├── metrics.json       # parsed metrics (counts + resolve_rate)
└── manifest.json      # index of key files + artifact storage location
```

`manifest.json` is the entry point: it points at config, predictions,
trajectories, eval report/logs, and metrics, and records where the canonical
artifacts live (`local_path` now; `remote_uri` once S3 upload is added).

---

## 4. MLflow tracking

- Backend: **SQLite** (`sqlite:///mlflow.db`) — MLflow 3.x rejects the legacy
  file store. Override with `MLFLOW_TRACKING_URI` (e.g. an `http://` server in a
  Compose deployment).
- Experiment: `swe-bench-eval` (override via `MLFLOW_EXPERIMENT_NAME`).
- Each run logs: **params** (run_id, model, split, subset, workers, task_slice,
  cost_limit, step_limit, eval_dataset), **metrics** (all instance counts +
  `resolve_rate`, `resolve_rate_total`), **tags** (`run_dir`, `artifact_remote_uri`),
  and **artifacts** (config.json, metrics.json, manifest.json).

Browse:
```bash
uv run mlflow ui --backend-store-uri sqlite:///mlflow.db   # then forward :5000
```

---

## 5. A completed run: `e2e-test-01`

Triggered with `subset=verified, split=test, task_slice=0:1, workers=1, step_limit=40`.

| Task | State | Duration |
|---|---|---|
| prepare_run | ✅ success | <1s |
| run_agent | ✅ success | ~2m50s |
| run_eval | ✅ success | ~1m17s |
| summarize_and_log | ✅ success | ~2s |

**Result:** the agent solved **`astropy__astropy-12907`** and the SWE-bench
harness confirmed it.

```json
// runs/e2e-test-01/metrics.json
{
  "submitted_instances": 1,
  "resolved_instances": 1,
  "unresolved_instances": 0,
  "resolve_rate": 1.0,
  "resolve_rate_total": 0.002
}
```

The fix changed `astropy/modeling/separable.py` (`_cstack`), turning the two
`FAIL_TO_PASS` tests green while keeping all 13 `PASS_TO_PASS` tests passing.
Full evidence in `runs/e2e-test-01/` (patch.diff, test_output.txt, report.json).

> Note on the model: `Kimi-K2.6` frequently hits its output-token limit
> (`finish_reason=length`), producing cut-off turns without a tool call. The
> bounded `step_limit` param keeps such runs from grinding to the upstream
> default of 250 steps.

---

## 6. Reproduce by run-id

The run folder fully determines the run. To re-evaluate existing predictions:
```bash
# re-run evaluation from a run's preds.json
uv run python -m swebench.harness.run_evaluation \
  --dataset_name princeton-nlp/SWE-bench_Verified --split test \
  --predictions_path runs/e2e-test-01/run-agent/preds.json \
  --run_id e2e-test-01 --report_dir runs/e2e-test-01/run-eval

# re-log metrics + manifest to MLflow
uv run python -m pipeline.tracking --run-dir runs/e2e-test-01
```
To reproduce the whole experiment, trigger `evaluate_agent` with the same
`config.json` params (a fresh `run_id`).

---

## 7. Deployment (docker-compose)

`docker-compose.yaml` brings up the full stack; `docker/Dockerfile.airflow`
builds the orchestrator image.

| Service | Image | Purpose | Port |
|---|---|---|---|
| `postgres` | postgres:16 | Airflow metadata DB | — |
| `minio` | minio/minio | S3-compatible Object Storage | 9000 / 9001 |
| `minio-init` | minio/mc | creates the `mlops-runs` bucket, exits | — |
| `mlflow` | python:3.12-slim | MLflow tracking server (artifacts → MinIO) | 5000 |
| `airflow` | built here | standalone Airflow running the DAG | 8080 |

**Execution isolation / Docker-out-of-Docker.** The Airflow image is built on
`ubuntu:24.04` (same as the host) and mounts the host Docker socket plus the repo
at the *same absolute path*. So when `mini-swe-agent` and SWE-bench spawn their
per-instance containers, those run on the host daemon with volume paths that
resolve correctly. Airflow stays in its own (uv tool) env and shells out to the
bind-mounted project `.venv` — the same separation used in dev. `python3` is
installed in the image because the venv's interpreter symlink targets the system
Python.

In-cluster endpoints (`S3_ENDPOINT_URL=http://minio:9000`,
`MLFLOW_TRACKING_URI=http://mlflow:5000`) are injected via compose `environment`
and override the localhost defaults in `.env`.

**Why not `DockerOperator`?** The goal behind that recommendation — keep the
heavy agent/eval workloads out of Airflow's own process — is already met. Two
facts make `DockerOperator` redundant here:

1. `mini-swe-agent` and the SWE-bench harness *already* spawn one Docker
   container **per instance** on the host daemon (via the mounted socket, DooD).
   The real work is containerized regardless of which operator triggers it;
   wrapping the trigger in `DockerOperator` would nest a container whose only job
   is to launch more containers.
2. Airflow shells out to the bind-mounted project **`.venv`** via `subprocess`,
   so its orchestration env never carries the `mini-swe-agent` / `swebench` /
   `mlflow` deps — the same dependency isolation `DockerOperator` would provide.

So the architectural intent (containerized, dependency-isolated execution) is
satisfied by DooD + venv subprocess. For *large-scale* isolated execution the
natural next step is `KubernetesPodOperator`, not `DockerOperator`.

## 8. Status & next steps

**Done:** configurable DAG (Phase 1), durable run folders + manifest (Phase 2),
MLflow tracking, S3/Object-Storage upload (MinIO) with URI logged to MLflow,
docker-compose deployment (Airflow + MLflow + MinIO + postgres), multiple
verified end-to-end runs (standalone and compose).

**Possible follow-ups:**
- Swap MinIO for cloud Object Storage by changing only `S3_ENDPOINT_URL` + keys.
- `KubernetesPodOperator` for large-scale isolated execution.
