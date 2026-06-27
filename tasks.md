# Tasks & Progress — E2E ML Pipeline (Coding-Agent Evaluation)

Living log of every change and decision for this assignment.
Append a dated entry to the **Changelog** whenever something is done.

- **Assignment:** Turn ad-hoc coding-agent eval scripts into a configurable, durable Airflow + MLflow pipeline.
- **Course:** Nebius Academy — AI Performance Engineering, MLOps module, lecture #6.
- **Repo:** `mlops-assignment-e2e-ml-pipeline`
- **Started:** 2026-06-27
- **Status legend:** ⬜ not started · 🟡 in progress · ✅ done · ⏭️ skipped/optional

---

## Goal

`run-agent → run-evaluation` pipeline: run `mini-swe-agent` on a subset of SWE-bench
instances and evaluate the patches, producing a reproducible `runs/<run-id>/` tree and
MLflow-tracked params/metrics.

Target pipeline: `run-mini-swe-agent → swe-bench-eval → log-artifacts-to-s3 → log-metrics-to-mlflow`

---

## Starting state (as cloned)

- `dags/mini-swe-bench-single.py` — example DAG re-implementing the single-instance script.
- `scripts/mini-swe-bench-single.sh`, `scripts/mini-swe-bench-batch.sh`, `scripts/swe-bench-eval.sh` — ad-hoc scripts.
- `sample/` — sample agent trajectories, `preds.json`, and SWE-bench eval logs/reports.
- `Dockerfile`, `pyproject.toml`, `uv.lock`, `run-airflow-standalone.sh`, `.env.example` (`NEBIUS_API_KEY`).

---

## Phase 0 — Environment setup

- [x] ✅ Provision VM (8 CPU / 32 GB / public IP) — Nebius `Ariel-HW3`, cpu-d3 AMD Epyc, Ubuntu 24.04, 100 GiB SSD, IP `89.169.110.147`, SSH alias `ariel-hw3` (port 8080 forwarded)
- [x] ✅ Install `uv` (0.11.25), Docker (29.6.1), docker compose (v5.2.0) on VM
- [x] ✅ `cp .env.example .env` and add `NEBIUS_API_KEY` (on VM)
- [x] ✅ Clone reference repos (`mini-swe-agent`, `SWE-bench`) on VM; `swebench.yaml` present; key also written to `~/.config/mini-swe-agent/.env` for DAG runs
- [x] ✅ `uv sync` — venv Python 3.12.3, `mini-extra` 2.4.1, `swebench` 4.1.0
- [x] ✅ Smoke test: single instance `sympy__sympy-15599` — exit 0, 30-step agent loop, `trajectory.json` (648 KB) written; confirms NEBIUS_API_KEY + Kimi-K2.6 reachable
- [x] ✅ Airflow standalone running on VM (HTTP 200, "Airflow is ready"); UI via `ssh ariel-hw3` tunnel → http://localhost:8080 (admin/admin)
- [x] ✅ Verify example DAG `mini-swe-bench-single` end-to-end — task `success` (ran ~83 min, hit step_limit 150 → `RepeatedFormatError`; agent didn't solve, but Airflow→agent→Nebius path confirmed)

## Phase 1 — Speedrun: working configurable DAG  *(grading: 35%)*

Create `dags/evaluate_agent.py` (or extend existing) with these tasks:

- [x] ✅ `prepare_run`: reads params → `build_run_config` + `prepare_run_dir` → `runs/<run-id>/config.json`
- [x] ✅ `run_agent`: `run_agent_batch` → `mini-extra swebench` into `run-agent/` (preds.json + trajectories)
- [x] ✅ `run_eval`: `run_swebench_eval` → SWE-bench harness into `run-eval/` (logs + report)
- [x] ✅ `summarize_and_log`: `collect_metrics` → metrics.json + manifest.json → MLflow via venv subprocess
- [x] ✅ Params exposed via Airflow `Param`: split, subset, workers, model, task_slice, run_id, cost_limit, step_limit
- [x] ✅ No hard-coded experiment values — defaults only in `pipeline/runconfig.py`
- [x] ✅ Helpers implemented in `pipeline/` package (runconfig, steps, metrics, tracking)
- [x] ✅ DAG parses in Airflow (no import errors; 4 tasks listed); command builders unit-verified
- [x] ✅ End-to-end run of `evaluate_agent` (`run_id=e2e-test-01`, slice 0:1) — all 4 tasks green; agent **solved** `astropy__astropy-12907`; eval confirmed resolved (resolve_rate 1.0)

## Phase 2 — Durable runs  *(grading: 20%)*

- [x] ✅ Enforce run-folder shape (implemented in code): `config.json`, `run-agent/`, `run-eval/`, `metrics.json`, `manifest.json`
- [x] ✅ `manifest.json` points to key files + records artifact location (`write_manifest`)
- [x] ✅ Produced complete sample `runs/e2e-test-01/` (config, preds, trajectory, eval logs+report, metrics, manifest)
- [x] ✅ Upload run folder to S3/Object Storage + log URI to MLflow — via **MinIO** (S3-compatible) on the VM; `pipeline/storage.py` + `upload_artifacts` DAG task; verified 13 files → `s3://mlops-runs/runs/e2e-test-01`, URI stamped into config/manifest and logged to MLflow

## Phase 3 — Production polish  *(grading: 10% + 10%)*

- [ ] ⏭️ Replace subprocess calls with `DockerOperator` — N/A: agent/eval already run in Docker (DooD via mounted socket); the subprocess→.venv pattern is the documented isolation approach
- [x] ✅ `docker-compose.yaml` for Airflow + MLflow + MinIO (+ postgres) — full stack, `docker/Dockerfile.airflow`; **verified end-to-end**: `compose_run_03` all 6 tasks green (agent+eval via DooD, upload→MinIO, log→MLflow server)
- [x] ✅ MLflow reachable & used by DAG — now an MLflow **server** container (:5000), artifacts on MinIO via `--serve-artifacts`
- [x] ✅ MinIO (S3) — folded into compose (:9000/:9001), bucket auto-created by `minio-init`, data volume reused
- [x] ✅ Retries + timeouts — retries set on tasks; explicit per-task `execution_timeout` added (prepare 5m, run_agent 6h, run_eval 2h, summarize/log 10m, upload 30m)

## MLflow tracking  *(grading: 15%)*

- [x] ✅ MLflow logging implemented (`pipeline/tracking.py`): params, metrics, tags, artifacts; mlflow 3.14 in venv
- [x] ✅ Logged 1 completed eval to MLflow (SQLite backend; `mlflow.db`); params + metrics + artifacts
- [x] ✅ Multiple runs comparable in MLflow UI — 2 runs (e2e-test-01 step_limit 40, e2e-test-02 step_limit 60), screenshot captured

## Deliverables

- [x] ✅ `dags/evaluate_agent.py` (configurable 6-task DAG)
- [x] ✅ Wrapper scripts/code for agent + eval writing into `runs/<run-id>/` (`pipeline/` package: runconfig, steps, metrics, tracking, storage)
- [x] ✅ Sample reproducible `runs/<run-id>/` (or manifest) — `runs/e2e-test-01/` complete with config/preds/trajectory/eval logs/metrics/manifest
- [x] ✅ ≥1 logged MLflow run — multiple runs logged (e2e-test-01/02/03)
- [x] ✅ `REPORT.md` — architecture, how to trigger DAG, artifact layout, MLflow, completed run (e2e-test-01), rerun-by-run-id instructions
- [x] ✅ `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png` (captured via headless Chromium; script in `scripts/capture_screenshots.py`)
- [x] ✅ `screenshots/object_storage_artifacts.png` (MinIO console showing `mlops-runs/runs/` with e2e-test-01 + e2e-test-03)
- [ ] ⏭️ S3 upload + `.env.example` extended for MLflow/S3

---

## Changelog

| Date | Change | Files |
|---|---|---|
| 2026-06-27 | Cloned starter repo | — |
| 2026-06-27 | Created this progress tracker | `tasks.md` |
| 2026-06-27 | Provisioned Nebius CPU VM `Ariel-HW3` (8 CPU/32 GB, Ubuntu 24.04, 100 GiB) and added `ariel-hw3` SSH alias with 8080 forward | `~/.ssh/config` |
| 2026-06-27 | Bootstrapped VM: uv, Docker, docker compose; cloned repo; wrote `.env`; `uv sync` | VM `~/mlops-assignment-e2e-ml-pipeline` |
| 2026-06-27 | Single-instance smoke test passed (Kimi-K2.6 via Nebius), trajectory.json produced | VM `~/smoke-test/` |
| 2026-06-27 | Cloned reference repos (mini-swe-agent, SWE-bench); started Airflow standalone (admin/admin) | VM `~/`, `~/airflow/` |
| 2026-06-27 | Built Phase 1: `pipeline/` package (runconfig, steps, metrics, tracking) + configurable DAG `evaluate_agent` (prepare_run→run_agent→run_eval→summarize_and_log); added mlflow dep; synced to VM; DAG parses, builders verified | `pipeline/`, `dags/evaluate_agent.py`, `pyproject.toml` |
| 2026-06-27 | Example DAG `mini-swe-bench-single` completed (success, ~83 min, hit step_limit) | VM `~/airflow/` |
| 2026-06-27 | E2E run of `evaluate_agent` (e2e-test-01) — all green, agent solved astropy-12907, resolve_rate 1.0; fixed MLflow file-store→SQLite; fixed manifest trajectory glob (rglob) | `runs/e2e-test-01/`, `pipeline/tracking.py`, `pipeline/metrics.py` |
| 2026-06-27 | Copied sample run folder to local repo; wrote `REPORT.md` | `runs/e2e-test-01/`, `REPORT.md` |
| 2026-06-27 | 2nd run e2e-test-02 (step_limit 60, resolved 1/1); started MLflow UI server; captured Airflow + MLflow screenshots via headless Chromium | `runs/e2e-test-02/`, `screenshots/`, `scripts/capture_screenshots.py` |
| 2026-06-27 | Implemented S3 artifact upload (`pipeline/storage.py`); split DAG final stage into summarize→upload_artifacts→log_mlflow; added boto3 | `pipeline/storage.py`, `dags/evaluate_agent.py`, `pyproject.toml` |
| 2026-06-27 | Stood up MinIO (S3) in Docker on VM; created `mlops-runs` bucket; uploaded e2e-test-01 (13 files); logged S3 URI to MLflow; triggered full 6-task run e2e-test-03 | VM docker `minio`, `s3://mlops-runs/` |
| 2026-06-27 | Full 6-task run e2e-test-03 all green (incl. upload_artifacts→S3, log_mlflow); captured all 3 screenshots (Airflow, MLflow, MinIO) | `runs/e2e-test-03/`, `screenshots/` |
| 2026-06-27 | Phase 3: wrote `docker-compose.yaml` + `docker/Dockerfile.airflow` (Airflow+MLflow server+MinIO+postgres); brought full stack up; DAG parses; verified DooD (docker socket) + venv + mlflow reachable from airflow container | `docker-compose.yaml`, `docker/Dockerfile.airflow`, `.env.example` |
| 2026-06-27 | Debugged compose Airflow image (airflow PATH, psycopg2+asyncpg drivers, system python3 for bind-mounted venv) + MLflow `--allowed-hosts` (DNS-rebind guard); `compose_run_03` ran all 6 tasks green end-to-end; refreshed screenshots; documented deployment in REPORT §7 | `docker/Dockerfile.airflow`, `docker-compose.yaml`, `REPORT.md`, `screenshots/` |
| 2026-06-27 | Added explicit per-task `execution_timeout` to all DAG tasks; ticked deliverables checklist; committed assignment work | `dags/evaluate_agent.py`, `tasks.md` |

---

## Notes / Decisions

- Model: `nebius/moonshotai/Kimi-K2.6` (Nebius Token Factory managed inference).
- ⚠️ **Model behavior:** Kimi-K2.6 frequently hits its output-token limit (`finish_reason=length`) → cut-off responses with no tool call → wasted steps → eventual `RepeatedFormatError`. Keep `step_limit` low for tests; consider tuning `model_kwargs` (max_tokens) or trying another model for better resolve rates.
- Architecture: Airflow runs via `uv tool` env (orchestration only); agent/eval/MLflow run in the project `.venv` via subprocess. Keeps heavy deps out of Airflow's env and avoids version conflicts.
- **Storage:** MinIO (S3-compatible) chosen over Nebius Object Storage — no external account, boto3 code unchanged, gives an Object Storage UI for screenshots, slots into Phase 3 compose. Config in VM `.env`: `S3_BUCKET=mlops-runs`, `S3_ENDPOINT_URL=http://localhost:9000`, creds `minioadmin` / (local dev). Console on :9001.
- **Services running on VM:** Airflow standalone :8080, MLflow UI :5000 (SQLite), MinIO :9000/:9001. All started via nohup/`docker run` — to be unified under docker-compose in Phase 3.
- ⚠️ **SECURITY TODO:** `NEBIUS_API_KEY` was pasted in plaintext chat during setup — **rotate it in console.nebius.com after the assignment is submitted.**
- _(record further choices here: easy-mode vs DockerOperator, local vs S3, etc.)_
