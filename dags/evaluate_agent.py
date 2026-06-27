"""Configurable Airflow DAG: run a coding agent on SWE-bench, then evaluate.

Workflow:  prepare_run -> run_agent -> run_eval -> summarize_and_log

Everything is driven by Airflow params (Trigger DAG w/ config in the UI). No
experiment values are hard-coded; defaults live in ``pipeline.runconfig`` only
so a bare trigger still works. Each run produces a self-contained
``runs/<run-id>/`` folder and one MLflow run.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

# Make the sibling ``pipeline`` package importable from the dags folder.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.runconfig import build_run_config, prepare_run_dir  # noqa: E402
from pipeline.steps import run_agent_batch, run_swebench_eval      # noqa: E402
from pipeline.metrics import collect_metrics, write_metrics, write_manifest  # noqa: E402

VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

# DockerOperator is the preferred production-style way to run a step in the
# project Dockerfile image. It's optional: the `docker` provider may be absent
# from a bare standalone Airflow, so import it defensively. If it's missing the
# DAG still parses and only the (verified) subprocess eval path is offered.
try:
    from airflow.providers.docker.operators.docker import DockerOperator  # noqa: E402
    from docker.types import Mount  # noqa: E402

    _HAS_DOCKER_PROVIDER = True
except ImportError:  # pragma: no cover - depends on the Airflow image
    _HAS_DOCKER_PROVIDER = False


def _xcom(key: str) -> str:
    """Jinja expression pulling one field from run_agent's XCom run_config."""
    return "{{ ti.xcom_pull(task_ids='run_agent')['" + key + "'] }}"

PARAMS = {
    # --- required ---
    "subset": Param(
        "verified", type="string", enum=["lite", "verified", "full", "multimodal"],
        title="SWE-bench subset",
    ),
    "split": Param("test", type="string", title="Dataset split"),
    "workers": Param(4, type="integer", minimum=1, maximum=16, title="Parallel workers"),
    # --- optional but useful ---
    "model": Param(
        "nebius/moonshotai/Kimi-K2.6", type="string", title="Model (litellm name)",
    ),
    "task_slice": Param(
        "0:3", type="string", title="Instance slice (e.g. 0:3)",
        description="Subset of instances to run; empty = all.",
    ),
    "run_id": Param(
        "", type="string", title="Run id (blank = auto timestamp)",
    ),
    "cost_limit": Param(2.0, type="number", minimum=0, title="Per-instance cost limit ($)"),
    "step_limit": Param(75, type="integer", minimum=1, title="Per-instance step limit"),
    # --- execution isolation ---
    "eval_executor": Param(
        "subprocess", type="string", enum=["subprocess", "docker"],
        title="Eval executor",
        description=(
            "subprocess = run the SWE-bench eval in the project .venv (verified). "
            "docker = run it via DockerOperator in the project Dockerfile image."
        ),
    ),
    "eval_image": Param(
        "mlops-assignment:latest", type="string",
        title="Eval Docker image (used when eval_executor=docker)",
        description="Build first with:  docker build -t mlops-assignment:latest .",
    ),
}


@dag(
    dag_id="evaluate_agent",
    description="Run mini-swe-agent on SWE-bench and evaluate the patches.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params=PARAMS,
    tags=["mlops", "swe-bench", "evaluation"],
)
def evaluate_agent():
    @task(execution_timeout=timedelta(minutes=5))
    def prepare_run(**context) -> dict:
        """Read params, build the run config, create runs/<id>/config.json."""
        run_config = build_run_config(context["params"])
        run_dir = prepare_run_dir(run_config)
        print(f"[prepare_run] run_id={run_config['run_id']} dir={run_dir}")
        return run_config

    @task(execution_timeout=timedelta(hours=6))
    def run_agent(run_config: dict) -> dict:
        """Run the agent batch -> run-agent/preds.json + trajectories."""
        preds = run_agent_batch(run_config)
        print(f"[run_agent] predictions at {preds}")
        return run_config

    @task(retries=1, execution_timeout=timedelta(hours=2))
    def run_eval(run_config: dict) -> dict:
        """Evaluate predictions with the SWE-bench harness -> run-eval/."""
        eval_dir = run_swebench_eval(run_config)
        print(f"[run_eval] eval output at {eval_dir}")
        return run_config

    @task(
        retries=2,
        execution_timeout=timedelta(minutes=10),
        trigger_rule="none_failed_min_one_success",
    )
    def summarize(run_config: dict) -> dict:
        """Parse reports -> metrics.json + manifest.json."""
        metrics = collect_metrics(run_config)
        write_metrics(run_config, metrics)
        write_manifest(run_config, metrics)
        print(f"[summarize] metrics={metrics}")
        return run_config

    @task(retries=2, execution_timeout=timedelta(minutes=30))
    def upload_artifacts(run_config: dict) -> dict:
        """Upload runs/<run-id>/ to Object Storage (no-op if S3 unconfigured)."""
        subprocess.run(
            [str(VENV_PYTHON), "-m", "pipeline.storage", "--run-dir", run_config["run_dir"]],
            cwd=str(PROJECT_ROOT),
            check=True,
        )
        return run_config

    @task(retries=2, execution_timeout=timedelta(minutes=10))
    def log_mlflow(run_config: dict) -> None:
        """Log params, metrics, and artifact references (incl. S3 URI) to MLflow."""
        # Runs in the project venv (mlflow not in Airflow's env). Reads the
        # on-disk config.json, which upload_artifacts has stamped with the URI.
        subprocess.run(
            [str(VENV_PYTHON), "-m", "pipeline.tracking", "--run-dir", run_config["run_dir"]],
            cwd=str(PROJECT_ROOT),
            check=True,
        )

    @task.branch
    def choose_eval(**context) -> str:
        """Route to the eval implementation selected by the eval_executor param."""
        mode = str(context["params"].get("eval_executor", "subprocess"))
        if mode == "docker" and _HAS_DOCKER_PROVIDER:
            return "run_eval_docker"
        if mode == "docker":
            print("[choose_eval] docker provider not installed; using subprocess")
        return "run_eval"

    cfg = prepare_run()
    agent = run_agent(cfg)
    branch = choose_eval()
    agent >> branch

    # Path A (default, verified): eval in the project .venv via subprocess.
    eval_subprocess = run_eval(agent)
    branch >> eval_subprocess
    eval_tasks: list = [eval_subprocess]

    # Path B (preferred production style): eval in the project Dockerfile image.
    # The repo is bind-mounted at the same absolute path and the docker socket is
    # shared, so the SWE-bench harness can spawn its per-instance containers on
    # the host daemon (Docker-out-of-Docker).
    if _HAS_DOCKER_PROVIDER:
        docker_eval_command = (
            f"set -eu && mkdir -p {_xcom('eval_dir')} && cd {_xcom('eval_dir')} && "
            "python -m swebench.harness.run_evaluation "
            f"--dataset_name {_xcom('eval_dataset')} "
            f"--split {_xcom('split')} "
            f"--predictions_path {_xcom('preds_path')} "
            f"--max_workers {_xcom('workers')} "
            f"--run_id {_xcom('run_id')} "
            f"--report_dir {_xcom('eval_dir')}"
        )
        run_eval_docker = DockerOperator(
            task_id="run_eval_docker",
            image="{{ params.eval_image }}",
            # DockerOperator execs argv directly (no implicit shell), so wrap the
            # script in `sh -c` — otherwise `&&`/`cd`/`set` aren't understood.
            command=["/bin/sh", "-c", docker_eval_command],
            docker_url="unix:///var/run/docker.sock",
            network_mode="bridge",
            auto_remove="success",
            mount_tmp_dir=False,
            working_dir=str(PROJECT_ROOT),
            mounts=[
                Mount(source=str(PROJECT_ROOT), target=str(PROJECT_ROOT), type="bind"),
                Mount(
                    source="/var/run/docker.sock",
                    target="/var/run/docker.sock",
                    type="bind",
                ),
            ],
            retries=1,
            execution_timeout=timedelta(hours=2),
            doc_md=(
                "Run the SWE-bench eval inside the project Dockerfile image "
                "(`mlops-assignment:latest`) via DockerOperator — the preferred "
                "production-style execution-isolation path. Alternative to the "
                "subprocess `run_eval` task; selected by `eval_executor=docker`."
            ),
        )
        branch >> run_eval_docker
        eval_tasks.append(run_eval_docker)

    summary = summarize(agent)
    for _eval in eval_tasks:
        _eval >> summary

    uploaded = upload_artifacts(summary)
    log_mlflow(uploaded)


evaluate_agent()
