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

    @task(retries=2, execution_timeout=timedelta(minutes=10))
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

    cfg = prepare_run()
    cfg = run_agent(cfg)
    cfg = run_eval(cfg)
    cfg = summarize(cfg)
    cfg = upload_artifacts(cfg)
    log_mlflow(cfg)


evaluate_agent()
