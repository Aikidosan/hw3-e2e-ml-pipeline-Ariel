"""Reusable helpers for the coding-agent evaluation pipeline.

The Airflow DAG in ``dags/evaluate_agent.py`` is a thin orchestrator; all the
real logic lives here so it can be unit-tested and reused outside Airflow.

Design split:
- ``runconfig``  -- turn Airflow params into a normalized run config + run dir.
- ``steps``      -- build + run the agent (mini-swe-agent) and eval (SWE-bench).
- ``metrics``    -- parse eval reports into metrics.json and a manifest.json.
- ``tracking``   -- log params/metrics/artifacts to MLflow.

Heavy work (agent, eval, MLflow) runs in the project ``.venv`` via subprocess,
so this package only imports the standard library at module import time.
``mlflow`` is imported lazily inside ``tracking`` so the rest stays importable
from the Airflow tool environment.
"""

from .runconfig import build_run_config, prepare_run_dir, PROJECT_ROOT, RUNS_DIR
from .steps import build_agent_command, run_agent_batch, build_eval_command, run_swebench_eval
from .metrics import collect_metrics, write_metrics, write_manifest

__all__ = [
    "build_run_config",
    "prepare_run_dir",
    "PROJECT_ROOT",
    "RUNS_DIR",
    "build_agent_command",
    "run_agent_batch",
    "build_eval_command",
    "run_swebench_eval",
    "collect_metrics",
    "write_metrics",
    "write_manifest",
]
