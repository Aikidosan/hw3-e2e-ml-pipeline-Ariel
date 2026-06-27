"""Log a completed run's params, metrics, and artifacts to MLflow.

``mlflow`` is imported lazily so importing this module never requires it; only
``log_mlflow_run`` does. This file is also runnable as a CLI
(``python -m pipeline.tracking --run-dir runs/<id>``) so the Airflow task can
invoke it inside the project ``.venv`` without MLflow living in Airflow's env.

Tracking URI / experiment are read from the environment:
- ``MLFLOW_TRACKING_URI``   (default: ``sqlite:///<repo>/mlflow.db`` local store;
  MLflow 3.x rejects the legacy bare-file store, so we default to SQLite. Point
  this at an ``http://`` server in Phase 3.)
- ``MLFLOW_EXPERIMENT_NAME`` (default: ``swe-bench-eval``)
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .runconfig import PROJECT_ROOT

_PARAM_KEYS = [
    "run_id", "model", "split", "subset", "workers",
    "task_slice", "cost_limit", "step_limit", "eval_dataset",
]
_METRIC_KEYS = [
    "total_instances", "submitted_instances", "completed_instances",
    "resolved_instances", "unresolved_instances", "empty_patch_instances",
    "error_instances", "resolve_rate", "resolve_rate_total",
]


def log_mlflow_run(run_config: dict, metrics: dict, run_dir: Path) -> str:
    """Log one MLflow run; return its MLflow run_id."""
    import mlflow

    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI", f"sqlite:///{PROJECT_ROOT / 'mlflow.db'}"
    )
    experiment = os.environ.get("MLFLOW_EXPERIMENT_NAME", "swe-bench-eval")
    mlflow.set_tracking_uri(tracking_uri)
    # Artifacts are stored locally next to the run; the run folder itself remains
    # the source of truth (manifest records the canonical location).
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=run_config["run_id"]) as active:
        mlflow.log_params({k: run_config.get(k) for k in _PARAM_KEYS})
        mlflow.log_metrics(
            {k: float(metrics[k]) for k in _METRIC_KEYS if k in metrics}
        )
        # Artifact references: the structured run folder is the source of truth.
        mlflow.set_tag("run_dir", str(run_dir))
        mlflow.set_tag("artifact_remote_uri", run_config.get("artifact_remote_uri", ""))
        for fname in ("config.json", "metrics.json", "manifest.json"):
            fpath = run_dir / fname
            if fpath.exists():
                mlflow.log_artifact(str(fpath))
        print(
            f"[mlflow] logged run {active.info.run_id} "
            f"to experiment '{experiment}' at {tracking_uri}"
        )
        return active.info.run_id


def _main() -> None:
    parser = argparse.ArgumentParser(description="Log a run dir to MLflow.")
    parser.add_argument("--run-dir", required=True, help="Path to runs/<run-id>/")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    run_config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    log_mlflow_run(run_config, metrics, run_dir)


if __name__ == "__main__":
    _main()
