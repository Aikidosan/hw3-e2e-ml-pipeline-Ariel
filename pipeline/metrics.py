"""Parse SWE-bench evaluation output into metrics.json and a run manifest."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

# Count fields emitted in the SWE-bench summary report.
_COUNT_FIELDS = [
    "total_instances",
    "submitted_instances",
    "completed_instances",
    "resolved_instances",
    "unresolved_instances",
    "empty_patch_instances",
    "error_instances",
]


def _find_summary_report(eval_dir: Path, run_id: str) -> Path:
    """Locate the ``<model_slug>.<run_id>.json`` summary report."""
    candidates = sorted(eval_dir.glob(f"*.{run_id}.json"))
    if not candidates:
        # Fall back to any top-level json that looks like a summary report.
        for p in sorted(eval_dir.glob("*.json")):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(data, dict) and "resolved_instances" in data:
                candidates.append(p)
    if not candidates:
        raise FileNotFoundError(
            f"No SWE-bench summary report (*.{run_id}.json) found in {eval_dir}"
        )
    return candidates[0]


def collect_metrics(run_config: dict) -> dict:
    """Read the eval summary report and compute metrics."""
    eval_dir = Path(run_config["eval_dir"])
    report_path = _find_summary_report(eval_dir, run_config["run_id"])
    report = json.loads(report_path.read_text(encoding="utf-8"))

    metrics = {field: int(report.get(field, 0)) for field in _COUNT_FIELDS}
    submitted = metrics["submitted_instances"]
    total = metrics["total_instances"]
    metrics["resolve_rate"] = round(metrics["resolved_instances"] / submitted, 4) if submitted else 0.0
    metrics["resolve_rate_total"] = round(metrics["resolved_instances"] / total, 4) if total else 0.0
    metrics["report_file"] = report_path.name
    return metrics


def write_metrics(run_config: dict, metrics: dict) -> Path:
    """Persist metrics.json into the run directory."""
    path = Path(run_config["metrics_path"])
    path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return path


def _rel(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def write_manifest(run_config: dict, metrics: dict) -> Path:
    """Write manifest.json: an index of key files + where artifacts live.

    The manifest is the single entry point for someone handed the run folder:
    it points at config, predictions, trajectories, eval logs/report, and
    metrics, and records the long-term storage location (local now; an S3 URI
    once remote upload is wired up in Phase 3).
    """
    run_dir = Path(run_config["run_dir"])
    eval_dir = Path(run_config["eval_dir"])
    agent_dir = Path(run_config["agent_dir"])

    # Trajectories live one-per-instance, sometimes in per-instance subdirs.
    trajectories = sorted(str(p.relative_to(run_dir)) for p in agent_dir.rglob("*.traj.json"))
    eval_logs = eval_dir / "logs" / "run_evaluation" / run_config["run_id"]

    manifest = {
        "run_id": run_config["run_id"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model": run_config["model"],
        "params": {
            k: run_config[k]
            for k in ("split", "subset", "workers", "task_slice", "cost_limit", "step_limit")
        },
        "files": {
            "config": _rel(Path(run_config["config_path"]), run_dir),
            "metrics": _rel(Path(run_config["metrics_path"]), run_dir),
            "predictions": _rel(Path(run_config["preds_path"]), run_dir),
            "eval_report": _rel(eval_dir / metrics.get("report_file", ""), run_dir),
            "eval_logs_dir": _rel(eval_logs, run_dir),
            "trajectories": trajectories,
        },
        "metrics_summary": {
            "submitted_instances": metrics.get("submitted_instances"),
            "resolved_instances": metrics.get("resolved_instances"),
            "resolve_rate": metrics.get("resolve_rate"),
        },
        "artifact_storage": {
            # Local now; Phase 3 uploads the folder and fills in remote_uri.
            "local_path": str(run_dir),
            "remote_uri": run_config.get("artifact_remote_uri", ""),
        },
    }
    path = Path(run_config["manifest_path"])
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path
