"""Build a normalized run configuration and the structured run directory.

A "run config" is a plain JSON-serializable dict so it can travel through
Airflow XComs and be written verbatim to ``runs/<run-id>/config.json``. Anyone
who later opens that folder can reconstruct exactly what was executed.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

# Repo root = parent of this package directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"

# mini-swe-agent reference repo (cloned as a sibling of this repo by default).
# Override with MINI_SWE_AGENT_DIR if it lives elsewhere.
MINI_SWE_AGENT_DIR = Path(
    os.environ.get("MINI_SWE_AGENT_DIR", PROJECT_ROOT.parent / "mini-swe-agent")
)
SWEBENCH_AGENT_CONFIG = (
    MINI_SWE_AGENT_DIR / "src/minisweagent/config/benchmarks/swebench.yaml"
)

# Subset -> HuggingFace dataset name used by the SWE-bench evaluation harness.
# (mini-swe-agent maps subsets internally for the agent step via --subset.)
EVAL_DATASET_MAPPING = {
    "verified": "princeton-nlp/SWE-bench_Verified",
    "lite": "princeton-nlp/SWE-bench_Lite",
    "full": "princeton-nlp/SWE-bench",
    "multimodal": "princeton-nlp/SWE-bench_Multimodal",
}

# Defaults for optional params. Required params (split/subset/workers) also get
# defaults so the DAG never crashes on a bare trigger, but they are meant to be
# set from the Airflow UI per experiment -- nothing here is an experiment value
# baked into the pipeline code.
DEFAULTS = {
    "split": "test",
    "subset": "verified",
    "workers": 4,
    "model": "nebius/moonshotai/Kimi-K2.6",
    "task_slice": "0:3",
    "run_id": "",        # empty -> auto-generated
    "cost_limit": 2.0,
    "step_limit": 75,    # bounded to keep wandering runs from grinding to 250
}


def _slugify_model(model: str) -> str:
    """Match SWE-bench's prediction/report naming (``/`` -> ``__``)."""
    return model.replace("/", "__")


def _safe_run_id(value: str) -> str:
    """Keep run ids filesystem- and SWE-bench-safe."""
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")


def build_run_config(params: dict) -> dict:
    """Turn raw Airflow params into a complete, normalized run config dict."""
    p = {**DEFAULTS, **{k: v for k, v in (params or {}).items() if v not in (None, "")}}

    run_id = _safe_run_id(str(p["run_id"])) if p["run_id"] else ""
    if not run_id:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        run_id = _safe_run_id(f"{stamp}-{p['subset']}")

    run_dir = RUNS_DIR / run_id
    config = {
        "run_id": run_id,
        # --- experiment params (from the UI) ---
        "split": str(p["split"]),
        "subset": str(p["subset"]),
        "workers": int(p["workers"]),
        "model": str(p["model"]),
        "task_slice": str(p["task_slice"]),
        "cost_limit": float(p["cost_limit"]),
        "step_limit": int(p["step_limit"]),
        # --- derived ---
        "model_slug": _slugify_model(str(p["model"])),
        "eval_dataset": EVAL_DATASET_MAPPING.get(str(p["subset"]), str(p["subset"])),
        "swebench_agent_config": str(SWEBENCH_AGENT_CONFIG),
        # --- paths (strings so the dict is JSON/XCom friendly) ---
        "project_root": str(PROJECT_ROOT),
        "run_dir": str(run_dir),
        "agent_dir": str(run_dir / "run-agent"),
        "eval_dir": str(run_dir / "run-eval"),
        "preds_path": str(run_dir / "run-agent" / "preds.json"),
        "config_path": str(run_dir / "config.json"),
        "metrics_path": str(run_dir / "metrics.json"),
        "manifest_path": str(run_dir / "manifest.json"),
        # --- provenance ---
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return config


def prepare_run_dir(run_config: dict) -> Path:
    """Create the structured run directory and persist config.json."""
    run_dir = Path(run_config["run_dir"])
    (run_dir / "run-agent").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval").mkdir(parents=True, exist_ok=True)
    Path(run_config["config_path"]).write_text(
        json.dumps(run_config, indent=2), encoding="utf-8"
    )
    return run_dir
