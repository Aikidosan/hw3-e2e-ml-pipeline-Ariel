"""Run the agent (mini-swe-agent) and the evaluation (SWE-bench).

Both steps shell out to the project ``.venv`` so they execute with the
mini-swe-agent / swebench dependencies regardless of which environment Airflow
itself runs in. Command builders are separated from the runners so they can be
unit-tested without launching anything.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from .runconfig import PROJECT_ROOT


def _venv_bin(name: str) -> str:
    """Path to an executable inside the project virtualenv."""
    binroot = PROJECT_ROOT / ".venv" / ("Scripts" if os.name == "nt" else "bin")
    return str(binroot / name)


# --------------------------------------------------------------------------- #
# Agent (mini-swe-agent batch)
# --------------------------------------------------------------------------- #
def build_agent_command(run_config: dict) -> list[str]:
    """Build the ``mini-extra swebench`` batch command from a run config.

    ``step_limit`` / ``cost_limit`` are injected as config overrides on top of
    the upstream ``swebench.yaml`` (the ``-c file -c key=value`` form), so we
    keep the upstream prompt/environment settings but bound the run.
    """
    cmd = [
        _venv_bin("mini-extra"), "swebench",
        "--subset", run_config["subset"],
        "--split", run_config["split"],
        "--model", run_config["model"],
        "-w", str(run_config["workers"]),
        "-o", run_config["agent_dir"],
        "-c", run_config["swebench_agent_config"],
        "-c", f"agent.step_limit={run_config['step_limit']}",
        "-c", f"agent.cost_limit={run_config['cost_limit']}",
    ]
    task_slice = run_config.get("task_slice")
    if task_slice:
        cmd += ["--slice", task_slice]
    return cmd


def run_agent_batch(run_config: dict, timeout: int | None = None) -> Path:
    """Execute the agent batch; return the path to ``preds.json``.

    mini-swe-agent loads ``NEBIUS_API_KEY`` from
    ``~/.config/mini-swe-agent/.env`` automatically; we still pass through the
    process environment and set ``MSWEA_COST_TRACKING=ignore_errors`` to match
    the reference scripts.
    """
    Path(run_config["agent_dir"]).mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"}
    cmd = build_agent_command(run_config)
    print("[run_agent] cwd=%s" % PROJECT_ROOT)
    print("[run_agent] cmd=%s" % " ".join(cmd))
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env, check=True, timeout=timeout)

    preds = Path(run_config["preds_path"])
    if not preds.exists():
        raise FileNotFoundError(
            f"Agent finished but no preds.json at {preds}. "
            f"Check {run_config['agent_dir']} for the actual output."
        )
    return preds


# --------------------------------------------------------------------------- #
# Evaluation (SWE-bench harness)
# --------------------------------------------------------------------------- #
def build_eval_command(run_config: dict) -> list[str]:
    """Build the ``swebench.harness.run_evaluation`` command."""
    return [
        _venv_bin("python"), "-m", "swebench.harness.run_evaluation",
        "--dataset_name", run_config["eval_dataset"],
        "--split", run_config["split"],
        "--predictions_path", run_config["preds_path"],
        "--max_workers", str(run_config["workers"]),
        "--run_id", run_config["run_id"],
        "--report_dir", run_config["eval_dir"],
    ]


def run_swebench_eval(run_config: dict, timeout: int | None = None) -> Path:
    """Run the SWE-bench evaluation; return the eval output directory.

    We run with ``cwd=run-eval`` so the harness' ``logs/run_evaluation/<run_id>``
    tree and the ``<model>.<run_id>.json`` summary both land inside the run dir.
    """
    eval_dir = Path(run_config["eval_dir"])
    eval_dir.mkdir(parents=True, exist_ok=True)
    cmd = build_eval_command(run_config)
    print("[run_eval] cwd=%s" % eval_dir)
    print("[run_eval] cmd=%s" % " ".join(cmd))
    subprocess.run(cmd, cwd=str(eval_dir), check=True, timeout=timeout)
    return eval_dir
