"""Upload a run folder to S3-compatible Object Storage (e.g. Nebius).

Configuration comes from the environment (or the repo ``.env``):
- ``S3_BUCKET``            target bucket (required; if unset, upload is skipped)
- ``S3_ENDPOINT_URL``      default: ``https://storage.eu-north1.nebius.cloud:443``
- ``S3_REGION``            default: ``eu-north1``
- ``S3_PREFIX``            key prefix, default: ``runs``
- ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``  (or ``S3_ACCESS_KEY_ID`` /
  ``S3_SECRET_ACCESS_KEY``)

Runnable as a CLI inside the project venv:
    python -m pipeline.storage --run-dir runs/<id>

Uploading is best-effort and *optional*: with no ``S3_BUCKET`` it prints a notice
and exits 0, so the pipeline still works fully on local storage.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .runconfig import PROJECT_ROOT


def _load_dotenv(path: Path) -> None:
    """Populate os.environ from a .env file without overriding existing vars."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def s3_settings() -> dict | None:
    """Return S3 settings from env/.env, or None if not configured."""
    _load_dotenv(PROJECT_ROOT / ".env")
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        return None
    return {
        "bucket": bucket,
        "endpoint_url": os.environ.get(
            "S3_ENDPOINT_URL", "https://storage.eu-north1.nebius.cloud:443"
        ),
        "region": os.environ.get("S3_REGION", "eu-north1"),
        "prefix": os.environ.get("S3_PREFIX", "runs").strip("/"),
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID")
        or os.environ.get("S3_ACCESS_KEY_ID"),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY")
        or os.environ.get("S3_SECRET_ACCESS_KEY"),
    }


def make_remote_uri(run_config: dict, settings: dict | None = None) -> str:
    """Deterministic s3:// URI for a run (empty string if S3 not configured)."""
    s = settings or s3_settings()
    if not s:
        return ""
    return f"s3://{s['bucket']}/{s['prefix']}/{run_config['run_id']}"


def _set_remote_uri(run_dir: Path, uri: str) -> None:
    """Stamp the remote URI into config.json and manifest.json before upload."""
    cfg_path = run_dir / "config.json"
    if cfg_path.exists():
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        cfg["artifact_remote_uri"] = uri
        cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    man_path = run_dir / "manifest.json"
    if man_path.exists():
        man = json.loads(man_path.read_text(encoding="utf-8"))
        man.setdefault("artifact_storage", {})["remote_uri"] = uri
        man_path.write_text(json.dumps(man, indent=2), encoding="utf-8")


def upload_run_dir(run_config: dict) -> str:
    """Upload every file under runs/<run-id>/ to Object Storage; return the URI."""
    s = s3_settings()
    if not s:
        print("[s3] S3_BUCKET not set — skipping upload (local artifacts only)")
        return ""

    import boto3  # imported lazily so the module stays light

    run_dir = Path(run_config["run_dir"])
    uri = make_remote_uri(run_config, s)
    # Stamp the URI first so the uploaded copies of config/manifest match.
    _set_remote_uri(run_dir, uri)

    client = boto3.client(
        "s3",
        endpoint_url=s["endpoint_url"],
        region_name=s["region"],
        aws_access_key_id=s["access_key"],
        aws_secret_access_key=s["secret_key"],
    )
    key_root = f"{s['prefix']}/{run_config['run_id']}"
    count = 0
    for f in sorted(run_dir.rglob("*")):
        if f.is_file():
            key = f"{key_root}/{f.relative_to(run_dir).as_posix()}"
            client.upload_file(str(f), s["bucket"], key)
            count += 1
    print(f"[s3] uploaded {count} files to {uri}")
    return uri


def _main() -> None:
    parser = argparse.ArgumentParser(description="Upload a run dir to Object Storage.")
    parser.add_argument("--run-dir", required=True, help="Path to runs/<run-id>/")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    run_config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    uri = upload_run_dir(run_config)
    print(f"REMOTE_URI={uri}")


if __name__ == "__main__":
    _main()
