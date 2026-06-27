"""Capture UI evidence screenshots (MLflow + Airflow) with headless Chromium.

Run inside the project venv on the VM where both UIs are reachable on localhost:
    .venv/bin/python scripts/capture_screenshots.py

Outputs PNGs into ./screenshots/.
"""

from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parents[1] / "screenshots"
OUT.mkdir(exist_ok=True)

AIRFLOW = "http://127.0.0.1:8080"
MLFLOW = "http://127.0.0.1:5000"
MINIO = "http://127.0.0.1:9001"
MINIO_USER = "minioadmin"
MINIO_PASS = "minioadmin123"
MINIO_BUCKET = "mlops-runs"
VIEWPORT = {"width": 1680, "height": 1000}


def shot(page, name: str) -> None:
    path = OUT / name
    page.screenshot(path=str(path), full_page=True)
    print(f"  saved {path}")


def capture_mlflow(ctx) -> None:
    page = ctx.new_page()
    print("[mlflow] experiments runs table")
    # MLflow's SPA holds polling connections open, so 'networkidle' never fires.
    page.goto(MLFLOW, wait_until="domcontentloaded")
    time.sleep(5)
    # Click the swe-bench-eval experiment in the sidebar if present.
    try:
        page.get_by_text("swe-bench-eval", exact=False).first.click(timeout=8000)
        time.sleep(4)
    except Exception as e:
        print("  (could not click experiment, capturing landing)", e)
    # Dismiss the "Detect Issues" onboarding popover that intercepts clicks.
    for label in ("Got it", "Close"):
        try:
            page.get_by_role("button", name=label).first.click(timeout=3000)
            time.sleep(1)
        except Exception:
            pass
    # MLflow 3.x opens experiments on the GenAI tracing tab; switch to the
    # classic "Model training" view to show the runs table (params + metrics).
    try:
        page.get_by_text("Model training", exact=False).first.click(timeout=8000, force=True)
        time.sleep(6)
        print("  switched to Model training view")
    except Exception as e:
        print("  (could not switch to Model training)", e)
    shot(page, "mlflow_runs.png")
    page.close()


def capture_airflow(ctx) -> None:
    page = ctx.new_page()
    print("[airflow] login + dag grid")
    page.goto(AIRFLOW, wait_until="networkidle")
    time.sleep(2)
    # Handle the simple-auth login form if shown.
    try:
        user = page.locator("input[name='username'], input#username, input[type='text']").first
        if user.is_visible(timeout=5000):
            user.fill("admin")
            page.locator("input[name='password'], input#password, input[type='password']").first.fill("admin")
            page.locator("button[type='submit'], button:has-text('Sign in'), button:has-text('Login')").first.click()
            page.wait_for_load_state("networkidle")
            time.sleep(3)
            print("  logged in")
    except Exception as e:
        print("  (no login form / already authed)", e)

    # Go straight to the evaluate_agent DAG page (grid is the default view).
    page.goto(f"{AIRFLOW}/dags/evaluate_agent", wait_until="domcontentloaded")
    time.sleep(6)
    shot(page, "airflow_dag.png")
    page.close()


def capture_minio(ctx) -> None:
    page = ctx.new_page()
    print("[minio] login + bucket browser")
    page.goto(MINIO, wait_until="domcontentloaded")
    time.sleep(3)
    try:
        page.locator("input[name='accessKey'], input#accessKey, input[type='text']").first.fill(MINIO_USER)
        page.locator("input[name='secretKey'], input#secretKey, input[type='password']").first.fill(MINIO_PASS)
        page.locator("button[type='submit']").first.click()
        page.wait_for_load_state("domcontentloaded")
        time.sleep(4)
        print("  logged in")
    except Exception as e:
        print("  (no login form / already authed)", e)
    # Object browser for the bucket.
    page.goto(f"{MINIO}/browser/{MINIO_BUCKET}", wait_until="domcontentloaded")
    time.sleep(4)
    # Dismiss the AGPL license modal if it appears.
    try:
        page.get_by_role("button", name="Acknowledge").first.click(timeout=4000)
        time.sleep(2)
    except Exception:
        pass
    # Drill into the runs/ prefix to show the per-run folders.
    try:
        page.get_by_text("runs", exact=True).first.click(timeout=6000)
        time.sleep(4)
    except Exception as e:
        print("  (could not open runs/ prefix)", e)
    shot(page, "object_storage_artifacts.png")
    page.close()


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        ctx = browser.new_context(viewport=VIEWPORT, device_scale_factor=1)
        for name, fn in (("mlflow", capture_mlflow), ("airflow", capture_airflow), ("minio", capture_minio)):
            try:
                fn(ctx)
            except Exception as e:
                print(f"{name} capture failed:", e)
        browser.close()
    print("done ->", OUT)


if __name__ == "__main__":
    main()
