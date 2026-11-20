"""Daily airport payments pipeline as a Prefect flow (Lab 11).

Stages call the same `make` targets learners run by hand, so there is one definition of each step.
Run once:            make pipeline DATE=2026-11-18
Serve on a schedule: make automate
"""
import json
import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, task, get_run_logger

ROOT = Path(__file__).resolve().parents[1]
MIN_PRECISION = 0.5          # quality gate on the scoring rules, measured against ground truth


def make(target: str, **env: str) -> str:
    result = subprocess.run(["make", "--no-print-directory", target], cwd=ROOT,
                            capture_output=True, text=True, env={**os.environ, **env})
    if result.returncode != 0:
        raise RuntimeError(f"make {target} failed:\n{result.stdout[-800:]}\n{result.stderr[-800:]}")
    return result.stdout


@task(retries=0)
def generate_data() -> str:
    return make("generate")


@task(retries=2, retry_delay_seconds=30)
def load_bronze() -> str:
    return make("bronze")


@task(retries=2, retry_delay_seconds=30)
def build_silver() -> str:
    return make("silver")


@task(retries=2, retry_delay_seconds=30)
def build_features() -> str:
    return make("features")


@task(retries=2, retry_delay_seconds=30)
def score_day(day: str) -> str:
    return make("score", DATE=day)


@task(retries=0)              # a failed gate is a decision, not a transient error: do not retry
def quality_gate(scored: str, min_precision: float) -> dict:
    # Read the actual scoring day even if DATE was left blank.
    day = next(line.split()[1].split("=", 1)[1] for line in scored.splitlines()
               if line.startswith("scored day="))
    logger = get_run_logger()
    make("evaluate", DATE=day)
    report = json.loads((ROOT / "data" / "reports" / "evaluation.json").read_text())
    logger.info("evaluation for %s: %s", report["event_date"], report)
    if report["precision"] < min_precision:
        raise RuntimeError(f"precision {report['precision']} below gate {min_precision}: fraud rules need review")
    return report


@flow(name="airport-payments-daily", log_prints=True)
def payments_daily(day: str = "", min_precision: float = MIN_PRECISION):
    gen = generate_data.submit()
    bronze = load_bronze.submit(wait_for=[gen])
    silver = build_silver.submit(wait_for=[bronze])
    feats = build_features.submit(wait_for=[silver])
    scored = score_day.submit(day, wait_for=[feats])
    # Passing the future adds a dependency; Prefect gives the task the actual output.
    return quality_gate.submit(scored, min_precision).result()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        payments_daily.serve(name="airport-payments-0600", cron="0 6 * * *")
    else:
        payments_daily(sys.argv[1] if len(sys.argv) > 1 else "",
                       min_precision=float(os.getenv("LAB_MIN_PRECISION", MIN_PRECISION)))
