"""Daily airport payments pipeline as a Prefect flow (Lab 11).

Each task runs the same explicit command learners run by hand (README), so there is one definition of each step.
Run once:            PREFECT_API_URL=http://localhost:4200/api uv run python flows/pipeline.py 2026-11-18
Serve on a schedule: PREFECT_API_URL=http://localhost:4200/api uv run python flows/pipeline.py serve
"""
import json
import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, task, get_run_logger

ROOT = Path(__file__).resolve().parents[1]
MIN_PRECISION = 0.5          # quality gate on the scoring rules, measured against ground truth
SPARK_SUBMIT = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit"]
MC = ["docker", "compose", "exec", "-T", "minio-client", "mc"]


def run(argv: list[str]) -> str:
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True,
                            env={**os.environ, "PYTHONPATH": "src"})
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(argv)} failed:\n{result.stdout[-800:]}\n{result.stderr[-800:]}")
    return result.stdout


def spark_job(job: str, *args: str) -> str:
    return run([*SPARK_SUBMIT, f"/opt/spark-apps/{job}", *args])


def date_arg(day: str) -> list[str]:
    return ["--date", day] if day else []


@task(retries=0)
def generate_data() -> str:
    return run(["uv", "run", "python", "-m", "lab.generate"])


@task(retries=2, retry_delay_seconds=30)
def load_bronze() -> str:
    out = run([*MC, "cp", "/data/raw/transactions.csv", "local/bronze/transactions/transactions.csv"])
    out += run([*MC, "cp", "/data/raw/passengers.csv", "local/bronze/passengers/passengers.csv"])
    return out + run([*MC, "ls", "--recursive", "local/bronze/"])


@task(retries=2, retry_delay_seconds=30)
def build_silver() -> str:
    return spark_job("silver_transactions.py")


@task(retries=2, retry_delay_seconds=30)
def build_features() -> str:
    return spark_job("features_30d.py")


@task(retries=2, retry_delay_seconds=30)
def score_day(day: str) -> str:
    return spark_job("score_batch.py", *date_arg(day))


@task(retries=0)              # a failed gate is a decision, not a transient error: do not retry
def quality_gate(scored: str, min_precision: float) -> dict:
    # Read the actual scoring day even if DATE was left blank.
    day = next(line.split()[1].split("=", 1)[1] for line in scored.splitlines()
               if line.startswith("scored day="))
    logger = get_run_logger()
    spark_job("evaluate.py", "--date", day)
    (ROOT / "data" / "reports").mkdir(parents=True, exist_ok=True)
    run(["docker", "compose", "cp", "spark-master:/tmp/airport-evaluation.json", "data/reports/evaluation.json"])
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
