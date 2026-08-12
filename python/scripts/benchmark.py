"""Measure end-to-end job completion time for an already scaled Compose stack."""

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from rustflow_client import RustFlowClient, generate_telemetry_csv

SQL = "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--workers", type=int, required=True, help="Worker replicas already running")
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    if arguments.runs <= 0:
        raise ValueError("--runs must be positive")

    durations: list[float] = []
    with tempfile.TemporaryDirectory(prefix="rustflow-benchmark-") as directory:
        csv_path = Path(directory) / "telemetry.csv"
        generate_telemetry_csv(csv_path, arguments.rows)

        with RustFlowClient(arguments.base_url, timeout_seconds=120) as client:
            for run_number in range(1, arguments.runs + 1):
                dataset = client.upload_dataset(csv_path)
                start = time.perf_counter()
                job = client.create_job(dataset.id, SQL)
                client.wait_for_result(job.job_id, timeout_seconds=120)
                duration = time.perf_counter() - start
                durations.append(duration)
                print(f"Run {run_number}: {duration:.3f}s")

    report = {
        "workers": arguments.workers,
        "rows_per_run": arguments.rows,
        "runs": arguments.runs,
        "durations_seconds": durations,
        "mean_seconds": statistics.mean(durations),
        "min_seconds": min(durations),
        "max_seconds": max(durations),
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote benchmark report to {arguments.output}")


if __name__ == "__main__":
    main()
