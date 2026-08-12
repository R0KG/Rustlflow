"""Generate a CSV, run it through RustFlow, and compare it with Polars."""

import argparse
import json
import tempfile
from pathlib import Path

from rustflow_client import (
    RustFlowClient,
    assert_grouped_count_matches,
    generate_telemetry_csv,
    grouped_count_with_polars,
)

SQL = "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    arguments = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="rustflow-validation-") as directory:
        csv_path = Path(directory) / "telemetry.csv"
        generate_telemetry_csv(csv_path, arguments.rows, arguments.seed)
        expected = grouped_count_with_polars(csv_path)

        with RustFlowClient(arguments.base_url) as client:
            dataset = client.upload_dataset(csv_path)
            job = client.create_job(dataset.id, SQL)
            result = client.wait_for_result(job.job_id)

        assert_grouped_count_matches(result.result, expected)

    print(
        "Validated RustFlow against Polars: "
        f"{expected['row_count']} endpoint groups from {arguments.rows} rows."
    )
    print(
        json.dumps(
            {
                "dataset_id": str(dataset.id),
                "job_id": str(job.job_id),
                "status": "completed",
            }
        )
    )


if __name__ == "__main__":
    main()
