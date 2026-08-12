import httpx
import pytest

from rustflow_client import (
    RustFlowClient,
    assert_grouped_count_matches,
    generate_telemetry_csv,
    grouped_count_with_polars,
)

SQL = "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint"


@pytest.mark.integration
def test_rustflow_matches_polars_for_multiple_partitions(tmp_path) -> None:
    base_url = "http://127.0.0.1:3000"
    try:
        response = httpx.get(f"{base_url}/health", timeout=2)
        response.raise_for_status()
    except httpx.HTTPError:
        pytest.skip("RustFlow Compose stack is not running at http://127.0.0.1:3000")

    csv_path = tmp_path / "telemetry.csv"
    generate_telemetry_csv(csv_path, row_count=12_000)
    expected = grouped_count_with_polars(csv_path)

    with RustFlowClient(base_url, timeout_seconds=120) as client:
        dataset = client.upload_dataset(csv_path)
        job = client.create_job(dataset.id, SQL)
        result = client.wait_for_result(job.job_id, timeout_seconds=120)

    assert_grouped_count_matches(result.result, expected)
