import csv
import io
import uuid

from locust import HttpUser, between, task


def telemetry_csv() -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["endpoint", "status_code"])
    writer.writeheader()
    for endpoint in ("/login", "/payments", "/profile", "/search"):
        writer.writerow({"endpoint": endpoint, "status_code": 200})
    return output.getvalue().encode("utf-8")


class RustFlowUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def upload_and_submit_job(self) -> None:
        with self.client.post(
            "/datasets",
            files={"file": ("telemetry.csv", telemetry_csv(), "text/csv")},
            name="upload dataset",
            catch_response=True,
        ) as upload:
            if upload.status_code != 201:
                upload.failure(f"Unexpected status: {upload.status_code}")
                return
            dataset_id = upload.json()["id"]

        self.client.post(
            "/jobs",
            headers={"Idempotency-Key": str(uuid.uuid4())},
            json={
                "dataset_id": dataset_id,
                "sql": "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint",
            },
            name="submit job",
        )
