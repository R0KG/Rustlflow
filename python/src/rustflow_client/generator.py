import csv
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

ENDPOINTS = ("/login", "/payments", "/profile", "/search")
STATUS_CODES = (200, 200, 200, 201, 400, 401, 404, 500)


def generate_telemetry_csv(csv_path: Path, row_count: int, seed: int = 42) -> None:
    """Write deterministic synthetic telemetry data to a CSV file."""
    if row_count <= 0:
        raise ValueError("row_count must be positive")

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=UTC)

    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["timestamp", "user_id", "endpoint", "status_code", "latency_ms"],
        )
        writer.writeheader()

        for index in range(row_count):
            writer.writerow(
                {
                    "timestamp": (start + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
                    "user_id": f"user-{randomizer.randint(1, 2_000)}",
                    "endpoint": randomizer.choice(ENDPOINTS),
                    "status_code": randomizer.choice(STATUS_CODES),
                    "latency_ms": randomizer.randint(5, 2_000),
                }
            )
