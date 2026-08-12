from pathlib import Path

from rustflow_client import grouped_count_with_polars


def test_grouped_count_with_polars_sorts_groups(tmp_path: Path) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text(
        "endpoint,status_code\n/payments,200\n/login,401\n/payments,200\n",
        encoding="utf-8",
    )

    result = grouped_count_with_polars(csv_path)

    assert result == {
        "row_count": 2,
        "rows": [
            {"endpoint": "/login", "requests": 1},
            {"endpoint": "/payments", "requests": 2},
        ],
    }
