from pathlib import Path
from typing import Any

import polars as pl


def grouped_count_with_polars(
    csv_path: Path,
    group_column: str = "endpoint",
    count_alias: str = "requests",
) -> dict[str, Any]:
    """Calculate the same grouped-count result locally, outside RustFlow."""
    rows = (
        pl.read_csv(csv_path)
        .group_by(group_column)
        .len(name=count_alias)
        .sort(group_column)
        .to_dicts()
    )
    return {"row_count": len(rows), "rows": rows}


def assert_grouped_count_matches(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    """Raise a clear assertion error when RustFlow differs from the reference result."""
    if actual != expected:
        raise AssertionError(f"RustFlow result differs from Polars reference.\n"
                             f"actual={actual}\nexpected={expected}")
