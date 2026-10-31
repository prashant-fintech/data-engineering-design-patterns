"""Write the three daily landing files every load in part 1 reads.

The batch for 2026-10-02 carries one late transaction whose business date
is 2026-10-01. That single row is what separates the delete keys compared
in the post.
"""
import csv
from pathlib import Path

from common import BATCH_DATES, LANDING_COLUMNS, landing_file

LANDING_ROWS = {
    "2026-10-01": [
        ("T-0101", "A-001", "2026-10-01", "120.00"),
        ("T-0102", "A-002", "2026-10-01", "75.50"),
        ("T-0103", "A-001", "2026-10-01", "-40.00"),
        ("T-0104", "A-003", "2026-10-01", "310.25"),
    ],
    "2026-10-02": [
        ("T-0201", "A-002", "2026-10-02", "18.00"),
        ("T-0202", "A-003", "2026-10-02", "-95.10"),
        ("T-0203", "A-001", "2026-10-02", "220.00"),
        ("T-0204", "A-004", "2026-10-02", "64.40"),
        # Booked on 1 October, delivered a day late in the next batch
        ("T-0105", "A-004", "2026-10-01", "52.75"),
    ],
    "2026-10-03": [
        ("T-0301", "A-001", "2026-10-03", "15.00"),
        ("T-0302", "A-004", "2026-10-03", "-12.30"),
        ("T-0303", "A-002", "2026-10-03", "480.00"),
        ("T-0304", "A-003", "2026-10-03", "33.33"),
    ],
}


def write_landing_file(path: Path, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(LANDING_COLUMNS)
        writer.writerows(rows)


def main() -> None:
    for batch_date in BATCH_DATES:
        path = landing_file(batch_date)
        rows = LANDING_ROWS[batch_date.isoformat()]
        write_landing_file(path, rows)
        print(f"wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
