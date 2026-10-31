"""Shared pieces for part 1: the batch calendar, the landing files, and the
recorder that writes a snapshot of the table after every step."""
import csv
from collections import Counter
from datetime import date
from pathlib import Path

LANDING_DIR = Path("data/landing")
OUT_DIR = Path("out")
BATCH_DATES = [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)]

LANDING_COLUMNS = ["txn_id", "account_id", "business_date", "amount"]
TABLE_COLUMNS = LANDING_COLUMNS + ["batch_date", "loaded_at"]
# loaded_at is wall-clock time, kept for forensics. A rerun is "the same
# table" when every other column matches.
COMPARED_COLUMNS = LANDING_COLUMNS + ["batch_date"]


class SimulatedCrash(RuntimeError):
    """Raised between two steps of a load to stand in for a killed worker."""


def landing_file(batch_date: date) -> Path:
    return LANDING_DIR / f"batch_date={batch_date.isoformat()}" / "transactions.csv"


def read_landing_rows(batch_date: date) -> list[dict]:
    with landing_file(batch_date).open(newline="") as f:
        return list(csv.DictReader(f))


def comparable(rows: list[dict]) -> list[tuple]:
    return sorted(tuple(str(row[c]) for c in COMPARED_COLUMNS) for row in rows)


def rows_per_business_date(rows: list[dict]) -> dict[str, int]:
    counts = Counter(str(row["business_date"]) for row in rows)
    return dict(sorted(counts.items()))


def duplicated_txn_ids(rows: list[dict]) -> list[str]:
    counts = Counter(row["txn_id"] for row in rows)
    return sorted(txn_id for txn_id, n in counts.items() if n > 1)


class Recorder:
    """Writes the table after each step to out/<engine>/<scenario>/ and keeps
    a one-line summary of it for the report."""

    def __init__(self, engine: str, scenario: str, description: str):
        self.engine = engine
        self.scenario = scenario
        self.description = description
        self.folder = OUT_DIR / engine / scenario
        self.folder.mkdir(parents=True, exist_ok=True)
        self.baseline: list[tuple] | None = None
        self.steps: list[dict] = []

    def step(self, label: str, rows: list[dict], outcome: str = "completed") -> dict:
        index = len(self.steps) + 1
        self.write_rows(f"{index:02d}-{slug(label)}.csv", rows)
        current = comparable(rows)
        if self.baseline is None:
            self.baseline = current
        summary = {
            "step": label,
            "outcome": outcome,
            "rows": len(rows),
            "per_business_date": rows_per_business_date(rows),
            "duplicated_txn_ids": duplicated_txn_ids(rows),
            "same_as_first_step": current == self.baseline,
        }
        self.steps.append(summary)
        return summary

    def write_rows(self, name: str, rows: list[dict]) -> None:
        ordered = sorted(rows, key=lambda r: (str(r["txn_id"]), str(r["batch_date"])))
        with (self.folder / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TABLE_COLUMNS)
            writer.writeheader()
            for row in ordered:
                writer.writerow({c: row[c] for c in TABLE_COLUMNS})

    def result(self) -> dict:
        return {
            "scenario": self.scenario,
            "description": self.description,
            "steps": self.steps,
        }


def slug(text: str) -> str:
    kept = "".join(ch if ch.isalnum() else "-" for ch in text.lower())
    return "-".join(part for part in kept.split("-") if part)


def write_report(engine: str, results: list[dict]) -> Path:
    lines = [f"# Part 1 results: {engine}", ""]
    for result in results:
        lines += [f"## {result['scenario']}", "", result["description"], ""]
        lines += [
            "| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |",
            "|---|---|---|---|---|---|",
        ]
        for s in result["steps"]:
            per_date = ", ".join(f"{d[5:]}: {n}" for d, n in s["per_business_date"].items())
            duplicates = ", ".join(s["duplicated_txn_ids"]) or "none"
            same = "yes" if s["same_as_first_step"] else "**no**"
            lines.append(
                f"| {s['step']} | {s['outcome']} | {s['rows']} | {per_date} | {duplicates} | {same} |"
            )
        lines.append("")
    path = OUT_DIR / engine / "report.md"
    path.write_text("\n".join(lines))
    return path
