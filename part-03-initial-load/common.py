"""Shared pieces for part 3: the source rows, the daily interval grid, the
transform the pipeline applies, the two ways a load can stamp its rows, and
the recorder that compares the target with what the pipeline should have
produced after every step."""
import csv
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

OUT_DIR = Path("out")
PG_DSN = os.environ.get("PG_DSN", "postgresql://dedp:dedp@localhost:5432/dedp")

ONE_DAY = timedelta(days=1)


def day(month: int, day_of_month: int) -> datetime:
    return datetime(2026, month, day_of_month, tzinfo=timezone.utc)


def at(month: int, day_of_month: int, hh_mm: str) -> datetime:
    hour, minute = (int(part) for part in hh_mm.split(":"))
    return datetime(2026, month, day_of_month, hour, minute, tzinfo=timezone.utc)


# --- the timeline ------------------------------------------------------------

# The source has rows from 1 September. The daily pipeline is scheduled from
# 1 October, so September is the history and 1 to 3 October are daily runs.
HISTORY_START = day(9, 1)
CUTOVER = day(10, 1)
SERIES_END = day(10, 4)
# In the cut-over scenarios the history is loaded at this moment, while the
# 30 September interval is still open.
HISTORY_LOAD_AT = at(9, 30, "18:00")
LAST_OPEN_DAY = day(9, 30)
# A past day that gets rerun after everything has loaded once
RERUN_DAY = day(9, 12)


@dataclass(frozen=True)
class Run:
    """One run of the pipeline and the data interval it is handed. A daily
    run's interval is one day wide; a wide run's covers many days."""

    interval_start: datetime
    interval_end: datetime

    @property
    def label(self) -> str:
        last_day = self.interval_end - ONE_DAY
        if last_day == self.interval_start:
            return f"{self.interval_start:%d %b}"
        return f"{self.interval_start:%d %b} to {last_day:%d %b}"


def daily_run(start: datetime) -> Run:
    return Run(start, start + ONE_DAY)


def daily_runs(start: datetime, end: datetime) -> list[Run]:
    """One daily run for every day in [start, end)."""
    runs = []
    cursor = start
    while cursor < end:
        runs.append(daily_run(cursor))
        cursor += ONE_DAY
    return runs


def wide_run(start: datetime, end: datetime) -> Run:
    return Run(start, end)


# --- the source rows ---------------------------------------------------------

SOURCE_COLUMNS = ["txn_id", "account_id", "txn_type", "amount", "updated_at"]
TARGET_COLUMNS = SOURCE_COLUMNS + ["interval_start", "loaded_at"]
# loaded_at is wall-clock time, so it is left out when two tables are compared
REPRODUCIBLE_COLUMNS = SOURCE_COLUMNS + ["interval_start"]

# (month, day) pairs. The source stores every amount as a positive number
# and says what it is in txn_type.
REFUND_DAYS = {(9, 5), (9, 10), (9, 15), (9, 20), (9, 25), (9, 30), (10, 2)}
CHARGEBACK_DAYS = {(9, 7), (9, 14), (9, 21), (9, 28), (10, 1)}
TEST_ACCOUNT_DAYS = {(9, 9), (9, 23), (10, 2)}
# 30 September also gets two rows after HISTORY_LOAD_AT
EVENING_ROWS = [
    ("T-0930-6", "A-003", "PURCHASE", Decimal("88.00"), at(9, 30, "19:45")),
    ("T-0930-7", "A-001", "PURCHASE", Decimal("132.50"), at(9, 30, "21:30")),
]


def source_row(txn_id: str, account_id: str, txn_type: str, amount: Decimal, updated_at: datetime) -> dict:
    return {
        "txn_id": txn_id,
        "account_id": account_id,
        "txn_type": txn_type,
        "amount": amount,
        "updated_at": updated_at,
    }


def rows_for_day(month: int, day_of_month: int) -> list[dict]:
    prefix = f"T-{month:02d}{day_of_month:02d}"
    rows = [
        source_row(f"{prefix}-1", "A-001", "PURCHASE", Decimal(100 + day_of_month), at(month, day_of_month, "09:15")),
        source_row(f"{prefix}-2", "A-002", "PURCHASE", Decimal(40 + day_of_month), at(month, day_of_month, "14:40")),
    ]
    if (month, day_of_month) in REFUND_DAYS:
        rows.append(source_row(f"{prefix}-3", "A-001", "REFUND", Decimal("25.00"), at(month, day_of_month, "11:05")))
    if (month, day_of_month) in CHARGEBACK_DAYS:
        rows.append(source_row(f"{prefix}-4", "A-002", "CHARGEBACK", Decimal("60.00"), at(month, day_of_month, "16:20")))
    if (month, day_of_month) in TEST_ACCOUNT_DAYS:
        rows.append(source_row(f"{prefix}-5", "TEST-001", "PURCHASE", Decimal("1.00"), at(month, day_of_month, "10:00")))
    return rows


def build_source_rows() -> list[dict]:
    rows = []
    for day_of_month in range(1, 31):
        rows += rows_for_day(9, day_of_month)
    rows += [source_row(*evening_row) for evening_row in EVENING_ROWS]
    for day_of_month in range(1, 4):
        rows += rows_for_day(10, day_of_month)
    return rows


SOURCE_ROWS = build_source_rows()


# --- the Postgres source, shared by both engines ----------------------------

CREATE_SOURCE = """
create table src_transactions (
    txn_id     text           primary key,
    account_id text           not null,
    txn_type   text           not null,
    amount     numeric(12, 2) not null,
    updated_at timestamptz    not null
)
"""

INSERT_SOURCE_ROW = """
insert into src_transactions (txn_id, account_id, txn_type, amount, updated_at)
values (%(txn_id)s, %(account_id)s, %(txn_type)s, %(amount)s, %(updated_at)s)
"""


def connect() -> psycopg.Connection:
    # autocommit, so each `with conn.transaction()` is a real BEGIN ... COMMIT
    return psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)


def reset_source(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute("drop table if exists src_transactions")
        conn.execute(CREATE_SOURCE)


def commit_source(conn: psycopg.Connection, low: datetime, high: datetime) -> None:
    """Commit the source rows stamped in [low, high): what the application
    wrote between those two moments."""
    arrived = [row for row in SOURCE_ROWS if low <= row["updated_at"] < high]
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(INSERT_SOURCE_ROW, arrived)


def read_source(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("select * from src_transactions").fetchall()


def extract_interval(conn: psycopg.Connection, run: Run) -> list[dict]:
    """Part 2's half-open interval read, whatever the interval's width."""
    return conn.execute(
        "select * from src_transactions where updated_at >= %s and updated_at < %s "
        "order by updated_at, txn_id",
        (run.interval_start, run.interval_end),
    ).fetchall()


# --- the transform -----------------------------------------------------------

# The pipeline's two rules. Version 1 of the sign rule has a bug: it leaves
# chargebacks positive. Version 2 is the fix that lands after 3 October.
NEGATIVE_TYPES_V1 = frozenset({"REFUND"})
NEGATIVE_TYPES_V2 = frozenset({"REFUND", "CHARGEBACK"})


def drop_test_accounts(rows: list[dict]) -> list[dict]:
    return [row for row in rows if not row["account_id"].startswith("TEST-")]


def sign_amounts(rows: list[dict], negative_types: frozenset) -> list[dict]:
    """Money going back to the customer is stored as a negative amount."""
    signed = []
    for row in rows:
        amount = -row["amount"] if row["txn_type"] in negative_types else row["amount"]
        signed.append({**row, "amount": amount})
    return signed


def transform(rows: list[dict], negative_types: frozenset) -> list[dict]:
    kept = drop_test_accounts(rows)
    signed = sign_amounts(kept, negative_types)
    return signed


# --- the two ways to stamp interval_start -----------------------------------


def stamp_run_key(rows: list[dict], run: Run) -> list[dict]:
    """Part 2's key: every row carries the start of the run that wrote it."""
    return [{**row, "interval_start": run.interval_start} for row in rows]


def owning_day(updated_at: datetime) -> datetime:
    """The start of the daily interval a row falls in."""
    in_utc = updated_at.astimezone(timezone.utc)
    return datetime(in_utc.year, in_utc.month, in_utc.day, tzinfo=timezone.utc)


def stamp_day_key(rows: list[dict]) -> list[dict]:
    """Every row carries the start of the daily interval it falls in, however
    wide the run that wrote it."""
    return [{**row, "interval_start": owning_day(row["updated_at"])} for row in rows]


# --- the recorder ------------------------------------------------------------


def reproducible(rows: list[dict]) -> list[tuple]:
    return sorted(tuple(str(row[c]) for c in REPRODUCIBLE_COLUMNS) for row in rows)


def compare_with_expected(expected: list[dict], target: list[dict]) -> dict:
    """What a reader of the target would find wrong, by txn_id."""
    expected_amount = {row["txn_id"]: row["amount"] for row in expected}
    target_count = Counter(row["txn_id"] for row in target)
    wrong_amount = {
        row["txn_id"]
        for row in target
        if row["txn_id"] in expected_amount and row["amount"] != expected_amount[row["txn_id"]]
    }
    return {
        "missing_txn_ids": sorted(set(expected_amount) - set(target_count)),
        "duplicated_txn_ids": sorted(txn_id for txn_id, n in target_count.items() if n > 1),
        "unexpected_txn_ids": sorted(set(target_count) - set(expected_amount)),
        "wrong_amount_txn_ids": sorted(wrong_amount),
    }


class Recorder:
    """Writes the target table after each step to out/<engine>/<scenario>/
    and keeps a one-line comparison of it with the expected table: the
    pipeline's transform applied to every source row committed so far."""

    def __init__(self, engine: str, scenario: str, description: str):
        self.engine = engine
        self.scenario = scenario
        self.description = description
        self.folder = OUT_DIR / engine / scenario
        self.folder.mkdir(parents=True, exist_ok=True)
        self.steps: list[dict] = []
        self.pipeline_runs = 0
        self.final_rows: list[tuple] = []

    def step(
        self,
        label: str,
        source: list[dict],
        target: list[dict],
        negative_types: frozenset,
        costs: dict | None = None,
    ) -> dict:
        index = len(self.steps) + 1
        self.write_rows(f"{index:02d}-{slug(label)}.csv", target)
        expected = transform(source, negative_types)
        differences = compare_with_expected(expected, target)
        summary = {
            "step": label,
            "pipeline_runs": self.pipeline_runs,
            "source_rows": len(source),
            "expected_rows": len(expected),
            "target_rows": len(target),
            **differences,
            "target_total": sum((row["amount"] for row in target), Decimal("0.00")),
            "expected_total": sum((row["amount"] for row in expected), Decimal("0.00")),
            "matches_expected": not any(differences.values()),
            "costs": costs or {},
        }
        self.steps.append(summary)
        self.final_rows = reproducible(target)
        return summary

    def write_rows(self, name: str, rows: list[dict]) -> None:
        ordered = sorted(rows, key=lambda r: (str(r["txn_id"]), str(r["interval_start"])))
        with (self.folder / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TARGET_COLUMNS)
            writer.writeheader()
            for row in ordered:
                writer.writerow({c: row[c] for c in TARGET_COLUMNS})

    def result(self) -> dict:
        return {
            "scenario": self.scenario,
            "description": self.description,
            "steps": self.steps,
            "final_rows": self.final_rows,
        }


def slug(text: str) -> str:
    kept = "".join(ch if ch.isalnum() else "-" for ch in text.lower())
    return "-".join(part for part in kept.split("-") if part)


def ids_text(txn_ids: list[str]) -> str:
    if not txn_ids:
        return "none"
    if len(txn_ids) <= 8:
        return ", ".join(txn_ids)
    return f"{len(txn_ids)} rows, first {txn_ids[0]}, last {txn_ids[-1]}"


def write_report(engine: str, results: list[dict]) -> Path:
    cost_columns = list(results[0]["steps"][0]["costs"])
    lines = [f"# Part 3 results: {engine}", ""]
    for result in results:
        lines += [f"## {result['scenario']}", "", result["description"], ""]
        header = [
            "Step",
            "Pipeline runs so far",
            *cost_columns,
            "Source rows",
            "Target rows",
            "Missing from target",
            "Duplicated txn_id",
            "Should not be there",
            "Wrong amount",
            "Target total",
            "Expected total",
            "Matches expected",
        ]
        lines += ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        for s in result["steps"]:
            cells = [
                s["step"],
                str(s["pipeline_runs"]),
                *[str(s["costs"][c]) for c in cost_columns],
                str(s["source_rows"]),
                str(s["target_rows"]),
                ids_text(s["missing_txn_ids"]),
                ids_text(s["duplicated_txn_ids"]),
                ids_text(s["unexpected_txn_ids"]),
                ids_text(s["wrong_amount_txn_ids"]),
                f"{s['target_total']:.2f}",
                f"{s['expected_total']:.2f}",
                "yes" if s["matches_expected"] else "**no**",
            ]
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    path = OUT_DIR / engine / "report.md"
    path.write_text("\n".join(lines))
    return path
