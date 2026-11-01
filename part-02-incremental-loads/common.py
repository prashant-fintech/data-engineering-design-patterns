"""Shared pieces for part 2: the source rows and the run each one is
committed before, the hourly run schedule, and the recorder that writes the
target table and the watermark after every step."""
import csv
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

OUT_DIR = Path("out")
PG_DSN = os.environ.get("PG_DSN", "postgresql://dedp:dedp@localhost:5432/dedp")
PIPELINE = "fact_transactions"

# The watermark before the first run: earlier than any source row
INITIAL_WATERMARK = datetime(1970, 1, 1, tzinfo=timezone.utc)
LOOKBACK = timedelta(minutes=10)


def at(hh_mm: str) -> datetime:
    hour, minute = (int(part) for part in hh_mm.split(":"))
    return datetime(2026, 10, 2, hour, minute, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Run:
    """One scheduled run and the data interval the scheduler hands it."""

    number: int
    interval_start: datetime
    interval_end: datetime

    @property
    def label(self) -> str:
        return f"run {self.number} ({self.interval_start:%H:%M}-{self.interval_end:%H:%M})"


RUNS = [
    Run(1, at("09:00"), at("10:00")),
    Run(2, at("10:00"), at("11:00")),
    Run(3, at("11:00"), at("12:00")),
]

SOURCE_COLUMNS = ["txn_id", "account_id", "amount", "updated_at"]
TARGET_COLUMNS = SOURCE_COLUMNS + ["interval_start", "loaded_at"]
# interval_start is the run's logical time, so a rerun reproduces it.
# loaded_at is wall-clock time. The target "matches the source" when the
# source columns match.
COMPARED_COLUMNS = SOURCE_COLUMNS

# (txn_id, account_id, amount, updated_at, committed before run)
SOURCE_ROWS = [
    ("T-01", "A-001", "120.00", at("09:05"), 1),
    ("T-02", "A-002", "75.50", at("09:20"), 1),
    ("T-03", "A-001", "-40.00", at("09:40"), 1),
    ("T-04", "A-003", "310.25", at("09:55"), 1),
    ("T-05", "A-002", "18.00", at("10:10"), 2),
    ("T-06", "A-003", "-95.10", at("10:30"), 2),
    ("T-07", "A-001", "220.00", at("10:59"), 2),
    # Exactly on the boundary between the 10:00 and 11:00 intervals
    ("T-08", "A-004", "64.40", at("11:00"), 2),
    ("T-10", "A-001", "15.00", at("11:15"), 3),
    ("T-11", "A-004", "-12.30", at("11:40"), 3),
]
# Stamped 10:58 when its statement ran, but its transaction is still open
# while run 2 reads the source, and commits only afterwards. Scenarios
# without an in-flight row commit it before run 2 like the rest of wave 2.
IN_FLIGHT_ROW = ("T-09", "A-003", "52.75", at("10:58"), 2)


class SimulatedCrash(RuntimeError):
    """Raised between two steps of a load to stand in for a killed worker."""


# --- the Postgres source and watermark, shared by both engines -------------

CREATE_SOURCE = """
create table src_transactions (
    txn_id     text           primary key,
    account_id text           not null,
    amount     numeric(12, 2) not null,
    updated_at timestamptz    not null
)
"""

CREATE_WATERMARK = """
create table etl_watermark (
    pipeline  text        primary key,
    watermark timestamptz not null
)
"""

INSERT_SOURCE_ROW = """
insert into src_transactions (txn_id, account_id, amount, updated_at)
values (%s, %s, %s, %s)
"""


def connect() -> psycopg.Connection:
    # autocommit, so each `with conn.transaction()` is a real BEGIN ... COMMIT
    return psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)


def reset_source_and_watermark(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute("drop table if exists src_transactions")
        conn.execute("drop table if exists etl_watermark")
        conn.execute(CREATE_SOURCE)
        conn.execute(CREATE_WATERMARK)
        conn.execute(
            "insert into etl_watermark (pipeline, watermark) values (%s, %s)",
            (PIPELINE, INITIAL_WATERMARK),
        )


def wave(run: Run, include_in_flight_row: bool) -> list[tuple]:
    rows = [r for r in SOURCE_ROWS if r[4] == run.number]
    if include_in_flight_row and IN_FLIGHT_ROW[4] == run.number:
        rows.append(IN_FLIGHT_ROW)
    return rows


def commit_source_rows(conn: psycopg.Connection, rows: list[tuple]) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(INSERT_SOURCE_ROW, [r[:4] for r in rows])


def commit_wave(conn: psycopg.Connection, run: Run, in_flight: bool = False) -> None:
    """Commit the source rows that arrive before `run`. With `in_flight`, the
    late-committing row is left out here; the scenario commits it itself."""
    commit_source_rows(conn, wave(run, include_in_flight_row=not in_flight))


def read_source(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("select * from src_transactions").fetchall()


def extract_after(conn: psycopg.Connection, low: datetime) -> list[dict]:
    return conn.execute(
        "select * from src_transactions where updated_at > %s order by updated_at, txn_id",
        (low,),
    ).fetchall()


def extract_interval(conn: psycopg.Connection, run: Run, inclusive_end: bool = False) -> list[dict]:
    upper = "<=" if inclusive_end else "<"
    return conn.execute(
        f"select * from src_transactions where updated_at >= %s and updated_at {upper} %s "
        "order by updated_at, txn_id",
        (run.interval_start, run.interval_end),
    ).fetchall()


def read_watermark(conn: psycopg.Connection) -> datetime:
    row = conn.execute("select watermark from etl_watermark where pipeline = %s", (PIPELINE,)).fetchone()
    return row["watermark"]


def save_watermark(conn: psycopg.Connection, rows: list[dict]) -> None:
    """Move the watermark to the newest updated_at read, never backwards."""
    if not rows:
        return
    newest = max(r["updated_at"] for r in rows)
    conn.execute(
        "update etl_watermark set watermark = greatest(watermark, %s) where pipeline = %s",
        (newest, PIPELINE),
    )


# --- the recorder ------------------------------------------------------------


def comparable(rows: list[dict]) -> list[tuple]:
    return sorted(tuple(str(row[c]) for c in COMPARED_COLUMNS) for row in rows)


def duplicated_txn_ids(rows: list[dict]) -> list[str]:
    counts = Counter(row["txn_id"] for row in rows)
    return sorted(txn_id for txn_id, n in counts.items() if n > 1)


def missing_txn_ids(source: list[dict], target: list[dict]) -> list[str]:
    return sorted({r["txn_id"] for r in source} - {r["txn_id"] for r in target})


class Recorder:
    """Writes the target table after each step to out/<engine>/<scenario>/
    and keeps a one-line summary of it, the source and the watermark."""

    def __init__(self, engine: str, scenario: str, description: str, uses_watermark: bool = True):
        self.engine = engine
        self.scenario = scenario
        self.description = description
        self.uses_watermark = uses_watermark
        self.folder = OUT_DIR / engine / scenario
        self.folder.mkdir(parents=True, exist_ok=True)
        self.steps: list[dict] = []

    def step(
        self,
        label: str,
        source: list[dict],
        target: list[dict],
        watermark: datetime | None,
        outcome: str = "completed",
    ) -> dict:
        index = len(self.steps) + 1
        self.write_rows(f"{index:02d}-{slug(label)}.csv", target)
        summary = {
            "step": label,
            "outcome": outcome,
            "source_rows": len(source),
            "target_rows": len(target),
            "missing_txn_ids": missing_txn_ids(source, target),
            "duplicated_txn_ids": duplicated_txn_ids(target),
            "watermark": f"{watermark:%H:%M}" if self.uses_watermark and watermark else None,
            "matches_source": comparable(target) == comparable(source),
        }
        self.steps.append(summary)
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
        }


def slug(text: str) -> str:
    kept = "".join(ch if ch.isalnum() else "-" for ch in text.lower())
    return "-".join(part for part in kept.split("-") if part)


def write_report(engine: str, results: list[dict]) -> Path:
    lines = [f"# Part 2 results: {engine}", ""]
    for result in results:
        lines += [f"## {result['scenario']}", "", result["description"], ""]
        lines += [
            "| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for s in result["steps"]:
            missing = ", ".join(s["missing_txn_ids"]) or "none"
            duplicates = ", ".join(s["duplicated_txn_ids"]) or "none"
            watermark = s["watermark"] or "not used"
            matches = "yes" if s["matches_source"] else "**no**"
            lines.append(
                f"| {s['step']} | {s['outcome']} | {s['source_rows']} | {s['target_rows']} "
                f"| {missing} | {duplicates} | {watermark} | {matches} |"
            )
        lines.append("")
    path = OUT_DIR / engine / "report.md"
    path.write_text("\n".join(lines))
    return path
