"""Part 2 on Postgres: incremental loads driven by a watermark table and by
the data interval the scheduler hands each run.

The source, the target and the watermark all live in one Postgres
database, so a load can commit the rows and the watermark together, or in
either order. Every scenario starts from an empty source and target and
runs three hourly runs; the source rows for each run are committed just
before it. The target is written to out/postgres/ after each step.
"""
from datetime import datetime, timezone

import psycopg

from common import (
    IN_FLIGHT_ROW,
    LOOKBACK,
    RUNS,
    Recorder,
    Run,
    SimulatedCrash,
    commit_source_rows,
    commit_wave,
    connect,
    extract_after,
    extract_interval,
    read_source,
    read_watermark,
    reset_source_and_watermark,
    save_watermark,
    write_report,
)

ENGINE = "postgres"

CREATE_TARGET = """
create table fact_transactions (
    txn_id         text           not null,
    account_id     text           not null,
    amount         numeric(12, 2) not null,
    updated_at     timestamptz    not null,
    interval_start timestamptz    not null,
    loaded_at      timestamptz    not null
)
"""

INSERT_TARGET_ROW = """
insert into fact_transactions
    (txn_id, account_id, amount, updated_at, interval_start, loaded_at)
values (%s, %s, %s, %s, %s, %s)
"""


def reset_all(conn: psycopg.Connection) -> None:
    reset_source_and_watermark(conn)
    with conn.transaction():
        conn.execute("drop table if exists fact_transactions")
        conn.execute(CREATE_TARGET)


def read_target(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("select * from fact_transactions").fetchall()


def stamp_rows(rows: list[dict], run: Run) -> list[tuple]:
    loaded_at = datetime.now(timezone.utc)
    return [
        (r["txn_id"], r["account_id"], r["amount"], r["updated_at"], run.interval_start, loaded_at)
        for r in rows
    ]


# --- the writes --------------------------------------------------------------


def write_append(conn: psycopg.Connection, rows: list[dict], run: Run) -> None:
    with conn.cursor() as cur:
        cur.executemany(INSERT_TARGET_ROW, stamp_rows(rows, run))


def write_keyed(conn: psycopg.Connection, rows: list[dict], run: Run) -> None:
    """Delete the txn_ids being written, then insert them: a re-read row
    replaces its earlier copy instead of adding a second one."""
    txn_ids = [r["txn_id"] for r in rows]
    conn.execute("delete from fact_transactions where txn_id = any(%s)", (txn_ids,))
    write_append(conn, rows, run)


def write_interval(conn: psycopg.Connection, rows: list[dict], run: Run) -> None:
    """Part 1's delete-then-insert, keyed on the run's data interval."""
    conn.execute("delete from fact_transactions where interval_start = %s", (run.interval_start,))
    write_append(conn, rows, run)


# --- the loads ---------------------------------------------------------------


def load_watermark_first(conn: psycopg.Connection, run: Run, crash_between: bool = False) -> None:
    """Advance the watermark in its own commit, then write the rows."""
    rows = extract_after(conn, read_watermark(conn))
    with conn.transaction():
        save_watermark(conn, rows)
    if crash_between:
        raise SimulatedCrash("worker killed after moving the watermark, before the write")
    with conn.transaction():
        write_keyed(conn, rows, run)


def load_write_first(conn: psycopg.Connection, run: Run, write, crash_between: bool = False) -> None:
    """Write the rows in one commit, then advance the watermark in another."""
    rows = extract_after(conn, read_watermark(conn))
    with conn.transaction():
        write(conn, rows, run)
    if crash_between:
        raise SimulatedCrash("worker killed after the write, before moving the watermark")
    with conn.transaction():
        save_watermark(conn, rows)


def load_one_transaction(
    conn: psycopg.Connection,
    run: Run,
    crash_between: bool = False,
    lookback=None,
) -> None:
    """Write the rows and advance the watermark in one transaction. With a
    lookback, re-read rows a little older than the watermark as well."""
    low = read_watermark(conn)
    if lookback is not None:
        low = low - lookback
    rows = extract_after(conn, low)
    with conn.transaction():
        write_keyed(conn, rows, run)
        if crash_between:
            raise SimulatedCrash("worker killed after the write, before moving the watermark")
        save_watermark(conn, rows)


def load_interval(conn: psycopg.Connection, run: Run, inclusive_end: bool = False) -> None:
    """Read exactly the run's data interval and replace that interval's rows."""
    rows = extract_interval(conn, run, inclusive_end)
    with conn.transaction():
        write_interval(conn, rows, run)


# --- the scenarios -----------------------------------------------------------


def record(rec: Recorder, conn: psycopg.Connection, label: str, outcome: str = "completed") -> None:
    rec.step(label, read_source(conn), read_target(conn), read_watermark(conn), outcome)


def attempt(load, *args, **kwargs) -> str:
    try:
        load(*args, **kwargs)
        return "completed"
    except SimulatedCrash as crash:
        return f"crashed: {crash}"


def run_with_row_in_flight(conn: psycopg.Connection, load, run: Run, **kwargs) -> None:
    """Run `load` while another session holds IN_FLIGHT_ROW uncommitted, and
    commit that row as soon as the load has finished."""
    with connect() as other:
        with other.transaction():
            commit_source_rows(other, [IN_FLIGHT_ROW])  # nested: committed by the outer block
            load(conn, run, **kwargs)


def crash_on_run_2(rec: Recorder, conn: psycopg.Connection, load, **kwargs) -> dict:
    """Shared shape of scenarios 01 to 04: run 1, run 2 crashes between its
    two steps, run 2 is retried, then run 3."""
    reset_all(conn)
    run_1, run_2, run_3 = RUNS

    commit_wave(conn, run_1)
    load(conn, run_1, **kwargs)
    record(rec, conn, run_1.label)

    commit_wave(conn, run_2)
    outcome = attempt(load, conn, run_2, crash_between=True, **kwargs)
    record(rec, conn, f"{run_2.label}, crash", outcome)

    load(conn, run_2, **kwargs)
    record(rec, conn, f"retry {run_2.label}")

    commit_wave(conn, run_3)
    load(conn, run_3, **kwargs)
    record(rec, conn, run_3.label)
    return rec.result()


def scenario_watermark_first(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "01-watermark-first",
        "The watermark is committed, then the rows are written in a second commit. "
        "Run 2 dies between the two.",
    )
    return crash_on_run_2(rec, conn, load_watermark_first)


def scenario_write_first_append(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "02-write-first-append",
        "The rows are appended, then the watermark is committed in a second commit. "
        "Run 2 dies between the two.",
    )
    return crash_on_run_2(rec, conn, load_write_first, write=write_append)


def scenario_write_first_keyed(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "03-write-first-keyed",
        "The rows are written keyed on txn_id, then the watermark is committed in a "
        "second commit. Run 2 dies between the two.",
    )
    return crash_on_run_2(rec, conn, load_write_first, write=write_keyed)


def scenario_one_transaction(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "04-one-transaction",
        "The keyed write and the watermark update share one transaction. "
        "Run 2 dies between them.",
    )
    return crash_on_run_2(rec, conn, load_one_transaction)


def in_flight_on_run_2(rec: Recorder, conn: psycopg.Connection, load, **kwargs) -> dict:
    """Shared shape of scenarios 05 to 07: three runs with no crash, and
    T-09 still uncommitted while run 2 reads the source."""
    reset_all(conn)
    run_1, run_2, run_3 = RUNS

    commit_wave(conn, run_1)
    load(conn, run_1, **kwargs)
    record(rec, conn, run_1.label)

    commit_wave(conn, run_2, in_flight=True)
    run_with_row_in_flight(conn, load, run_2, **kwargs)
    record(rec, conn, f"{run_2.label}, T-09 in flight")

    commit_wave(conn, run_3)
    load(conn, run_3, **kwargs)
    record(rec, conn, run_3.label)
    return rec


def scenario_in_flight_watermark(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "05-in-flight-watermark",
        "One transaction per load, watermark = newest updated_at read. T-09 (10:58) "
        "commits after run 2 has read the source.",
    )
    return in_flight_on_run_2(rec, conn, load_one_transaction).result()


def scenario_in_flight_lookback(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "06-in-flight-lookback",
        f"As 05, but every run also re-reads the {int(LOOKBACK.total_seconds() // 60)} minutes "
        "before the watermark, keyed on txn_id.",
    )
    return in_flight_on_run_2(rec, conn, load_one_transaction, lookback=LOOKBACK).result()


def scenario_in_flight_interval(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "07-in-flight-interval",
        "Each run reads updated_at >= interval start and < interval end, and replaces "
        "that interval's rows. T-09 commits after run 2. Run 2's interval is then rerun twice.",
        uses_watermark=False,
    )
    in_flight_on_run_2(rec, conn, load_interval)
    run_2 = RUNS[1]
    load_interval(conn, run_2)
    record(rec, conn, f"rerun {run_2.label}")
    load_interval(conn, run_2)
    record(rec, conn, f"rerun {run_2.label} again")
    return rec.result()


def scenario_interval_inclusive_end(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "08-interval-inclusive-end",
        "As 07 with no in-flight row, but each run reads updated_at >= start and <= end. "
        "T-08 is stamped exactly 11:00.",
        uses_watermark=False,
    )
    reset_all(conn)
    for run in RUNS:
        commit_wave(conn, run)
        load_interval(conn, run, inclusive_end=True)
        record(rec, conn, run.label)
    return rec.result()


SCENARIOS = [
    scenario_watermark_first,
    scenario_write_first_append,
    scenario_write_first_keyed,
    scenario_one_transaction,
    scenario_in_flight_watermark,
    scenario_in_flight_lookback,
    scenario_in_flight_interval,
    scenario_interval_inclusive_end,
]


def main() -> None:
    with connect() as conn:
        results = [scenario(conn) for scenario in SCENARIOS]
    print(f"wrote {write_report(ENGINE, results)}")


if __name__ == "__main__":
    main()
