"""Part 1 on Postgres: a plain append, delete-then-insert keyed on the
business date and on the batch date, and a crash between the delete and
the insert with and without one transaction around them.

Every scenario starts from an empty table, loads the three batches, then
reruns one or more of them. The table is written to out/postgres/ after
each step.
"""
import os
from datetime import date, datetime, timezone

import psycopg
from psycopg.rows import dict_row

from common import (
    BATCH_DATES,
    Recorder,
    SimulatedCrash,
    read_landing_rows,
    write_report,
)

ENGINE = "postgres"
PG_DSN = os.environ.get("PG_DSN", "postgresql://dedp:dedp@localhost:5432/dedp")

CREATE_TABLE = """
create table fact_transactions (
    txn_id        text           not null,
    account_id    text           not null,
    business_date date           not null,
    amount        numeric(12, 2) not null,
    batch_date    date           not null,
    loaded_at     timestamptz    not null
)
"""

INSERT_ROW = """
insert into fact_transactions
    (txn_id, account_id, business_date, amount, batch_date, loaded_at)
values (%s, %s, %s, %s, %s, %s)
"""


def connect() -> psycopg.Connection:
    # autocommit, so each `with conn.transaction()` below is a real
    # BEGIN ... COMMIT and nothing else is ever left open
    return psycopg.connect(PG_DSN, autocommit=True, row_factory=dict_row)


def reset_table(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute("drop table if exists fact_transactions")
        conn.execute(CREATE_TABLE)


def stamp_rows(batch_date: date) -> list[tuple]:
    loaded_at = datetime.now(timezone.utc)
    landing_rows = read_landing_rows(batch_date)
    return [
        (r["txn_id"], r["account_id"], r["business_date"], r["amount"], batch_date, loaded_at)
        for r in landing_rows
    ]


def insert_rows(conn: psycopg.Connection, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.executemany(INSERT_ROW, rows)


def read_table(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("select * from fact_transactions").fetchall()


# --- the loads -------------------------------------------------------------


def load_append(conn: psycopg.Connection, batch_date: date) -> None:
    rows = stamp_rows(batch_date)
    with conn.transaction():
        insert_rows(conn, rows)


def load_delete_insert(
    conn: psycopg.Connection,
    batch_date: date,
    delete_column: str,
    crash_after_delete: bool = False,
) -> None:
    """Delete the slice this batch owns and insert it again, in one transaction."""
    rows = stamp_rows(batch_date)
    with conn.transaction():
        conn.execute(f"delete from fact_transactions where {delete_column} = %s", (batch_date,))
        if crash_after_delete:
            raise SimulatedCrash(f"worker killed after deleting {delete_column} = {batch_date}")
        insert_rows(conn, rows)


def load_delete_insert_two_commits(
    conn: psycopg.Connection,
    batch_date: date,
    crash_after_delete: bool = False,
) -> None:
    """The same delete and insert, each committed on its own."""
    rows = stamp_rows(batch_date)
    with conn.transaction():
        conn.execute("delete from fact_transactions where batch_date = %s", (batch_date,))
    if crash_after_delete:
        raise SimulatedCrash(f"worker killed after deleting batch_date = {batch_date}")
    with conn.transaction():
        insert_rows(conn, rows)


# --- the scenarios ---------------------------------------------------------


def scenario_append(conn: psycopg.Connection) -> dict:
    rec = Recorder(ENGINE, "01-append", "Plain INSERT of each batch; batch 2026-10-02 is then run again.")
    reset_table(conn)
    for batch_date in BATCH_DATES:
        load_append(conn, batch_date)
    rec.step("load three batches", read_table(conn))

    load_append(conn, date(2026, 10, 2))
    rec.step("rerun batch 2026-10-02", read_table(conn))
    return rec.result()


def scenario_delete_by_business_date(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "02-delete-by-business-date",
        "One transaction: DELETE WHERE business_date = run date, then INSERT the batch. "
        "Batch 2026-10-02 carries a late row for 2026-10-01.",
    )
    reset_table(conn)
    for batch_date in BATCH_DATES:
        load_delete_insert(conn, batch_date, "business_date")
    rec.step("load three batches", read_table(conn))

    load_delete_insert(conn, date(2026, 10, 2), "business_date")
    rec.step("rerun batch 2026-10-02", read_table(conn))

    load_delete_insert(conn, date(2026, 10, 1), "business_date")
    rec.step("rerun batch 2026-10-01", read_table(conn))
    return rec.result()


def scenario_delete_by_batch_date(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "03-delete-by-batch-date",
        "One transaction: DELETE WHERE batch_date = run date, then INSERT the batch.",
    )
    reset_table(conn)
    for batch_date in BATCH_DATES:
        load_delete_insert(conn, batch_date, "batch_date")
    rec.step("load three batches", read_table(conn))

    load_delete_insert(conn, date(2026, 10, 2), "batch_date")
    rec.step("rerun batch 2026-10-02", read_table(conn))

    load_delete_insert(conn, date(2026, 10, 1), "batch_date")
    rec.step("rerun batch 2026-10-01", read_table(conn))
    return rec.result()


def scenario_crash_two_commits(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "04-crash-two-commits",
        "DELETE and INSERT committed separately; the rerun of 2026-10-02 dies between them.",
    )
    reset_table(conn)
    for batch_date in BATCH_DATES:
        load_delete_insert_two_commits(conn, batch_date)
    rec.step("load three batches", read_table(conn))

    try:
        load_delete_insert_two_commits(conn, date(2026, 10, 2), crash_after_delete=True)
        outcome = "completed"
    except SimulatedCrash as crash:
        outcome = f"crashed: {crash}"
    rec.step("rerun batch 2026-10-02, crash after delete", read_table(conn), outcome)

    load_delete_insert_two_commits(conn, date(2026, 10, 2))
    rec.step("retry batch 2026-10-02", read_table(conn))
    return rec.result()


def scenario_crash_one_transaction(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "05-crash-one-transaction",
        "DELETE and INSERT in one transaction; the rerun of 2026-10-02 dies between them.",
    )
    reset_table(conn)
    for batch_date in BATCH_DATES:
        load_delete_insert(conn, batch_date, "batch_date")
    rec.step("load three batches", read_table(conn))

    try:
        load_delete_insert(conn, date(2026, 10, 2), "batch_date", crash_after_delete=True)
        outcome = "completed"
    except SimulatedCrash as crash:
        outcome = f"crashed: {crash}"
    rec.step("rerun batch 2026-10-02, crash after delete", read_table(conn), outcome)

    load_delete_insert(conn, date(2026, 10, 2), "batch_date")
    rec.step("retry batch 2026-10-02", read_table(conn))
    return rec.result()


SCENARIOS = [
    scenario_append,
    scenario_delete_by_business_date,
    scenario_delete_by_batch_date,
    scenario_crash_two_commits,
    scenario_crash_one_transaction,
]


def main() -> None:
    with connect() as conn:
        results = [scenario(conn) for scenario in SCENARIOS]
    print(f"wrote {write_report(ENGINE, results)}")


if __name__ == "__main__":
    main()
