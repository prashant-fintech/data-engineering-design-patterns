"""Part 3 on Postgres: loading a month of history before the daily pipeline
takes over.

Scenarios 01 to 04 load September with a one-off SQL script and October
with the daily pipeline. Scenarios 05 to 08 load September through the
pipeline itself: one run per day, or one run with a month-wide interval.
The target is written to out/postgres/ after each step, next to a
comparison with what the pipeline's transform should have produced.
"""
import psycopg

from common import (
    CUTOVER,
    HISTORY_LOAD_AT,
    HISTORY_START,
    LAST_OPEN_DAY,
    NEGATIVE_TYPES_V1,
    NEGATIVE_TYPES_V2,
    RERUN_DAY,
    SERIES_END,
    Recorder,
    Run,
    commit_source,
    connect,
    daily_run,
    daily_runs,
    extract_interval,
    read_source,
    reset_source,
    stamp_day_key,
    stamp_run_key,
    transform,
    wide_run,
    write_report,
)

ENGINE = "postgres"

CREATE_TARGET = """
create table fact_transactions (
    txn_id         text           not null,
    account_id     text           not null,
    txn_type       text           not null,
    amount         numeric(12, 2) not null,
    updated_at     timestamptz    not null,
    interval_start timestamptz    not null,
    loaded_at      timestamptz    not null
)
"""

INSERT_TARGET_ROW = """
insert into fact_transactions
    (txn_id, account_id, txn_type, amount, updated_at, interval_start, loaded_at)
values
    (%(txn_id)s, %(account_id)s, %(txn_type)s, %(amount)s, %(updated_at)s, %(interval_start)s, now())
"""

# --- the one-off history scripts --------------------------------------------

# Copies the source as it is: no sign rule, no test-account rule.
SCRIPT_PLAIN_COPY = """
insert into fact_transactions
    (txn_id, account_id, txn_type, amount, updated_at, interval_start, loaded_at)
select txn_id, account_id, txn_type, amount, updated_at, %(script_key)s, now()
from src_transactions
where updated_at < %(cutover)s
"""

# Re-implements both of the pipeline's rules in SQL, correctly, as they
# stood on the day the script was written (version 1).
SCRIPT_WITH_V1_RULES = """
insert into fact_transactions
    (txn_id, account_id, txn_type, amount, updated_at, interval_start, loaded_at)
select txn_id,
       account_id,
       txn_type,
       case when txn_type = 'REFUND' then -amount else amount end,
       updated_at,
       %(script_key)s,
       now()
from src_transactions
where updated_at < %(cutover)s
  and account_id not like 'TEST-%%'
"""


def run_history_script(conn: psycopg.Connection, script: str) -> None:
    """Every history row gets one interval_start: the first day of the history."""
    with conn.transaction():
        conn.execute(script, {"script_key": HISTORY_START, "cutover": CUTOVER})


# --- the pipeline ------------------------------------------------------------


def reset_all(conn: psycopg.Connection) -> None:
    reset_source(conn)
    with conn.transaction():
        conn.execute("drop table if exists fact_transactions")
        conn.execute(CREATE_TARGET)


def read_target(conn: psycopg.Connection) -> list[dict]:
    return conn.execute("select * from fact_transactions").fetchall()


def insert_rows(conn: psycopg.Connection, rows: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.executemany(INSERT_TARGET_ROW, rows)


def load_run_key(conn: psycopg.Connection, run: Run, negative_types: frozenset) -> None:
    """The daily load as part 2 left it: read the run's interval, transform
    it, and replace the rows stamped with this run's interval_start."""
    extracted = extract_interval(conn, run)
    transformed = transform(extracted, negative_types)
    stamped = stamp_run_key(transformed, run)
    with conn.transaction():
        conn.execute("delete from fact_transactions where interval_start = %s", (run.interval_start,))
        insert_rows(conn, stamped)


def load_day_key(conn: psycopg.Connection, run: Run, negative_types: frozenset) -> None:
    """The same load with the key taken from each row: stamp every row with
    the daily interval it falls in, and replace every daily interval inside
    the run's interval."""
    extracted = extract_interval(conn, run)
    transformed = transform(extracted, negative_types)
    stamped = stamp_day_key(transformed)
    with conn.transaction():
        conn.execute(
            "delete from fact_transactions where interval_start >= %s and interval_start < %s",
            (run.interval_start, run.interval_end),
        )
        insert_rows(conn, stamped)


# --- helpers for the scenarios ----------------------------------------------


def record(rec: Recorder, conn: psycopg.Connection, label: str, negative_types: frozenset = NEGATIVE_TYPES_V1) -> None:
    rec.step(label, read_source(conn), read_target(conn), negative_types)


def run_pipeline(rec: Recorder, conn: psycopg.Connection, load, run: Run, negative_types: frozenset = NEGATIVE_TYPES_V1) -> None:
    load(conn, run, negative_types)
    rec.pipeline_runs += 1


def run_october(rec: Recorder, conn: psycopg.Connection, load) -> None:
    """The three scheduled daily runs. Each day's source rows are committed
    before the run for that day."""
    for run in daily_runs(CUTOVER, SERIES_END):
        commit_source(conn, run.interval_start, run.interval_end)
        run_pipeline(rec, conn, load, run)


def script_then_october(rec: Recorder, conn: psycopg.Connection, script: str) -> None:
    """Shared opening of scenarios 01, 03 and 04: the script runs just after
    midnight on 1 October, when all of September has been committed."""
    reset_all(conn)
    commit_source(conn, HISTORY_START, CUTOVER)
    run_history_script(conn, script)
    record(rec, conn, "history script, 01 Sep to 30 Sep")
    run_october(rec, conn, load_run_key)
    record(rec, conn, "daily runs, 01 Oct to 03 Oct")


# --- scenarios 01 to 04: a one-off script loads the history ------------------


def scenario_script_plain_copy(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "01-script-plain-copy",
        "A one-off script copies September as the source stores it. The daily pipeline, "
        "which signs refunds and drops test accounts, loads October.",
    )
    script_then_october(rec, conn, SCRIPT_PLAIN_COPY)
    return rec.result()


def scenario_script_cutover(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "02-script-cutover",
        "The script applies both rules correctly, and runs at 18:00 on 30 September. Two more "
        "rows are committed that evening. The daily pipeline starts with 1 October, and then "
        "30 September is run through the pipeline to pick up the evening.",
    )
    reset_all(conn)
    commit_source(conn, HISTORY_START, HISTORY_LOAD_AT)
    run_history_script(conn, SCRIPT_WITH_V1_RULES)
    record(rec, conn, "history script at 18:00 on 30 Sep")

    commit_source(conn, HISTORY_LOAD_AT, CUTOVER)
    run_october(rec, conn, load_run_key)
    record(rec, conn, "daily runs, 01 Oct to 03 Oct")

    run_pipeline(rec, conn, load_run_key, daily_run(LAST_OPEN_DAY))
    record(rec, conn, "30 Sep run through the pipeline")
    return rec.result()


def scenario_script_ownership(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "03-script-ownership",
        "The script applies both rules correctly and runs after midnight, so the first load "
        "is right. Then one September day is rerun through the pipeline.",
    )
    script_then_october(rec, conn, SCRIPT_WITH_V1_RULES)
    run_pipeline(rec, conn, load_run_key, daily_run(RERUN_DAY))
    record(rec, conn, f"rerun {daily_run(RERUN_DAY).label}")
    return rec.result()


def scenario_script_later_fix(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "04-script-later-fix",
        "The script applies both rules correctly as they stood (version 1). After 3 October "
        "the sign rule is fixed in the pipeline so that chargebacks are negative too, and "
        "the October days are rerun.",
    )
    script_then_october(rec, conn, SCRIPT_WITH_V1_RULES)
    record(rec, conn, "fix deployed, nothing rerun", NEGATIVE_TYPES_V2)
    for run in daily_runs(CUTOVER, SERIES_END):
        run_pipeline(rec, conn, load_run_key, run, NEGATIVE_TYPES_V2)
    record(rec, conn, "rerun 01 Oct to 03 Oct with the fix", NEGATIVE_TYPES_V2)
    return rec.result()


# --- scenarios 05 to 08: the pipeline loads its own history -------------------


def history_then_october(rec: Recorder, conn: psycopg.Connection, load, history_runs: list[Run], history_label: str) -> None:
    """Shared opening of scenarios 05 to 07. At 18:00 on 30 September the
    pipeline is run for the intervals that have already ended, 1 to 29
    September. 30 September runs after midnight like any other day."""
    reset_all(conn)
    commit_source(conn, HISTORY_START, HISTORY_LOAD_AT)
    for run in history_runs:
        run_pipeline(rec, conn, load, run)
    record(rec, conn, history_label)

    commit_source(conn, HISTORY_LOAD_AT, CUTOVER)
    run_pipeline(rec, conn, load, daily_run(LAST_OPEN_DAY))
    record(rec, conn, "daily run, 30 Sep")

    run_october(rec, conn, load)
    record(rec, conn, "daily runs, 01 Oct to 03 Oct")


def scenario_pipeline_one_run_per_day(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "05-pipeline-one-run-per-day",
        "No script. The daily pipeline is run once for each closed day of September, then "
        "daily as scheduled. One day is rerun, and after the fix every day is rerun.",
    )
    history_runs = daily_runs(HISTORY_START, LAST_OPEN_DAY)
    history_then_october(rec, conn, load_run_key, history_runs, "29 daily runs at 18:00 on 30 Sep")

    run_pipeline(rec, conn, load_run_key, daily_run(RERUN_DAY))
    record(rec, conn, f"rerun {daily_run(RERUN_DAY).label}")

    record(rec, conn, "fix deployed, nothing rerun", NEGATIVE_TYPES_V2)
    for run in daily_runs(HISTORY_START, SERIES_END):
        run_pipeline(rec, conn, load_run_key, run, NEGATIVE_TYPES_V2)
    record(rec, conn, "33 daily reruns with the fix", NEGATIVE_TYPES_V2)
    return rec.result()


def scenario_pipeline_wide_run_key(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "06-pipeline-wide-run-key",
        "The same load function is called once with the interval 1 to 29 September. It stamps "
        "every row with the run's interval_start, as it does for a daily run. Then two "
        "September days are rerun as daily runs.",
    )
    history_runs = [wide_run(HISTORY_START, LAST_OPEN_DAY)]
    history_then_october(rec, conn, load_run_key, history_runs, "1 wide run at 18:00 on 30 Sep")

    run_pipeline(rec, conn, load_run_key, daily_run(RERUN_DAY))
    record(rec, conn, f"rerun {daily_run(RERUN_DAY).label}")

    run_pipeline(rec, conn, load_run_key, daily_run(HISTORY_START))
    record(rec, conn, f"rerun {daily_run(HISTORY_START).label}")
    return rec.result()


def scenario_pipeline_wide_day_key(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "07-pipeline-wide-day-key",
        "As 06, but the load stamps each row with the daily interval it falls in and replaces "
        "every daily interval inside the run's interval. After the fix, one wide run "
        "reloads everything.",
    )
    history_runs = [wide_run(HISTORY_START, LAST_OPEN_DAY)]
    history_then_october(rec, conn, load_day_key, history_runs, "1 wide run at 18:00 on 30 Sep")

    run_pipeline(rec, conn, load_day_key, daily_run(RERUN_DAY))
    record(rec, conn, f"rerun {daily_run(RERUN_DAY).label}")

    run_pipeline(rec, conn, load_day_key, daily_run(HISTORY_START))
    record(rec, conn, f"rerun {daily_run(HISTORY_START).label}")

    record(rec, conn, "fix deployed, nothing rerun", NEGATIVE_TYPES_V2)
    run_pipeline(rec, conn, load_day_key, wide_run(HISTORY_START, SERIES_END), NEGATIVE_TYPES_V2)
    record(rec, conn, "1 wide rerun with the fix", NEGATIVE_TYPES_V2)
    return rec.result()


def scenario_pipeline_open_interval(conn: psycopg.Connection) -> dict:
    rec = Recorder(
        ENGINE,
        "08-pipeline-open-interval",
        "As 07, but the wide run at 18:00 on 30 September is handed the whole month, "
        "including the day that has not ended. Later the 30 September interval is rerun.",
    )
    reset_all(conn)
    commit_source(conn, HISTORY_START, HISTORY_LOAD_AT)
    run_pipeline(rec, conn, load_day_key, wide_run(HISTORY_START, CUTOVER))
    record(rec, conn, "1 wide run at 18:00 on 30 Sep, 30 Sep still open")

    commit_source(conn, HISTORY_LOAD_AT, CUTOVER)
    run_october(rec, conn, load_day_key)
    record(rec, conn, "daily runs, 01 Oct to 03 Oct")

    run_pipeline(rec, conn, load_day_key, daily_run(LAST_OPEN_DAY))
    record(rec, conn, f"rerun {daily_run(LAST_OPEN_DAY).label}")
    return rec.result()


SCENARIOS = [
    scenario_script_plain_copy,
    scenario_script_cutover,
    scenario_script_ownership,
    scenario_script_later_fix,
    scenario_pipeline_one_run_per_day,
    scenario_pipeline_wide_run_key,
    scenario_pipeline_wide_day_key,
    scenario_pipeline_open_interval,
]


def main() -> None:
    with connect() as conn:
        results = [scenario(conn) for scenario in SCENARIOS]
    print(f"wrote {write_report(ENGINE, results)}")


if __name__ == "__main__":
    main()
