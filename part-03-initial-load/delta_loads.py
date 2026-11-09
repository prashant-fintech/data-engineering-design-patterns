"""Part 3 on Spark and Delta Lake: the source stays in Postgres and the
target is a Delta table.

The three scenarios load September through the pipeline itself, as
scenarios 05 to 07 do on Postgres: one run per day, one wide run keyed on
the run, and one wide run keyed on each row's day. After each step the
target is written to out/delta/ with the number of commits and data files
the table has reached.
"""
import csv
import shutil
from datetime import timezone
from pathlib import Path

import psycopg
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

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

ENGINE = "delta"
TABLE_PATH = Path("data/delta/fact_transactions")

STAMPED_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("txn_type", StringType(), False),
        StructField("amount", DecimalType(12, 2), False),
        StructField("updated_at", TimestampType(), False),
        StructField("interval_start", TimestampType(), False),
    ]
)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("dedp-part-03")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.ivy", str(Path(".cache/ivy").resolve()))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def reset_all(conn: psycopg.Connection) -> None:
    reset_source(conn)
    shutil.rmtree(TABLE_PATH, ignore_errors=True)


def read_target(spark: SparkSession) -> list[dict]:
    rows = [row.asDict() for row in spark.read.format("delta").load(str(TABLE_PATH)).collect()]
    # collect() returns naive datetimes; the container runs in UTC, so mark
    # them as UTC to compare them with the timestamptz values from Postgres
    for row in rows:
        row["updated_at"] = row["updated_at"].replace(tzinfo=timezone.utc)
        row["interval_start"] = row["interval_start"].replace(tzinfo=timezone.utc)
    return rows


def table_costs(spark: SparkSession) -> dict:
    """What the table has cost so far: commits in its log, and data files a
    reader of the current version has to open."""
    table = DeltaTable.forPath(spark, str(TABLE_PATH))
    commits = table.history().count()
    data_files = table.detail().select("numFiles").first()[0]
    return {"Commits": commits, "Data files": data_files}


def write_history(spark: SparkSession, rec: Recorder) -> None:
    history = (
        DeltaTable.forPath(spark, str(TABLE_PATH))
        .history()
        .select(
            "version",
            "operation",
            F.col("operationParameters")["predicate"].alias("predicate"),
            F.col("operationMetrics")["numFiles"].alias("files_added"),
            F.col("operationMetrics")["numRemovedFiles"].alias("files_removed"),
            F.col("operationMetrics")["numOutputRows"].alias("rows_written"),
        )
        .orderBy("version")
        .collect()
    )
    with (rec.folder / "history.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["version", "operation", "predicate", "files_added", "files_removed", "rows_written"])
        for h in history:
            writer.writerow([h.version, h.operation, h.predicate, h.files_added, h.files_removed, h.rows_written])


# --- the pipeline ------------------------------------------------------------


def timestamp_literal(moment) -> str:
    return f"'{moment:%Y-%m-%d %H:%M:%S}'"


def write_replacing(spark: SparkSession, stamped: list[dict], predicate: str) -> None:
    """One commit that removes the rows matching `predicate` and adds `stamped`."""
    batch = spark.createDataFrame(
        [tuple(row[c] for c in STAMPED_SCHEMA.names) for row in stamped], STAMPED_SCHEMA
    ).withColumn("loaded_at", F.current_timestamp())
    (
        batch.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .save(str(TABLE_PATH))
    )


def load_run_key(spark: SparkSession, conn: psycopg.Connection, run: Run, negative_types: frozenset) -> None:
    """The daily load: replace the rows stamped with this run's interval_start."""
    extracted = extract_interval(conn, run)
    transformed = transform(extracted, negative_types)
    stamped = stamp_run_key(transformed, run)
    predicate = f"interval_start = {timestamp_literal(run.interval_start)}"
    write_replacing(spark, stamped, predicate)


def load_day_key(spark: SparkSession, conn: psycopg.Connection, run: Run, negative_types: frozenset) -> None:
    """The same load keyed on each row's day: replace every daily interval
    inside the run's interval."""
    extracted = extract_interval(conn, run)
    transformed = transform(extracted, negative_types)
    stamped = stamp_day_key(transformed)
    predicate = (
        f"interval_start >= {timestamp_literal(run.interval_start)} "
        f"AND interval_start < {timestamp_literal(run.interval_end)}"
    )
    write_replacing(spark, stamped, predicate)


# --- helpers for the scenarios ----------------------------------------------


def record(rec: Recorder, spark, conn, label: str, negative_types: frozenset = NEGATIVE_TYPES_V1) -> None:
    rec.step(label, read_source(conn), read_target(spark), negative_types, table_costs(spark))


def run_pipeline(rec: Recorder, spark, conn, load, run: Run, negative_types: frozenset = NEGATIVE_TYPES_V1) -> None:
    load(spark, conn, run, negative_types)
    rec.pipeline_runs += 1


def history_then_october(rec: Recorder, spark, conn, load, history_runs: list[Run], history_label: str) -> None:
    """At 18:00 on 30 September the pipeline is run for 1 to 29 September,
    the intervals that have ended. 30 September and 1 to 3 October then run
    daily, each after its day's source rows are committed."""
    reset_all(conn)
    commit_source(conn, HISTORY_START, HISTORY_LOAD_AT)
    for run in history_runs:
        run_pipeline(rec, spark, conn, load, run)
    record(rec, spark, conn, history_label)

    commit_source(conn, HISTORY_LOAD_AT, CUTOVER)
    run_pipeline(rec, spark, conn, load, daily_run(LAST_OPEN_DAY))
    record(rec, spark, conn, "daily run, 30 Sep")

    for run in daily_runs(CUTOVER, SERIES_END):
        commit_source(conn, run.interval_start, run.interval_end)
        run_pipeline(rec, spark, conn, load, run)
    record(rec, spark, conn, "daily runs, 01 Oct to 03 Oct")


# --- the scenarios -----------------------------------------------------------


def scenario_one_run_per_day(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "01-one-run-per-day",
        "The daily pipeline is run once for each closed day of September, then daily as "
        "scheduled. One day is rerun.",
    )
    history_runs = daily_runs(HISTORY_START, LAST_OPEN_DAY)
    history_then_october(rec, spark, conn, load_run_key, history_runs, "29 daily runs at 18:00 on 30 Sep")

    run_pipeline(rec, spark, conn, load_run_key, daily_run(RERUN_DAY))
    record(rec, spark, conn, f"rerun {daily_run(RERUN_DAY).label}")
    write_history(spark, rec)
    return rec.result()


def scenario_wide_run_key(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "02-wide-run-key",
        "The same load is called once with the interval 1 to 29 September and stamps every "
        "row with the run's interval_start. Then two September days are rerun as daily runs.",
    )
    history_runs = [wide_run(HISTORY_START, LAST_OPEN_DAY)]
    history_then_october(rec, spark, conn, load_run_key, history_runs, "1 wide run at 18:00 on 30 Sep")

    run_pipeline(rec, spark, conn, load_run_key, daily_run(RERUN_DAY))
    record(rec, spark, conn, f"rerun {daily_run(RERUN_DAY).label}")

    run_pipeline(rec, spark, conn, load_run_key, daily_run(HISTORY_START))
    record(rec, spark, conn, f"rerun {daily_run(HISTORY_START).label}")
    write_history(spark, rec)
    return rec.result()


def scenario_wide_day_key(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "03-wide-day-key",
        "As 02, but the load stamps each row with the daily interval it falls in and replaces "
        "every daily interval inside the run's interval. After the fix, one wide run "
        "reloads everything.",
    )
    history_runs = [wide_run(HISTORY_START, LAST_OPEN_DAY)]
    history_then_october(rec, spark, conn, load_day_key, history_runs, "1 wide run at 18:00 on 30 Sep")

    run_pipeline(rec, spark, conn, load_day_key, daily_run(RERUN_DAY))
    record(rec, spark, conn, f"rerun {daily_run(RERUN_DAY).label}")

    run_pipeline(rec, spark, conn, load_day_key, daily_run(HISTORY_START))
    record(rec, spark, conn, f"rerun {daily_run(HISTORY_START).label}")

    record(rec, spark, conn, "fix deployed, nothing rerun", NEGATIVE_TYPES_V2)
    run_pipeline(rec, spark, conn, load_day_key, wide_run(HISTORY_START, SERIES_END), NEGATIVE_TYPES_V2)
    record(rec, spark, conn, "1 wide rerun with the fix", NEGATIVE_TYPES_V2)
    write_history(spark, rec)
    return rec.result()


SCENARIOS = [
    scenario_one_run_per_day,
    scenario_wide_run_key,
    scenario_wide_day_key,
]


def main() -> None:
    spark = build_spark()
    try:
        with connect() as conn:
            results = [scenario(spark, conn) for scenario in SCENARIOS]
    finally:
        spark.stop()
    print(f"wrote {write_report(ENGINE, results)}")


if __name__ == "__main__":
    main()
