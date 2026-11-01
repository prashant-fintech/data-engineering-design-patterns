"""Part 2 on Spark and Delta Lake: the source and a watermark table stay in
Postgres, and the target is a Delta table.

The watermark and the rows now live in two systems, so no transaction can
cover both. The scenarios compare moving the watermark before the write,
writing before moving the watermark, and keeping no watermark table at
all: reading the newest updated_at from the target itself. The target and
its commit history are written to out/delta/ after each step.
"""
import shutil
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from common import (
    INITIAL_WATERMARK,
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
    read_source,
    read_watermark,
    reset_source_and_watermark,
    save_watermark,
    write_report,
)

ENGINE = "delta"
TABLE_PATH = Path("data/delta/fact_transactions")

SOURCE_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", DecimalType(12, 2), False),
        StructField("updated_at", TimestampType(), False),
    ]
)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("dedp-part-02")
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
    reset_source_and_watermark(conn)
    shutil.rmtree(TABLE_PATH, ignore_errors=True)


def table_exists(spark: SparkSession) -> bool:
    return DeltaTable.isDeltaTable(spark, str(TABLE_PATH))


def read_target(spark: SparkSession) -> list[dict]:
    if not table_exists(spark):
        return []
    rows = [row.asDict() for row in spark.read.format("delta").load(str(TABLE_PATH)).collect()]
    # collect() returns naive datetimes; the container runs in UTC, so mark
    # them as UTC to compare them with the timestamptz values from Postgres
    for row in rows:
        row["updated_at"] = row["updated_at"].replace(tzinfo=timezone.utc)
    return rows


def write_history(spark: SparkSession, rec: Recorder) -> None:
    history = (
        DeltaTable.forPath(spark, str(TABLE_PATH))
        .history()
        .select("version", "operation", F.col("operationParameters").cast("string").alias("parameters"))
        .orderBy("version")
        .collect()
    )
    lines = ["version,operation,parameters"]
    lines += [f'{h.version},{h.operation},"{h.parameters}"' for h in history]
    (rec.folder / "history.csv").write_text("\n".join(lines) + "\n")


# --- the write and the two watermarks ---------------------------------------


def write_keyed(spark: SparkSession, rows: list[dict], run: Run) -> None:
    """Replace the txn_ids being written with their new copies, in one commit."""
    if not rows:
        return
    txn_ids = ", ".join(f"'{r['txn_id']}'" for r in rows)
    batch = (
        spark.createDataFrame([tuple(r[c] for c in SOURCE_SCHEMA.names) for r in rows], SOURCE_SCHEMA)
        .withColumn("interval_start", F.lit(run.interval_start))
        .withColumn("loaded_at", F.current_timestamp())
    )
    (
        batch.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"txn_id IN ({txn_ids})")
        .save(str(TABLE_PATH))
    )


def target_watermark(spark: SparkSession) -> datetime:
    """The newest updated_at already in the target: it cannot run ahead of
    the rows, because it is read from them."""
    if not table_exists(spark):
        return INITIAL_WATERMARK
    newest = spark.read.format("delta").load(str(TABLE_PATH)).agg(F.max("updated_at")).first()[0]
    return newest.replace(tzinfo=timezone.utc)


# --- the loads ---------------------------------------------------------------


def load_watermark_first(spark, conn, run: Run, crash_between: bool = False) -> None:
    """Commit the new watermark in Postgres, then write the rows to Delta."""
    rows = extract_after(conn, read_watermark(conn))
    with conn.transaction():
        save_watermark(conn, rows)
    if crash_between:
        raise SimulatedCrash("worker killed after moving the watermark, before the write")
    write_keyed(spark, rows, run)


def load_write_first(spark, conn, run: Run, crash_between: bool = False) -> None:
    """Write the rows to Delta, then commit the new watermark in Postgres."""
    rows = extract_after(conn, read_watermark(conn))
    write_keyed(spark, rows, run)
    if crash_between:
        raise SimulatedCrash("worker killed after the write, before moving the watermark")
    with conn.transaction():
        save_watermark(conn, rows)


def load_from_target_watermark(spark, conn, run: Run, crash_before_write: bool = False) -> None:
    """No watermark table: read from the target's newest updated_at, minus
    the lookback, and write keyed on txn_id."""
    rows = extract_after(conn, target_watermark(spark) - LOOKBACK)
    if crash_before_write:
        raise SimulatedCrash("worker killed before the write")
    write_keyed(spark, rows, run)


# --- the scenarios -----------------------------------------------------------


def record(rec: Recorder, spark, conn, label: str, watermark: datetime, outcome: str = "completed") -> None:
    rec.step(label, read_source(conn), read_target(spark), watermark, outcome)


def attempt(load, *args, **kwargs) -> str:
    try:
        load(*args, **kwargs)
        return "completed"
    except SimulatedCrash as crash:
        return f"crashed: {crash}"


def crash_on_run_2(rec: Recorder, spark, conn, load) -> dict:
    reset_all(conn)
    run_1, run_2, run_3 = RUNS

    commit_wave(conn, run_1)
    load(spark, conn, run_1)
    record(rec, spark, conn, run_1.label, read_watermark(conn))

    commit_wave(conn, run_2)
    outcome = attempt(load, spark, conn, run_2, crash_between=True)
    record(rec, spark, conn, f"{run_2.label}, crash", read_watermark(conn), outcome)

    load(spark, conn, run_2)
    record(rec, spark, conn, f"retry {run_2.label}", read_watermark(conn))

    commit_wave(conn, run_3)
    load(spark, conn, run_3)
    record(rec, spark, conn, run_3.label, read_watermark(conn))
    write_history(spark, rec)
    return rec.result()


def scenario_watermark_first(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "01-watermark-first",
        "Watermark committed in Postgres, then the rows written to Delta. Run 2 dies between the two.",
    )
    return crash_on_run_2(rec, spark, conn, load_watermark_first)


def scenario_write_first(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "02-write-first",
        "Rows written to Delta keyed on txn_id, then the watermark committed in Postgres. "
        "Run 2 dies between the two.",
    )
    return crash_on_run_2(rec, spark, conn, load_write_first)


def scenario_target_watermark(spark, conn) -> dict:
    rec = Recorder(
        ENGINE,
        "03-target-watermark",
        "No watermark table: each run reads from max(updated_at) in the target minus "
        f"{int(LOOKBACK.total_seconds() // 60)} minutes. T-09 commits after run 2 has read "
        "the source, and run 3 dies before its write.",
    )
    reset_all(conn)
    run_1, run_2, run_3 = RUNS

    commit_wave(conn, run_1)
    load_from_target_watermark(spark, conn, run_1)
    record(rec, spark, conn, run_1.label, target_watermark(spark))

    commit_wave(conn, run_2, in_flight=True)
    with connect() as other:
        with other.transaction():
            commit_source_rows(other, [IN_FLIGHT_ROW])  # nested: committed by the outer block
            load_from_target_watermark(spark, conn, run_2)
    record(rec, spark, conn, f"{run_2.label}, T-09 in flight", target_watermark(spark))

    commit_wave(conn, run_3)
    outcome = attempt(load_from_target_watermark, spark, conn, run_3, crash_before_write=True)
    record(rec, spark, conn, f"{run_3.label}, crash", target_watermark(spark), outcome)

    load_from_target_watermark(spark, conn, run_3)
    record(rec, spark, conn, f"retry {run_3.label}", target_watermark(spark))
    write_history(spark, rec)
    return rec.result()


SCENARIOS = [
    scenario_watermark_first,
    scenario_write_first,
    scenario_target_watermark,
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
