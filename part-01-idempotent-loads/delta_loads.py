"""Part 1 on Spark and Delta Lake: a plain append, delete-then-append as two
commits with a crash between them, and replaceWhere, which does the delete
and the insert in one commit.

Delta Lake has no multi-statement transaction. The one-commit form of
delete-then-insert is an overwrite with a replaceWhere predicate. The
table and its commit history are written to out/delta/ after each step.
"""
import shutil
from datetime import date
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from common import (
    BATCH_DATES,
    Recorder,
    SimulatedCrash,
    landing_file,
    write_report,
)

ENGINE = "delta"
TABLE_PATH = Path("data/delta/fact_transactions")

LANDING_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("business_date", DateType(), False),
        StructField("amount", DecimalType(12, 2), False),
    ]
)


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("dedp-part-01")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.ivy", str(Path(".cache/ivy").resolve()))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # scenario 03 fails on purpose; keep its JVM stack trace out of the console
    spark.sparkContext.setLogLevel("OFF")
    return spark


def reset_table() -> None:
    shutil.rmtree(TABLE_PATH, ignore_errors=True)


def read_batch(spark: SparkSession, batch_date: date) -> DataFrame:
    return (
        spark.read.csv(str(landing_file(batch_date)), header=True, schema=LANDING_SCHEMA)
        .withColumn("batch_date", F.lit(batch_date))
        .withColumn("loaded_at", F.current_timestamp())
    )


def table_exists(spark: SparkSession) -> bool:
    return DeltaTable.isDeltaTable(spark, str(TABLE_PATH))


def read_table(spark: SparkSession) -> list[dict]:
    if not table_exists(spark):
        return []
    rows = spark.read.format("delta").load(str(TABLE_PATH)).collect()
    return [row.asDict() for row in rows]


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


# --- the loads -------------------------------------------------------------


def load_append(spark: SparkSession, batch_date: date) -> None:
    batch = read_batch(spark, batch_date)
    batch.write.format("delta").mode("append").save(str(TABLE_PATH))


def load_delete_then_append(spark: SparkSession, batch_date: date, crash_after_delete: bool = False) -> None:
    """Delete the batch's rows in one commit, append them again in another."""
    batch = read_batch(spark, batch_date)
    if table_exists(spark):
        DeltaTable.forPath(spark, str(TABLE_PATH)).delete(f"batch_date = DATE'{batch_date.isoformat()}'")
    if crash_after_delete:
        raise SimulatedCrash(f"worker killed after deleting batch_date = {batch_date}")
    batch.write.format("delta").mode("append").save(str(TABLE_PATH))


def load_replace_where(spark: SparkSession, batch_date: date, column: str) -> None:
    """Replace the rows matching `column = batch_date` with the batch, in one commit."""
    batch = read_batch(spark, batch_date)
    (
        batch.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{column} = DATE'{batch_date.isoformat()}'")
        .save(str(TABLE_PATH))
    )


# --- the scenarios ---------------------------------------------------------


def scenario_append(spark: SparkSession) -> dict:
    rec = Recorder(ENGINE, "01-append", "mode('append') for each batch; batch 2026-10-02 is then run again.")
    reset_table()
    for batch_date in BATCH_DATES:
        load_append(spark, batch_date)
    rec.step("load three batches", read_table(spark))

    load_append(spark, date(2026, 10, 2))
    rec.step("rerun batch 2026-10-02", read_table(spark))
    write_history(spark, rec)
    return rec.result()


def scenario_crash_two_commits(spark: SparkSession) -> dict:
    rec = Recorder(
        ENGINE,
        "02-crash-two-commits",
        "DeltaTable.delete() then mode('append'), two commits; the rerun of 2026-10-02 dies between them.",
    )
    reset_table()
    for batch_date in BATCH_DATES:
        load_delete_then_append(spark, batch_date)
    rec.step("load three batches", read_table(spark))

    try:
        load_delete_then_append(spark, date(2026, 10, 2), crash_after_delete=True)
        outcome = "completed"
    except SimulatedCrash as crash:
        outcome = f"crashed: {crash}"
    rec.step("rerun batch 2026-10-02, crash after delete", read_table(spark), outcome)

    load_delete_then_append(spark, date(2026, 10, 2))
    rec.step("retry batch 2026-10-02", read_table(spark))
    write_history(spark, rec)
    return rec.result()


def scenario_replace_where_business_date(spark: SparkSession) -> dict:
    rec = Recorder(
        ENGINE,
        "03-replace-where-business-date",
        "replaceWhere business_date = run date. Batch 2026-10-02 carries a late row for 2026-10-01.",
    )
    reset_table()
    for batch_date in BATCH_DATES:
        try:
            load_replace_where(spark, batch_date, "business_date")
            outcome = "completed"
        except Exception as error:  # Delta's constraint check surfaces as a JVM-side analysis error
            outcome = f"failed: {first_line(error)}"
        rec.step(f"load batch {batch_date.isoformat()}", read_table(spark), outcome)
    write_history(spark, rec)
    return rec.result()


def scenario_replace_where_batch_date(spark: SparkSession) -> dict:
    rec = Recorder(
        ENGINE,
        "04-replace-where-batch-date",
        "replaceWhere batch_date = run date, one commit per load.",
    )
    reset_table()
    for batch_date in BATCH_DATES:
        load_replace_where(spark, batch_date, "batch_date")
    rec.step("load three batches", read_table(spark))

    load_replace_where(spark, date(2026, 10, 2), "batch_date")
    rec.step("rerun batch 2026-10-02", read_table(spark))

    load_replace_where(spark, date(2026, 10, 1), "batch_date")
    rec.step("rerun batch 2026-10-01", read_table(spark))
    write_history(spark, rec)
    return rec.result()


def first_line(error: Exception) -> str:
    return str(error).strip().splitlines()[0][:160]


SCENARIOS = [
    scenario_append,
    scenario_crash_two_commits,
    scenario_replace_where_business_date,
    scenario_replace_where_batch_date,
]


def main() -> None:
    spark = build_spark()
    try:
        results = [scenario(spark) for scenario in SCENARIOS]
    finally:
        spark.stop()
    print(f"wrote {write_report(ENGINE, results)}")


if __name__ == "__main__":
    main()
