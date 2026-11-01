# Part 1: Idempotent Loads

Companion code for [Data Engineering Design Patterns: Idempotent Loads](https://singhprashant.in/writing/data-engineering-design-patterns-01-idempotent-loads).

Three small daily batches are loaded into one fact table, then some of
them are run again. Every load style in the post is run on Postgres and
on Spark with Delta Lake, and the table is written out after each step,
so you can open the CSV and see exactly what a rerun did.

## Setup

You need Docker. Everything else (Python 3.11, Java 17, Spark 3.5,
Delta Lake 3.2, Postgres 16) runs in containers. From this folder:

```bash
docker compose build
docker compose up -d postgres
```

The first Spark run downloads the Delta Lake jars into `.cache/ivy`, so it
needs network access once and takes a minute longer.

## Run in order

```bash
docker compose run --rm lab python make_landing_files.py
docker compose run --rm lab python postgres_loads.py
docker compose run --rm lab python delta_loads.py
docker compose run --rm lab python -m unittest -v test_idempotency
```

Then stop Postgres:

```bash
docker compose down
```

## What each script writes

| Path | Contents |
|---|---|
| `data/landing/batch_date=YYYY-MM-DD/transactions.csv` | The three source batches. The 2026-10-02 batch carries one late row dated 2026-10-01. |
| `out/postgres/<scenario>/NN-<step>.csv` | The whole Postgres table after each step of a scenario. |
| `out/delta/<scenario>/NN-<step>.csv` | The same for the Delta table. |
| `out/delta/<scenario>/history.csv` | The Delta commit log for that scenario: one row per commit. |
| `out/<engine>/report.md` | One line per step: row counts per business date, duplicated `txn_id`s, and whether the table matches the first step. |

`example-results/` holds the two reports and two Delta commit histories
from the run used in the post.

`delta_loads.py` prints a Java stack trace while it runs. That is scenario
`03-replace-where-business-date` failing on purpose; the script carries on
and records the failure in the report.

## Scenarios

| Engine | Scenario | Load |
|---|---|---|
| Postgres | `01-append` | `INSERT` only |
| Postgres | `02-delete-by-business-date` | One transaction: `DELETE WHERE business_date = run date`, then `INSERT` |
| Postgres | `03-delete-by-batch-date` | One transaction: `DELETE WHERE batch_date = run date`, then `INSERT` |
| Postgres | `04-crash-two-commits` | `DELETE` and `INSERT` committed separately, crash between them |
| Postgres | `05-crash-one-transaction` | `DELETE` and `INSERT` in one transaction, crash between them |
| Delta | `01-append` | `mode("append")` |
| Delta | `02-crash-two-commits` | `DeltaTable.delete()` then `mode("append")`, crash between them |
| Delta | `03-replace-where-business-date` | `replaceWhere business_date = run date` |
| Delta | `04-replace-where-batch-date` | `replaceWhere batch_date = run date` |

"The same table" means every column except `loaded_at` matches.
`batch_date` is the run's logical date, so a rerun reproduces it.
`loaded_at` is wall-clock time and records when the rerun actually
happened.
