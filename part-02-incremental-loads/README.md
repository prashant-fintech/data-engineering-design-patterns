# Part 2: incremental loads and watermarks

Companion code for [Data engineering design patterns, part 2](https://singhprashant.in/writing/data-engineering-design-patterns-02-incremental-loads-watermarks).

A Postgres source table receives rows between three hourly runs. Each
run copies the new rows into a target table, either from a watermark
(the newest `updated_at` already loaded) or from the data interval a
scheduler hands the run. The target is Postgres in one script and Delta
Lake in the other, and it is written out after each step, so you can open
the CSV and see which rows a crash or a late commit cost.

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
docker compose run --rm lab python postgres_loads.py
docker compose run --rm lab python delta_loads.py
docker compose run --rm lab python -m unittest -v test_incremental
```

There are no landing files in this part: the source rows are in
`common.py`, and each scenario commits them to `src_transactions` just
before the run they belong to.

Then stop Postgres:

```bash
docker compose down
```

## What each script writes

| Path | Contents |
|---|---|
| `out/postgres/<scenario>/NN-<step>.csv` | The whole Postgres target after each step of a scenario. |
| `out/delta/<scenario>/NN-<step>.csv` | The same for the Delta target. |
| `out/delta/<scenario>/history.csv` | The Delta commit log for that scenario: one row per commit. |
| `out/<engine>/report.md` | One line per step: source and target row counts, `txn_id`s missing from the target or duplicated in it, the watermark, and whether the target matches the source. |

`example-results/` holds the two reports and two Delta commit histories
from the run used in the post.

## The source rows

| Committed before | Rows |
|---|---|
| Run 1 (09:00-10:00) | `T-01` to `T-04`, 09:05 to 09:55 |
| Run 2 (10:00-11:00) | `T-05` 10:10, `T-06` 10:30, `T-07` 10:59, `T-08` exactly 11:00, `T-09` 10:58 |
| Run 3 (11:00-12:00) | `T-10` 11:15, `T-11` 11:40 |

In the in-flight scenarios, `T-09` is inserted by a second session whose
transaction stays open while run 2 reads the source, and commits right
after. Its `updated_at` says 10:58, but it only becomes visible later.

## Scenarios

| Engine | Scenario | Load |
|---|---|---|
| Postgres | `01-watermark-first` | Watermark committed, then rows written; crash between |
| Postgres | `02-write-first-append` | Rows appended, then watermark committed; crash between |
| Postgres | `03-write-first-keyed` | Rows written keyed on `txn_id`, then watermark committed; crash between |
| Postgres | `04-one-transaction` | Keyed write and watermark in one transaction; crash between |
| Postgres | `05-in-flight-watermark` | As 04, no crash; `T-09` commits after run 2 reads |
| Postgres | `06-in-flight-lookback` | As 05, each run re-reads 10 minutes before the watermark |
| Postgres | `07-in-flight-interval` | `updated_at >= start and < end`, delete-then-insert per interval; run 2 rerun twice |
| Postgres | `08-interval-inclusive-end` | `updated_at >= start and <= end` |
| Delta | `01-watermark-first` | Watermark committed in Postgres, then `replaceWhere` write to Delta; crash between |
| Delta | `02-write-first` | `replaceWhere` write to Delta, then watermark committed in Postgres; crash between |
| Delta | `03-target-watermark` | No watermark table: read from `max(updated_at)` in the target minus 10 minutes; `T-09` in flight on run 2, crash before run 3's write |

"Matches the source" means every source column (`txn_id`, `account_id`,
`amount`, `updated_at`) matches. `interval_start` is the run's logical
time, so a rerun reproduces it. `loaded_at` is wall-clock time.
