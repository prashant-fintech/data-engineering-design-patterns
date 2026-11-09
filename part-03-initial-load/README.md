# Part 3: Initial Load versus Daily Load

Companion code for [Data Engineering Design Patterns: Initial Load versus Daily Load](https://singhprashant.in/writing/data-engineering-design-patterns-03-initial-load-daily-load).

A Postgres source table holds a month of history (September) before a
daily pipeline is scheduled from 1 October. The scenarios load that
history two ways: with a one-off SQL script, and through the daily
pipeline's own load function, called once per day or once with a
month-wide interval. The target is Postgres in one script and Delta Lake
in the other, and it is written out after each step next to a comparison
with what the pipeline's transform should have produced.

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
docker compose run --rm lab python -m unittest -v test_initial_load
```

The Delta script and the Delta tests are slow, because one scenario
makes 34 separate Spark writes, which is part of what it measures. On
Docker Desktop for Windows, with the table on a bind mount, each of the
two took about 20 minutes.

Then stop Postgres:

```bash
docker compose down
```

## What each script writes

| Path | Contents |
|---|---|
| `out/postgres/<scenario>/NN-<step>.csv` | The whole Postgres target after each step of a scenario. |
| `out/delta/<scenario>/NN-<step>.csv` | The same for the Delta target. |
| `out/delta/<scenario>/history.csv` | The Delta commit log for that scenario: one row per commit, with its `replaceWhere` predicate and the files it added and removed. |
| `out/<engine>/report.md` | One line per step: pipeline runs so far, row counts, `txn_id`s missing, duplicated, not supposed to be there or carrying the wrong amount, and the target's total against the expected total. The Delta report adds commits and data files. |

`example-results/` holds the two reports and the Delta commit histories
from the run used in the post.

## The source rows

The source stores every amount as a positive number and says what it is
in `txn_type`. Rows are in `common.py`.

| Days | Rows per day | Extra rows |
|---|---|---|
| 1 to 30 September | Two purchases, `-1` at 09:15 and `-2` at 14:40 | A refund `-3` on the 5th, 10th, 15th, 20th, 25th and 30th. A chargeback `-4` on the 7th, 14th, 21st and 28th. A test-account row `-5` on the 9th and 23rd. |
| 30 September, evening | | `T-0930-6` at 19:45 and `T-0930-7` at 21:30, committed after the history load at 18:00 |
| 1 to 3 October | Two purchases | A chargeback on the 1st, a refund and a test-account row on the 2nd |

That is 83 source rows, 74 of them in September.

## The pipeline's transform

| Step | Rule |
|---|---|
| `drop_test_accounts` | Rows whose `account_id` starts with `TEST-` are not loaded. |
| `sign_amounts`, version 1 | Refunds become negative. Chargebacks stay positive, which is a bug. |
| `sign_amounts`, version 2 | Refunds and chargebacks become negative. This is the fix that lands after 3 October. |

"Matches expected" means the target holds exactly the transform of every
source row committed so far, with nothing missing, duplicated, extra or
carrying a different amount.

## Scenarios

| Engine | Scenario | History load |
|---|---|---|
| Postgres | `01-script-plain-copy` | One-off script that copies the source as it is |
| Postgres | `02-script-cutover` | Script with both rules, run at 18:00 on 30 September; 30 September later run through the pipeline |
| Postgres | `03-script-ownership` | Script with both rules, run after midnight; 12 September rerun through the pipeline |
| Postgres | `04-script-later-fix` | Script with both rules as they stood; the fix reruns October only |
| Postgres | `05-pipeline-one-run-per-day` | 29 daily runs of the pipeline; 12 September rerun; 33 reruns with the fix |
| Postgres | `06-pipeline-wide-run-key` | One run with the interval 1 to 29 September, rows stamped with the run's `interval_start`; 12 September and 1 September rerun |
| Postgres | `07-pipeline-wide-day-key` | One wide run, rows stamped with their own day; the same reruns; one wide rerun with the fix |
| Postgres | `08-pipeline-open-interval` | One wide run that includes 30 September while that day is still open; 30 September rerun |
| Delta | `01-one-run-per-day` | As Postgres 05, without the reruns for the fix |
| Delta | `02-wide-run-key` | As Postgres 06 |
| Delta | `03-wide-day-key` | As Postgres 07 |

Scenarios 01, 03 and 04 run the script just after midnight on 1 October,
when all of September has been committed. Scenarios 05 to 07 run the
history at 18:00 on 30 September for the intervals that have ended
(1 to 29 September), and 30 September runs after midnight like any other
day.
