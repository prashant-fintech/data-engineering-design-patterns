# Part 2 results: postgres

## 01-watermark-first

The watermark is committed, then the rows are written in a second commit. Run 2 dies between the two.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after moving the watermark, before the write | 9 | 4 | T-05, T-06, T-07, T-08, T-09 | none | 11:00 | **no** |
| retry run 2 (10:00-11:00) | completed | 9 | 4 | T-05, T-06, T-07, T-08, T-09 | none | 11:00 | **no** |
| run 3 (11:00-12:00) | completed | 11 | 6 | T-05, T-06, T-07, T-08, T-09 | none | 11:40 | **no** |

## 02-write-first-append

The rows are appended, then the watermark is committed in a second commit. Run 2 dies between the two.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after the write, before moving the watermark | 9 | 9 | none | none | 09:55 | yes |
| retry run 2 (10:00-11:00) | completed | 9 | 14 | none | T-05, T-06, T-07, T-08, T-09 | 11:00 | **no** |
| run 3 (11:00-12:00) | completed | 11 | 16 | none | T-05, T-06, T-07, T-08, T-09 | 11:40 | **no** |

## 03-write-first-keyed

The rows are written keyed on txn_id, then the watermark is committed in a second commit. Run 2 dies between the two.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after the write, before moving the watermark | 9 | 9 | none | none | 09:55 | yes |
| retry run 2 (10:00-11:00) | completed | 9 | 9 | none | none | 11:00 | yes |
| run 3 (11:00-12:00) | completed | 11 | 11 | none | none | 11:40 | yes |

## 04-one-transaction

The keyed write and the watermark update share one transaction. Run 2 dies between them.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after the write, before moving the watermark | 9 | 4 | T-05, T-06, T-07, T-08, T-09 | none | 09:55 | **no** |
| retry run 2 (10:00-11:00) | completed | 9 | 9 | none | none | 11:00 | yes |
| run 3 (11:00-12:00) | completed | 11 | 11 | none | none | 11:40 | yes |

## 05-in-flight-watermark

One transaction per load, watermark = newest updated_at read. T-09 (10:58) commits after run 2 has read the source.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), T-09 in flight | completed | 9 | 8 | T-09 | none | 11:00 | **no** |
| run 3 (11:00-12:00) | completed | 11 | 10 | T-09 | none | 11:40 | **no** |

## 06-in-flight-lookback

As 05, but every run also re-reads the 10 minutes before the watermark, keyed on txn_id.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), T-09 in flight | completed | 9 | 8 | T-09 | none | 11:00 | **no** |
| run 3 (11:00-12:00) | completed | 11 | 11 | none | none | 11:40 | yes |

## 07-in-flight-interval

Each run reads updated_at >= interval start and < interval end, and replaces that interval's rows. T-09 commits after run 2. Run 2's interval is then rerun twice.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | not used | yes |
| run 2 (10:00-11:00), T-09 in flight | completed | 9 | 7 | T-08, T-09 | none | not used | **no** |
| run 3 (11:00-12:00) | completed | 11 | 10 | T-09 | none | not used | **no** |
| rerun run 2 (10:00-11:00) | completed | 11 | 11 | none | none | not used | yes |
| rerun run 2 (10:00-11:00) again | completed | 11 | 11 | none | none | not used | yes |

## 08-interval-inclusive-end

As 07 with no in-flight row, but each run reads updated_at >= start and <= end. T-08 is stamped exactly 11:00.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | not used | yes |
| run 2 (10:00-11:00) | completed | 9 | 9 | none | none | not used | yes |
| run 3 (11:00-12:00) | completed | 11 | 12 | none | T-08 | not used | **no** |
