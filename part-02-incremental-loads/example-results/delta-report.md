# Part 2 results: delta

## 01-watermark-first

Watermark committed in Postgres, then the rows written to Delta. Run 2 dies between the two.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after moving the watermark, before the write | 9 | 4 | T-05, T-06, T-07, T-08, T-09 | none | 11:00 | **no** |
| retry run 2 (10:00-11:00) | completed | 9 | 4 | T-05, T-06, T-07, T-08, T-09 | none | 11:00 | **no** |
| run 3 (11:00-12:00) | completed | 11 | 6 | T-05, T-06, T-07, T-08, T-09 | none | 11:40 | **no** |

## 02-write-first

Rows written to Delta keyed on txn_id, then the watermark committed in Postgres. Run 2 dies between the two.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), crash | crashed: worker killed after the write, before moving the watermark | 9 | 9 | none | none | 09:55 | yes |
| retry run 2 (10:00-11:00) | completed | 9 | 9 | none | none | 11:00 | yes |
| run 3 (11:00-12:00) | completed | 11 | 11 | none | none | 11:40 | yes |

## 03-target-watermark

No watermark table: each run reads from max(updated_at) in the target minus 10 minutes. T-09 commits after run 2 has read the source, and run 3 dies before its write.

| Step | Outcome | Source rows | Target rows | Missing from target | Duplicated txn_id | Watermark | Matches source |
|---|---|---|---|---|---|---|---|
| run 1 (09:00-10:00) | completed | 4 | 4 | none | none | 09:55 | yes |
| run 2 (10:00-11:00), T-09 in flight | completed | 9 | 8 | T-09 | none | 11:00 | **no** |
| run 3 (11:00-12:00), crash | crashed: worker killed before the write | 11 | 8 | T-09, T-10, T-11 | none | 11:00 | **no** |
| retry run 3 (11:00-12:00) | completed | 11 | 11 | none | none | 11:40 | yes |
