# Part 1 results: delta

## 01-append

mode('append') for each batch; batch 2026-10-02 is then run again.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02 | completed | 18 | 10-01: 6, 10-02: 8, 10-03: 4 | T-0105, T-0201, T-0202, T-0203, T-0204 | **no** |

## 02-crash-two-commits

DeltaTable.delete() then mode('append'), two commits; the rerun of 2026-10-02 dies between them.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02, crash after delete | crashed: worker killed after deleting batch_date = 2026-10-02 | 8 | 10-01: 4, 10-03: 4 | none | **no** |
| retry batch 2026-10-02 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |

## 03-replace-where-business-date

replaceWhere business_date = run date. Batch 2026-10-02 carries a late row for 2026-10-01.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load batch 2026-10-01 | completed | 4 | 10-01: 4 | none | yes |
| load batch 2026-10-02 | failed: [DELTA_REPLACE_WHERE_MISMATCH] Written data does not conform to partial table overwrite condition or constraint 'business_date = DATE'2026-10-02''. | 4 | 10-01: 4 | none | yes |
| load batch 2026-10-03 | completed | 8 | 10-01: 4, 10-03: 4 | none | **no** |

## 04-replace-where-batch-date

replaceWhere batch_date = run date, one commit per load.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-01 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
