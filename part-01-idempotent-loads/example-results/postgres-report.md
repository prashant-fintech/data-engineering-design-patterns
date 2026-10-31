# Part 1 results: postgres

## 01-append

Plain INSERT of each batch; batch 2026-10-02 is then run again.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02 | completed | 18 | 10-01: 6, 10-02: 8, 10-03: 4 | T-0105, T-0201, T-0202, T-0203, T-0204 | **no** |

## 02-delete-by-business-date

One transaction: DELETE WHERE business_date = run date, then INSERT the batch. Batch 2026-10-02 carries a late row for 2026-10-01.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02 | completed | 14 | 10-01: 6, 10-02: 4, 10-03: 4 | T-0105 | **no** |
| rerun batch 2026-10-01 | completed | 12 | 10-01: 4, 10-02: 4, 10-03: 4 | none | **no** |

## 03-delete-by-batch-date

One transaction: DELETE WHERE batch_date = run date, then INSERT the batch.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-01 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |

## 04-crash-two-commits

DELETE and INSERT committed separately; the rerun of 2026-10-02 dies between them.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02, crash after delete | crashed: worker killed after deleting batch_date = 2026-10-02 | 8 | 10-01: 4, 10-03: 4 | none | **no** |
| retry batch 2026-10-02 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |

## 05-crash-one-transaction

DELETE and INSERT in one transaction; the rerun of 2026-10-02 dies between them.

| Step | Outcome | Rows | Rows per business date | Duplicated txn_id | Same as first step |
|---|---|---|---|---|---|
| load three batches | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| rerun batch 2026-10-02, crash after delete | crashed: worker killed after deleting batch_date = 2026-10-02 | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
| retry batch 2026-10-02 | completed | 13 | 10-01: 5, 10-02: 4, 10-03: 4 | none | yes |
