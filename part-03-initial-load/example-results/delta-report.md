# Part 3 results: delta

## 01-one-run-per-day

The daily pipeline is run once for each closed day of September, then daily as scheduled. One day is rerun.

| Step | Pipeline runs so far | Commits | Data files | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 29 daily runs at 18:00 on 30 Sep | 29 | 29 | 29 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 30 | 30 | 30 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 33 | 33 | 33 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 34 | 34 | 33 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |

## 02-wide-run-key

The same load is called once with the interval 1 to 29 September and stamps every row with the run's interval_start. Then two September days are rerun as daily runs.

| Step | Pipeline runs so far | Commits | Data files | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 wide run at 18:00 on 30 Sep | 1 | 1 | 1 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 2 | 2 | 2 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 5 | 5 | 5 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 6 | 6 | 6 | 83 | 82 | none | T-0912-1, T-0912-2 | none | none | 6071.50 | 5907.50 | **no** |
| rerun 01 Sep | 7 | 7 | 6 | 83 | 17 | 63 rows, first T-0902-1, last T-0929-2 | none | none | none | 1168.50 | 5907.50 | **no** |

## 03-wide-day-key

As 02, but the load stamps each row with the daily interval it falls in and replaces every daily interval inside the run's interval. After the fix, one wide run reloads everything.

| Step | Pipeline runs so far | Commits | Data files | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 wide run at 18:00 on 30 Sep | 1 | 1 | 1 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 2 | 2 | 2 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 5 | 5 | 5 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 6 | 6 | 6 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 01 Sep | 7 | 7 | 7 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| fix deployed, nothing rerun | 7 | 7 | 7 | 83 | 80 | none | none | none | T-0907-4, T-0914-4, T-0921-4, T-0928-4, T-1001-4 | 5907.50 | 5307.50 | **no** |
| 1 wide rerun with the fix | 8 | 8 | 1 | 83 | 80 | none | none | none | none | 5307.50 | 5307.50 | yes |
