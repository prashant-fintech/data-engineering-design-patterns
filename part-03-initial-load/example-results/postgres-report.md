# Part 3 results: postgres

## 01-script-plain-copy

A one-off script copies September as the source stores it. The daily pipeline, which signs refunds and drops test accounts, loads October.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| history script, 01 Sep to 30 Sep | 0 | 74 | 74 | none | none | T-0909-5, T-0923-5 | T-0905-3, T-0910-3, T-0915-3, T-0920-3, T-0925-3, T-0930-3 | 5742.50 | 5440.50 | **no** |
| daily runs, 01 Oct to 03 Oct | 3 | 83 | 82 | none | none | T-0909-5, T-0923-5 | T-0905-3, T-0910-3, T-0915-3, T-0920-3, T-0925-3, T-0930-3 | 6209.50 | 5907.50 | **no** |

## 02-script-cutover

The script applies both rules correctly, and runs at 18:00 on 30 September. Two more rows are committed that evening. The daily pipeline starts with 1 October, and then 30 September is run through the pipeline to pick up the evening.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| history script at 18:00 on 30 Sep | 0 | 72 | 70 | none | none | none | none | 5220.00 | 5220.00 | yes |
| daily runs, 01 Oct to 03 Oct | 3 | 83 | 78 | T-0930-6, T-0930-7 | none | none | none | 5687.00 | 5907.50 | **no** |
| 30 Sep run through the pipeline | 4 | 83 | 83 | none | T-0930-1, T-0930-2, T-0930-3 | none | none | 6082.50 | 5907.50 | **no** |

## 03-script-ownership

The script applies both rules correctly and runs after midnight, so the first load is right. Then one September day is rerun through the pipeline.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| history script, 01 Sep to 30 Sep | 0 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 3 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 4 | 83 | 82 | none | T-0912-1, T-0912-2 | none | none | 6071.50 | 5907.50 | **no** |

## 04-script-later-fix

The script applies both rules correctly as they stood (version 1). After 3 October the sign rule is fixed in the pipeline so that chargebacks are negative too, and the October days are rerun.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| history script, 01 Sep to 30 Sep | 0 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 3 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| fix deployed, nothing rerun | 3 | 83 | 80 | none | none | none | T-0907-4, T-0914-4, T-0921-4, T-0928-4, T-1001-4 | 5907.50 | 5307.50 | **no** |
| rerun 01 Oct to 03 Oct with the fix | 6 | 83 | 80 | none | none | none | T-0907-4, T-0914-4, T-0921-4, T-0928-4 | 5787.50 | 5307.50 | **no** |

## 05-pipeline-one-run-per-day

No script. The daily pipeline is run once for each closed day of September, then daily as scheduled. One day is rerun, and after the fix every day is rerun.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| 29 daily runs at 18:00 on 30 Sep | 29 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 30 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 33 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 34 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| fix deployed, nothing rerun | 34 | 83 | 80 | none | none | none | T-0907-4, T-0914-4, T-0921-4, T-0928-4, T-1001-4 | 5907.50 | 5307.50 | **no** |
| 33 daily reruns with the fix | 67 | 83 | 80 | none | none | none | none | 5307.50 | 5307.50 | yes |

## 06-pipeline-wide-run-key

The same load function is called once with the interval 1 to 29 September. It stamps every row with the run's interval_start, as it does for a daily run. Then two September days are rerun as daily runs.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 wide run at 18:00 on 30 Sep | 1 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 2 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 5 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 6 | 83 | 82 | none | T-0912-1, T-0912-2 | none | none | 6071.50 | 5907.50 | **no** |
| rerun 01 Sep | 7 | 83 | 17 | 63 rows, first T-0902-1, last T-0929-2 | none | none | none | 1168.50 | 5907.50 | **no** |

## 07-pipeline-wide-day-key

As 06, but the load stamps each row with the daily interval it falls in and replaces every daily interval inside the run's interval. After the fix, one wide run reloads everything.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 wide run at 18:00 on 30 Sep | 1 | 72 | 67 | T-0930-1, T-0930-2, T-0930-3 | none | none | none | 5045.00 | 5220.00 | **no** |
| daily run, 30 Sep | 2 | 74 | 72 | none | none | none | none | 5440.50 | 5440.50 | yes |
| daily runs, 01 Oct to 03 Oct | 5 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 12 Sep | 6 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| rerun 01 Sep | 7 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
| fix deployed, nothing rerun | 7 | 83 | 80 | none | none | none | T-0907-4, T-0914-4, T-0921-4, T-0928-4, T-1001-4 | 5907.50 | 5307.50 | **no** |
| 1 wide rerun with the fix | 8 | 83 | 80 | none | none | none | none | 5307.50 | 5307.50 | yes |

## 08-pipeline-open-interval

As 07, but the wide run at 18:00 on 30 September is handed the whole month, including the day that has not ended. Later the 30 September interval is rerun.

| Step | Pipeline runs so far | Source rows | Target rows | Missing from target | Duplicated txn_id | Should not be there | Wrong amount | Target total | Expected total | Matches expected |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 wide run at 18:00 on 30 Sep, 30 Sep still open | 1 | 72 | 70 | none | none | none | none | 5220.00 | 5220.00 | yes |
| daily runs, 01 Oct to 03 Oct | 4 | 83 | 78 | T-0930-6, T-0930-7 | none | none | none | 5687.00 | 5907.50 | **no** |
| rerun 30 Sep | 5 | 83 | 80 | none | none | none | none | 5907.50 | 5907.50 | yes |
