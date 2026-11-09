"""Checks that pin down what part 3 claims, on both engines.

Run inside the lab container with Postgres up:
    python -m unittest -v test_initial_load

Each scenario runs once per test class. The Delta class is slow, because
one scenario makes 34 separate Spark writes.
"""
import unittest

import delta_loads
import postgres_loads
from common import connect

SEPTEMBER_REFUNDS = ["T-0905-3", "T-0910-3", "T-0915-3", "T-0920-3", "T-0925-3", "T-0930-3"]
SEPTEMBER_TEST_ROWS = ["T-0909-5", "T-0923-5"]
SEPTEMBER_CHARGEBACKS = ["T-0907-4", "T-0914-4", "T-0921-4", "T-0928-4"]
EVENING_ROWS = ["T-0930-6", "T-0930-7"]
DAYTIME_30_SEP = ["T-0930-1", "T-0930-2", "T-0930-3"]
RERUN_DAY_ROWS = ["T-0912-1", "T-0912-2"]


def step(result: dict, label: str) -> dict:
    return next(s for s in result["steps"] if s["step"] == label)


def last(result: dict) -> dict:
    return result["steps"][-1]


class PostgresScriptLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_plain_copy_skips_both_rules_for_september_only(self):
        result = postgres_loads.scenario_script_plain_copy(self.conn)
        self.assertEqual(last(result)["wrong_amount_txn_ids"], SEPTEMBER_REFUNDS)
        self.assertEqual(last(result)["unexpected_txn_ids"], SEPTEMBER_TEST_ROWS)

    def test_script_run_before_midnight_loses_the_evening(self):
        result = postgres_loads.scenario_script_cutover(self.conn)
        self.assertTrue(step(result, "history script at 18:00 on 30 Sep")["matches_expected"])
        self.assertEqual(step(result, "daily runs, 01 Oct to 03 Oct")["missing_txn_ids"], EVENING_ROWS)

    def test_running_the_last_day_through_the_pipeline_duplicates_the_daytime(self):
        result = postgres_loads.scenario_script_cutover(self.conn)
        self.assertEqual(last(result)["missing_txn_ids"], [])
        self.assertEqual(last(result)["duplicated_txn_ids"], DAYTIME_30_SEP)

    def test_rerunning_a_script_loaded_day_duplicates_it(self):
        result = postgres_loads.scenario_script_ownership(self.conn)
        self.assertTrue(step(result, "daily runs, 01 Oct to 03 Oct")["matches_expected"])
        self.assertEqual(last(result)["duplicated_txn_ids"], RERUN_DAY_ROWS)

    def test_a_later_fix_reaches_october_and_not_september(self):
        result = postgres_loads.scenario_script_later_fix(self.conn)
        self.assertTrue(step(result, "daily runs, 01 Oct to 03 Oct")["matches_expected"])
        self.assertEqual(last(result)["wrong_amount_txn_ids"], SEPTEMBER_CHARGEBACKS)


class PostgresPipelineLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect()
        cls.per_day = postgres_loads.scenario_pipeline_one_run_per_day(cls.conn)
        cls.wide_run_key = postgres_loads.scenario_pipeline_wide_run_key(cls.conn)
        cls.wide_day_key = postgres_loads.scenario_pipeline_wide_day_key(cls.conn)
        cls.open_interval = postgres_loads.scenario_pipeline_open_interval(cls.conn)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_one_run_per_day_matches_after_the_load_the_rerun_and_the_fix(self):
        self.assertTrue(step(self.per_day, "daily runs, 01 Oct to 03 Oct")["matches_expected"])
        self.assertTrue(step(self.per_day, "rerun 12 Sep")["matches_expected"])
        self.assertTrue(last(self.per_day)["matches_expected"])
        self.assertEqual(last(self.per_day)["pipeline_runs"], 67)

    def test_closed_intervals_leave_the_open_day_for_its_own_run(self):
        history = step(self.per_day, "29 daily runs at 18:00 on 30 Sep")
        self.assertEqual(history["missing_txn_ids"], DAYTIME_30_SEP)
        self.assertTrue(step(self.per_day, "daily run, 30 Sep")["matches_expected"])

    def test_wide_run_keyed_on_the_run_duplicates_a_rerun_day(self):
        self.assertTrue(step(self.wide_run_key, "daily runs, 01 Oct to 03 Oct")["matches_expected"])
        self.assertEqual(step(self.wide_run_key, "rerun 12 Sep")["duplicated_txn_ids"], RERUN_DAY_ROWS)

    def test_wide_run_keyed_on_the_run_loses_the_history_when_its_first_day_is_rerun(self):
        self.assertEqual(len(last(self.wide_run_key)["missing_txn_ids"]), 63)
        self.assertEqual(last(self.wide_run_key)["target_rows"], 17)

    def test_wide_run_keyed_on_the_day_survives_reruns_and_the_fix(self):
        self.assertTrue(step(self.wide_day_key, "rerun 12 Sep")["matches_expected"])
        self.assertTrue(step(self.wide_day_key, "rerun 01 Sep")["matches_expected"])
        self.assertTrue(last(self.wide_day_key)["matches_expected"])
        self.assertEqual(last(self.wide_day_key)["pipeline_runs"], 8)

    def test_wide_and_daily_runs_write_the_same_table(self):
        self.assertEqual(self.wide_day_key["final_rows"], self.per_day["final_rows"])

    def test_open_interval_misses_the_evening_until_its_day_is_rerun(self):
        self.assertEqual(step(self.open_interval, "daily runs, 01 Oct to 03 Oct")["missing_txn_ids"], EVENING_ROWS)
        self.assertTrue(last(self.open_interval)["matches_expected"])


class DeltaPipelineLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = delta_loads.build_spark()
        cls.conn = connect()
        cls.per_day = delta_loads.scenario_one_run_per_day(cls.spark, cls.conn)
        cls.wide_run_key = delta_loads.scenario_wide_run_key(cls.spark, cls.conn)
        cls.wide_day_key = delta_loads.scenario_wide_day_key(cls.spark, cls.conn)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.spark.stop()

    def test_one_run_per_day_makes_one_commit_and_one_file_per_run(self):
        loaded = step(self.per_day, "daily runs, 01 Oct to 03 Oct")
        self.assertTrue(loaded["matches_expected"])
        self.assertEqual(loaded["costs"], {"Commits": 33, "Data files": 33})

    def test_wide_run_loads_the_same_days_in_five_commits(self):
        loaded = step(self.wide_day_key, "daily runs, 01 Oct to 03 Oct")
        self.assertTrue(loaded["matches_expected"])
        self.assertEqual(loaded["costs"], {"Commits": 5, "Data files": 5})

    def test_wide_run_keyed_on_the_run_breaks_as_it_does_on_postgres(self):
        self.assertEqual(step(self.wide_run_key, "rerun 12 Sep")["duplicated_txn_ids"], RERUN_DAY_ROWS)
        self.assertEqual(len(last(self.wide_run_key)["missing_txn_ids"]), 63)

    def test_wide_run_keyed_on_the_day_survives_reruns_and_the_fix(self):
        self.assertTrue(step(self.wide_day_key, "rerun 12 Sep")["matches_expected"])
        self.assertTrue(step(self.wide_day_key, "rerun 01 Sep")["matches_expected"])
        self.assertTrue(last(self.wide_day_key)["matches_expected"])

    def test_wide_and_daily_runs_write_the_same_table(self):
        before_fix = step(self.wide_day_key, "rerun 01 Sep")
        self.assertEqual(before_fix["target_rows"], last(self.per_day)["target_rows"])
        self.assertEqual(before_fix["target_total"], last(self.per_day)["target_total"])


if __name__ == "__main__":
    unittest.main()
