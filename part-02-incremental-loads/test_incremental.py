"""Checks that pin down what part 2 claims, on both engines.

Run inside the lab container with Postgres up:
    python -m unittest -v test_incremental
"""
import unittest

import delta_loads
import postgres_loads
from common import connect

LOST_IN_RUN_2 = ["T-05", "T-06", "T-07", "T-08", "T-09"]


def step(result: dict, label: str) -> dict:
    return next(s for s in result["steps"] if s["step"] == label)


def last(result: dict) -> dict:
    return result["steps"][-1]


class PostgresLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_watermark_first_loses_the_crashed_run_for_good(self):
        result = postgres_loads.scenario_watermark_first(self.conn)
        self.assertEqual(step(result, "retry run 2 (10:00-11:00)")["missing_txn_ids"], LOST_IN_RUN_2)
        self.assertEqual(last(result)["missing_txn_ids"], LOST_IN_RUN_2)

    def test_write_first_append_duplicates_the_retried_run(self):
        result = postgres_loads.scenario_write_first_append(self.conn)
        self.assertEqual(last(result)["duplicated_txn_ids"], LOST_IN_RUN_2)

    def test_write_first_keyed_survives_the_crash(self):
        result = postgres_loads.scenario_write_first_keyed(self.conn)
        self.assertTrue(all(s["matches_source"] for s in result["steps"]))

    def test_one_transaction_rolls_back_and_retries_cleanly(self):
        result = postgres_loads.scenario_one_transaction(self.conn)
        self.assertEqual(step(result, "run 2 (10:00-11:00), crash")["watermark"], "09:55")
        self.assertTrue(last(result)["matches_source"])

    def test_newest_row_watermark_skips_a_late_commit(self):
        result = postgres_loads.scenario_in_flight_watermark(self.conn)
        self.assertEqual(last(result)["missing_txn_ids"], ["T-09"])

    def test_lookback_picks_up_the_late_commit(self):
        result = postgres_loads.scenario_in_flight_lookback(self.conn)
        self.assertTrue(last(result)["matches_source"])

    def test_interval_rerun_picks_up_the_late_commit_and_repeats_cleanly(self):
        result = postgres_loads.scenario_in_flight_interval(self.conn)
        self.assertEqual(step(result, "run 3 (11:00-12:00)")["missing_txn_ids"], ["T-09"])
        self.assertTrue(step(result, "rerun run 2 (10:00-11:00)")["matches_source"])
        self.assertTrue(step(result, "rerun run 2 (10:00-11:00) again")["matches_source"])

    def test_inclusive_interval_end_loads_the_boundary_row_twice(self):
        result = postgres_loads.scenario_interval_inclusive_end(self.conn)
        self.assertEqual(last(result)["duplicated_txn_ids"], ["T-08"])


class DeltaLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = delta_loads.build_spark()
        cls.conn = connect()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        cls.spark.stop()

    def test_watermark_first_loses_the_crashed_run_for_good(self):
        result = delta_loads.scenario_watermark_first(self.spark, self.conn)
        self.assertEqual(last(result)["missing_txn_ids"], LOST_IN_RUN_2)

    def test_write_first_keyed_survives_the_crash(self):
        result = delta_loads.scenario_write_first(self.spark, self.conn)
        self.assertTrue(all(s["matches_source"] for s in result["steps"]))

    def test_target_watermark_survives_a_crash_and_a_late_commit(self):
        result = delta_loads.scenario_target_watermark(self.spark, self.conn)
        self.assertEqual(step(result, "run 3 (11:00-12:00), crash")["watermark"], "11:00")
        self.assertTrue(last(result)["matches_source"])


if __name__ == "__main__":
    unittest.main()
