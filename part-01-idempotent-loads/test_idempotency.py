"""Checks that pin down what part 1 claims, on both engines.

Run inside the lab container after make_landing_files.py:
    python -m unittest -v test_idempotency
"""
import unittest

import delta_loads
import postgres_loads


def step(result: dict, label: str) -> dict:
    return next(s for s in result["steps"] if s["step"] == label)


class PostgresLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = postgres_loads.connect()

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_append_doubles_the_rerun_batch(self):
        result = postgres_loads.scenario_append(self.conn)
        rerun = step(result, "rerun batch 2026-10-02")
        self.assertFalse(rerun["same_as_first_step"])
        self.assertEqual(rerun["rows"], 18)

    def test_business_date_key_duplicates_then_loses_the_late_row(self):
        result = postgres_loads.scenario_delete_by_business_date(self.conn)
        self.assertEqual(step(result, "rerun batch 2026-10-02")["duplicated_txn_ids"], ["T-0105"])
        after_day_one = step(result, "rerun batch 2026-10-01")
        self.assertEqual(after_day_one["per_business_date"]["2026-10-01"], 4)
        self.assertEqual(after_day_one["duplicated_txn_ids"], [])

    def test_batch_date_key_reruns_to_the_same_table(self):
        result = postgres_loads.scenario_delete_by_batch_date(self.conn)
        self.assertTrue(all(s["same_as_first_step"] for s in result["steps"]))

    def test_two_commits_leave_a_hole_after_a_crash(self):
        result = postgres_loads.scenario_crash_two_commits(self.conn)
        crashed = step(result, "rerun batch 2026-10-02, crash after delete")
        self.assertTrue(crashed["outcome"].startswith("crashed"))
        self.assertNotIn("2026-10-02", crashed["per_business_date"])
        self.assertTrue(step(result, "retry batch 2026-10-02")["same_as_first_step"])

    def test_one_transaction_rolls_back_a_crash(self):
        result = postgres_loads.scenario_crash_one_transaction(self.conn)
        self.assertTrue(all(s["same_as_first_step"] for s in result["steps"]))


class DeltaLoads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = delta_loads.build_spark()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_append_doubles_the_rerun_batch(self):
        result = delta_loads.scenario_append(self.spark)
        self.assertEqual(step(result, "rerun batch 2026-10-02")["rows"], 18)

    def test_two_commits_leave_a_hole_after_a_crash(self):
        result = delta_loads.scenario_crash_two_commits(self.spark)
        crashed = step(result, "rerun batch 2026-10-02, crash after delete")
        self.assertNotIn("2026-10-02", crashed["per_business_date"])
        self.assertTrue(step(result, "retry batch 2026-10-02")["same_as_first_step"])

    def test_replace_where_rejects_rows_outside_its_predicate(self):
        result = delta_loads.scenario_replace_where_business_date(self.spark)
        self.assertTrue(step(result, "load batch 2026-10-02")["outcome"].startswith("failed"))

    def test_replace_where_on_batch_date_reruns_to_the_same_table(self):
        result = delta_loads.scenario_replace_where_batch_date(self.spark)
        self.assertTrue(all(s["same_as_first_step"] for s in result["steps"]))


if __name__ == "__main__":
    unittest.main()
