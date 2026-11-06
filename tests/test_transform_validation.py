import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import transform_validation as validation


class TransformValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.quarantine = self.root / "quarantine"
        self.batch = "batch-20261106"
        self.addCleanup(self.temp.cleanup)

    def test_valid_records_pass_and_reconcile(self):
        table = pa.table({"_id": ["a", "b"], "value": [1, 2]})
        report, rejected = validation.validate_dataset(
            table, "property_listings", self.batch, required_fields=("value",)
        )
        self.assertEqual(report.accepted_count, 2)
        self.assertEqual(report.rejected_count, 0)
        self.assertFalse(rejected)
        self.assertTrue(all(check.passed for check in report.checks))

    def test_reconciliation_check_fails_for_mismatched_counts(self):
        check = validation.reconcile_counts(3, 1, 1, "properties")
        self.assertFalse(check.passed)
        self.assertEqual(check.affected_count, 1)

    def test_duplicate_primary_keys_are_critical_and_quarantined(self):
        table = pa.table({"_id": ["a", "a"], "value": [1, 2]})
        report, rejected = validation.validate_dataset(table, "properties", self.batch)
        self.assertEqual(report.rejected_count, 1)
        self.assertEqual(rejected[0]["rule_id"], validation.RULE_PRIMARY_KEY)
        self.assertTrue(report.critical_failures)
        path = validation.write_quarantine(rejected, "properties", self.batch, self.quarantine)
        saved = pq.read_table(path).to_pylist()[0]
        self.assertEqual(saved["_id"], "a")
        self.assertEqual(saved["batch_id"], self.batch)

    def test_missing_required_values_are_detected(self):
        table = pa.table({"_id": ["a", "b"], "status": ["ok", None]})
        report, rejected = validation.validate_dataset(
            table, "properties", self.batch, required_fields=("status",)
        )
        self.assertEqual(report.accepted_count, 1)
        self.assertEqual(rejected[0]["rule_id"], validation.RULE_REQUIRED_FIELD)
        self.assertEqual(rejected[0]["status"], None)

    def test_referential_integrity_failure_is_critical(self):
        child = pa.table({"_id": ["t1", "t2"], "propertyId": ["p1", "missing"]})
        parent = pa.table({"_id": ["p1"]})
        check, rejected = validation.validate_referential_integrity(
            child, "transactions", "propertyId", parent, "properties", batch_id=self.batch
        )
        self.assertFalse(check.passed)
        self.assertEqual(check.severity, "Critical")
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["rule_id"], validation.RULE_REFERENTIAL_INTEGRITY)

    def test_null_child_and_null_parent_references_fail(self):
        child = pa.table({"_id": ["t1"], "ref": [None]})
        parent = pa.table({"_id": [None]})
        check, rejected = validation.validate_referential_integrity(
            child, "child", "ref", parent, "parent", batch_id=self.batch
        )
        self.assertFalse(check.passed)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["__validation_row_index"], 0)

    def test_noncritical_and_critical_results_are_distinguished(self):
        table = pa.table({"_id": ["a"], "extra": [1]})
        expected = pa.schema([pa.field("_id", pa.string()), pa.field("extra", pa.int64())])
        report, _ = validation.validate_dataset(
            table, "properties", self.batch, expected_schema=expected, expected_columns=("_id",)
        )
        self.assertIn("Non-critical", {check.severity for check in report.checks})
        self.assertIn("Critical", {check.severity for check in report.checks})
        report, _ = validation.validate_dataset(
            pa.table({"_id": ["a"], "extra": [1]}), "properties", self.batch,
            expected_schema=pa.schema([pa.field("_id", pa.int64()), pa.field("extra", pa.int64())]),
        )
        self.assertEqual(report.critical_failures[0].severity, "Critical")

    def test_expected_columns_report_missing_and_unexpected_columns(self):
        table = pa.table({"_id": ["a"], "extra": [1]})
        report, _ = validation.validate_dataset(
            table, "properties", self.batch, expected_columns=("_id", "status")
        )
        checks = {check.check_name: check for check in report.checks}
        self.assertFalse(checks["missing_columns"].passed)
        self.assertEqual(checks["missing_columns"].target, "status")
        self.assertFalse(checks["unexpected_columns"].passed)
        self.assertEqual(checks["unexpected_columns"].target, "extra")
        self.assertEqual(checks["missing_columns"].severity, "Non-critical")

    def test_full_processed_validation_writes_quarantine_and_counts(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        transactions = pa.table({"_id": ["t1", "t2"], "propertyId": ["p1", "missing"], "buyerId": ["u1", "u1"]})
        pq.write_table(transactions, processed / "transaction_listings.parquet")
        raw_before = transactions
        reports = validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        self.assertEqual(reports["transaction_listings"].input_count, 2)
        self.assertEqual(reports["transaction_listings"].rejected_count, 1)
        self.assertEqual(
            reports["transaction_listings"].quarantined_path,
            self.quarantine / f"{self.batch}_transaction_listings.parquet",
        )
        saved = pq.read_table(self.quarantine / f"{self.batch}_transaction_listings.parquet")
        self.assertEqual(saved.to_pylist()[0]["_id"], "t2")
        self.assertEqual(saved.to_pylist()[0]["batch_id"], self.batch)
        self.assertTrue(raw_before.equals(pq.read_table(processed / "transaction_listings.parquet")))

    def test_multiple_failures_are_one_rejected_record_with_all_details(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        transactions = pa.table({"_id": ["t1"], "propertyId": ["missing"], "buyerId": ["bad"], "status": [None]})
        pq.write_table(transactions, processed / "transaction_listings.parquet")
        reports = validation.validate_processed_datasets(
            processed, self.quarantine, self.batch,
            required_fields={"transaction_listings": ("status",)},
        )
        report = reports["transaction_listings"]
        self.assertEqual(report.input_count, 1)
        self.assertEqual(report.accepted_count, 0)
        self.assertEqual(report.rejected_count, 1)
        saved = pq.read_table(self.quarantine / f"{self.batch}_transaction_listings.parquet").to_pylist()
        self.assertEqual(len(saved), 1)
        self.assertIn(validation.RULE_REQUIRED_FIELD, saved[0]["rule_id"])
        self.assertIn(validation.RULE_REFERENTIAL_INTEGRITY, saved[0]["rule_id"])

    def test_missing_batch_id_is_rejected(self):
        with self.assertRaises(ValueError):
            validation.validate_dataset(pa.table({"_id": ["a"]}), "properties", None)
        with self.assertRaises(ValueError):
            validation.validate_processed_datasets(self.root, self.quarantine, None)

    def test_buyer_relationship_is_configurable_and_verified(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        pq.write_table(pa.table({"_id": ["t1"], "propertyId": ["p1"], "buyerId": ["not-u1"]}), processed / "transaction_listings.parquet")
        reports = validation.validate_processed_datasets(processed, self.quarantine, self.batch, buyer_relationship=None)
        skipped = [c for c in reports["transaction_listings"].checks
                   if c.check_name == "referential_integrity_skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertFalse(skipped[0].passed)
        self.assertEqual(skipped[0].severity, "Non-critical")
        self.assertIn("SKIPPED", skipped[0].details)
        reports = validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        self.assertTrue(any(c.check_name == "referential_integrity" and not c.passed
                            for c in reports["transaction_listings"].checks))

    def test_zero_rejection_rerun_removes_stale_quarantine_after_success(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        bad = pa.table({"_id": ["t1"], "propertyId": ["missing"], "buyerId": ["u1"]})
        pq.write_table(bad, processed / "transaction_listings.parquet")
        validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        stale_path = self.quarantine / f"{self.batch}_transaction_listings.parquet"
        self.assertTrue(stale_path.exists())
        processed_before = {p.name: p.read_bytes() for p in processed.glob("*.parquet")}

        good = pa.table({"_id": ["t1"], "propertyId": ["p1"], "buyerId": ["u1"]})
        pq.write_table(good, processed / "transaction_listings.parquet")
        validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        self.assertFalse(stale_path.exists())
        self.assertIsNone(
            validation.validate_processed_datasets(processed, self.quarantine, self.batch)
            ["transaction_listings"].quarantined_path
        )
        self.assertEqual(processed_before["property_listings.parquet"],
                         (processed / "property_listings.parquet").read_bytes())
        self.assertEqual(processed_before["buyer_listings.parquet"],
                         (processed / "buyer_listings.parquet").read_bytes())
        self.assertTrue((processed / "transaction_listings.parquet").exists())

    def test_first_zero_rejection_run_without_quarantine_directory_succeeds(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        pq.write_table(
            pa.table({"_id": ["t1"], "propertyId": ["p1"], "buyerId": ["u1"]}),
            processed / "transaction_listings.parquet",
        )
        quarantine = self.root / "not-created-yet"
        reports = validation.validate_processed_datasets(processed, quarantine, self.batch)
        self.assertFalse(quarantine.exists())
        self.assertTrue(all(report.quarantined_path is None for report in reports.values()))
        self.assertTrue((processed / "property_listings.parquet").exists())
        self.assertTrue((processed / "buyer_listings.parquet").exists())
        self.assertTrue((processed / "transaction_listings.parquet").exists())

    def test_quarantine_write_failure_does_not_change_processed_output(self):
        processed = self.root / "processed"
        processed.mkdir()
        for name, table in {
            "property_listings": pa.table({"_id": ["p1"]}),
            "buyer_listings": pa.table({"_id": ["b1"], "userId": ["u1"]}),
            "transaction_listings": pa.table({"_id": ["t1"], "propertyId": ["missing"], "buyerId": ["u1"]}),
        }.items():
            pq.write_table(table, processed / f"{name}.parquet")
        before = {p.name: p.read_bytes() for p in processed.glob("*.parquet")}
        from unittest.mock import patch
        with patch.object(validation, "_prepare_quarantine", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        self.assertEqual(before, {p.name: p.read_bytes() for p in processed.glob("*.parquet")})

    def test_publication_failure_cleans_remaining_temporary_files(self):
        processed = self.root / "processed"
        processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1", "p1"]}), processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), processed / "buyer_listings.parquet")
        pq.write_table(pa.table({"_id": ["t1"], "propertyId": ["missing"], "buyerId": ["bad"]}), processed / "transaction_listings.parquet")
        from unittest.mock import patch
        real_replace = validation.os.replace
        calls = []

        def fail_on_second_replace(source, destination):
            calls.append((source, destination))
            if len(calls) == 2:
                raise OSError("publish failed")
            return real_replace(source, destination)

        with patch.object(validation.os, "replace", side_effect=fail_on_second_replace):
            with self.assertRaises(OSError):
                validation.validate_processed_datasets(processed, self.quarantine, self.batch)
        self.assertFalse(list(self.quarantine.glob("*.tmp")))

    def test_missing_processed_dataset_is_explicit(self):
        with self.assertRaises(FileNotFoundError):
            validation.validate_processed_datasets(self.root / "missing", self.quarantine, self.batch)


if __name__ == "__main__":
    unittest.main()
