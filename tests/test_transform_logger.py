import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import transform_logger
from transform_validation import CheckResult, ValidationReport


class TransformationLoggerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.log_path = Path(self.temp.name) / "logs" / "transformation.jsonl"
        self.addCleanup(self.temp.cleanup)

    def read_records(self):
        return [json.loads(line) for line in self.log_path.read_text().splitlines()]

    def test_utc_iso_normalizes_positive_offset_string(self):
        self.assertEqual(
            transform_logger._utc_iso("2026-11-07T08:00:00+08:00"),
            "2026-11-07T00:00:00Z",
        )

    def test_utc_iso_normalizes_negative_offset_string(self):
        self.assertEqual(
            transform_logger._utc_iso("2026-11-06T19:00:00-05:00"),
            "2026-11-07T00:00:00Z",
        )

    def test_utc_iso_treats_naive_iso_string_as_utc(self):
        self.assertEqual(
            transform_logger._utc_iso("2026-11-07T00:00:00"),
            "2026-11-07T00:00:00Z",
        )

    def test_utc_iso_rejects_invalid_string(self):
        with self.assertRaisesRegex(ValueError, "Invalid ISO 8601 timestamp"):
            transform_logger._utc_iso("not-a-timestamp")

    def test_utc_iso_preserves_datetime_timestamp_behavior(self):
        aware = datetime(2026, 11, 7, 8, 0, tzinfo=timezone.utc)
        naive = datetime(2026, 11, 7, 0, 0)
        self.assertEqual(transform_logger._utc_iso(aware), "2026-11-07T08:00:00Z")
        self.assertEqual(transform_logger._utc_iso(naive), "2026-11-07T00:00:00Z")

    def test_successful_record_contains_required_metadata(self):
        transform_logger.log_transformation(
            "property_listings", run_id="run-1", batch_id="batch-1",
            input_file="raw.parquet", output_file="processed.parquet",
            start_time=datetime(2026, 11, 7, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 11, 7, 0, 1, tzinfo=timezone.utc),
            records_read=2, records_output=2, log_path=self.log_path,
        )
        record = self.read_records()[0]
        required = {
            "run_id", "parent_run_id", "batch_id", "dataset_name", "input_file", "output_file",
            "start_time", "end_time", "duration_seconds", "status", "records_read", "records_output",
            "records_rejected", "records_modified", "rules_applied", "validation_status",
            "validation_checks", "error_code", "error_message", "code_version", "rule_version",
        }
        self.assertTrue(required.issubset(record))
        self.assertEqual(record["duration_seconds"], 60.0)
        self.assertTrue(record["start_time"].endswith("Z"))
        self.assertEqual(record["rules_applied"], [])

    def test_failed_run_and_optional_parent_are_logged(self):
        transform_logger.log_transformation(
            "buyer_listings", run_id="run-2", parent_run_id="extract-1", batch_id="batch-2",
            status="FAILED", error_code="READ_ERROR", error_message="unreadable",
            log_path=self.log_path,
        )
        record = self.read_records()[0]
        self.assertEqual(record["status"], "FAILED")
        self.assertEqual(record["parent_run_id"], "extract-1")
        self.assertEqual(record["error_code"], "READ_ERROR")

    def test_shared_run_and_batch_and_multiple_jsonl_records(self):
        logger = transform_logger.TransformationLogger(self.log_path)
        for dataset in ("property_listings", "buyer_listings", "transaction_listings"):
            logger.log_dataset(dataset, run_id="shared-run", batch_id="shared-batch")
        records = self.read_records()
        self.assertEqual(len(records), 3)
        self.assertEqual({r["run_id"] for r in records}, {"shared-run"})
        self.assertEqual({r["batch_id"] for r in records}, {"shared-batch"})

    def test_validation_report_and_checks_are_serialized(self):
        check = CheckResult("V-1", "example", "properties", "_id", "test", True, 0, "Critical")
        report = ValidationReport("properties", "batch", 1, 1, 0, (check,))
        transform_logger.log_transformation(
            "property_listings", batch_id="batch", validation_report=report, log_path=self.log_path
        )
        record = self.read_records()[0]
        self.assertEqual(record["validation_status"], "PASSED")
        self.assertEqual(record["validation_checks"][0]["rule_id"], "V-1")

    def test_configurable_directory_and_versions(self):
        with patch.dict("os.environ", {
            "TRANSFORMATION_CODE_VERSION": "code-1",
            "TRANSFORMATION_RULE_VERSION": "rules-1",
        }):
            transform_logger.log_transformation("transactions", log_path=self.log_path)
        record = self.read_records()[0]
        self.assertEqual(record["code_version"], "code-1")
        self.assertEqual(record["rule_version"], "rules-1")

    def test_environment_log_directory_is_supported(self):
        configured_dir = Path(self.temp.name) / "configured-logs"
        with patch.dict("os.environ", {"TRANSFORMATION_LOG_DIR": str(configured_dir)}, clear=False):
            transform_logger.log_transformation("transactions")
        self.assertTrue((configured_dir / "transformation_log.jsonl").exists())

    def test_logger_write_failure_is_explicit(self):
        with patch.object(Path, "open", side_effect=OSError("disk full")):
            with self.assertRaises(transform_logger.TransformationLoggingError):
                transform_logger.log_transformation("transactions", log_path=self.log_path)


if __name__ == "__main__":
    unittest.main()
