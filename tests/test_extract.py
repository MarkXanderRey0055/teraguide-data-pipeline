import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from pymongo.errors import ConnectionFailure, InvalidURI, OperationFailure

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import extract


class FakeCursor:
    def __init__(self, documents):
        self.documents = documents

    def sort(self, *_args):
        return self

    def __iter__(self):
        return iter(self.documents)


class FakeCollection:
    def __init__(self, documents=(), error=None):
        self.documents = documents
        self.error = error

    def find(self, _query):
        if self.error is not None:
            raise self.error
        return FakeCursor(self.documents)


class FakeDatabase:
    def __init__(self, collections=None):
        self.collections = collections or {}

    def list_collection_names(self):
        return list(self.collections)

    def __getitem__(self, name):
        return self.collections[name]


class FakeAdmin:
    def __init__(self, error=None):
        self.error = error

    def command(self, _command):
        if self.error is not None:
            raise self.error
        return {"ok": 1}


class FakeClient:
    def __init__(self, database, ping_error=None, database_error=None):
        self.database = database
        self.admin = FakeAdmin(ping_error)
        self.database_error = database_error
        self.closed = False

    def __getitem__(self, _name):
        return self.database

    def get_default_database(self):
        if self.database_error is not None:
            raise self.database_error
        return self.database

    def close(self):
        self.closed = True


class ExtractionErrorHandlingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.raw_dir = self.root / "raw"
        self.log_file = self.root / "logs" / "extraction.jsonl"

        self.raw_dir_patch = patch.object(extract, "RAW_DIR", self.raw_dir)
        self.log_file_patch = patch.object(extract, "LOG_FILE", self.log_file)
        self.env_patch = patch.dict(
            os.environ,
            {
                "MONGODB_URI": "mongodb://user:topsecret@localhost/testdb",
                "MONGODB_DATABASE": "testdb",
            },
            clear=True,
        )
        self.raw_dir_patch.start()
        self.log_file_patch.start()
        self.env_patch.start()
        self.addCleanup(self.raw_dir_patch.stop)
        self.addCleanup(self.log_file_patch.stop)
        self.addCleanup(self.env_patch.stop)

    def read_records(self):
        return [
            json.loads(line)
            for line in self.log_file.read_text(encoding="utf-8").splitlines()
        ]

    def make_collections(self, documents=None):
        documents = documents or {}
        return {
            config["collection"]: FakeCollection(documents.get(dataset, ()))
            for dataset, config in extract.DATASETS.items()
        }

    def run_with_client(self, client):
        with patch.object(extract, "MongoClient", return_value=client):
            return extract.main()

    def test_missing_uri_is_logged_and_returns_nonzero(self):
        with patch.dict(
            os.environ, {"MONGODB_DATABASE": "testdb"}, clear=True
        ), patch.object(extract, "MongoClient") as mongo_client:
            result = extract.main()

        self.assertEqual(result, 1)
        mongo_client.assert_not_called()
        record = self.read_records()[0]
        self.assertEqual(record["dataset_name"], "pipeline")
        self.assertEqual(record["operation"], "validate_configuration")
        self.assertEqual(record["extraction_status"], "FAILED")
        self.assertEqual(record["error_code"], "MONGODB_CONFIGURATION_ERROR")

    def test_connection_failure_is_logged_redacted_and_client_is_closed(self):
        client = FakeClient(
            FakeDatabase(),
            ping_error=ConnectionFailure(
                "connection failed mongodb://user:topsecret@localhost/testdb"
            ),
        )

        result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertTrue(client.closed)
        record = self.read_records()[0]
        self.assertEqual(record["operation"], "ping")
        self.assertEqual(record["extraction_status"], "FAILED")
        self.assertNotIn("topsecret", record["error_message"])
        self.assertNotIn("mongodb://user:", record["error_message"])

    def test_invalid_uri_is_logged_and_returns_nonzero(self):
        with patch.object(extract, "MongoClient", side_effect=InvalidURI("password=secret")):
            result = extract.main()

        self.assertEqual(result, 1)
        record = self.read_records()[0]
        self.assertEqual(record["extraction_status"], "FAILED")
        self.assertEqual(record["error_code"], "MONGODB_CONFIGURATION_ERROR")
        self.assertNotIn("secret", record["error_message"])

    def test_missing_collections_are_logged_as_failed(self):
        client = FakeClient(FakeDatabase())

        result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertTrue(client.closed)
        records = self.read_records()
        self.assertEqual(len(records), len(extract.DATASETS))
        self.assertTrue(all(item["extraction_status"] == "FAILED" for item in records))
        self.assertTrue(
            all(item["error_code"] == "COLLECTION_NOT_FOUND" for item in records)
        )
        self.assertTrue(
            all(item["operation"] == "list_collections" for item in records)
        )
        self.assertTrue(all(item["dataset_name"] in extract.DATASETS for item in records))

    def test_query_failure_records_dataset_operation_error_code_and_closes_client(self):
        first_dataset = next(iter(extract.DATASETS))
        first_collection = extract.DATASETS[first_dataset]["collection"]
        collections = self.make_collections()
        collections[first_collection] = FakeCollection(
            error=OperationFailure(
                "authentication failed password=topsecret",
                code=13,
            )
        )
        client = FakeClient(FakeDatabase(collections))

        result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertTrue(client.closed)
        record = next(
            item for item in self.read_records()
            if item["dataset_name"] == first_dataset
        )
        self.assertEqual(record["operation"], "query")
        self.assertEqual(record["error_code"], 13)
        self.assertEqual(record["extraction_status"], "FAILED")
        self.assertNotIn("topsecret", record["error_message"])

    def test_parquet_write_failure_removes_partial_output_and_preserves_old_files(self):
        self.raw_dir.mkdir(parents=True)
        previous_output = self.raw_dir / "previous_success.parquet"
        previous_output.write_bytes(b"previous successful output")

        def partial_write_then_fail(_table, destination, **_kwargs):
            Path(destination).write_bytes(b"incomplete parquet")
            raise OSError("write failed password=parquet-secret")

        client = FakeClient(FakeDatabase(self.make_collections()))
        with patch.object(extract.pq, "write_table", side_effect=partial_write_then_fail):
            result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertTrue(client.closed)
        self.assertEqual(previous_output.read_bytes(), b"previous successful output")
        self.assertEqual(list(self.raw_dir.glob("*.parquet")), [previous_output])
        self.assertEqual(list(self.raw_dir.glob(".*.tmp")), [])
        records = self.read_records()
        self.assertTrue(all(item["extraction_status"] == "FAILED" for item in records))
        self.assertTrue(all(item["operation"] == "write_parquet" for item in records))
        self.assertTrue(all("parquet-secret" not in item["error_message"] for item in records))

    def test_existing_timestamped_output_is_never_replaced(self):
        fixed_time = datetime(2026, 10, 28, 1, 2, 3, tzinfo=timezone.utc)

        class FixedDateTime:
            @staticmethod
            def now(_timezone=None):
                return fixed_time

        self.raw_dir.mkdir(parents=True)
        existing_output = self.raw_dir / "property_listings_20261028T010203Z.parquet"
        existing_output.write_bytes(b"previous successful output")
        client = FakeClient(FakeDatabase(self.make_collections()))

        with patch.object(extract, "datetime", FixedDateTime):
            result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertEqual(existing_output.read_bytes(), b"previous successful output")
        record = next(
            item for item in self.read_records()
            if item["dataset_name"] == "property_listings"
        )
        self.assertEqual(record["extraction_status"], "FAILED")
        self.assertEqual(record["operation"], "publish_parquet")
        self.assertIsNone(record["output_file"])

    def test_log_write_failure_reports_stderr_without_masking_original_error(self):
        collections = {
            config["collection"]: FakeCollection(error=RuntimeError("query failed secret=query-secret"))
            for config in extract.DATASETS.values()
        }
        client = FakeClient(FakeDatabase(collections))
        stderr = io.StringIO()

        with patch.object(
            extract,
            "log_extraction",
            side_effect=OSError("log unavailable password=log-secret"),
        ), redirect_stderr(stderr):
            result = self.run_with_client(client)

        self.assertEqual(result, 1)
        self.assertTrue(client.closed)
        self.assertIn("Extraction log write failed", stderr.getvalue())
        self.assertIn("Original extraction failure", stderr.getvalue())
        self.assertNotIn("log-secret", stderr.getvalue())
        self.assertNotIn("query-secret", stderr.getvalue())
        self.assertFalse(self.log_file.exists())

    def test_successful_full_snapshot_outputs_parquet_and_success_logs(self):
        documents = {
            "property_listings": [
                {"_id": "p1", "location": "North", "price": 100, "status": "active"}
            ],
            "buyer_listings": [{"_id": "b1", "userId": "u1"}],
            "transaction_listings": [
                {
                    "_id": "t1",
                    "buyerId": "b1",
                    "propertyId": "p1",
                    "amount": 100,
                    "status": "complete",
                }
            ],
        }
        client = FakeClient(FakeDatabase(self.make_collections(documents)))

        result = self.run_with_client(client)

        self.assertEqual(result, 0)
        self.assertTrue(client.closed)
        records = self.read_records()
        self.assertEqual(len(records), len(extract.DATASETS))
        self.assertTrue(all(item["extraction_status"] == "SUCCESS" for item in records))
        self.assertTrue(all(item["operation"] == "publish_parquet" for item in records))
        for item in records:
            output_path = Path(item["output_file"])
            self.assertTrue(output_path.is_file())
            self.assertEqual(
                extract.pq.read_metadata(output_path).num_rows,
                item["records_read"],
            )

    def test_logger_handler_is_not_duplicated_when_main_runs_repeatedly(self):
        client = FakeClient(FakeDatabase(self.make_collections()))
        logger = extract.logging.getLogger("terraguide.extract")
        before = len(logger.handlers)

        self.run_with_client(client)
        self.run_with_client(FakeClient(FakeDatabase(self.make_collections())))

        self.assertEqual(len(logger.handlers), before + (1 if before == 0 else 0))


if __name__ == "__main__":
    unittest.main()
