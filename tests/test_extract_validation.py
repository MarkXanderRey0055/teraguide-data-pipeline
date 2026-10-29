import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import extract


class Cursor:
    def __init__(self, documents):
        self.documents = documents

    def sort(self, *_args):
        return self

    def __iter__(self):
        return iter(self.documents)


class Collection:
    def __init__(self, documents, source_count=None, count_error=None):
        self.documents = documents
        self.source_count = source_count
        self.count_error = count_error

    def count_documents(self, _query):
        if self.count_error:
            raise self.count_error
        return self.source_count

    def find(self, _query):
        return Cursor(self.documents)


class Database:
    def __init__(self, collection):
        self.collection = collection

    def list_collection_names(self):
        return ["properties"]

    def __getitem__(self, _name):
        return self.collection


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.raw = Path(self.temp.name) / "raw"
        self.log = Path(self.temp.name) / "extract.jsonl"
        self.patches = [
            patch.object(extract, "RAW_DIR", self.raw),
            patch.object(extract, "LOG_FILE", self.log),
        ]
        for item in self.patches:
            item.start()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(lambda: [item.stop() for item in self.patches])

    def run_property(self, documents, source_count=None, count_error=None):
        config = extract.DATASETS["property_listings"]
        return extract.extract_collection(
            Database(Collection(documents, source_count, count_error)),
            "property_listings", config, "run-test", extract.logging.getLogger("test"),
        )

    def test_valid_dataset_passes_all_integrity_checks(self):
        result = self.run_property(
            [{"_id": "p1", "location": "North", "price": 10, "status": "active"}], 1
        )
        self.assertEqual(result["extraction_status"], "SUCCESS")
        self.assertEqual(result["validation_results"]["primary_key"]["status"], "PASS")
        self.assertEqual(result["validation_results"]["source_extracted_count"]["status"], "PASS")

    def test_missing_column_is_critical(self):
        result = self.run_property([{"_id": "p1", "location": "North", "price": 10}], 1)
        self.assertEqual(result["extraction_status"], "FAILED")
        self.assertEqual(result["validation_results"]["schema"]["status"], "FAIL")

    def test_required_null_missing_id_and_duplicate_id_are_reported(self):
        result = self.run_property([
            {"_id": "p1", "location": None, "price": 10, "status": "active"},
            {"_id": "p1", "location": "South", "price": 20, "status": "active"},
            {"location": "East", "price": 30, "status": "active"},
        ], 3)
        checks = result["validation_results"]
        self.assertEqual(result["extraction_status"], "FAILED")
        self.assertEqual(checks["required_fields"]["affected_count"], 2)
        self.assertEqual(checks["primary_key"]["actual"], {"missing_ids": 1, "duplicate_ids": 1})

    def test_count_mismatch_fails_and_count_query_failure_is_skipped(self):
        mismatch = self.run_property([{"_id": "p1", "location": "N", "price": 1, "status": "a"}], 2)
        self.assertEqual(mismatch["extraction_status"], "FAILED")
        unavailable = self.run_property([{"_id": "p1", "location": "N", "price": 1, "status": "a"}], count_error=RuntimeError("down"))
        self.assertEqual(unavailable["extraction_status"], "SUCCESS")
        self.assertEqual(unavailable["validation_results"]["source_record_count"]["status"], "SKIPPED")
        self.assertEqual(unavailable["validation_results"]["source_record_count"]["severity"], "WARNING")


if __name__ == "__main__":
    unittest.main()
