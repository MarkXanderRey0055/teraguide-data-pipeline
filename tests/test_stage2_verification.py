import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import transform_business as business
import transform_logger
import transform_validation as validation


class Stage2VerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw"
        self.processed = self.root / "processed"
        self.quarantine = self.root / "quarantine"
        self.log_path = self.root / "logs" / "transformation.jsonl"
        self.raw.mkdir()
        self.addCleanup(self.temp.cleanup)

    def write_b01_inputs(self):
        transactions = [
            {
                "_id": "t1", "buyerId": "u1", "propertyId": "p1", "amount": 10.0,
                "status": "Completed", "notes": None, "createdBy": "system",
                "completedAt": datetime(2026, 1, 1), "createdAt": datetime(2026, 1, 1),
                "updatedAt": datetime(2026, 1, 1), "__v": 0,
            },
            {
                "_id": "t2", "buyerId": "missing", "propertyId": "p1", "amount": 20.0,
                "status": "Reserved", "notes": "unmatched", "createdBy": "system",
                "completedAt": None, "createdAt": datetime(2026, 1, 2),
                "updatedAt": datetime(2026, 1, 2), "__v": 0,
            },
        ]
        buyers = [{
            "_id": "b1", "userId": "u1", "__v": 0, "budgetMax": 100,
            "budgetMin": 10, "createdAt": datetime(2026, 1, 1),
            "intendedUse": "Home", "landType": "Residential", "location": "North",
            "minLotSize": 50, "updatedAt": datetime(2026, 1, 1),
        }]
        properties = [{
            "_id": "p1", "name": "House", "owner": "Owner", "location": "North",
            "type": "House", "price": 1000, "status": "Available", "lat": 1.0,
            "lng": 2.0, "lotSize": 50, "documents": "{}",
            "createdAt": datetime(2026, 1, 1), "updatedAt": datetime(2026, 1, 1),
            "__v": 0, "images": "[]", "pricePerSqm": 20, "size": 50,
        }]
        tables = {
            "transaction_listings": pa.Table.from_pylist(transactions, schema=pa.schema(business.TRANSACTION_FIELDS)),
            "buyer_listings": pa.Table.from_pylist(buyers),
            "property_listings": pa.Table.from_pylist(properties),
        }
        paths = {}
        for name, table in tables.items():
            path = self.raw / f"{name}_20261107T000000Z.parquet"
            pq.write_table(table, path)
            paths[name] = path
        return paths

    def test_b01_contract_reconciliation_and_raw_preservation(self):
        raw_paths = self.write_b01_inputs()
        raw_before = {path: path.read_bytes() for path in raw_paths.values()}

        result = business.transform_business(
            "verification-batch", raw_dir=self.raw,
            processed_dir=self.processed, quarantine_dir=self.quarantine,
        )

        integrated = pq.read_table(result.integrated_path)
        quarantined = pq.read_table(result.quarantine_path)
        self.assertEqual(integrated.schema, business.INTEGRATED_SCHEMA)
        self.assertEqual(integrated.column_names, business.INTEGRATED_SCHEMA.names)
        self.assertEqual(quarantined.schema, business.QUARANTINE_SCHEMA)
        self.assertEqual(result.input_transactions, result.integrated_transactions + result.quarantined_transactions)
        self.assertEqual(result.exclusion_counts["MISSING_BUYER"], 1)
        self.assertEqual(quarantined.to_pylist()[0]["_id"], "t2")
        self.assertEqual(quarantined.to_pylist()[0]["exclusion_reason"], "MISSING_BUYER")
        self.assertEqual(raw_before, {path: path.read_bytes() for path in raw_before})

    def test_b01_serialized_physical_types_and_repeatability(self):
        self.write_b01_inputs()
        first = business.transform_business(
            "repeatable-batch", raw_dir=self.raw,
            processed_dir=self.processed, quarantine_dir=self.quarantine,
        )
        first_integrated = pq.read_table(first.integrated_path)
        first_quarantine = pq.read_table(first.quarantine_path)
        integrated_types = {field.name: field.type for field in pq.read_schema(first.integrated_path)}
        quarantine_types = {field.name: field.type for field in pq.read_schema(first.quarantine_path)}
        self.assertEqual(integrated_types, {field.name: field.type for field in business.INTEGRATED_SCHEMA})
        self.assertEqual(quarantine_types, {field.name: field.type for field in business.QUARANTINE_SCHEMA})

        second = business.transform_business(
            "repeatable-batch", raw_dir=self.raw,
            processed_dir=self.processed, quarantine_dir=self.quarantine,
        )
        self.assertTrue(first_integrated.equals(pq.read_table(second.integrated_path)))
        self.assertTrue(first_quarantine.equals(pq.read_table(second.quarantine_path)))

    def test_validation_quarantine_and_reconciliation_metadata(self):
        self.processed.mkdir()
        pq.write_table(pa.table({"_id": ["p1"]}), self.processed / "property_listings.parquet")
        pq.write_table(pa.table({"_id": ["b1"], "userId": ["u1"]}), self.processed / "buyer_listings.parquet")
        pq.write_table(
            pa.table({"_id": ["t1", "t2"], "propertyId": ["p1", "missing"], "buyerId": ["u1", "u1"]}),
            self.processed / "transaction_listings.parquet",
        )
        reports = validation.validate_processed_datasets(
            self.processed, self.quarantine, "validation-batch",
        )
        report = reports["transaction_listings"]
        self.assertEqual(report.input_count, report.accepted_count + report.rejected_count)
        self.assertEqual(report.rejected_count, 1)
        self.assertEqual(report.quarantined_path, self.quarantine / "validation-batch_transaction_listings.parquet")
        saved = pq.read_table(report.quarantined_path).to_pylist()[0]
        self.assertEqual(saved["batch_id"], "validation-batch")
        self.assertIn(validation.RULE_REFERENTIAL_INTEGRITY, saved["rule_id"])

    def test_validation_metadata_is_serialized_by_transformation_logger(self):
        report, _ = validation.validate_dataset(
            pa.table({"_id": ["a"], "status": ["ok"]}),
            "properties", "logging-batch", required_fields=("status",),
        )
        transform_logger.log_transformation(
            "property_listings", run_id="stage2-run", batch_id="logging-batch",
            validation_report=report, log_path=self.log_path,
        )
        record = json.loads(self.log_path.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(record["run_id"], "stage2-run")
        self.assertEqual(record["batch_id"], "logging-batch")
        self.assertEqual(record["validation_status"], "PASSED")
        self.assertTrue(record["validation_checks"])
        self.assertEqual(record["rules_applied"], [])


if __name__ == "__main__":
    unittest.main()
