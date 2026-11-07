import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import transform_business as business


def table_rows():
    tx = [
        {"_id": "t1", "buyerId": "u1", "propertyId": "p1", "amount": 10.0, "status": "Completed", "notes": None, "createdBy": "c", "completedAt": datetime(2026, 1, 1), "createdAt": datetime(2026, 1, 1), "updatedAt": datetime(2026, 1, 1), "__v": 0},
        {"_id": "t2", "buyerId": "missing", "propertyId": "p1", "amount": 20.0, "status": "Reserved", "notes": "n", "createdBy": "c", "completedAt": None, "createdAt": datetime(2026, 1, 2), "updatedAt": datetime(2026, 1, 2), "__v": 0},
        {"_id": "t3", "buyerId": "u1", "propertyId": "missing", "amount": 30.0, "status": "Reserved", "notes": None, "createdBy": "c", "completedAt": None, "createdAt": datetime(2026, 1, 3), "updatedAt": datetime(2026, 1, 3), "__v": 0},
        {"_id": "t4", "buyerId": "missing", "propertyId": "missing", "amount": 40.0, "status": "Reserved", "notes": None, "createdBy": "c", "completedAt": None, "createdAt": datetime(2026, 1, 4), "updatedAt": datetime(2026, 1, 4), "__v": 0},
    ]
    buyers = [{"_id": "b1", "userId": "u1", "__v": 0, "budgetMax": 100, "budgetMin": 10, "createdAt": datetime(2026, 1, 1), "intendedUse": "Home", "landType": "Residential", "location": "North", "minLotSize": 50, "updatedAt": datetime(2026, 1, 1)}]
    properties = [{"_id": "p1", "name": "House", "owner": "Owner", "location": "North", "type": "House", "price": 1000, "status": "Available", "lat": 1.0, "lng": 2.0, "lotSize": 50, "documents": "{}", "createdAt": datetime(2026, 1, 1), "updatedAt": datetime(2026, 1, 1), "__v": 0, "images": "[]", "pricePerSqm": 20, "size": 50}]
    return tx, buyers, properties


class BusinessTransformTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw"
        self.processed = self.root / "processed"
        self.quarantine = self.root / "quarantine"
        self.raw.mkdir()
        self.write_inputs(*table_rows())
        self.addCleanup(self.temp.cleanup)

    def write_inputs(self, transactions, buyers, properties):
        base_transactions, base_buyers, base_properties = table_rows()
        pq.write_table(
            pa.Table.from_pylist(transactions, schema=pa.Table.from_pylist(base_transactions).schema),
            self.raw / "transaction_listings_20261107T000000Z.parquet",
        )
        pq.write_table(
            pa.Table.from_pylist(buyers, schema=pa.Table.from_pylist(base_buyers).schema),
            self.raw / "buyer_listings_20261107T000000Z.parquet",
        )
        pq.write_table(
            pa.Table.from_pylist(properties, schema=pa.Table.from_pylist(base_properties).schema),
            self.raw / "property_listings_20261107T000000Z.parquet",
        )

    def run_with_rows(self, transactions, buyers, properties, batch_id="test"):
        self.write_inputs(transactions, buyers, properties)
        return business.transform_business(batch_id, raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)

    def test_inner_join_schema_values_counts_and_categories(self):
        result = business.transform_business("batch-1", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        output = pq.read_table(result.integrated_path)
        quarantine = pq.read_table(result.quarantine_path)
        self.assertEqual(output.schema, business.INTEGRATED_SCHEMA)
        self.assertEqual(output.num_rows, 1)
        self.assertEqual(output.column_names[0:3], ["_id", "buyerId", "propertyId"])
        self.assertEqual(output.to_pylist()[0]["buyer_userId"], "u1")
        self.assertEqual(quarantine.schema, business.QUARANTINE_SCHEMA)
        self.assertEqual(quarantine.num_rows, 3)
        self.assertEqual(result.exclusion_counts, {"MISSING_BUYER": 1, "MISSING_PROPERTY": 1, "MISSING_BUYER_AND_PROPERTY": 1})
        self.assertTrue(result.reconciliation_passed)

    def test_duplicate_lookup_uses_latest_updated_at(self):
        transactions, buyers, properties = table_rows()
        buyers.extend([{**buyers[0], "_id": "b2", "budgetMin": 99, "updatedAt": datetime(2026, 2, 1)}])
        self.write_inputs(transactions, buyers, properties)
        result = business.transform_business("batch-2", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        self.assertEqual(pq.read_table(result.integrated_path).to_pylist()[0]["buyer_budgetMin"], 99)
        self.assertEqual(result.duplicate_groups["buyers"], 1)

    def test_property_duplicate_lookup_uses_latest_updated_at(self):
        transactions, buyers, properties = table_rows()
        properties.extend([{**properties[0], "name": "Updated House", "price": 2000, "updatedAt": datetime(2026, 2, 1)}])
        result = self.run_with_rows(transactions, buyers, properties, "property-duplicate")
        self.assertEqual(pq.read_table(result.integrated_path).to_pylist()[0]["property_name"], "Updated House")
        self.assertEqual(result.duplicate_groups["properties"], 1)

    def test_null_lookup_keys_fail_for_buyers_and_properties(self):
        transactions, buyers, properties = table_rows()
        buyers[0]["userId"] = None
        with self.assertRaisesRegex(business.NullLookupKeyError, "buyers.*userId"):
            self.run_with_rows(transactions, buyers, properties, "null-buyer-key")

        transactions, buyers, properties = table_rows()
        properties[0]["_id"] = None
        with self.assertRaisesRegex(business.NullLookupKeyError, "properties.*_id"):
            self.run_with_rows(transactions, buyers, properties, "null-property-key")

    def test_null_transaction_keys_are_classified_as_unmatched(self):
        transactions, buyers, properties = table_rows()
        transactions = [
            {**transactions[0], "_id": "null-buyer", "buyerId": None},
            {**transactions[0], "_id": "null-property", "propertyId": None},
            {**transactions[0], "_id": "null-both", "buyerId": None, "propertyId": None},
        ]
        result = self.run_with_rows(transactions, buyers, properties, "null-tx-keys")
        self.assertEqual(result.integrated_transactions, 0)
        self.assertEqual(result.quarantined_transactions, 3)
        self.assertEqual(result.exclusion_counts, {
            "MISSING_BUYER": 1,
            "MISSING_PROPERTY": 1,
            "MISSING_BUYER_AND_PROPERTY": 1,
        })

    def test_empty_datasets_and_no_transactions_publish_empty_integrated_output(self):
        _, buyers, properties = table_rows()
        transaction_schema = pa.Table.from_pylist(table_rows()[0]).schema
        result = self.run_with_rows([], buyers, properties, "empty-transactions")
        output = pq.read_table(result.integrated_path)
        self.assertEqual(output.schema, business.INTEGRATED_SCHEMA)
        self.assertEqual(output.num_rows, 0)
        self.assertIsNone(result.quarantine_path)

        empty_buyers = pa.Table.from_pylist([], schema=pa.Table.from_pylist(buyers).schema)
        empty_properties = pa.Table.from_pylist([], schema=pa.Table.from_pylist(properties).schema)
        pq.write_table(pa.Table.from_pylist([], schema=transaction_schema), self.raw / "transaction_listings_20261107T000000Z.parquet")
        pq.write_table(empty_buyers, self.raw / "buyer_listings_20261107T000000Z.parquet")
        pq.write_table(empty_properties, self.raw / "property_listings_20261107T000000Z.parquet")
        result = business.transform_business("empty-all", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        self.assertEqual(pq.read_table(result.integrated_path).num_rows, 0)

    def test_duplicate_missing_or_tied_timestamp_fails(self):
        transactions, buyers, properties = table_rows()
        buyers.extend([{**buyers[0], "_id": "b2", "updatedAt": None}])
        self.write_inputs(transactions, buyers, properties)
        with self.assertRaises(business.DuplicateLookupError):
            business.transform_business("batch-3", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        buyers[-1]["updatedAt"] = buyers[0]["updatedAt"]
        self.write_inputs(transactions, buyers, properties)
        with self.assertRaises(business.DuplicateLookupError):
            business.transform_business("batch-4", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)

    def test_quarantine_preserves_transaction_fields_and_reason(self):
        result = business.transform_business("batch-5", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        rows = pq.read_table(result.quarantine_path).to_pylist()
        self.assertEqual(set(rows[0]), set(business.QUARANTINE_SCHEMA.names))
        self.assertEqual({row["_id"] for row in rows}, {"t2", "t3", "t4"})

    def test_repeated_execution_is_deterministic(self):
        first = business.transform_business("batch-6", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        first_integrated = pq.read_table(first.integrated_path)
        first_quarantine = pq.read_table(first.quarantine_path)
        second = business.transform_business("batch-6", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        self.assertTrue(first_integrated.equals(pq.read_table(second.integrated_path)))
        self.assertTrue(first_quarantine.equals(pq.read_table(second.quarantine_path)))

    def test_publication_failure_preserves_existing_outputs(self):
        first = business.transform_business("batch-7", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        before = {path: path.read_bytes() for path in (first.integrated_path, first.quarantine_path)}
        real_replace = business.os.replace
        calls = []

        def fail_on_second(source, destination):
            calls.append((source, destination))
            if len(calls) == 2:
                raise OSError("publish failed")
            return real_replace(source, destination)

        with patch.object(business.os, "replace", side_effect=fail_on_second):
            with self.assertRaises(OSError):
                business.transform_business("batch-7", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        self.assertEqual(before, {path: path.read_bytes() for path in before})

    def test_all_transactions_unmatched_are_quarantined_once(self):
        transactions, buyers, properties = table_rows()
        transactions = [{**row, "buyerId": "no-buyer", "propertyId": "no-property"} for row in transactions]
        result = self.run_with_rows(transactions, buyers, properties, "all-unmatched")
        self.assertEqual(result.integrated_transactions, 0)
        self.assertEqual(result.quarantined_transactions, len(transactions))
        self.assertEqual(sum(result.exclusion_counts.values()), len(transactions))

    def test_output_verification_failure_during_preparation_does_not_publish(self):
        with patch.object(business, "_prepare_parquet", side_effect=business.BusinessTransformationError("verification failed")):
            with self.assertRaisesRegex(business.BusinessTransformationError, "verification failed"):
                business.transform_business("verification-failure", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        self.assertFalse((self.processed / "integrated_transactions.parquet").exists())
        self.assertFalse((self.quarantine / "verification-failure_unmatched_transactions.parquet").exists())

    def test_serialized_outputs_have_exact_physical_types(self):
        result = business.transform_business("physical-types", raw_dir=self.raw, processed_dir=self.processed, quarantine_dir=self.quarantine)
        expected = {field.name: field.type for field in business.INTEGRATED_SCHEMA}
        actual = {field.name: field.type for field in pq.read_schema(result.integrated_path)}
        self.assertEqual(actual, expected)
        expected_quarantine = {field.name: field.type for field in business.QUARANTINE_SCHEMA}
        actual_quarantine = {field.name: field.type for field in pq.read_schema(result.quarantine_path)}
        self.assertEqual(actual_quarantine, expected_quarantine)


if __name__ == "__main__":
    unittest.main()
