import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import transform


class TransformTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw"
        self.processed = self.root / "processed"
        self.raw.mkdir()
        self.table = pa.table({"_id": ["a", "b"], "status": ["Available", None], "amount": [1.5, 2.0]})
        pq.write_table(self.table, self.raw / "property_listings_20261106T000000Z.parquet")
        self.addCleanup(self.temp.cleanup)

    def test_reads_losslessly_and_preserves_raw(self):
        result = transform.transform_dataset("property_listings", self.raw, self.processed)
        self.assertEqual(result.input_rows, result.output_rows)
        self.assertEqual(pq.read_table(result.output_path), self.table)
        self.assertTrue(result.input_path.exists())

    def test_missing_input_is_explicit(self):
        with self.assertRaises(transform.TransformationError):
            transform.transform_dataset("buyer_listings", self.raw, self.processed)

    def test_unreadable_input_does_not_replace_existing_output(self):
        output = self.processed / "property_listings.parquet"
        self.processed.mkdir()
        pq.write_table(pa.table({"old": [1]}), output)
        (self.raw / "property_listings_20261106T010000Z.parquet").write_bytes(b"not parquet")
        with self.assertRaises(transform.TransformationError):
            transform.transform_dataset("property_listings", self.raw, self.processed)
        self.assertEqual(pq.read_table(output), pa.table({"old": [1]}))

    def test_repeated_execution_is_idempotent(self):
        first = transform.transform_dataset("property_listings", self.raw, self.processed)
        second = transform.transform_dataset("property_listings", self.raw, self.processed)
        self.assertEqual(first.output_path, second.output_path)
        self.assertEqual(list(self.processed.glob("*.parquet")), [first.output_path])

    def write_dataset(self, name, timestamp="20261106T000000Z", table=None):
        table = table or self.table
        path = self.raw / f"{name}_{timestamp}.parquet"
        pq.write_table(table, path)
        return path

    def test_all_dataset_prefixes_and_newest_timestamp_selection(self):
        for name in transform.DATASETS:
            self.write_dataset(name, "20261105T000000Z")
            newest = self.write_dataset(name, "20261106T000000Z")
            newest.touch()
            selected = transform.find_latest_input(name, self.raw)
            self.assertEqual(selected, newest)

    def test_invalid_input_timestamp_is_explicit(self):
        self.write_dataset("buyer_listings", "20261106T250000Z")
        with self.assertRaises(transform.TransformationError):
            transform.find_latest_input("buyer_listings", self.raw)

    def test_full_pipeline_processes_all_datasets(self):
        for name in transform.DATASETS:
            self.write_dataset(name)
        results = transform.transform_all(self.raw, self.processed)
        self.assertEqual([r.dataset_name for r in results], list(transform.DATASETS))
        self.assertEqual({p.name for p in self.processed.glob("*.parquet")},
                         {f"{name}.parquet" for name in transform.DATASETS})

    def test_failure_preparation_preserves_all_existing_outputs(self):
        for name in transform.DATASETS:
            self.write_dataset(name)
            output = self.processed / f"{name}.parquet"
            self.processed.mkdir(exist_ok=True)
            pq.write_table(pa.table({"old": [name]}), output)
        (self.raw / "buyer_listings_20261106T010000Z.parquet").write_bytes(b"not parquet")
        before = {p.name: p.read_bytes() for p in self.processed.glob("*.parquet")}
        with self.assertRaises(transform.TransformationError):
            transform.transform_all(self.raw, self.processed)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.processed.glob("*.parquet")})

    def test_schema_nulls_and_values_are_preserved(self):
        table = pa.table({"id": pa.array(["x", None]), "value": pa.array([1, None], type=pa.int64())})
        self.write_dataset("transaction_listings", table=table)
        result = transform.transform_dataset("transaction_listings", self.raw, self.processed)
        written = pq.read_table(result.output_path)
        self.assertEqual(written.schema, table.schema)
        self.assertEqual(written.num_rows, table.num_rows)
        self.assertTrue(written.equals(table))


if __name__ == "__main__":
    unittest.main()
