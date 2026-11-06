"""Transform Stage 1 Parquet snapshots into Stage 2 processed datasets.

Issue #4 deliberately implements only the currently verified part of the
transformation contract.  The transformation is lossless: the input schema,
rows, nulls, and values are preserved because the documentation contains no
approved cleaning or Section 4 standardization mappings.
"""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", ROOT / "data" / "raw"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", ROOT / "data" / "processed"))
DATASETS = ("property_listings", "buyer_listings", "transaction_listings")
SNAPSHOT_RE = re.compile(r"^(?P<dataset>property_listings|buyer_listings|transaction_listings)_(?P<timestamp>\d{8}T\d{6}Z)\.parquet$")


class TransformationError(RuntimeError):
    """Raised when an input cannot be transformed or published."""


@dataclass(frozen=True)
class TransformationResult:
    dataset_name: str
    input_path: Path
    output_path: Path
    input_rows: int
    output_rows: int
    schema: pa.Schema


def find_latest_input(dataset_name: str, raw_dir: Path = RAW_DIR) -> Path:
    """Return the newest timestamped Stage 1 snapshot for a dataset."""
    candidates = sorted(raw_dir.glob(f"{dataset_name}_*.parquet"))
    matches = []
    invalid = []
    for path in candidates:
        match = SNAPSHOT_RE.fullmatch(path.name)
        if match is None or match.group("dataset") != dataset_name:
            invalid.append(path.name)
            continue
        try:
            timestamp = datetime.strptime(match.group("timestamp"), "%Y%m%dT%H%M%SZ")
        except ValueError as exc:
            raise TransformationError(f"Invalid timestamp in Stage 1 filename {path.name!r}.") from exc
        matches.append((timestamp, path))
    if invalid:
        raise TransformationError(f"Unexpected Stage 1 filename(s) for {dataset_name!r}: {', '.join(invalid)}")
    if not matches:
        raise TransformationError(
            f"No valid timestamped Stage 1 Parquet input found for {dataset_name!r} in {raw_dir}."
        )
    return max(matches, key=lambda item: (item[0], item[1].name))[1]


def _prepare_output(table: pa.Table, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=output_path.parent, prefix=f".{output_path.name}.", suffix=".tmp", delete=False
    ) as handle:
        temporary_path = Path(handle.name)
    try:
        pq.write_table(table, temporary_path, compression="snappy")
        written = pq.read_table(temporary_path)
        if written.schema != table.schema or written.num_rows != table.num_rows or not written.equals(table):
            raise TransformationError(f"Output verification failed for {output_path}.")
        return temporary_path
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _prepare_dataset(dataset_name: str, raw_dir: Path, processed_dir: Path):
    if dataset_name not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset_name!r}")
    input_path = find_latest_input(dataset_name, raw_dir)
    try:
        table = pq.read_table(input_path)
    except (OSError, pa.ArrowException, ValueError) as exc:
        raise TransformationError(f"Could not read {input_path}: {exc}") from exc

    output_path = processed_dir / f"{dataset_name}.parquet"
    try:
        temporary_path = _prepare_output(table, output_path)
    except (OSError, pa.ArrowException, TransformationError, ValueError) as exc:
        raise TransformationError(f"Could not write {output_path}: {exc}") from exc
    return input_path, output_path, temporary_path, table


def transform_dataset(dataset_name: str, raw_dir: Path = RAW_DIR,
                      processed_dir: Path = PROCESSED_DIR) -> TransformationResult:
    """Prepare, verify, and publish one Stage 1 dataset."""
    input_path, output_path, temporary_path, table = _prepare_dataset(dataset_name, raw_dir, processed_dir)
    try:
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return TransformationResult(dataset_name, input_path, output_path,
                                 table.num_rows, pq.read_metadata(output_path).num_rows,
                                 table.schema)


def transform_all(raw_dir: Path = RAW_DIR,
                  processed_dir: Path = PROCESSED_DIR) -> list[TransformationResult]:
    """Prepare all datasets first, then publish the complete prepared set."""
    prepared = []
    try:
        for name in DATASETS:
            prepared.append(_prepare_dataset(name, raw_dir, processed_dir))
    except Exception:
        for _, _, temporary_path, _ in prepared:
            temporary_path.unlink(missing_ok=True)
        raise
    try:
        for _, output_path, temporary_path, _ in prepared:
            os.replace(temporary_path, output_path)
    finally:
        for _, _, temporary_path, _ in prepared:
            temporary_path.unlink(missing_ok=True)
    return [TransformationResult(name, input_path, output_path, table.num_rows,
                                 pq.read_metadata(output_path).num_rows, table.schema)
            for name, (input_path, output_path, _, table) in zip(DATASETS, prepared)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    args = parser.parse_args()
    try:
        for result in transform_all(args.raw_dir, args.processed_dir):
            print(f"{result.dataset_name}: {result.input_rows} rows -> {result.output_path}")
    except (TransformationError, OSError, ValueError) as exc:
        print(f"Transformation failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
