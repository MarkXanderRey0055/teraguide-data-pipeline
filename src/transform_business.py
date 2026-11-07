"""Approved B-01 business integration for Stage 2."""

from __future__ import annotations

import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

import transform

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = Path(os.getenv("RAW_DATA_DIR", ROOT / "data" / "raw"))
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", ROOT / "data" / "processed"))
QUARANTINE_DIR = Path(os.getenv("QUARANTINE_DATA_DIR", ROOT / "data" / "quarantine"))

TRANSACTION_FIELDS = (
    ("_id", pa.string()), ("buyerId", pa.string()), ("propertyId", pa.string()),
    ("amount", pa.float64()), ("status", pa.string()), ("notes", pa.string()),
    ("createdBy", pa.string()), ("completedAt", pa.timestamp("us")),
    ("createdAt", pa.timestamp("us")), ("updatedAt", pa.timestamp("us")),
    ("__v", pa.int64()),
)
BUYER_FIELDS = (
    ("buyer_userId", pa.string()), ("buyer_budgetMin", pa.int64()),
    ("buyer_budgetMax", pa.int64()), ("buyer_intendedUse", pa.string()),
    ("buyer_landType", pa.string()), ("buyer_location", pa.string()),
    ("buyer_minLotSize", pa.int64()),
)
PROPERTY_FIELDS = (
    ("property_name", pa.string()), ("property_owner", pa.string()),
    ("property_location", pa.string()), ("property_type", pa.string()),
    ("property_price", pa.int64()), ("property_status", pa.string()),
    ("property_lat", pa.float64()), ("property_lng", pa.float64()),
    ("property_lotSize", pa.int64()), ("property_pricePerSqm", pa.int64()),
    ("property_size", pa.int64()),
)
INTEGRATED_SCHEMA = pa.schema(TRANSACTION_FIELDS + BUYER_FIELDS + PROPERTY_FIELDS)
QUARANTINE_SCHEMA = pa.schema(TRANSACTION_FIELDS + (("exclusion_reason", pa.string()),))

PHYSICAL_DATASETS = {
    "transactions": "transaction_listings",
    "buyers": "buyer_listings",
    "properties": "property_listings",
}


class BusinessTransformationError(RuntimeError):
    """Raised when the approved B-01 contract cannot be safely fulfilled."""


class DuplicateLookupError(BusinessTransformationError):
    """Raised when a duplicate lookup key cannot be deterministically resolved."""


class NullLookupKeyError(BusinessTransformationError):
    """Raised when a buyer or property lookup record has a null key."""


@dataclass(frozen=True)
class BusinessTransformationResult:
    batch_id: str
    input_paths: dict[str, Path]
    integrated_path: Path
    quarantine_path: Path | None
    input_transactions: int
    integrated_transactions: int
    quarantined_transactions: int
    exclusion_counts: dict[str, int]
    duplicate_groups: dict[str, int]

    @property
    def reconciliation_passed(self) -> bool:
        return self.input_transactions == self.integrated_transactions + self.quarantined_transactions


def _read_inputs(raw_dir: Path) -> tuple[dict[str, Path], dict[str, pa.Table]]:
    paths: dict[str, Path] = {}
    tables: dict[str, pa.Table] = {}
    for logical, physical in PHYSICAL_DATASETS.items():
        try:
            path = transform.find_latest_input(physical, raw_dir)
            table = pq.read_table(path)
        except (OSError, pa.ArrowException, ValueError, transform.TransformationError) as exc:
            raise BusinessTransformationError(f"Could not read {logical} input: {exc}") from exc
        paths[logical] = path
        tables[logical] = table
    return paths, tables


def _require_schema(table: pa.Table, fields: tuple[tuple[str, pa.DataType], ...], dataset: str) -> None:
    missing = [name for name, _ in fields if name not in table.column_names]
    mismatched = [
        f"{name}: expected {expected}, got {table.schema.field(name).type}"
        for name, expected in fields if name in table.column_names and table.schema.field(name).type != expected
    ]
    if missing or mismatched:
        details = []
        if missing:
            details.append(f"missing columns={missing}")
        if mismatched:
            details.append(f"type mismatches={mismatched}")
        raise BusinessTransformationError(f"{dataset} schema does not satisfy B-01: {'; '.join(details)}")


def _deduplicate_lookup(table: pa.Table, dataset: str, key: str) -> tuple[dict[Any, dict[str, Any]], int]:
    rows = table.to_pylist()
    groups: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        value = row.get(key)
        if value is None:
            raise NullLookupKeyError(
                f"{dataset} contains a null lookup key in column {key!r}."
            )
        groups.setdefault(value, []).append(row)
    result: dict[Any, dict[str, Any]] = {}
    duplicate_groups = 0
    for value, group in groups.items():
        if len(group) == 1:
            result[value] = group[0]
            continue
        duplicate_groups += 1
        timestamps = [row.get("updatedAt") for row in group]
        if any(timestamp is None for timestamp in timestamps):
            raise DuplicateLookupError(
                f"{dataset} duplicate key {key}={value!r} has a missing updatedAt."
            )
        latest = max(timestamps)
        latest_rows = [row for row in group if row["updatedAt"] == latest]
        if len(latest_rows) != 1:
            raise DuplicateLookupError(
                f"{dataset} duplicate key {key}={value!r} has tied latest updatedAt={latest!r}."
            )
        result[value] = latest_rows[0]
    return result, duplicate_groups


def _integrated_row(transaction: dict[str, Any], buyer: dict[str, Any], property_row: dict[str, Any]) -> dict[str, Any]:
    row = {name: transaction.get(name) for name, _ in TRANSACTION_FIELDS}
    row.update({
        "buyer_userId": buyer.get("userId"),
        "buyer_budgetMin": buyer.get("budgetMin"),
        "buyer_budgetMax": buyer.get("budgetMax"),
        "buyer_intendedUse": buyer.get("intendedUse"),
        "buyer_landType": buyer.get("landType"),
        "buyer_location": buyer.get("location"),
        "buyer_minLotSize": buyer.get("minLotSize"),
    })
    row.update({
        "property_name": property_row.get("name"),
        "property_owner": property_row.get("owner"),
        "property_location": property_row.get("location"),
        "property_type": property_row.get("type"),
        "property_price": property_row.get("price"),
        "property_status": property_row.get("status"),
        "property_lat": property_row.get("lat"),
        "property_lng": property_row.get("lng"),
        "property_lotSize": property_row.get("lotSize"),
        "property_pricePerSqm": property_row.get("pricePerSqm"),
        "property_size": property_row.get("size"),
    })
    return row


def _prepare_parquet(table: pa.Table, final_path: Path) -> Path:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=final_path.parent, prefix=f".{final_path.name}.", suffix=".tmp", delete=False) as handle:
        temporary_path = Path(handle.name)
    try:
        pq.write_table(table, temporary_path, compression="snappy")
        written = pq.read_table(temporary_path)
        if written.schema != table.schema or written.num_rows != table.num_rows or not written.equals(table):
            raise BusinessTransformationError(f"Output verification failed for {final_path}.")
        return temporary_path
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _publish_pair(integrated_temp: Path, integrated_path: Path,
                  quarantine_temp: Path | None, quarantine_path: Path) -> None:
    """Publish both outputs with rollback for ordinary exceptions.

    This sequential filesystem operation is not crash-safe: process termination
    or machine interruption between replacements can leave a temporarily
    inconsistent pair of final files. A durable journal or atomic directory
    swap would be required to eliminate that limitation and is outside B-01.
    """
    targets = ((integrated_path, integrated_temp), (quarantine_path, quarantine_temp))
    backups: list[tuple[Path, Path]] = []
    published: list[Path] = []
    try:
        for final_path, _ in targets:
            if final_path.exists():
                backup = final_path.parent / f".{final_path.name}.{uuid.uuid4().hex}.bak"
                os.replace(final_path, backup)
                backups.append((final_path, backup))
        os.replace(integrated_temp, integrated_path)
        published.append(integrated_path)
        if quarantine_temp is not None:
            os.replace(quarantine_temp, quarantine_path)
            published.append(quarantine_path)
    except Exception:
        for path in published:
            path.unlink(missing_ok=True)
        for final_path, backup in reversed(backups):
            if backup.exists():
                os.replace(backup, final_path)
        raise
    else:
        for _, backup in backups:
            backup.unlink(missing_ok=True)


def transform_business(
    batch_id: str,
    *,
    raw_dir: Path = RAW_DIR,
    processed_dir: Path = PROCESSED_DIR,
    quarantine_dir: Path = QUARANTINE_DIR,
) -> BusinessTransformationResult:
    """Execute approved B-01 integration and publish its two outputs safely."""
    if not batch_id:
        raise ValueError("batch_id is required")
    input_paths, tables = _read_inputs(raw_dir)
    _require_schema(tables["transactions"], TRANSACTION_FIELDS, "transactions")
    _require_schema(tables["buyers"], (("userId", pa.string()), ("updatedAt", pa.timestamp("us"))), "buyers")
    _require_schema(tables["properties"], (("_id", pa.string()), ("updatedAt", pa.timestamp("us"))), "properties")
    _require_schema(tables["buyers"], (
        ("budgetMin", pa.int64()), ("budgetMax", pa.int64()), ("intendedUse", pa.string()),
        ("landType", pa.string()), ("location", pa.string()), ("minLotSize", pa.int64()),
    ), "buyers")
    _require_schema(tables["properties"], tuple((name.removeprefix("property_"), type_) for name, type_ in PROPERTY_FIELDS), "properties")

    buyers, buyer_duplicates = _deduplicate_lookup(tables["buyers"], "buyers", "userId")
    properties, property_duplicates = _deduplicate_lookup(tables["properties"], "properties", "_id")
    integrated_rows = []
    quarantine_rows = []
    exclusion_counts = {"MISSING_BUYER": 0, "MISSING_PROPERTY": 0, "MISSING_BUYER_AND_PROPERTY": 0}
    for transaction in tables["transactions"].to_pylist():
        buyer = buyers.get(transaction.get("buyerId"))
        property_row = properties.get(transaction.get("propertyId"))
        if buyer is not None and property_row is not None:
            integrated_rows.append(_integrated_row(transaction, buyer, property_row))
            continue
        if buyer is None and property_row is None:
            reason = "MISSING_BUYER_AND_PROPERTY"
        elif buyer is None:
            reason = "MISSING_BUYER"
        else:
            reason = "MISSING_PROPERTY"
        exclusion_counts[reason] += 1
        quarantine_rows.append({**{name: transaction.get(name) for name, _ in TRANSACTION_FIELDS}, "exclusion_reason": reason})

    input_count = tables["transactions"].num_rows
    if input_count != len(integrated_rows) + len(quarantine_rows):
        raise BusinessTransformationError("Transaction reconciliation failed.")
    integrated_table = pa.Table.from_pylist(integrated_rows, schema=INTEGRATED_SCHEMA)
    quarantine_table = pa.Table.from_pylist(quarantine_rows, schema=QUARANTINE_SCHEMA)
    integrated_path = processed_dir / "integrated_transactions.parquet"
    quarantine_path = quarantine_dir / f"{batch_id}_unmatched_transactions.parquet"
    integrated_temp = _prepare_parquet(integrated_table, integrated_path)
    quarantine_temp = _prepare_parquet(quarantine_table, quarantine_path) if quarantine_rows else None
    try:
        _publish_pair(integrated_temp, integrated_path, quarantine_temp, quarantine_path)
    except Exception:
        integrated_temp.unlink(missing_ok=True)
        if quarantine_temp is not None:
            quarantine_temp.unlink(missing_ok=True)
        raise
    return BusinessTransformationResult(
        batch_id, input_paths, integrated_path,
        quarantine_path if quarantine_rows else None,
        input_count, len(integrated_rows), len(quarantine_rows), exclusion_counts,
        {"buyers": buyer_duplicates, "properties": property_duplicates},
    )
