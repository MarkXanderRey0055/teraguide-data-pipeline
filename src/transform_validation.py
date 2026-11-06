"""Validation and quarantine for Stage 2 transformed Parquet datasets.

This module contains structural validation only.  Cleaning, standardization,
business rules, and rejection-rate acceptance are intentionally configurable
because the project documentation does not yet approve those rules.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = Path(os.getenv("PROCESSED_DATA_DIR", ROOT / "data" / "processed"))
QUARANTINE_DIR = Path(os.getenv("QUARANTINE_DATA_DIR", ROOT / "data" / "quarantine"))
DATASETS = ("property_listings", "buyer_listings", "transaction_listings")

RULE_PRIMARY_KEY = "V-PK-001"
RULE_REQUIRED_FIELD = "V-REQ-001"
RULE_REFERENTIAL_INTEGRITY = "V-REF-001"
RULE_SCHEMA = "V-SCHEMA-001"
RULE_RECONCILIATION = "V-REC-001"
RULE_BUYER_RELATIONSHIP_SKIPPED = "V-REF-002"
_ROW_INDEX = "__validation_row_index"


@dataclass(frozen=True)
class CheckResult:
    rule_id: str
    check_name: str
    dataset: str
    target: str
    purpose: str
    passed: bool
    affected_count: int
    severity: str  # Critical or Non-critical
    details: str = ""


@dataclass(frozen=True)
class ValidationReport:
    dataset: str
    batch_id: str
    input_count: int
    accepted_count: int
    rejected_count: int
    checks: tuple[CheckResult, ...]
    quarantined_path: Path | None = None

    @property
    def critical_failures(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if check.severity == "Critical" and not check.passed)


def _check(rule_id: str, name: str, dataset: str, target: str, purpose: str,
           passed: bool, affected: int, severity: str, details: str = "") -> CheckResult:
    if severity not in {"Critical", "Non-critical"}:
        raise ValueError("severity must be Critical or Non-critical")
    return CheckResult(rule_id, name, dataset, target, purpose, passed, affected, severity, details)


def reconcile_counts(input_count: int, accepted_count: int, rejected_count: int,
                     dataset: str = "", *, details: str = "") -> CheckResult:
    """Check that uniquely accepted and rejected rows account for the input."""
    valid = (
        input_count >= 0 and accepted_count >= 0 and rejected_count >= 0
        and accepted_count + rejected_count == input_count
    )
    return _check(
        RULE_RECONCILIATION, "record_reconciliation", dataset, "record counts",
        "Every input record must be uniquely accepted or explicitly quarantined.",
        valid, abs(input_count - accepted_count - rejected_count), "Critical", details,
    )


def validate_dataset(
    table: pa.Table,
    dataset: str,
    batch_id: str,
    *,
    primary_key: str = "_id",
    required_fields: Iterable[str] = (),
    expected_schema: pa.Schema | None = None,
    expected_columns: Iterable[str] | None = None,
    row_validators: dict[str, Callable[[dict[str, Any]], bool]] | None = None,
) -> tuple[ValidationReport, list[dict[str, Any]]]:
    """Validate rows and return a report plus rejected original rows.

    ``required_fields``, ``expected_schema``, and ``row_validators`` are
    opt-in.  They must be supplied by an approved project rule; this function
    does not infer business requirements from values.
    """
    if not batch_id:
        raise ValueError("An explicit batch_id is required for validation.")
    rows = table.to_pylist()
    required_fields = tuple(required_fields)
    checks: list[CheckResult] = []
    rejected: dict[int, list[tuple[str, str]]] = {}

    def reject(index: int, rule_id: str, reason: str) -> None:
        rejected.setdefault(index, []).append((rule_id, reason))

    seen: dict[Any, int] = {}
    duplicate_count = 0
    missing_key_count = 0
    for index, row in enumerate(rows):
        key = row.get(primary_key)
        if key is None:
            missing_key_count += 1
            reject(index, RULE_PRIMARY_KEY, f"missing primary key: {primary_key}")
        elif key in seen:
            duplicate_count += 1
            reject(index, RULE_PRIMARY_KEY, f"duplicate primary key: {primary_key}={key}")
        else:
            seen[key] = index
    pk_affected = missing_key_count + duplicate_count
    checks.append(_check(RULE_PRIMARY_KEY, "primary_key_uniqueness", dataset, primary_key,
                         "Each transformed record must have a unique non-null primary key.",
                         pk_affected == 0, pk_affected, "Critical"))

    required_affected = 0
    for field in required_fields:
        field_count = sum(row.get(field) is None for row in rows)
        required_affected += field_count
        for index, row in enumerate(rows):
            if row.get(field) is None:
                reject(index, RULE_REQUIRED_FIELD, f"missing required field: {field}")
    checks.append(_check(RULE_REQUIRED_FIELD, "required_field_null_check", dataset,
                         ",".join(required_fields) if required_fields else "configured fields",
                         "Configured required fields must not be null.", required_affected == 0,
                         required_affected, "Critical"))

    if expected_schema is not None:
        passed = table.schema == expected_schema
        checks.append(_check(RULE_SCHEMA, "schema_and_data_type_conformance", dataset, "schema",
                             "Configured transformed columns must retain the approved data types.",
                             passed, 0 if passed else 1, "Critical"))
    if expected_columns is not None:
        expected = set(expected_columns)
        actual = set(table.column_names)
        missing = sorted(expected - actual)
        unexpected = sorted(set(table.column_names) - expected)
        checks.append(_check(RULE_SCHEMA, "missing_columns", dataset, ",".join(missing) or "none",
                             "Expected columns must be reported when absent; no columns are added implicitly.",
                             not missing, len(missing), "Non-critical"))
        checks.append(_check(RULE_SCHEMA, "unexpected_columns", dataset, ",".join(unexpected) or "none",
                             "Unexpected columns are reported without silently dropping them.",
                             not unexpected, len(unexpected), "Non-critical"))

    for rule_id, validator in (row_validators or {}).items():
        failed = 0
        for index, row in enumerate(rows):
            try:
                valid = bool(validator(row))
            except Exception:
                valid = False
            if not valid:
                failed += 1
                reject(index, rule_id, f"row validation failed: {rule_id}")
        checks.append(_check(rule_id, "configured_row_rule", dataset, "row",
                             "An explicitly configured validation rule must pass.", failed == 0,
                             failed, "Critical"))

    rejected_rows = []
    for index, reasons in rejected.items():
        row = dict(rows[index])
        row[_ROW_INDEX] = index
        row["quarantine_reason"] = "; ".join(reason for _, reason in reasons)
        row["batch_id"] = batch_id
        row["rule_id"] = ";".join(rule_id for rule_id, _ in reasons)
        rejected_rows.append(row)
    checks.append(reconcile_counts(len(rows), len(rows) - len(rejected_rows),
                                   len(rejected_rows), dataset))
    return ValidationReport(dataset, batch_id, len(rows), len(rows) - len(rejected_rows),
                            len(rejected_rows), tuple(checks)), rejected_rows


def validate_referential_integrity(
    child: pa.Table, child_dataset: str, child_field: str,
    parent: pa.Table, parent_dataset: str, parent_field: str = "_id",
    batch_id: str | None = None,
) -> tuple[CheckResult, list[dict[str, Any]]]:
    """Check a documented child-to-parent identifier relationship."""
    if not batch_id:
        raise ValueError("An explicit batch_id is required for validation.")
    parent_values = {row.get(parent_field) for row in parent.to_pylist() if row.get(parent_field) is not None}
    rejected = []
    for index, row in enumerate(child.to_pylist()):
        value = row.get(child_field)
        if value is None or value not in parent_values:
            item = dict(row)
            item[_ROW_INDEX] = index
            item["quarantine_reason"] = f"unmatched reference: {child_field} -> {parent_dataset}.{parent_field}"
            item["batch_id"] = batch_id
            item["rule_id"] = RULE_REFERENTIAL_INTEGRITY
            rejected.append(item)
    check = _check(RULE_REFERENTIAL_INTEGRITY, "referential_integrity", child_dataset,
                   f"{child_field} -> {parent_dataset}.{parent_field}",
                   "Every configured child reference must match a parent identifier.",
                   not rejected, len(rejected), "Critical")
    return check, rejected


def _prepare_quarantine(rows: list[dict[str, Any]], dataset: str, batch_id: str,
                        quarantine_dir: Path) -> tuple[Path, Path] | None:
    """Prepare quarantine output without replacing its final path."""
    if not rows:
        return None
    if not batch_id:
        raise ValueError("An explicit batch_id is required for quarantine output.")
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    path = quarantine_dir / f"{batch_id}_{dataset}.parquet"
    serializable = [{key: value for key, value in row.items() if key != _ROW_INDEX} for row in rows]
    with tempfile.NamedTemporaryFile(dir=quarantine_dir, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temporary_path = Path(handle.name)
    try:
        pq.write_table(pa.Table.from_pylist(serializable), temporary_path, compression="snappy")
        return path, temporary_path
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def write_quarantine(rows: list[dict[str, Any]], dataset: str, batch_id: str,
                     quarantine_dir: Path = QUARANTINE_DIR) -> Path | None:
    """Write rejected rows atomically, preserving any prior output on failure."""
    prepared = _prepare_quarantine(rows, dataset, batch_id, quarantine_dir)
    if prepared is None:
        return None
    path, temporary_path = prepared
    try:
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


def _merge_rejected_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge failures by source row index, preserving every reason and rule."""
    merged: dict[int, dict[str, Any]] = {}
    reasons: dict[int, list[str]] = {}
    rules: dict[int, list[str]] = {}
    for row in rows:
        index = row.get(_ROW_INDEX)
        if index is None:
            raise ValueError("Rejected records must carry a validation row identity.")
        if index not in merged:
            merged[index] = dict(row)
            reasons[index] = []
            rules[index] = []
        reason = row.get("quarantine_reason")
        rule_ids = str(row.get("rule_id", "")).split(";")
        if reason and reason not in reasons[index]:
            reasons[index].append(reason)
        for rule_id in rule_ids:
            if rule_id and rule_id not in rules[index]:
                rules[index].append(rule_id)
    output = []
    for index in sorted(merged):
        row = merged[index]
        row["quarantine_reason"] = "; ".join(reasons[index])
        row["rule_id"] = ";".join(rules[index])
        output.append(row)
    return output


def _with_merged_rejections(report: ValidationReport, check: CheckResult,
                            rows: list[dict[str, Any]]) -> ValidationReport:
    unique_rejected = len(_merge_rejected_rows(rows))
    reconciliation = reconcile_counts(
        report.input_count, report.input_count - unique_rejected, unique_rejected,
        report.dataset,
    )
    checks = tuple(existing for existing in report.checks if existing.rule_id != RULE_RECONCILIATION)
    return ValidationReport(
        report.dataset, report.batch_id, report.input_count,
        report.input_count - unique_rejected, unique_rejected,
        checks + (check, reconciliation), report.quarantined_path,
    )


def validate_processed_datasets(
    processed_dir: Path = PROCESSED_DIR,
    quarantine_dir: Path = QUARANTINE_DIR,
    batch_id: str | None = None,
    required_fields: dict[str, Iterable[str]] | None = None,
    buyer_relationship: tuple[str, str] | None = ("buyer_listings", "userId"),
) -> dict[str, ValidationReport]:
    """Validate the three Issue #4 outputs and write explicit quarantines.

    The two transaction relationships are enabled because the inspected
    snapshots contain matching ``propertyId``/property ``_id`` and
    ``buyerId``/buyer ``userId`` identifiers.  No inferred business rules are
    enabled here.
    """
    if not batch_id:
        raise ValueError("An explicit batch_id is required for validation.")
    tables = {}
    for dataset in DATASETS:
        path = processed_dir / f"{dataset}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Missing processed dataset: {path}")
        try:
            tables[dataset] = pq.read_table(path)
        except (OSError, pa.ArrowException, ValueError) as exc:
            raise TransformationError(f"Could not read processed dataset {path}: {exc}") from exc

    reports: dict[str, ValidationReport] = {}
    quarantine_rows: dict[str, list[dict[str, Any]]] = {}
    for dataset in DATASETS:
        report, rejected = validate_dataset(
            tables[dataset], dataset, batch_id,
            required_fields=(required_fields or {}).get(dataset, ()),
        )
        reports[dataset] = report
        quarantine_rows[dataset] = rejected

    relationship_specs = (("transaction_listings", "propertyId", "property_listings", "_id"),)
    for child_dataset, child_field, parent_dataset, parent_field in relationship_specs:
        check, rejected = validate_referential_integrity(
            tables[child_dataset], child_dataset, child_field,
            tables[parent_dataset], parent_dataset, parent_field, batch_id,
        )
        report = reports[child_dataset]
        quarantine_rows[child_dataset].extend(rejected)
        reports[child_dataset] = report
        reports[child_dataset] = _with_merged_rejections(report, check, quarantine_rows[child_dataset])
    if buyer_relationship is None:
        # The relationship is intentionally configurable until the project
        # approves a stable buyer key contract.
        reports["transaction_listings"] = ValidationReport(
            reports["transaction_listings"].dataset,
            reports["transaction_listings"].batch_id,
            reports["transaction_listings"].input_count,
            reports["transaction_listings"].accepted_count,
            reports["transaction_listings"].rejected_count,
            reports["transaction_listings"].checks + (_check(
                RULE_BUYER_RELATIONSHIP_SKIPPED,
                "referential_integrity_skipped",
                "transaction_listings",
                "buyerId -> buyer_listings.<configured field>",
                "Report that buyer referential integrity was not configured.",
                False, 0, "Non-critical", "SKIPPED: buyer relationship not configured",
            ),),
            reports["transaction_listings"].quarantined_path,
        )
    else:
        buyer_dataset, buyer_field = buyer_relationship
        check, rejected = validate_referential_integrity(
            tables["transaction_listings"], "transaction_listings", "buyerId",
            tables[buyer_dataset], buyer_dataset, buyer_field, batch_id=batch_id,
        )
        quarantine_rows["transaction_listings"].extend(rejected)
        report = reports["transaction_listings"]
        reports["transaction_listings"] = _with_merged_rejections(
            report, check, quarantine_rows["transaction_listings"]
        )
    # Prepare every non-empty quarantine file before changing any final file.
    # For a zero-rejection result, remove a stale file only after all prepares
    # succeed, so a failed validation/output step cannot erase prior evidence.
    prepared_outputs: list[tuple[str, Path, Path] | tuple[str, None, None]] = []
    try:
        for dataset, rejected in quarantine_rows.items():
            merged = _merge_rejected_rows(rejected)
            prepared = _prepare_quarantine(merged, dataset, batch_id, quarantine_dir)
            prepared_outputs.append((dataset, prepared[0], prepared[1]) if prepared else (dataset, None, None))
    except Exception:
        for _, _, temporary_path in prepared_outputs:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        raise
    try:
        for dataset, path, temporary_path in prepared_outputs:
            final_path = quarantine_dir / f"{batch_id}_{dataset}.parquet"
            if temporary_path is None:
                if quarantine_dir.exists():
                    final_path.unlink(missing_ok=True)
                published_path = None
            else:
                os.replace(temporary_path, path)
                published_path = path
            report = reports[dataset]
            reports[dataset] = ValidationReport(
                report.dataset, report.batch_id, report.input_count,
                report.accepted_count, report.rejected_count, report.checks,
                published_path,
            )
    except Exception:
        for _, _, temporary_path in prepared_outputs:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        raise
    return reports


class TransformationError(RuntimeError):
    """Raised when a processed dataset cannot be read for validation."""
