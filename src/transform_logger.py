"""Structured JSONL logging for Stage 2 transformation runs.

This logger is intentionally separate from the Stage 1 extraction logger.  It
records execution metadata only; it does not perform transformation or
validation work.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = ROOT / "logs" / "transformation"
DEFAULT_LOG_PATH = DEFAULT_LOG_DIR / "transformation_log.jsonl"


class TransformationLoggingError(RuntimeError):
    """Raised when transformation metadata cannot be written."""


def _utc_iso(value: datetime | str | None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Invalid ISO 8601 timestamp: {value!r}") from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_dict(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)
    raise TypeError(f"Unsupported validation metadata type: {type(value).__name__}")


def serialize_validation_checks(checks: Iterable[Any] | None) -> list[dict[str, Any]]:
    """Serialize CheckResult dataclasses or dictionaries for JSON output."""
    return [_as_dict(check) for check in (checks or ())]


def _validation_metadata(report: Any | None) -> tuple[str | None, list[dict[str, Any]]]:
    if report is None:
        return None, []
    if hasattr(report, "checks"):
        raw_checks = report.checks
    elif isinstance(report, dict):
        raw_checks = report.get("checks", [])
    else:
        raise TypeError(f"Unsupported validation report type: {type(report).__name__}")
    checks = serialize_validation_checks(raw_checks)
    if hasattr(report, "critical_failures"):
        validation_status = "FAILED" if report.critical_failures else "PASSED"
    else:
        failures = [check for check in checks if not check.get("passed", True)]
        critical = [check for check in failures if check.get("severity") == "Critical"]
        validation_status = "FAILED" if critical else "PASSED"
    return validation_status, checks


def _log_path(log_path: Path | str | None = None) -> Path:
    if log_path is not None:
        return Path(log_path)
    configured = os.getenv("TRANSFORMATION_LOG_PATH")
    if configured:
        return Path(configured)
    configured_dir = os.getenv("TRANSFORMATION_LOG_DIR")
    return Path(configured_dir) / "transformation_log.jsonl" if configured_dir else DEFAULT_LOG_PATH


def _write_jsonl(record: dict[str, Any], log_path: Path) -> None:
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except (OSError, TypeError, ValueError) as exc:
        raise TransformationLoggingError(f"Could not write transformation log {log_path}: {exc}") from exc


def log_transformation(
    dataset_name: str,
    *,
    run_id: str | None = None,
    parent_run_id: str | None = None,
    batch_id: str | None = None,
    input_file: Path | str | None = None,
    output_file: Path | str | None = None,
    start_time: datetime | str | None = None,
    end_time: datetime | str | None = None,
    status: str = "SUCCESS",
    records_read: int = 0,
    records_output: int = 0,
    records_rejected: int = 0,
    records_modified: int = 0,
    rules_applied: Iterable[str] | None = None,
    validation_report: Any | None = None,
    validation_status: str | None = None,
    validation_checks: Iterable[Any] | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    code_version: str | None = None,
    rule_version: str | None = None,
    log_path: Path | str | None = None,
) -> dict[str, Any]:
    """Create and append one dataset-level transformation record."""
    if not dataset_name:
        raise ValueError("dataset_name is required")
    if not batch_id:
        batch_id = str(uuid.uuid4())
    start = _utc_iso(start_time)
    end = _utc_iso(end_time)
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    validation_from_report, checks_from_report = _validation_metadata(validation_report)
    checks = serialize_validation_checks(validation_checks) if validation_checks is not None else checks_from_report
    record = {
        "run_id": run_id or str(uuid.uuid4()),
        "parent_run_id": parent_run_id,
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "input_file": str(input_file) if input_file is not None else None,
        "output_file": str(output_file) if output_file is not None else None,
        "start_time": start,
        "end_time": end,
        "duration_seconds": (end_dt - start_dt).total_seconds(),
        "status": status,
        "records_read": records_read,
        "records_output": records_output,
        "records_rejected": records_rejected,
        "records_modified": records_modified,
        "rules_applied": list(rules_applied or []),
        "validation_status": validation_status if validation_status is not None else validation_from_report,
        "validation_checks": checks,
        "error_code": error_code,
        "error_message": error_message,
        "code_version": code_version or os.getenv("TRANSFORMATION_CODE_VERSION", "unknown"),
        "rule_version": rule_version or os.getenv("TRANSFORMATION_RULE_VERSION", "unknown"),
    }
    _write_jsonl(record, _log_path(log_path))
    return record


class TransformationLogger:
    """Small configurable wrapper for repeated records in one execution."""

    def __init__(self, log_path: Path | str | None = None):
        self.log_path = log_path

    def log_dataset(self, dataset_name: str, **metadata: Any) -> dict[str, Any]:
        return log_transformation(dataset_name, log_path=self.log_path, **metadata)
