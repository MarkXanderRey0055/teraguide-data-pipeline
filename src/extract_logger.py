"""Structured JSONL logging for the TerraGuide extraction pipeline."""

import json
from datetime import datetime, timezone
from pathlib import Path


def log_extraction(
    *, log_file, run_id, dataset_name, collection_name, started, status,
    records_read, records_extracted, records_rejected, checks, output_file,
    error_code=None, error_message=None,
):
    """Append one structured extraction record and return it."""
    ended = datetime.now(timezone.utc)
    record = {
        "run_id": run_id,
        "dataset_name": dataset_name,
        "collection_name": collection_name,
        "start_timestamp": started.isoformat(),
        "end_timestamp": ended.isoformat(),
        "extraction_status": status,
        "records_read": records_read,
        "records_extracted": records_extracted,
        "records_rejected": records_rejected,
        "extraction_window_start": None,
        "extraction_window_end": None,
        "validation_results": checks,
        "error_code": error_code,
        "error_message": error_message,
        "duration_seconds": round((ended - started).total_seconds(), 3),
        "output_file": output_file,
        "extraction_mode": "full_snapshot",
    }

    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    return record