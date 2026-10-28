"""Extract TerraGuide dashboard datasets from MongoDB to Parquet."""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConfigurationError, PyMongoError

from extract_logger import log_extraction, redact_error_message

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RAW_DIR = Path(os.getenv("RAW_DATA_DIR", ROOT / "data" / "raw"))
LOG_FILE = Path(os.getenv("EXTRACTION_LOG_PATH", ROOT / "logs" / "extraction_log.jsonl"))

# Confirm these names against the actual MongoDB database before running.
DATASETS = {
    "property_listings": {
        "collection": os.getenv("MONGODB_PROPERTIES_COLLECTION", "properties"),
        "required": ["_id", "location", "price", "status"],
    },
    "buyer_listings": {
        "collection": os.getenv("MONGODB_BUYERS_COLLECTION", "buyerpreferences"),
        "required": ["_id", "userId"],
    },
    "transaction_listings": {
        "collection": os.getenv("MONGODB_TRANSACTIONS_COLLECTION", "transactions"),
        "required": ["_id", "buyerId", "propertyId", "amount", "status"],
    },
}


def to_parquet_value(value):
    """Convert MongoDB-specific or nested values into Parquet-compatible values."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return json.dumps(value, default=str, ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, default=str, ensure_ascii=False)
    return value


def _safe_log_extraction(**kwargs):
    """Write an extraction record without allowing logging errors to mask failures."""
    try:
        return log_extraction(**kwargs)
    except Exception as exc:
        print(
            f"Extraction log write failed: {redact_error_message(exc)}",
            file=sys.stderr,
        )
        if kwargs.get("error_message"):
            print(
                "Original extraction failure "
                f"({kwargs.get('error_code')}): "
                f"{redact_error_message(kwargs['error_message'])}",
                file=sys.stderr,
            )
        return {
            "run_id": kwargs["run_id"],
            "dataset_name": kwargs["dataset_name"],
            "extraction_status": kwargs["status"],
            "_log_write_failed": True,
        }


def _remove_file(path, description):
    """Remove a file created by this run, reporting cleanup errors to stderr."""
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        print(
            f"Could not remove {description}: {redact_error_message(exc)}",
            file=sys.stderr,
        )


def _error_code(exc, fallback):
    """Use a MongoDB server error code when available, otherwise a stable category."""
    code = getattr(exc, "code", None)
    return code if isinstance(code, (int, str)) and code else fallback


def extract_collection(database, dataset_name, config, run_id, logger):
    """Extract one collection, validate basic requirements, and save a Parquet file."""
    started = datetime.now(timezone.utc)
    collection_name = config["collection"]
    output_file = None
    output_path = None
    temporary_path = None
    output_created = False
    records = []
    records_read = 0
    rejected = 0
    status = "FAILED"
    error_code = None
    error_message = None
    checks = {}
    operation = "list_collections"

    try:
        if collection_name not in database.list_collection_names():
            raise LookupError(f"Collection '{collection_name}' was not found.")

        operation = "query"
        documents = list(database[collection_name].find({}).sort("_id", 1))
        records_read = len(documents)

        missing_fields = sum(
            any(field not in doc or doc[field] is None for field in config["required"])
            for doc in documents
        )
        ids = [str(doc["_id"]) for doc in documents if "_id" in doc]
        duplicate_ids = len(ids) - len(set(ids))

        # Preserve all raw documents. Missing fields and duplicate IDs are logged
        # as warnings rather than silently dropping source records.
        records = [
            {key: to_parquet_value(value) for key, value in doc.items()}
            for doc in documents
        ]

        operation = "create_output_directory"
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime("%Y%m%dT%H%M%SZ")
        output_path = RAW_DIR / f"{dataset_name}_{stamp}.parquet"

        operation = "serialize"
        if records:
            table = pa.Table.from_pylist(records)
        else:
            table = pa.table({"_id": pa.array([], type=pa.string())})

        temporary_path = RAW_DIR / f".{dataset_name}_{run_id}.parquet.tmp"
        file_descriptor = os.open(
            temporary_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o666,
        )
        os.close(file_descriptor)

        operation = "write_parquet"
        pq.write_table(table, temporary_path, compression="snappy")
        operation = "read_parquet_metadata"
        saved_count = pq.read_metadata(temporary_path).num_rows

        checks = {
            "collection_exists": True,
            "required_fields_missing_records": missing_fields,
            "duplicate_ids": duplicate_ids,
            "parquet_count_matches": saved_count == records_read,
        }

        if saved_count != records_read:
            error_code = "RECORD_COUNT_MISMATCH"
            error_message = f"Read {records_read} records but wrote {saved_count} rows."
            operation = "validate_parquet_count"
            status = "FAILED"
        elif missing_fields or duplicate_ids:
            status = "PARTIAL"
            error_code = "DATA_VALIDATION_WARNING"
            error_message = (
                f"Missing-field records: {missing_fields}; duplicate IDs: {duplicate_ids}. "
                "All records were retained in the raw file."
            )
        else:
            status = "SUCCESS"

        if status != "FAILED":
            operation = "publish_parquet"
            # A hard link publishes atomically and fails rather than replacing an
            # earlier extraction that happens to have the same timestamp.
            os.link(temporary_path, output_path)
            output_created = True
            try:
                output_file = str(output_path.relative_to(ROOT))
            except ValueError:
                output_file = str(output_path)
            logger.info("%s: %s (%d records)", dataset_name, status, saved_count)

    except LookupError as exc:
        status = "FAILED"
        error_code, error_message = "COLLECTION_NOT_FOUND", redact_error_message(exc)
    except PyMongoError as exc:
        status = "FAILED"
        error_code = _error_code(exc, "MONGODB_EXTRACTION_ERROR")
        error_message = redact_error_message(exc)
    except (pa.ArrowException, OSError, TypeError, ValueError) as exc:
        status = "FAILED"
        error_code = _error_code(exc, "PARQUET_OUTPUT_ERROR")
        error_message = redact_error_message(exc)
    except Exception as exc:
        status = "FAILED"
        error_code = _error_code(exc, "UNEXPECTED_ERROR")
        error_message = redact_error_message(exc)
    finally:
        if temporary_path is not None:
            _remove_file(temporary_path, "temporary Parquet output")
        if status == "FAILED" and output_created:
            _remove_file(output_path, "failed Parquet output")
            output_file = None

    return _safe_log_extraction(
        log_file=LOG_FILE, run_id=run_id, dataset_name=dataset_name,
        collection_name=collection_name, started=started, status=status,
        records_read=records_read,
        records_extracted=records_read if status in ("SUCCESS", "PARTIAL") else 0,
        records_rejected=rejected,
        checks=checks, output_file=output_file, error_code=error_code,
        error_message=error_message, operation=operation,
    )


def main():
    """Run full-snapshot extraction for the three dashboard datasets."""
    logger = logging.getLogger("terraguide.extract")
    logger.setLevel(logging.INFO)
    if not any(
        getattr(handler, "_terraguide_extract_handler", False)
        for handler in logger.handlers
    ):
        handler = logging.StreamHandler()
        handler._terraguide_extract_handler = True
        logger.addHandler(handler)

    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE")
    run_id = str(uuid.uuid4())
    client = None
    started = datetime.now(timezone.utc)
    operation = "validate_configuration"
    exit_status = 1

    try:
        if not isinstance(uri, str) or not uri.strip():
            raise ValueError("MONGODB_URI is missing or empty.")
        if database_name is not None and not database_name.strip():
            raise ValueError("MONGODB_DATABASE must not be empty when set.")

        operation = "connect"
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        operation = "ping"
        client.admin.command("ping")

        operation = "select_database"
        database = (
            client[database_name]
            if database_name
            else client.get_default_database()
        )
        if database is None:
            raise ValueError("Set MONGODB_DATABASE or include a database name in MONGODB_URI.")

        results = [
            extract_collection(database, name, config, run_id, logger)
            for name, config in DATASETS.items()
        ]
        exit_status = (
            1
            if any(
                item["extraction_status"] == "FAILED"
                or item.get("_log_write_failed", False)
                for item in results
            )
            else 0
        )

    except Exception as exc:
        fallback_error_code = (
            "MONGODB_CONFIGURATION_ERROR"
            if isinstance(exc, ConfigurationError) or operation == "validate_configuration"
            else (
                "MONGODB_DATABASE_SELECTION_ERROR"
                if operation == "select_database"
                else "MONGODB_CONNECTION_ERROR"
            )
        )
        error_code = _error_code(
            exc,
            fallback_error_code,
        )
        error_message = redact_error_message(exc)
        logger.error("Pipeline startup failed during %s: %s", operation, error_message)
        _safe_log_extraction(
            log_file=LOG_FILE, run_id=run_id, dataset_name="pipeline",
            collection_name=None, started=started, operation=operation,
            status="FAILED", records_read=0, records_extracted=0,
            records_rejected=0, checks={"connection": "FAIL"},
            output_file=None, error_code=error_code, error_message=error_message,
        )
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                print(
                    f"MongoDB client cleanup failed: {redact_error_message(exc)}",
                    file=sys.stderr,
                )
                exit_status = 1

    return exit_status


if __name__ == "__main__":
    raise SystemExit(main())