"""Extract TerraGuide dashboard datasets from MongoDB to Parquet."""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from extract_logger import log_extraction

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


def extract_collection(database, dataset_name, config, run_id, logger):
    """Extract one collection, validate basic requirements, and save a Parquet file."""
    started = datetime.now(timezone.utc)
    collection_name = config["collection"]
    output_file = None
    records = []
    rejected = 0
    status = "FAILED"
    error_code = None
    error_message = None
    checks = {}

    try:
        if collection_name not in database.list_collection_names():
            raise LookupError(f"Collection '{collection_name}' was not found.")

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

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime("%Y%m%dT%H%M%SZ")
        output_path = RAW_DIR / f"{dataset_name}_{stamp}.parquet"

        if records:
            table = pa.Table.from_pylist(records)
        else:
            table = pa.table({"_id": pa.array([], type=pa.string())})

        pq.write_table(table, output_path, compression="snappy")
        saved_count = pq.read_metadata(output_path).num_rows

        checks = {
            "collection_exists": True,
            "required_fields_missing_records": missing_fields,
            "duplicate_ids": duplicate_ids,
            "parquet_count_matches": saved_count == records_read,
        }

        if saved_count != records_read:
            error_code = "RECORD_COUNT_MISMATCH"
            error_message = f"Read {records_read} records but wrote {saved_count} rows."
        elif missing_fields or duplicate_ids:
            status = "PARTIAL"
            error_code = "DATA_VALIDATION_WARNING"
            error_message = (
                f"Missing-field records: {missing_fields}; duplicate IDs: {duplicate_ids}. "
                "All records were retained in the raw file."
            )
        else:
            status = "SUCCESS"

        output_file = str(output_path.relative_to(ROOT))
        logger.info("%s: %s (%d records)", dataset_name, status, saved_count)
        return log_extraction(
            log_file=LOG_FILE, run_id=run_id, dataset_name=dataset_name,
            collection_name=collection_name, started=started,
            status=status, records_read=records_read, records_extracted=saved_count,
            records_rejected=rejected, checks=checks, output_file=output_file,
            error_code=error_code, error_message=error_message,
        )

    except LookupError as exc:
        error_code, error_message = "COLLECTION_NOT_FOUND", str(exc)
    except PyMongoError as exc:
        error_code, error_message = "MONGODB_EXTRACTION_ERROR", str(exc)
        logger.exception("MongoDB extraction failed for %s", dataset_name)
    except (pa.ArrowException, OSError, TypeError, ValueError) as exc:
        error_code, error_message = "PARQUET_OR_VALIDATION_ERROR", str(exc)
        logger.exception("Parquet writing or validation failed for %s", dataset_name)
    except Exception as exc:
        error_code, error_message = "UNEXPECTED_ERROR", str(exc)
        logger.exception("Unexpected error for %s", dataset_name)

    return log_extraction(
        log_file=LOG_FILE, run_id=run_id, dataset_name=dataset_name,
        collection_name=collection_name, started=started, status="FAILED",
        records_read=len(records), records_extracted=0, records_rejected=rejected,
        checks=checks, output_file=output_file, error_code=error_code,
        error_message=error_message,
    )


def main():
    """Run full-snapshot extraction for the three dashboard datasets."""
    logger = logging.getLogger("terraguide.extract")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE")
    if not uri:
        logger.error("MONGODB_URI is missing from .env or the environment.")
        return 1

    run_id = str(uuid.uuid4())
    client = None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        database = client[database_name] if database_name else client.get_default_database()
        if database is None:
            raise ValueError("Set MONGODB_DATABASE or include a database name in MONGODB_URI.")

        results = [
            extract_collection(database, name, config, run_id, logger)
            for name, config in DATASETS.items()
        ]
        return 1 if any(item["extraction_status"] == "FAILED" for item in results) else 0

    except (PyMongoError, ValueError) as exc:
        logger.exception("Could not connect to MongoDB or select the database.")
        log_extraction(
            log_file=LOG_FILE, run_id=run_id, dataset_name="pipeline",
            collection_name=None, started=datetime.now(timezone.utc),
            status="FAILED", records_read=0, records_extracted=0,
            records_rejected=0, checks={"connection": "FAIL"},
            output_file=None, error_code="CONNECTION_OR_CONFIGURATION_ERROR",
            error_message=str(exc),
        )
        return 1
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())