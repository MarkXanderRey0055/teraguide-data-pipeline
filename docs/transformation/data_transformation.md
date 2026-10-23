# Data Transformation Documentation

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

The transformation stage receives the extracted datasets produced by Stage 1. The expected input format is Parquet, stored in the `data/raw/` directory.

The input datasets correspond to the following collections:

- `properties`
- `buyerlistings`
- `transactions`

The transformation stage must use the extracted files handed over by Stage 1 rather than querying the source database directly.

### 1.2 Processing Script

The transformation logic will be implemented in `src/transform.py`.

### 1.3 Output Destination

Transformed datasets are intended to be stored in `data/processed/`. Records that cannot be processed may be stored separately in `data/quarantine/`, with processing logs stored in `logs/transformation/`.

These paths describe the planned Stage 2 structure and must be verified against the implementation.