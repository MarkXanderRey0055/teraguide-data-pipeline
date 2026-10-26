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

## 2. Data Profiling and Issue Identification

### 2.1 Purpose

Data profiling is performed before transformation to understand the structure, completeness, consistency, and quality of the extracted datasets. The profiling results will help identify potential data quality issues and determine the appropriate cleaning and transformation rules.

### 2.2 Datasets to Be Profiled

The following datasets are included in the profiling process:

- `properties`
- `buyerlistings`
- `transactions`

The datasets will be read from the extracted Parquet files provided by Stage 1.

### 2.3 Profiling Checks

The following checks will be performed on the extracted datasets:

| Profiling Check | Description | Purpose |
|---|---|---|
| Record Count | Count the number of records in each dataset. | Establish the initial dataset size for reconciliation. |
| Missing Values | Identify null or missing values in relevant fields. | Determine whether required information is incomplete. |
| Duplicate Records | Check for duplicate records and repeated identifiers where applicable. | Identify possible duplication issues. |
| Data Types | Inspect the data types of the available fields. | Identify fields that may require type standardization. |
| Invalid Values | Check for values that do not meet the expected format or constraints. | Identify records that may require correction or rejection. |
| Dataset Relationships | Check relevant identifiers and references between datasets. | Identify unmatched or inconsistent relationships. |

### 2.4 Profiling Results

The results of these checks will be recorded after inspecting the actual extracted datasets. Each identified issue should include the affected dataset, relevant field, observed number of records, and proposed handling.

Profiling counts and findings must be based on the actual data and must not be estimated or fabricated.

### 2.5 Use of Profiling Results

The profiling results will serve as the basis for defining the data cleaning rules in Section 3 and the standardization rules in Section 4. Issues that are not observed in the extracted data should not be reported as confirmed findings.