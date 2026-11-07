# Data Transformation Documentation

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

The transformation stage receives the extracted datasets produced by Stage 1. The expected input format is Parquet, stored in the `data/raw/` directory.

The Stage 2 input datasets are exactly these three logical datasets:

- `transactions`
- `buyers`
- `properties`

Stage 2 reads the existing Stage 1 Parquet outputs from `data/raw/`. The
logical-to-physical naming relationship is:

| Logical Stage 2 dataset | Stage 1 physical dataset key/output prefix | MongoDB source collection |
|---|---|---|
| `transactions` | `transaction_listings` | `transactions` |
| `buyers` | `buyer_listings` | `buyerpreferences` |
| `properties` | `property_listings` | `properties` |

The Stage 1 physical files and extraction scripts are not renamed or modified.

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

- `transactions`
- `buyers`
- `properties`

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

## 3. Data Cleaning Rules

### 3.1 Purpose

Data cleaning prepares the extracted datasets for subsequent transformation by addressing data quality issues identified during profiling. Cleaning operations must follow documented rules to ensure that records are handled consistently and that changes can be traced.

### 3.2 Proposed Cleaning Rules

| Rule ID | Data Quality Issue | Proposed Handling | Record Handling |
|---|---|---|---|
| C-01 | Missing values in required fields | Identify records with missing required values and evaluate them according to the applicable dataset requirements. | Retain or quarantine according to the defined rule. |
| C-02 | Duplicate records | Identify duplicate records using the appropriate identifiers or documented duplicate criteria. | Retain one valid record or quarantine duplicates according to the approved rule. |
| C-03 | Invalid field values | Validate values against the expected format, data type, or applicable constraints. | Correct only when a reliable correction rule exists; otherwise quarantine the affected record. |
| C-04 | Inconsistent values | Identify inconsistent representations of the same type of information. | Standardize values when a documented mapping rule is available. |
| C-05 | Unmatched dataset references | Identify records whose relevant references do not match the related dataset. | Handle according to the documented relationship and business requirements. |

### 3.3 Record Retention and Rejection

Records must not be silently removed during cleaning. Any record that is rejected or quarantined must be counted and documented, including the affected dataset and the reason for rejection.

Records should only be corrected when the applicable correction rule is clear and supported by the available data. Missing or invalid information must not be replaced with arbitrary values.

### 3.4 Traceability

Each cleaning operation should be documented so that the team can identify the rule applied and determine its effect on the dataset. Where implemented, processing logs should record the number of records affected by each rule.

### 3.5 Implementation Status

The rules in this section are proposed cleaning specifications. They must be reviewed against the actual profiling results and implemented in the transformation script before they can be considered operational.

## 4. Standardization and Data Type Rules

### 4.1 Purpose

Standardization ensures that data across the extracted datasets follows consistent data types, formats, and reference values. These rules define how important fields will be converted and standardized during the transformation stage.

### 4.2 Standardization Rules

The following table defines the proposed standardization rule categories to be
evaluated for the `properties`, `buyers`, and `transactions` datasets.

| Rule ID | Column | Source Format | Standard Format | On Failure |
|---|---|---|---|---|
| S-01 | Date fields | Existing date, datetime, or string representation | Consistent date or datetime format, where applicable | Reject and log if conversion fails |
| S-02 | Numeric fields | Integer, float, or numeric string | Appropriate numeric data type and precision | Reject and log if conversion fails |
| S-03 | Text fields | String values with inconsistent casing or whitespace | Consistent text representation according to field requirements | Log or handle according to the applicable rule |
| S-04 | Reference values and codes | Inconsistent representations of the same value | Approved standard value or code | Flag and log unmapped values |
| S-05 | Missing or null values | Null, empty, or missing values | Retain null or apply an approved default, where appropriate | Apply the documented missing-value rule |

*Note: These are proposed rule categories. Exact column names, source formats, target formats, and failure handling must be verified against the extracted schemas and project requirements.*

### 4.3 Data Type Mapping

The target data type for each important column must be documented before conversion. The mapping should identify the dataset, source column, source data type, target data type, and conversion rule.

### 4.4 Format Standardization

Date and time values must follow a consistent representation where applicable. Numeric fields must use appropriate precision and rounding rules. Text casing and whitespace should be standardized only when appropriate for the field.

Currency, units of measurement, and country or region codes must follow documented conventions where those fields exist.

### 4.5 Reference Values and Codes

Inconsistent values representing the same meaning should be mapped to an approved standard value when supported by the data and project requirements.

For example, values such as `M`, `Male`, and `male` may be mapped to `MALE` if the field represents gender and the mapping is approved. This is an illustrative example, not a confirmed rule for the current datasets.

### 4.6 Handling Conversion Failures

Values that cannot be converted or mapped must be handled according to the applicable rule. Depending on the issue, the value may be flagged, logged, or the record may be rejected and quarantined.

Conversion failures must not be silently ignored. The affected records and reasons for failure should be traceable.

### 4.7 Implementation Status

The rules in this section describe the intended standardization requirements. Final mappings and conversion behavior will be confirmed after inspecting the extracted dataset schemas and validating the applicable rules.

## 5. Business Transformation Rules

### 5.1 Purpose and Dataset Scope

The approved Stage 2 business integration uses exactly these three logical
datasets:

- `transactions`
- `buyers`
- `properties`

Stage 2 must read the corresponding Stage 1 Parquet outputs from `data/raw/`.
`Audit` and `BuyerPreference` are not independent Stage 2 input datasets.

### 5.2 Approved Dataset Relationships and Joins

The approved join conditions are:

- `transactions.buyerId = buyers.userId`
- `transactions.propertyId = properties._id`

Both joins are `INNER JOIN`s. Their approved cardinalities are:

- One buyer can have many transactions (1:N).
- One property can have many transactions (1:N).
- Each transaction references one buyer and one property (N:1).
- The integrated output preserves one row per matched transaction.

The equivalent SQL is:

```sql
transactions
INNER JOIN buyers
    ON transactions.buyerId = buyers.userId
INNER JOIN properties
    ON transactions.propertyId = properties._id
```

#### B-01 — Approved Dataset Integration

| Item | Approved requirement |
|---|---|
| Rule ID | B-01 |
| Rule name | Dataset Integration |
| Required inputs | `transactions`, `buyers`, `properties` and the approved join keys above |
| Logic | Deduplicate lookup datasets according to Section 5.3, then apply the two approved inner joins |
| Output | `integrated_transactions`, one row per matched transaction |
| Business justification | Combine each transaction with its verified buyer and property context |

Only matched transactions are included in the integrated output. Unmatched
transactions are preserved in quarantine according to Section 5.4.

### 5.3 Approved Duplicate-Key Handling

Lookup datasets must be deduplicated before joining:

| Dataset | Lookup key | Approved handling |
|---|---|---|
| `buyers` | `userId` | Retain the record with the most recent `updatedAt` |
| `properties` | `_id` | Retain the record with the most recent `updatedAt` |

Validation must fail when `updatedAt` is missing in a duplicate-key group or
when multiple records share the latest `updatedAt`. Duplicate records must not
be selected arbitrarily.

### 5.4 Unmatched Transaction Handling

Unmatched transactions are classified into exactly one mutually exclusive
category:

| Condition | Reason |
|---|---|
| No buyer match, property match | `MISSING_BUYER` |
| Buyer match, no property match | `MISSING_PROPERTY` |
| Neither buyer nor property match | `MISSING_BUYER_AND_PROPERTY` |

For every unmatched transaction:

- Preserve the original transaction record in quarantine.
- Record the transaction key and exclusion reason.
- Classify the transaction exactly once.
- Maintain a separate count for each category.
- Exclude the unmatched transaction from `integrated_transactions`.

The reconciliation requirement is:

`Input Transactions = Integrated Transactions + Quarantined Transactions`

Any reconciliation difference causes validation to fail.

### 5.5 Integrated Output Contract

The approved integrated output is named `integrated_transactions` and has one
row per successfully matched transaction. It contains exactly 29 selected
columns:

- 11 transaction fields, unchanged.
- 7 buyer fields prefixed with `buyer_`.
- 11 property fields prefixed with `property_`.

Buyer and property internal `_id` and `__v` fields, along with all other
unselected source fields, are excluded. No inferred type conversions are
introduced; the following physical Parquet types are authoritative.

#### Transactions

| Column | Type |
|---|---|
| `_id` | `string` |
| `buyerId` | `string` |
| `propertyId` | `string` |
| `amount` | `double` |
| `status` | `string` |
| `notes` | `string` |
| `createdBy` | `string` |
| `completedAt` | `timestamp[us]` |
| `createdAt` | `timestamp[us]` |
| `updatedAt` | `timestamp[us]` |
| `__v` | `int64` |

#### Buyers

| Column | Type |
|---|---|
| `buyer_userId` | `string` |
| `buyer_budgetMin` | `int64` |
| `buyer_budgetMax` | `int64` |
| `buyer_intendedUse` | `string` |
| `buyer_landType` | `string` |
| `buyer_location` | `string` |
| `buyer_minLotSize` | `int64` |

#### Properties

| Column | Type |
|---|---|
| `property_name` | `string` |
| `property_owner` | `string` |
| `property_location` | `string` |
| `property_type` | `string` |
| `property_price` | `int64` |
| `property_status` | `string` |
| `property_lat` | `double` |
| `property_lng` | `double` |
| `property_lotSize` | `int64` |
| `property_pricePerSqm` | `int64` |
| `property_size` | `int64` |

### 5.4 Handling Excluded and Unmatched Records

Records excluded by business filters or left unmatched during dataset integration must be accounted for and documented.

The reason for exclusion or non-matching must be identifiable. Records must not be silently discarded during business transformation.

### 5.5 Implementation Status

The transformation operations listed in this section represent the proposed business transformation specifications. Specific join keys, filtering conditions, derived fields, and aggregation formulas must be confirmed against the project requirements and actual dataset structures before implementation.
