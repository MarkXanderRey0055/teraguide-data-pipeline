# Lab 4 — Data Transformation Documentation

This document describes the implemented Stage 2 behavior and approved B-01
integration. It is based on the available Stage 1 Parquet snapshot and Issues
#4–#8. Proposed cleaning and standardization rules are not described as
implemented. B-02, B-03, and B-04 are unimplemented.

## 1. Transformation Input and Processing Specification

Stage 2 reads exactly these logical datasets from timestamped Stage 1 Parquet
files in `data/raw/`:

| Logical dataset | Stage 1 prefix | Source collection |
|---|---|---|
| `transactions` | `transaction_listings_` | `transactions` |
| `buyers` | `buyer_listings_` | `buyerpreferences` |
| `properties` | `property_listings_` | `properties` |

The current available snapshot is `20261019T020004Z` for each dataset. Stage 2
does not connect directly to MongoDB and does not rename or modify Stage 1
files.

| Module | Responsibility |
|---|---|
| `src/transform.py` | Issue #4 lossless Parquet snapshot processing. |
| `src/transform_validation.py` | Issue #5 structural validation and quarantine. |
| `src/transform_logger.py` | Issue #6 transformation JSON Lines metadata logging. |
| `src/transform_business.py` | Approved B-01 dataset integration. |

Issue #4 selects the newest valid snapshot using the timestamp embedded in the
filename, not filesystem modification time. Invalid or unexpected filenames
and missing inputs fail explicitly.

There is no implemented scheduler, recurring frequency, automatic batch
window, or orchestration layer. Processing is invoked by the caller or command
line. The snapshot timestamp in the filename is the available handoff time;
an extraction run ID and extraction-to-transformation batch-window linkage are
not automatically populated. Validation and B-01 require an explicit
`batch_id`, while the logger accepts caller-supplied `run_id` and `batch_id`.
Reruns reuse deterministic output names and re-read the selected snapshots.

The transformation batch method is a local Python/Apache Arrow Parquet batch.
The batch size is the complete selected snapshot for each of the three input
datasets; no incremental window is implemented. Processing dependencies are:
read and verify all Stage 1 inputs, validate required schemas, then prepare
and publish outputs. B-01 requires buyer and property lookup data before
matching transactions. Late-arriving data is not separately handled; a later
Stage 1 snapshot must be selected and rerun. Failed runs are retried by
invoking the same operation again after the cause is corrected. Existing raw
files are not re-extracted by Stage 2.

The implemented scope is lossless copying for Issue #4, structural validation
and quarantine for Issue #5, metadata logging for Issue #6, and the approved
B-01 integration. No cleaning, standardization, derived fields, aggregation,
or general filtering is implemented.

Processed outputs:

```text
data/processed/property_listings.parquet
data/processed/buyer_listings.parquet
data/processed/transaction_listings.parquet
```

B-01 outputs:

```text
data/processed/integrated_transactions.parquet
data/quarantine/{batch_id}_unmatched_transactions.parquet
```

Issue #5 quarantine outputs use
`data/quarantine/{batch_id}_{dataset}.parquet`. Transformation logs default
to `logs/transformation/transformation_log.jsonl`; the path is configurable.

Outputs are prepared in temporary files and verified by reading them back before
publication. Repeated executions use deterministic names and do not append
duplicate records. Ordinary exceptions are handled explicitly. B-01 rollback is
not fully crash-safe: process termination or machine interruption between
sequential replacements could temporarily leave outputs inconsistent.

## 2. Data Profiling and Issue Identification

### 2.1 Source and method

The following files were profiled without modification:

```text
data/raw/buyer_listings_20261019T020004Z.parquet
data/raw/property_listings_20261019T020004Z.parquet
data/raw/transaction_listings_20261019T020004Z.parquet
```

The findings below apply only to this available snapshot and are not estimates
for future extracts.

### 2.2 Counts and schemas

| Dataset | Rows | Columns |
|---|---:|---:|
| `buyers` | 12 | 11 |
| `properties` | 12 | 17 |
| `transactions` | 3 | 11 |

Observed schemas:

- `buyers`: `_id string`, `userId string`, `__v int64`,
  `budgetMax int64`, `budgetMin int64`, `createdAt timestamp[us]`,
  `intendedUse string`, `landType string`, `location string`,
  `minLotSize int64`, `updatedAt timestamp[us]`.
- `properties`: `_id string`, `name string`, `owner string`,
  `location string`, `type string`, `price int64`, `status string`,
  `lat double`, `lng double`, `lotSize int64`, `documents string`,
  `createdAt timestamp[us]`, `updatedAt timestamp[us]`, `__v int64`,
  `images string`, `pricePerSqm int64`, `size int64`.
- `transactions`: `_id string`, `buyerId string`, `propertyId string`,
  `amount double`, `status string`, `notes string`, `createdBy string`,
  `completedAt timestamp[us]`, `createdAt timestamp[us]`,
  `updatedAt timestamp[us]`, `__v int64`.

### 2.3 Missing values

| Dataset | Field | Null records |
|---|---|---:|
| `buyers` | `budgetMin` | 1 |
| `buyers` | `budgetMax` | 1 |
| `buyers` | `intendedUse` | 1 |
| `buyers` | `landType` | 1 |
| `buyers` | `location` | 1 |
| `transactions` | `completedAt` | 2 |

No nulls were observed in buyer `userId`, property `_id`, transaction
`_id`, `buyerId`, or `propertyId`.

### 2.4 Issue register

The following register uses only observed counts. Percentages are calculated
against the row count of the affected dataset.

| Dataset/column | Issue type | Example | Affected | Planned treatment |
|---|---|---|---:|---|
| `buyers.budgetMin`, `budgetMax`, `intendedUse`, `landType`, `location` | Missing values | Buyer `_id` `6a65eacc376ea82640fdbde1` has nulls in all five fields | 1/12 records, 8.3% per field | C-01 remains proposed; no fill or rejection is implemented. |
| `transactions.completedAt` | Missing values | Transaction `_id` `6a94e5036d1d2a14d25a7be9` and `6aa8c65a4cddb221dddcefef` have null `completedAt` | 2/3 records, 66.7% | C-01 remains proposed; Issue #4 retains nulls. |
| `buyers.userId` | Duplicate identifiers | No duplicate values observed | 0/12, 0% | C-02 remains proposed; B-01 still requires deterministic lookup handling. |
| `properties._id` | Duplicate identifiers | No duplicate values observed | 0/12, 0% | C-02 remains proposed; B-01 still requires deterministic lookup handling. |
| `transactions._id` | Duplicate identifiers | No duplicate values observed | 0/3, 0% | C-02 remains proposed. |
| Transaction buyer/property references | Orphan references | No unmatched IDs observed | 0/3, 0% | B-01 uses its approved unmatched categories if a future snapshot contains one. |

The available data did not provide an observed example for whitespace,
inconsistent casing, invalid numeric/date values, invalid reference codes, or
outliers. These checks were not assessed by an approved implementation, so no
counts or sample values are claimed. Their possible treatments remain pending
C-03/C-04 or S-01–S-05 approval.

### 2.5 Duplicate and relationship findings

- Buyer `userId`: 12 non-null values, 12 unique, 0 duplicate extras.
- Property `_id`: 12 non-null values, 12 unique, 0 duplicate extras.
- Transaction `_id`: 3 non-null values, 3 unique, 0 duplicate extras.
- All 3 transaction `buyerId` values matched buyer `userId` values.
- All 3 transaction `propertyId` values matched property `_id` values.
- All 3 transactions had both B-01 lookup matches in this snapshot.

The profiling checked identifier uniqueness and documented relationships. No
business validity, casing, range, or correction findings are asserted because
no approved rules define those checks. The observed nulls are retained by
Issue #4; no values are silently replaced.

### 2.6 Profiling disposition

The profiling findings establish observed issues but do not authorize a new
cleaning or standardization behavior. Any future treatment must reference an
approved rule and preserve record-level traceability.

## 3. Data Cleaning Rules

C-01 through C-05 remain proposed and unapproved. The table uses the Lab 4
rule format; “no action” means no general cleaning behavior is currently
implemented.

| Rule ID and name | Target | Condition | Action | Justification | Status/traceability |
|---|---|---|---|---|---|
| C-01 Missing required values | `buyers`/`transactions`, required columns to be approved | A required value is null | Pending decision: retain, default, or quarantine; current Issue #4 retains the null | Required-field handling must be approved per dataset | Proposed; traces to observed nulls in Section 2.4. |
| C-02 Duplicate records | Identifier columns and duplicate criteria to be approved | A record or lookup key is duplicated | Pending decision on record retention; B-01 separately resolves approved lookup duplicates by latest `updatedAt` | Prevent duplicate records or join multiplication | Proposed; no duplicate extras were observed in this snapshot. |
| C-03 Invalid values | Fields and valid ranges/formats to be approved | A value fails an approved type, format, or range rule | Pending correction or quarantine; no general correction is implemented | Prevent unsafe values without inventing replacements | Proposed; no invalid-value finding was assessed. |
| C-04 Inconsistent values/whitespace | Text/reference columns to be approved | Whitespace, casing, characters, or representations differ | Pending mapping or trimming rule; no normalization is implemented | Prevent false mismatches while preserving traceability | Proposed; no such finding was confirmed. |
| C-05 Orphan references | Transaction `buyerId`/`propertyId` | A foreign key has no parent match | B-01 classifies and quarantines unmatched transactions using its approved reasons | Preserve rejected records and make relationship failures measurable | Proposed as cleaning; B-01 is the implemented approved treatment. |

Issue #4 preserves schemas, rows, nulls, and values. It does not replace
missing values, normalize text, correct invalid values, or silently filter
records. No cleaning rule is operational until its logic and approval are
defined.

## 4. Standardization and Data Type Rules

S-01 through S-05 remain proposed categories. The physical types below are
observed source types, not approved conversion targets.

| Rule ID and name | Target/source type | Proposed target/format | Conversion or failure behavior | Status |
|---|---|---|---|---|
| S-01 Date fields | `createdAt`, `updatedAt`, `completedAt`; observed `timestamp[us]` | Target date/time and timezone policy not approved | No conversion; failures are not handled by a general standardizer | Proposed. |
| S-02 Numeric fields | Amount/coordinates observed as `double`; integer measures observed as `int64` | Precision, scale, and rounding not approved | No conversion or rounding; failures are not silently transformed | Proposed. |
| S-03 Text fields | Text columns observed as `string` | Casing, whitespace, and character policy not approved | No trim/case conversion | Proposed. |
| S-04 Reference values/codes | IDs and status-like fields observed as `string` | Approved code mappings not defined | Unmapped-value behavior is pending | Proposed. |
| S-05 Missing/null values | Nulls observed in Section 2.3 | Retain-null/default policy not approved | Issue #4 preserves nulls; no defaults are invented | Proposed. |

No currency, unit, region-code, or reference-value mapping is implemented or
asserted. No source-to-target type conversion is introduced by Issue #4.

Issue #4 preserves source Parquet schemas and values. B-01 constructs and
verifies its approved output schema; this is contract enforcement, not a
general standardization rule.

## 5. Business Transformation Rules

### 5.1 Approved B-01 scope

B-01 is approved and implemented in `src/transform_business.py`. It uses only
`transactions`, `buyers`, and `properties`. `Audit` and
`BuyerPreference` are not independent inputs. B-02, B-03, and B-04 are
unimplemented.

### 5.2 Joins and cardinality

Both approved joins are inner joins:

```sql
transactions INNER JOIN buyers
    ON transactions.buyerId = buyers.userId
INNER JOIN properties
    ON transactions.propertyId = properties._id
```

One buyer and one property may match many transactions; each integrated
transaction has one matched buyer and one matched property. The output has one
row per matched transaction.

### 5.3 Lookup deduplication

Buyers are keyed by `userId`; properties by `_id`. Each duplicate-key group
retains the record with the latest `updatedAt`. A missing or tied latest
timestamp fails. A null buyer `userId` or property `_id` fails explicitly.

### 5.4 Unmatched handling

| Condition | Reason |
|---|---|
| No buyer match, property match | `MISSING_BUYER` |
| Buyer match, no property match | `MISSING_PROPERTY` |
| Neither buyer nor property match | `MISSING_BUYER_AND_PROPERTY` |

Each unmatched transaction is classified once, retains its original 11
transaction fields, and receives `exclusion_reason` in the B-01 quarantine
output. Null transaction foreign keys are missing matches.

```text
Input Transactions = Integrated Transactions + Quarantined Transactions
```

B-01 does not introduce business filters, derived fields, aggregations, revenue
calculations, or unapproved joins. Ordinary exceptions roll back, but
publication is not fully crash-safe across process or machine interruption.

| Rule ID/name | Input columns | Logic | Output | Business reason |
|---|---|---|---|---|
| B-01 Dataset Integration | Transaction `buyerId`, `propertyId`; buyer `userId`; property `_id`; lookup `updatedAt` | Deduplicate lookups by latest timestamp, then perform the two approved inner joins | One integrated row per matched transaction; unmatched rows go to categorized quarantine | Add verified buyer and property context to transactions. |

No B-01 derived columns, calculations, aggregations, dimensional-modeling
steps, surrogate keys, or business filters are approved. B-02–B-04 remain
unimplemented.

## 6. Transformed Data Contract

### 6.1 Issue #4 outputs

Each latest Stage 1 table is written losslessly to:

```text
data/processed/property_listings.parquet
data/processed/buyer_listings.parquet
data/processed/transaction_listings.parquet
```

### 6.2 B-01 integrated output

`data/processed/integrated_transactions.parquet` contains exactly 29 columns
in this order:

```text
_id, buyerId, propertyId, amount, status, notes, createdBy, completedAt,
createdAt, updatedAt, __v, buyer_userId, buyer_budgetMin, buyer_budgetMax,
buyer_intendedUse, buyer_landType, buyer_location, buyer_minLotSize,
property_name, property_owner, property_location, property_type, property_price,
property_status, property_lat, property_lng, property_lotSize,
property_pricePerSqm, property_size
```

| Columns | Physical type |
|---|---|
| `_id`, `buyerId`, `propertyId` | `string` |
| `amount` | `double` |
| `status`, `notes`, `createdBy` | `string` |
| `completedAt`, `createdAt`, `updatedAt` | `timestamp[us]` |
| `__v` | `int64` |
| `buyer_userId`, `buyer_intendedUse`, `buyer_landType`, `buyer_location` | `string` |
| `buyer_budgetMin`, `buyer_budgetMax`, `buyer_minLotSize` | `int64` |
| `property_name`, `property_owner`, `property_location`, `property_type`, `property_status` | `string` |
| `property_price`, `property_lotSize`, `property_pricePerSqm`, `property_size` | `int64` |
| `property_lat`, `property_lng` | `double` |

The first 11 fields are unchanged transaction fields. Buyer/property internal
`_id`, `__v`, and other unselected source fields are excluded. Written
Parquet schemas and values are read back for verification.

The implementation verifies physical types and column order. It does not add
business nullability constraints beyond the source schema; observed nullable
fields remain nullable. B-01 requires transaction `_id`, buyer `userId`, and
property `_id` to be usable identifiers. The approved logical foreign keys are
`transactions.buyerId -> buyers.userId` and
`transactions.propertyId -> properties._id`; unmatched transaction rows are
quarantined rather than silently removed. No surrogate keys are generated.

The source-to-target mapping is:

| Target group | Source columns | Target columns |
|---|---|---|
| Transactions | The 11 transaction source fields | Same names and order. |
| Buyers | `userId`, `budgetMin`, `budgetMax`, `intendedUse`, `landType`, `location`, `minLotSize` | Corresponding `buyer_`-prefixed columns. |
| Properties | `name`, `owner`, `location`, `type`, `price`, `status`, `lat`, `lng`, `lotSize`, `pricePerSqm`, `size` | Corresponding `property_`-prefixed columns. |

Missing or mismatched required input columns/types fail B-01 validation. Issue
#4 rejects unexpected snapshot filenames and missing inputs; it preserves the
selected input schema rather than silently adding, removing, or renaming
columns. Configured Issue #5 validation reports missing and unexpected columns.

### 6.3 B-01 quarantine output

`data/quarantine/{batch_id}_unmatched_transactions.parquet` contains the 11
transaction fields with their required types plus
`exclusion_reason: string`.

## 7. Validation, Reconciliation, and Quarantine

Issue #5 implements these structural checks:

| Rule ID/check name | Target and purpose | Measurable pass/fail criterion | Severity |
|---|---|---|---|
| `V-PK-001` primary key uniqueness | Configured dataset primary key; prevent duplicate or missing identifiers | Pass when every key is non-null and unique; fail when affected count is greater than 0 | Critical |
| `V-REQ-001` required-field null check | Configured required columns; prevent incomplete accepted records | Pass when configured required-field null count is 0; fail otherwise | Critical |
| `V-REF-001` referential integrity | Configured child reference to parent identifier; prevent orphan relationships | Pass when every non-null child reference matches a non-null parent ID; null child references and unmatched values fail | Critical |
| `V-SCHEMA-001` schema/type conformance | Configured output schema and data types | Pass when configured schema exactly matches; fail on type/schema mismatch | Critical |
| `V-SCHEMA-001` expected/unexpected columns | Configured column set; detect schema drift without silently changing data | Pass independently when missing or unexpected count is 0; current column-list severity is Non-critical | Non-critical |
| `V-REC-001` record reconciliation | Dataset accepted/rejected disposition; detect loss | Pass when `input_count = accepted_count + uniquely_rejected_count`; fail otherwise | Critical |
| `V-REF-002` buyer relationship skipped | Transaction buyer relationship when no approved/configured field is supplied | Report `SKIPPED`; do not mark passed and do not quarantine from this skipped check | Non-critical |

For B-01 joins, one output row per matched transaction is expected. Lookup
deduplication must prevent row multiplication; unmatched counts must equal the
three documented quarantine categories. No aggregation, measure reconciliation,
or negative-value business check is implemented because B-01 defines no such
calculation. Extraction connectivity and source-to-staging checks remain Stage
1 responsibilities and are not duplicated here.

Rows failing multiple checks are merged by source row identity and appear once
with all reasons and rule IDs. Modified-but-retained rows are not rejected.
Referential-integrity failures participate in reconciliation.

Rejected records under `data/quarantine/` retain source fields plus
`quarantine_reason`, `batch_id`, and `rule_id`. Quarantine is prepared
before publication; temporary files are cleaned on failure. Stale quarantine
files are not removed during preparation and are cleaned during publication
after all new quarantine files have been prepared. An explicit batch ID is
required. No rejection-rate threshold is approved or implemented.

## 8. Transformation Logging and Metadata

Issue #6 provides a separate structured JSON Lines logger in
`src/transform_logger.py`. Each dataset-level record supports:

```text
run_id, parent_run_id, batch_id, dataset_name, input_file, output_file,
start_time, end_time, duration_seconds, status, records_read, records_output,
records_rejected, records_modified, rules_applied, validation_status,
validation_checks, error_code, error_message, code_version, rule_version
```

Timestamps are UTC ISO 8601 strings ending in `Z`. Validation reports/checks
can be serialized. `rules_applied` is empty when no rules are supplied.
Versions come from environment variables or use `unknown`. The default path
is `logs/transformation/transformation_log.jsonl`, configurable by argument,
`TRANSFORMATION_LOG_PATH`, or `TRANSFORMATION_LOG_DIR`. Write failures
raise `TransformationLoggingError`. The logger does not add orchestration.

| Field | Purpose | Example or format | Current status |
|---|---|---|---|
| `run_id` | Identifies a transformation execution | `RUN_20261019_0200` | Accepted by logger; caller supplied. |
| `parent_run_id` | Links to an extraction/pipeline run | `EXT_20261019_0200` or `null` | Accepted; not automatically populated. |
| `batch_id` | Identifies the validation/quarantine batch | `batch-20261019` | Explicit for validation/B-01; logger generates one if omitted. |
| Batch window | Describes the processed date range | `2026-10-18/2026-10-19` | Not available in the snapshot filenames and not automatically populated. |
| `dataset_name` | Identifies the dataset/file | `transaction_listings` | Supported. |
| `input_file`, `output_file` | Records lineage paths | `data/raw/transaction_listings_...parquet` | Supported when supplied by caller. |
| `start_time`, `end_time` | Records execution boundaries | UTC ISO 8601 ending in `Z` | Implemented. |
| `duration_seconds` | Measures execution duration | `12.5` | Implemented. |
| `status` | Records success/failure state | `SUCCESS` or `FAILED` | Caller supplied; logger does not infer transformation outcome. |
| `records_read`, `records_output`, `records_rejected` | Records row dispositions | `3`, `3`, `0` | Supported; caller supplied. |
| `records_modified` | Counts retained modified rows | `0` | Supported; no cleaning rules currently populate it. |
| `rules_applied` | Identifies affected rules/counts | `[]` | Empty when none supplied; no rules invented. |
| `validation_status`, `validation_checks` | Captures validation result/detail | `PASSED`, serialized check objects | Supported from `ValidationReport`/`CheckResult`. |
| `error_code`, `error_message` | Supports troubleshooting | `READ_ERROR`, message text | Supported for failed records. |
| `code_version`, `rule_version` | Identifies implementation/rule versions | Environment value or `unknown` | Implemented fallback; not auto-derived. |

The metadata supports monitoring through status/counts, troubleshooting through
errors and checks, auditing through run/batch/file lineage, and recovery by
identifying the failed batch and its outputs. Because the logger is not wired
into an automatic orchestrator, these fields are populated only when the
caller invokes the logger with the available metadata.

## 9. Retention, Access, and Deletion

Raw Stage 1 files are preserved. Processed outputs are not silently deleted by
validation. Rejected records are retained in quarantine; stale quarantine
output is prepared for removal during publication only after the new quarantine
preparations succeed; a later publication failure remains an explicit
limitation. Temporary files are removed after publication or failure cleanup.

No retention periods, archival rules, access roles, encryption requirements,
or deletion schedules are approved in the project requirements. None are
invented here. Logs and rejected records should not be deleted while needed
for troubleshooting, auditing, or recovery; the formal period and responsible
deletion process require team approval. Repository/filesystem permissions and
implemented cleanup are the only documented controls until policy approval.

| Record type | Owner/use | Location | Retention/access/deletion status |
|---|---|---|---|
| Transformation logs | Stage 2 operations; monitoring, audit, troubleshooting | `logs/transformation/` | Period, read/write/delete roles, archive process, and responsible deletion owner are pending approval. |
| Issue #5 validation quarantine | Stage 2 operations/data-quality review; rejected records and reasons | `data/quarantine/{batch_id}_{dataset}.parquet` | No approved retention period or access role; do not delete while needed for troubleshooting, audit, or recovery. |
| B-01 unmatched transactions | Stage 2 operations/data-quality review; unmatched source records | `data/quarantine/{batch_id}_unmatched_transactions.parquet` | No approved retention period or deletion owner; preserved by implementation until an approved policy applies. |
| Raw Stage 1 snapshots | Extraction handover and transformation reproducibility | `data/raw/` | Stage 2 preserves them; archival and deletion policy is pending. |

No unnecessary sensitive source data is added to logs. Quarantine preserves the
source records required for traceability by the approved validation/B-01
contracts; any future minimization policy requires team approval.

## 10. Transformation Acceptance and Handover

A technically acceptable run must read valid timestamped inputs, preserve raw
files, produce verified documented schemas, reconcile records where applicable,
avoid silent rejection, publish quarantine metadata, and clean temporary files
after ordinary success or failure.

The project has not approved a rejection-rate threshold. No numeric threshold
or post hoc acceptance value is invented.

| Validation condition | Acceptance decision | Pipeline action | Decision type | Responsible role |
|---|---|---|---|---|
| Required inputs/schema/output verification succeeds; critical checks pass; reconciliation passes | Technically eligible for handover | Retain outputs and provide handover artifacts | Automatic behavior of the implemented components; final Load approval is not implemented | Not assigned in project requirements |
| Non-critical warnings or skipped/configuration checks are present | Review required before handover | Hold for review; no automatic Load action exists | Manual decision required, but reviewer role is not assigned | Pending team assignment |
| Missing/unreadable input, schema failure, duplicate timestamp failure, critical validation failure, reconciliation failure, or output publication failure | Rejected/failed | Stop the affected operation; preserve raw/existing outputs; quarantine records where the implemented validator/B-01 supports it; retry by rerunning after correction | Automatic failure behavior | Not assigned in project requirements |

The implementation does not automatically retry, approve Load, or enforce a
rejection-rate threshold. Validation and B-01 require explicit batch context;
the caller decides whether and when to rerun. Load Stage handover consists of
the processed/B-01 files, quarantine paths and counts, validation reports, and
transformation metadata logs when invoked. Database loading is out of scope.

Cleaning and standardization acceptance cannot be declared until C-01–C-05 and
S-01–S-05 receive approval and complete specifications.

Handover artifacts include input snapshot paths/timestamps, processed and B-01
outputs, quarantine paths and counts, validation reports, transformation logs
when invoked, and `docs/testing/transformation_test_results.md`.

The Issue #8 verification recorded 4 focused tests passed and 69 complete-suite
tests passed.

## 11. Common Transformation Problems and Mitigation

| Problem | Possible cause | Impact | Mitigation |
|---|---|---|---|
| Missing Stage 1 snapshot | Extraction handover absent or wrong path | Batch cannot begin | Fail explicitly with `TransformationError`; correct handover and rerun. |
| Invalid/unexpected filename | Snapshot naming or timestamp does not match contract | Wrong/latest input cannot be selected | Validate filename before processing; correct handoff or rerun. |
| Unreadable/corrupt Parquet | Incomplete or corrupted Stage 1 file | No safe transformed output | Fail read; preserve existing output and raw files. |
| Schema/type mismatch | Source schema changed or required column is missing | Output contract cannot be guaranteed | Fail required checks; Issue #5 can report configured schema drift; obtain schema decision. |
| Duplicate lookup key | Repeated buyer/property identifier | Arbitrary match or row multiplication | Select latest `updatedAt`; fail missing/tied latest timestamps. |
| Null lookup key | Buyer `userId` or property `_id` is null | Lookup record cannot be safely matched | Fail B-01 with dataset/key; do not discard silently. |
| Unmatched reference | Transaction foreign key has no parent | Transaction would be lost from inner join | Classify once and preserve in B-01 quarantine. |
| Multiple validation failures | One row violates several checks | Duplicate quarantine counts or incomplete reasons | Merge by row identity with all reasons/rule IDs. |
| Quarantine publication failure | Filesystem/write/replace error | Rejected evidence may be incomplete | Clean temporary files and preserve processed outputs; raise failure. |
| Stale zero-rejection quarantine | Rerun has no current rejects | Old file misrepresents current result | Clean stale file during publication after preparation. |
| Logger write failure | Log directory or filesystem unavailable | Monitoring/audit metadata is missing | Raise `TransformationLoggingError`; caller must handle failed logging. |
| Process/machine interruption | Crash between sequential replacements | Outputs may temporarily be inconsistent | Documented limitation; future atomic/journaled publication decision required. |
| Unapproved rule request | Missing approved cleaning/standardization specification | Unsafe guessed transformation | Stop and obtain rule approval and complete specification. |

## Implementation and approval status

| Area | Status |
|---|---|
| Issue #4 core transformation | Implemented and tested. |
| Issue #5 validation/quarantine | Implemented and tested. |
| Issue #6 logging/metadata | Implemented and tested as a separate component. |
| B-01 integration | Approved, implemented, and tested. |
| C-01–C-05 cleaning | Proposed; pending approval. |
| S-01–S-05 standardization | Proposed; pending approval. |
| B-02–B-04 | Unimplemented. |
| Database loading/load stage | Out of scope and unimplemented. |
| Retention/access/deletion policy | Not approved; no assumptions made. |
