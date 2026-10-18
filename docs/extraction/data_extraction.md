# Data Extraction Documentation

## Data Source

###  1. Source System
- System Name:
TerraGuide
- Purpose:
TerraGuide is a property management and real estate platform used to manage property listings, buyer-related data, transactions, and other operational records.

### 2. Source Database
- **Database Management System:** MongoDB
- **Source:** TerraGuide operational database


### 3. Extraction Method

- **Field and logic used to determine the extraction window:**  
   The field used is **`updatedAt`**, which records when a document was created or modified. The extraction window uses `Last_Successful_Run_Timestamp` as the starting point and `Current_Execution_Time` as the endpoint to identify newly added or updated records.

- **How data will be extracted:**  
   Data will be extracted directly from the **MongoDB Atlas** collections using a filtered database query. The retrieved documents will be processed, cleaned, and transformed into a tabular format for further ETL processing.

- **Extraction tool, script, or technology:**  
   A **Python script** will be used, utilizing **PyMongo** to connect to MongoDB and retrieve records, **Pandas** to process the extracted data, and **PyArrow** to serialize the data into Parquet format.

- **Full or incremental extraction:**  
   The system will primarily use **incremental extraction** to retrieve only newly added or modified records, reducing unnecessary database and network processing. An initial **full extraction** will be performed once to establish the baseline dataset.

- **Extraction schedule:**  
   The extraction process will run automatically every **60 minutes (hourly batch)**.
### 4. Extraction Scope

The extraction process will focus only on the data required for the analytics application.

The selected datasets are:

- **Property Listings** — used to determine the total number of properties listed in the TerraGuide system and support property-related analytics.
- **Buyer Listings** — used to determine the total number of buyers listed in the TerraGuide system and support buyer-related analytics.
- **Transactions** — used to support transaction-related analytics, including monthly revenue.

These datasets will serve as the primary source for the analytics dashboard.


### 5. Source Limitations and Assumptions

- The TerraGuide MongoDB database must be accessible during extraction.
- Required collections and fields are assumed to be available.
- Historical analytics depend on the availability of historical records in the
  source database.
- Changes to collection names, field names, or data types may affect the
  extraction process.
- The accuracy of the extracted data depends on the quality and completeness of
  the source records.


  ## 2. Source Collections and Field Specification

Since TerraGuide uses MongoDB, the source tables are represented as *collections*, while columns are represented as *fields*.

Only fields relevant to the analytics pipeline are included.

### 2.1 Property Listings

| Item | Details |
|---|---|
| *Name* | Property Listings |
| *Purpose* | Stores property listing records available in the TerraGuide system. |
| *Primary Key / Unique Identifier* | _id |
| *Relevant Relationship* | Property listings can be associated with transaction records through the property identifier. |
| *Reason for Inclusion* | Used to determine the total number of property listings and support property-related analytics. |

#### Relevant Fields

| Field | Data Type | Key | Description / Purpose |
|---|---|---|---|
| _id | ObjectId | Primary Key | Uniquely identifies each property listing. |
| location | String | - | Stores the location of the property. |
| price | Number | - | Stores the listed property price. |
| status | String | - | Identifies whether the property is Available, Reserved, or Sold. |

### 2.2 Buyer Listings

| Item | Details |
|---|---|
| *Name* | Buyer Listings |
| *Purpose* | Stores buyer listing records in the TerraGuide system. |
| *Primary Key / Unique Identifier* | _id |
| *Relevant Relationship* | Buyer listings can be associated with transaction records through the buyer identifier. |
| *Reason for Inclusion* | Used to determine the total number of buyer listings and support buyer-related analytics. |

#### Relevant Fields

| Field | Data Type | Key | Description / Purpose |
|---|---|---|---|
| _id | ObjectId | Primary Key | Uniquely identifies each buyer listing. |
| userId | ObjectId | Foreign Key | References the user associated with the buyer listing. |

### 2.3 Transactions

| Item | Details |
|---|---|
| *Name* | Transactions |
| *Purpose* | Stores transaction records involving buyers and properties in the TerraGuide system. |
| *Primary Key / Unique Identifier* | _id |
| *Relevant Relationships* | Transactions connect buyer listings and property listings through buyerId and propertyId. |
| *Reason for Inclusion* | Used for transaction-related analytics and as the source data for calculating monthly revenue. |

#### Relevant Fields

| Field | Data Type | Key | Description / Purpose |
|---|---|---|---|
| _id | ObjectId | Primary Key | Uniquely identifies each transaction. |
| buyerId | ObjectId | Foreign Key | References the buyer involved in the transaction. |
| propertyId | ObjectId | Foreign Key | References the associated property. |
| amount | Number | - | Stores the transaction amount. |
| status | String | - | Stores the current transaction status. |
| completedAt | Date | - | Stores the date when the transaction was completed. |


## 3. Extraction Validation and Data Quality Checks

Validation checks will be performed during each extraction run to ensure that the required TerraGuide data is accessible, complete, and suitable for handover to the Transformation Stage.

The validation process focuses only on extraction-level checks. Data cleaning, standardization, and business transformations will be handled in the Transformation Stage.

| Check Name | Target | Purpose | Validation Criteria |
|---|---|---|---|
| Source Connection Check | TerraGuide MongoDB Atlas database | Ensures that the source database is available before extraction begins. | PASS if a connection to MongoDB Atlas is successfully established. FAIL if the connection cannot be established. |
| Required Collection Check | Property Listings, Buyer Listings, Transactions | Ensures that all collections required by the analytics pipeline are available. | PASS if all required collections exist and are accessible. FAIL if one or more required collections are missing or inaccessible. |
| Required Field Check | Required fields from each selected collection | Ensures that the fields needed by the extraction and analytics pipeline are available in the source data. | PASS if all required fields are present in the extracted records or collection schema. FAIL if a required field is unavailable. |
| Required Identifier Check | `_id`, `userId`, `buyerId`, `propertyId` where applicable | Ensures that identifiers needed to uniquely identify and relate records are available. | PASS if required identifiers are present for records where they are expected. FAIL if required identifiers are missing. |
| Primary Key Uniqueness Check | `_id` in Property Listings, Buyer Listings, and Transactions | Verifies that extracted records can be uniquely identified. | PASS if no duplicate `_id` values exist within each extracted dataset. FAIL if duplicate `_id` values are detected. |
| Extraction Window Check | `updatedAt` and extraction timestamps | Ensures that incremental extraction retrieves records only within the intended extraction window. | PASS if extracted records have an `updatedAt` value greater than the `Last_Successful_Run_Timestamp` and less than or equal to the `Current_Execution_Time`. FAIL if records fall outside the defined extraction window. |
| Record Count Check | Each extracted collection | Verifies that the number of records retrieved from the source matches the number of records successfully included in the extracted output. | PASS if the source query record count matches the number of successfully extracted records, excluding documented rejected records. FAIL if an unexplained count difference exists. |
| Extraction Completeness Check | Extracted Property Listings, Buyer Listings, and Transactions datasets | Ensures that all records returned by the defined source query are processed during the extraction run. | PASS if all records returned by the source query are either successfully extracted or explicitly recorded as rejected. FAIL if records are unaccounted for. |
| Extraction Error Check | Entire extraction process | Detects database query errors, connection interruptions, serialization failures, or other extraction failures. | PASS if the extraction finishes without critical errors. FAIL if a critical error prevents the extraction from completing successfully. |


## 4. Extraction Metadata and Log Specification
Extraction metadata and logs will be used to monitor, trace, and troubleshoot each execution of the TerraGuide data extraction process. The extraction script will use Python's logging module to record execution events, validation results, record counts, and errors encountered during extraction.

The extraction process retrieves data from the TerraGuide MongoDB Atlas operational database using Python and PyMongo. Extracted data will be serialized into Parquet files using PyArrow for handover to the Transformation Stage.

### 4.1 Extraction Metadata Fields

The following metadata fields define the information that should be recorded for each extraction run.

| Metadata Field | Purpose | Example Value or Format |
|---|---|---|
| run_id | Uniquely identifies an extraction execution for traceability. | extract_20261018_120000 |
| collection_name | Identifies the MongoDB collection being extracted. | Property Listings |
| start_timestamp | Records when the extraction process begins. | 2026-10-18T12:00:00+08:00 |
| end_timestamp | Records when the extraction process finishes. | 2026-10-18T12:00:08+08:00 |
| extraction_status | Indicates the final result of the extraction operation. | SUCCESS, FAILED, or PARTIAL |
| records_read | Records the number of source documents retrieved by the extraction query. | 1250 |
| records_extracted | Records the number of source documents successfully included in the extracted output. | 1250 |
| records_rejected | Records the number of documents excluded from the output because of extraction-level validation or serialization failures, where applicable. | 0 |
| extraction_window_start | Identifies the beginning of the incremental extraction window. | 2026-10-18T11:00:00+08:00 |
| extraction_window_end | Identifies the end of the incremental extraction window. | 2026-10-18T12:00:00+08:00 |
| validation_results | Records the outcome of the applicable extraction validation checks. | {"connection": "PASS", "record_count": "PASS"} |
| error_code | Identifies the category of an extraction error, if one occurs. | PARQUET_WRITE_ERROR |
| error_message | Describes the error encountered during extraction, if applicable. | Failed to write extracted data to Parquet. |
| duration_seconds | Records the total time taken to complete the extraction run. | 8.42 |
| output_file | Identifies the generated Parquet file associated with the extraction run. | property_listings_20261018_120000.parquet |

The examples above illustrate the proposed metadata format. Actual values will depend on the extraction execution and the fields implemented in the Python logging and extraction scripts.

### 4.2 Logging Implementation

The extraction script will use Python's built-in logging module to record operational events. Log messages should include a timestamp, severity level, and descriptive message. Where practical, messages should also identify the run ID and affected collection.

The following log levels are proposed:

| Log Level | Purpose | Example Event |
|---|---|---|
| INFO | Records normal extraction activities and successful operations. | Extraction started; 1,250 records retrieved. |
| WARNING | Records non-critical conditions requiring attention. | An unexpected record-count difference was detected. |
| ERROR | Records errors that prevent an operation from completing successfully. | MongoDB query or Parquet serialization failed. |
| CRITICAL | Records severe failures that prevent the extraction run from safely completing. | A required collection is unavailable and extraction cannot proceed. |

### 4.3 Use of Metadata and Logs

Extraction metadata and logs will support the following operational activities:

- *Monitoring:* Determine whether an extraction run completed successfully and review its duration and record counts.
- *Troubleshooting:* Identify database connection failures, missing collections, query errors, validation failures, and Parquet serialization errors.
- *Auditing and traceability:* Associate extracted files with their execution run, source collection, extraction window, and validation results.
- *Recovery:* Identify failed or incomplete runs and determine the extraction window and output files that need to be checked before retrying.
- *Handover verification:* Provide evidence that the extracted files were generated and that the required validation checks were performed.

### 4.4 Implementation Status

*Confirmed project decisions:*
- Python will be used for the extraction script.
- PyMongo will retrieve records from MongoDB Atlas.
- PyArrow will be used to write extracted data in Parquet format.
- Python's logging module will be used for extraction logs.

*Proposed specifications pending implementation or verification:*
- The complete metadata field set listed in Section 4.1.
- The standardized status values, error codes, and log levels.
- Recording metadata in a structured form that can be associated with each extraction run and output file.
- Using the recorded metadata and validation results to support recovery and handover decisions.

The metadata and logging implementation should be reviewed to ensure that the documented fields match the actual behavior of the extraction script.
## 5. Log Retention and Access

Extraction logs will be retained to support monitoring, troubleshooting, auditing, and recovery of the TerraGuide data pipeline.

- *Retention Period:* Extraction logs will be retained for 90 days to provide sufficient historical information for troubleshooting and reviewing previous extraction runs.
- *Storage Location:* Logs will be stored within the data pipeline environment in a dedicated logging directory or log storage location.
- *Access Permissions:* Only authorized project members responsible for development, monitoring, and maintenance of the data pipeline will have access to the extraction logs.
- *Archiving:* Older logs may be archived if they are still required for auditing, troubleshooting, or project documentation.
- *Deletion Rules:* Logs may only be deleted after the defined retention period has passed and when they are no longer required for troubleshooting, auditing, or recovery.
- *Responsible Process:* Log retention and deletion will be managed by the data pipeline maintainers or designated project members.




## 6.Extraction Data Contract and Naming Convention

## 1. Required Schemas

#### Property Listings

| Field | Expected Data Type | Requirement |
|---|---|---|
| `_id` | ObjectId | Required; unique identifier for each property listing |
| `location` | String | Required for property-related analytics |
| `price` | Number | Required for property value-related analytics |
| `status` | String | Required for property status analytics |

#### Buyer Listings

| Field | Expected Data Type | Requirement |
|---|---|---|
| `_id` | ObjectId | Required; unique identifier for each buyer listing |
| `userId` | ObjectId | Required to identify the user associated with the buyer listing |

#### Transactions

| Field | Expected Data Type | Requirement |
|---|---|---|
| `_id` | ObjectId | Required; unique identifier for each transaction |
| `buyerId` | ObjectId | Required to identify the buyer involved in the transaction |
| `propertyId` | ObjectId | Required to identify the associated property |
| `amount` | Number | Required for transaction and revenue analytics |
| `status` | String | Required to determine the current transaction status |
| `completedAt` | Date | Required for time-based transaction and monthly revenue analytics |


2. Naming Convention

The extraction process will preserve the original field names used by the TerraGuide MongoDB collections to maintain traceability between the source database and the extracted dataset.

The following naming rules will be followed:

- Collection names will be clearly identified according to their corresponding TerraGuide source collection.
- MongoDB's `_id` field will remain unchanged and will serve as the primary unique identifier.
- Existing source fields that use camelCase, such as `userId`, `buyerId`, `propertyId`, and `completedAt`, will retain their original names during extraction.
- Foreign key fields will retain the `Id` suffix used by the TerraGuide source system.
- Field names will not be abbreviated or renamed during extraction unless a documented mapping is required.
- Any standardization of field names, such as conversion from camelCase to snake_case, will be handled in the Transformation Stage.

3. Schema Consistency

The extraction process will validate the expected collection structure and required fields before extracted data is handed over to the next pipeline stage.

If an unexpected schema change is detected:

- Missing required fields will cause the affected extraction validation check to fail.
- Unexpected changes to required field data types will be recorded and flagged for review.
- Renamed or removed fields will require the extraction specification and extraction script to be reviewed before processing continues.
- Additional fields that are not required by the extraction contract may be ignored.
- Changes to primary or foreign key fields will be treated as critical because they may affect record identification and relationships between datasets.
- The documented schema must be updated whenever an approved change is made to the TerraGuide source database.

## 7. Extraction Acceptance and Handover Rules

This section defines the conditions under which extracted TerraGuide data may proceed to the Transformation Stage. The acceptance decision will be based on extraction validation results, extraction status, record-count reconciliation, and the successful generation of the required Parquet output files.

Stage 1 is responsible for extracting, validating, and packaging source data. Data cleaning, standardization, and business transformations remain the responsibility of the Transformation Stage.

### 7.1 Acceptance Decision Rules

The following rules are proposed for determining the outcome of an extraction run.

| Overall Condition | Acceptance Decision | Pipeline Action | Decision Type |
|---|---|---|---|
| All required collections are accessible, critical validation checks pass, record counts are reconciled, and all required Parquet files are successfully generated. | APPROVED | Allow the extracted data and associated metadata to proceed to the Transformation Stage. | Automatic, once implemented |
| Extraction completes, but a non-critical warning or an unexplained but non-critical record-count variation requires investigation. | MANUAL REVIEW | Hold the affected output until the discrepancy is reviewed and resolved or formally accepted. | Manual |
| A critical validation check fails, a required collection is unavailable, or required identifiers are missing in a way that makes the extracted data unusable. | REJECTED | Prevent the affected output from proceeding to the Transformation Stage. Record the failure and investigate before retrying. | Automatic rejection, once implemented |
| Extraction is interrupted, a required Parquet file cannot be generated, or the output is incomplete or unreadable. | REJECTED | Mark the run as failed, preserve diagnostic logs, and retry after the underlying issue is resolved. | Automatic rejection, once implemented |

A rejected extraction must not be treated as approved simply because some records were successfully retrieved. Any partial output must be identified and prevented from being mistaken for a complete, accepted dataset.

### 7.2 Automatic Approval

An extraction run may be approved automatically when all of the following conditions are satisfied:

1. The connection to MongoDB Atlas is successful.
2. All required source collections are available and accessible.
3. Required fields and identifiers satisfy the extraction contract.
4. Primary key uniqueness checks pass for each extracted dataset.
5. Record counts are reconciled, with any excluded records explicitly accounted for.
6. Extraction completes without unresolved critical errors.
7. All required Parquet files are successfully written and are readable.
8. The applicable validation results and extraction metadata are recorded.

*Proposed pipeline action:* Once automatic acceptance has been implemented, the extraction process may mark the run as APPROVED and make the accepted Parquet files available to the Transformation Stage.

### 7.3 Manual Review

Manual review is required when extraction completes but a condition needs further investigation before the output can be accepted.

Examples include:

- An unexpected record-count difference that has not yet been explained.
- A non-critical validation warning.
- An unexpected source schema variation that does not immediately prevent extraction but requires confirmation.
- An output discrepancy that needs verification against the source database.

*Proposed pipeline action:* Place the affected extraction run on hold. The designated data pipeline maintainer or project member responsible for extraction should review the logs, validation results, and output files. The reviewer must record the decision and its justification before the data proceeds.

Manual review must not be used to bypass a critical validation failure.

### 7.4 Rejection and Failure Handling

An extraction run must be rejected when a critical failure prevents the output from meeting the extraction requirements.

Examples include:

- MongoDB Atlas cannot be accessed.
- A required collection is missing or inaccessible.
- A critical required field or identifier is unavailable.
- Duplicate primary keys are detected in the extracted dataset.
- An unexplained record loss or incomplete extraction is detected.
- A required Parquet file cannot be written or read successfully.

*Proposed recovery procedure:*

1. Record the failure status, error details, affected collection, and run ID.
2. Preserve the log records and any partial output for diagnostic purposes.
3. Prevent incomplete or rejected output from being handed over as an accepted dataset.
4. Correct the underlying cause, such as a connectivity issue, extraction query error, schema incompatibility, or serialization failure.
5. Retry the extraction using the appropriate extraction window.
6. Repeat the required validation checks before making the output available for handover.

Retry behavior and partial-output quarantine are proposed operational rules and must be implemented or otherwise established before they can be considered active pipeline behavior.

### 7.5 Handover to the Transformation Stage

An approved extraction run should provide the following handover package:

| Handover Item | Description | Purpose |
|---|---|---|
| Extracted Parquet files | The successfully extracted datasets for Property Listings, Buyer Listings, and Transactions. | Supplies the source data for downstream transformation. |
| Run metadata | Run ID, collection name, timestamps, extraction status, record counts, and extraction window, where applicable. | Establishes traceability between the output and its extraction execution. |
| Validation summary | The results of the required extraction validation checks. | Provides evidence that the extracted data meets the Stage 1 acceptance criteria. |
| Error and warning information | Relevant errors, warnings, or rejected-record counts, if applicable. | Supports troubleshooting and documents any limitations affecting the output. |
| Output manifest | A proposed record of the files produced, their associated collections, and their extraction run. | Helps the receiving stage identify and verify the expected input files. |

The Transformation Stage should receive only output that has satisfied the applicable acceptance rules. The handover package should preserve the original source field names and data types according to the extraction data contract; data cleaning, standardization, and business transformations should occur in the next stage.

### 7.6 Implementation Status and Responsibility

*Confirmed project decisions:*
- Stage 1 will produce Parquet files using PyArrow.
- The extraction script will use Python logging.
- The extracted data is intended for downstream transformation.

*Proposed policies pending implementation or approval:*
- The APPROVED, MANUAL REVIEW, and REJECTED decision states.
- Automatic acceptance and rejection logic.
- Manual review responsibility and approval recording.
- Retry handling and the treatment of partial output.
- Output manifests and formal handover packaging.

The data pipeline maintainer or designated project member should be responsible for reviewing exceptions and confirming that the implemented extraction process follows the documented acceptance and handover rules.

## 8. Common Extraction Problems and Mitigation

The following issues may occur during the extraction of data from the TerraGuide MongoDB database.

| Problem | Possible Cause | Impact on Extraction | Proposed Mitigation |
|---|---|---|---|
| MongoDB Connection Failure | Network interruption, incorrect connection string, authentication failure, or MongoDB Atlas service unavailability. | The extraction process cannot access the source collections and the current extraction run may fail. | Validate the database connection before extraction, record the error in the extraction log, and retry the extraction after the connection is restored. |
| Missing Required Collection | A required collection is renamed, removed, or unavailable in the source database. | Data required by the analytics pipeline cannot be extracted, resulting in incomplete output. | Validate that all required collections exist before extraction and stop or flag the extraction run if a required collection is missing. |
| Missing Required Field | A required field is removed, renamed, or unavailable in some source records. | The extracted dataset may be incomplete or required analytics fields may not be available. | Validate required fields during extraction and record affected records or extraction failures for review. |
| Schema or Data Type Change | The structure or data type of a field in MongoDB changes after the extraction pipeline has been implemented. | The extraction script may fail, produce invalid output, or serialize data incorrectly. | Validate expected field names and data types before processing and update the extraction specification when approved schema changes occur. |
| Duplicate Records | Duplicate records may be retrieved due to incorrect extraction logic or repeated processing of the same extraction window. | Duplicate data may appear in the extracted dataset and affect downstream processing. | Use the MongoDB _id field to identify unique records and validate primary key uniqueness after extraction. |
| Incorrect Extraction Window | The Last_Successful_Run_Timestamp or Current_Execution_Time is incorrect, missing, or improperly applied. | Records may be skipped or extracted more than once during incremental extraction. | Validate the extraction window before running the query and store the timestamp of the last successful extraction run. |
| Extraction Interruption | Network loss, script failure, system shutdown, or unexpected runtime error occurs while data is being extracted. | The extraction may stop before all required records are processed, resulting in incomplete output. | Record the extraction status and error details, mark the run as failed, and retry the extraction from the appropriate extraction window. |
| Parquet Serialization Failure | Extracted data contains unsupported or inconsistent values that cannot be serialized correctly by PyArrow. | The extracted dataset may not be written successfully to Parquet format. | Validate extracted field structures before serialization, record the error, and prevent incomplete output from being passed to the next stage. |
