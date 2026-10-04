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
