-- Script: Create CSV File Format for Data Loading 2.0.0
-- Author: M'hamed Issam ED-DAOU
-- Version: 2.0.0
-- Description:
--   This script creates a new CSV file format named 'my_csv_format2' in Snowflake, configured specifically
--   for loading data from CSV files with detailed specifications on delimiter, enclosure, timestamp format,
--   and handling of empty fields and headers. This format is intended to standardize CSV data ingestion,
--   for our SAP technical tables,
--   ensuring consistent handling of common data anomalies and format issues.

CREATE OR REPLACE FILE FORMAT DEV_DB_VISEO.SAP_RAW_LANDING.FF_CSV_SAP
  TYPE = 'CSV' -- Specifies the file type to be CSV.
  FIELD_DELIMITER = ',' -- Sets the field delimiter to a comma, which is typical for CSV files.
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- Allows text fields to be enclosed in double quotes, useful for handling fields that contain delimiter characters.
  NULL_IF = ('', 'NULL') -- Designates empty strings and the string 'NULL' as null values in Snowflake.
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 UTC' -- Sets the format for timestamp fields to include microseconds and the UTC timezone.
  EMPTY_FIELD_AS_NULL = TRUE -- Treats empty CSV columns as null values, which can help prevent data quality issues with missing information.
  PARSE_HEADER = TRUE -- Instructs Snowflake to interpret the first row in CSV files as header rows containing column names.
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- Prevents errors from halting the load process if a row in the CSV file has a different number of columns than expected.

-- Note:
-- This file format should be used when loading data from CSV files that meet the described configurations.
-- Ensure specicifying the database and schema when creating the file format through the worksheet setup context
-- Ensure that CSV files are properly formatted according to the specified settings to avoid data ingestion errors.
