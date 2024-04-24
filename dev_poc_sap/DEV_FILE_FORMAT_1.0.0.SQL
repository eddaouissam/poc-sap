-- Script: Create Initial CSV File Format for Data Loading
-- Author: M'hamed Issam ED-DAOU
-- Version: 1.0.0
-- Description:
--   This script establishes the initial CSV file format configuration for data loading into Snowflake.
--   Named 'my_csv_file_format', this format is designed to handle generic CSV data ingestion
--   with basic settings for delimiters, headers, and error handling. It serves as the baseline
--   format prior to more specialized configurations in subsequent versions.

CREATE OR REPLACE FILE FORMAT my_csv_file_format
  TYPE = 'CSV'  -- Specifies that the file type is CSV, appropriate for comma-separated values files.
  FIELD_DELIMITER = ','  -- Sets the field delimiter to a comma, standard for CSV files.
  RECORD_DELIMITER = '\n'  -- Uses newline as the record delimiter, defining the end of a data row.
  SKIP_HEADER = 1  -- Instructs Snowflake to skip the first line of the file, which is assumed to contain column headers.
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'  -- Allows fields to be optionally enclosed in double quotes, which is useful for fields containing delimiters.
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Configures the load process to not fail when a row has a different number of columns than the target table.
  EMPTY_FIELD_AS_NULL = TRUE  -- Treats any empty fields as NULL in the database, which helps maintain data integrity by explicitly handling missing values.
  COMPRESSION = 'AUTO';  -- Enables automatic detection and handling of compressed files, supporting file efficiency and reduced storage.

-- Note:
-- This configuration (v1.0.0) is designed for general use and may not handle specific cases such as timestamps or advanced encoding.
-- It should be used as a baseline for simple CSV data loads where no complex parsing is required.