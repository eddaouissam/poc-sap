-- DEV_BULK_LOAD_SP.sql
-- 
-- Author : M'hamed Issam ED-DAOU
--
-- Description: This stored procedure automates the process of loading CSV data from 
--              the SAP_RAW_LANDING schema into corresponding tables in the SAP_BRONZE schema.
--              It uses a Snowpark Python script to handle the data loading.
--              The Python script is stored and versioned in a Git repository.
-- 
-- Usage:
--   - Execute this procedure to bulk load CSV data into the SAP_BRONZE tables.
--   - The procedure uses Snowflake's Git integration to import the latest version of the Python script.
--   - This Stored Proc will be called later in a TASK call to orchestrate the workflow.
-- Example:
--   CALL BULK_LOAD_SP();

use database DEV_DB_VISEO;
use schema SAP_RAW_LANDING;
use warehouse 'DEV_WH';
CREATE OR REPLACE PROCEDURE BULK_LOAD_SP()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS=('@git_repo_stage/branches/main/dev_poc_sap/POC_SAP_LANDING/DEV_BULK_LOAD_CSV_TO_TABLES.py')
HANDLER = 'DEV_BULK_LOAD_CSV_TO_TABLES.load_data_automated';