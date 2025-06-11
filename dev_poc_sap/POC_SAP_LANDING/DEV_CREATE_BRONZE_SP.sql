-- DEV_CREATE_BRONZE_SP.sql
-- 
-- Author : M'hamed Issam ED-DAOU
--
-- Description: This stored procedure creates tables in the SAP_BRONZE schema by dynamically 
--              generating and executing SQL statements using a Snowpark Python script.
--              The Python script is stored and versioned in a Git repository.
-- 
-- Usage:
--   - Execute this procedure to create tables based on the schema definitions in the SAP_BRONZE schema.
--   - The procedure uses Snowflake's Git integration to import the latest version of the Python script.
--   - This Stored Proc will be called later in a TASK call to orchestrate the workflow.
-- Example:
--   CALL DEV_DB_VISEO.SAP_BRONZE.CREATE_SILVER_SP();
use database DEV_DB_VISEO;
use schema SAP_RAW_LANDING;
use warehouse DEV_WH;
CREATE OR REPLACE PROCEDURE CREATE_BRONZE_SP()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS=('@git_repo_stage/branches/main/dev_poc_sap/POC_SAP_LANDING/DEV_CREATE_TABLES_BRONZE.py')
HANDLER = 'DEV_CREATE_TABLES_BRONZE.create_tables';
