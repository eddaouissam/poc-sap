-- DEV_CREATE_SILVER_SP.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This stored procedure creates tables in the SAP_SILVER schema by dynamically 
--              generating and executing SQL statements using a Snowpark Python script.
--              The Python script is stored and versioned in a Git repository.
-- 
-- Usage:
--   - Execute this procedure to create tables based on the schema definitions in the SAP_SILVER schema.
--   - The procedure uses Snowflake's Git integration to import the latest version of the Python script.
--   - This Stored Proc will be called later in a TASK call to orchestrate the workflow.
-- 
-- Example:
--   CALL DEV_DB_VISEO.SAP_BRONZE.CREATE_SILVER_SP();
use warehouse DEV_WH;
CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_BRONZE.CREATE_SILVER_SP()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'DEV_CREATE_SILVER.create_target_tables'
IMPORTS = ('@DEV_DB_VISEO.SAP_RAW_LANDING.GIT_REPO_STAGE/branches/main/dev_poc_sap/POC_SAP_BRONZE/DEV_CREATE_SILVER.py');
