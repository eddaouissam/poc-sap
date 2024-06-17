-- DEV_SAP_MERGE.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This stored procedure transforms data from the SAP_BRONZE schema to the SAP_SILVER schema
--              by executing a Snowpark Python script. The script automates Change Data Capture (CDC)
--              to ensure that changes in the source data are accurately reflected in the target tables.
--              The Python script is stored and versioned in a Git repository.
-- 
-- Usage:
--   - Execute this procedure to transform and merge data from the SAP_BRONZE schema to the SAP_SILVER schema.
--   - The procedure uses Snowflake's Git integration to import the latest version of the Python script.
--   - This Stored Proc will be called later in a TASK call to orchestrate the workflow.
-- 
-- Example:
--   CALL DEV_DB_VISEO.SAP_BRONZE.TRANSFORM_BRONZE_TO_SILVER();
use warehouse DEV_WH;

CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_BRONZE.SAP_MERGE()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'DEV_SAP_SNOWPARK_CDC.automate_cdc'
IMPORTS = ('@DEV_DB_VISEO.SAP_RAW_LANDING.GIT_REPO_STAGE/branches/main/dev_poc_sap/POC_SAP_BRONZE/DEV_SAP_SNOWPARK_CDC.py');
