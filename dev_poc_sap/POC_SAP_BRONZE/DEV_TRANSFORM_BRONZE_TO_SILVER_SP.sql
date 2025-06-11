-- DEV_TRANSFORM_BRONZE_TO_SILVER.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This stored procedure orchestrates the workflow for transforming data
--              from the SAP_BRONZE schema to the SAP_SILVER schema. It checks whether
--              the required tables in the SAP_SILVER schema exist and, if not, creates them.
--              After ensuring the tables exist, it proceeds to transform and merge the data
--              from SAP_BRONZE to SAP_SILVER. The orchestration logic is implemented in a 
--              Snowpark Python script stored in our Git repo.
-- 
-- Usage:
--   - Execute this procedure to transform and merge data from the SAP_BRONZE schema to the SAP_SILVER schema.
--   - The procedure uses Snowflake's Git integration to import the latest version of the Python script.
--   - This Stored Proc will be called later in a TASK call to orchestrate the workflow.
-- 
-- Example:
--   CALL DEV_DB_VISEO.SAP_BRONZE.TRANSFORM_BRONZE_TO_SILVER();
use warehouse DEV_WH;

CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_BRONZE.TRANSFORM_BRONZE_TO_SILVER()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'DEV_ORCHESTRATE_WORKFLOW.orchestrate_workflow'
IMPORTS = ('@DEV_DB_VISEO.SAP_RAW_LANDING.GIT_REPO_STAGE/branches/main/dev_poc_sap/POC_SAP_BRONZE/DEV_ORCHESTRATE_WORKFLOW.py');
