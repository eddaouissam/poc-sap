-- DEV_BULK_LOAD_TASK.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This task orchestrates the bulk loading of CSV data into the SAP_BRONZE schema. 
--              It is triggered after the CREATE_BRONZE_TASK completes, ensuring that the tables
--              are created before attempting to load data into them.
-- 
-- Usage:
--   - Execute this script to create a task that performs bulk loading of CSV data.
--   - This task is set to run after the CREATE_BRONZE_TASK to ensure the necessary tables are in place.

CREATE OR REPLACE TASK DEV_DB_VISEO.SAP_RAW_LANDING.BULK_LOAD_TASK
    WAREHOUSE = DEV_WH
    AFTER DEV_DB_VISEO.SAP_RAW_LANDING.CREATE_BRONZE_TASK
    AS CALL bulk_load_sp();
