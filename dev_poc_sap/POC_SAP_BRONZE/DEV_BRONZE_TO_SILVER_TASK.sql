-- DEV_BRONZE_TO_SILVER_TASK.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This task orchestrates the workflow for transforming data from the SAP_BRONZE schema to the SAP_SILVER schema.
--              It is scheduled to run every hour at the 5th minute using a CRON expression. The task calls the stored procedure 
--              TRANSFORM_BRONZE_TO_SILVER which ensures the required tables in the SAP_SILVER schema exist and then proceeds 
--              to transform and merge the data.
-- 
-- Usage:
--   - Execute this script to create a task that automatically runs the transformation process on a schedule.
--   - The task will run the stored procedure TRANSFORM_BRONZE_TO_SILVER.

CREATE OR REPLACE TASK DEV_DB_VISEO.SAP_BRONZE.BRONZE_TO_SILVER
    WAREHOUSE = DEV_WH
    SCHEDULE = 'USING CRON 5 * * * * UTC'
    AS CALL TRANSFORM_BRONZE_TO_SILVER();
