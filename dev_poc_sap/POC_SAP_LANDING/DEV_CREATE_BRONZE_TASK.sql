-- DEV_CREATE_BRONZE_TASK.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This task creates tables in the SAP_BRONZE schema when new data is detected in the CSV stage stream.
--              It leverages Snowflake's task scheduling and streaming capabilities to automatically trigger
--              the creation of tables based on incoming CSV files.
-- 
-- Usage:
--   - Execute this script to create a task that monitors the CSV stage stream for new data.
--   - When new data is detected, the task will automatically call the stored procedure to create the tables.

CREATE OR REPLACE TASK DEV_DB_VISEO.SAP_RAW_LANDING.CREATE_BRONZE_TASK
    WAREHOUSE = DEV_WH
    WHEN SYSTEM$STREAM_HAS_DATA('csv_stage_stream')
    AS CALL create_bronze_sp();
