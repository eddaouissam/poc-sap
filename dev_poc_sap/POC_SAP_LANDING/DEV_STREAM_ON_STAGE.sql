-- DEV_STREAM_ON_STAGE.sql
-- 
-- Author: M'hamed Issam ED-DAOU
--
-- Description: This SQL script creates a stream on the directory of the SAP_CSV_FILES stage.
--              This allows and enables efficient and automated data processing workflow.
-- 
-- Usage:
--   - Execute this script to create a stream on the specified stage directory.
--   - The stream will be used to capture changes in the CSV files directory/stage.
--   - This stream will be referenced in the first task to trigger the first task of our workflow

CREATE OR REPLACE STREAM DEV_DB_VISEO.SAP_RAW_LANDING.CSV_STAGE_STREAM 
ON directory(@DEV_DB_VISEO.SAP_RAW_LANDING.SAP_CSV_FILES);
