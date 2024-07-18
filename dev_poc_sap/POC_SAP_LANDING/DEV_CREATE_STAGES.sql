-- DEV_CREATE_STAGES.sql 
-- Author : M'hamed Issam ED-DAOU
--
-- This script creates two stages in Snowflake for storing CSV and JSON files.
-- The stages are configured to enable directory support, which allows for hierarchical storage and management of files.

-- Create a stage for storing CSV files
-- Stage Name: SAP_CSV_FILES
-- DIRECTORY: This option enables hierarchical directory support within the stage.
CREATE STAGE IF NOT EXISTS DEV_DB_VISEO.SAP_RAW_LANDING.SAP_CSV_FILES 
    DIRECTORY = ( ENABLE = true );

-- Create a stage for storing JSON schema files
-- Stage Name: SAP_JSON_SCHEMAS
-- DIRECTORY: This option enables hierarchical directory support within the stage.
CREATE STAGE IF NOT EXISTS DEV_DB_VISEO.SAP_RAW_LANDING.SAP_JSON_SCHEMAS 
    DIRECTORY = ( ENABLE = true );
