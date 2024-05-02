-- Script: Automated JSON Schema Loader for Snowflake
-- Author: M'hamed Issam ED-DAOU
-- Procedure: LOAD_JSON_SCHEMAS
-- Database and Schema: DEV_DB_VISEO.SAP_TABLE_SCHEMA
-- Version: 1.0.0
-- Description:
--   This Snowflake stored procedure is designed as an in-house alternative to third-party data integration tools like Fivetran.
--   It automates the process of loading JSON schema files from a specified stage into dynamically created tables. Each table
--   corresponds to a JSON schema file and is named according to the file's basename, suffixed with '_SCHEMA'. This method
--   provides flexibility and control over the data loading process, making it suitable for environments where direct management
--   of data workflows is preferred.
CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_TABLE_SCHEMA.LOAD_JSON_SCHEMAS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  files_result RESULTSET;
  file_name VARCHAR;
  table_name VARCHAR;
  create_table_sql VARCHAR;
  copy_sql VARCHAR;
BEGIN
  -- List files in the stage
  files_result := (SELECT METADATA$FILENAME AS filename 
                   FROM @DEV_DB_VISEO.SAP_TABLE_SCHEMA.SAP_JSON_SCHEMAS);

  -- Loop through each file
  FOR file_record IN files_result DO
    file_name := file_record.filename;
    -- Extract the base name for the table
    table_name := REPLACE(SPLIT_PART(file_name, ''/'', -1), ''.schema.json'', '''') || ''_SCHEMA'';

    -- Create table if it does not exist
    create_table_sql := ''CREATE TABLE IF NOT EXISTS '' || table_name || '' (mode STRING, name STRING, type STRING)'';
    EXECUTE IMMEDIATE create_table_sql;

    -- Formulate the COPY INTO command
    copy_sql := ''COPY INTO '' || table_name || '' FROM (SELECT $1:mode::STRING as mode, $1:name::STRING as name, $1:type::STRING as type FROM @DEV_DB_VISEO.SAP_TABLE_SCHEMA.SAP_JSON_SCHEMAS/'' || file_name || '' (file_format => ff_json_logs))'';
    EXECUTE IMMEDIATE copy_sql;
  END FOR;

  RETURN ''Load completed successfully.'';
END;
';