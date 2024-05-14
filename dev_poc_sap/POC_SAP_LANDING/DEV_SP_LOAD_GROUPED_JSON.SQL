-- Script: Create Grouped Schemas Table
-- Author: M'hamed Issam ED-DAOU
-- Procedure: create_grouped_schemas
-- Database and Schema: DEV_DB_VISEO.SAP_RAW_LANDING
-- Version: 1.0.0
-- Description:
--   This Snowflake stored procedure creates or replaces a table named `GROUPED_SCHEMAS`.
--   It takes JSON files from a specified stage and uses a given file format to extract and
--   format the schema information. The resulting table contains three columns: name, type,
--   and source_table, with the latter being derived from the file name.
--   This stored procedure is intended to load all the staged json files into on big table,
--   Providing an alternative for the Fivetran loading mechanism for our SAP Accelerator.
-- Parameters:
--   - stage_location (STRING): The stage location where the JSON files reside.
--   - file_format (STRING): The file format to use for parsing the JSON files.

CREATE OR REPLACE PROCEDURE create_grouped_schemas(
    stage_location STRING,
    file_format STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    CREATE OR REPLACE TABLE GROUPED_SCHEMAS AS
    SELECT
        $1:name::STRING AS name,
        $1:type::STRING AS type,
        UPPER(
            REPLACE(
                REGEXP_REPLACE(METADATA$FILENAME, '.*[/]', ''), 
                '.schema.json', ''
            )
        ) || '_SCHEMA' AS source_table
    FROM
        @DEV_DB_VISEO.SAP_RAW_LANDING.SAP_JSON_SCHEMAS
        (file_format => :file_format);
$$;

-- Usage Example:
-- The following example demonstrates how to call the `create_grouped_schemas` procedure
-- using a specific stage location and file format.

CALL create_grouped_schemas(
    '@DEV_DB_VISEO.SAP_RAW_LANDING.SAP_JSON_SCHEMAS',
    'ff_json_schemas'
);
