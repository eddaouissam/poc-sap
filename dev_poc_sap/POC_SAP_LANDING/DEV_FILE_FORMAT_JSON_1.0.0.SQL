-- Script: Create JSON File Format for SAP Schema Loading
-- Author: M'hamed Issam ED-DAOU
-- Version: 1.0.0
-- Description:
--   This script creates a new JSON file format named 'FF_JSON_LOGS' in Snowflake, configured specifically
--   for loading JSON files that describe the schema of SAP tables. The format is set up to handle JSON arrays
--   by stripping the outer array, enabling direct mapping of JSON objects to table rows. This setup is
--   particularly useful for ingesting structured JSON that represents table schemas, ensuring efficient and
--   error-free loading of schema descriptions directly into Snowflake.
--   The format also uses automatic compression detection to optimize storage and transfer speeds.

CREATE OR REPLACE FILE FORMAT FF_JSON_SCHEMAS
  TYPE = 'JSON', -- Specifies the file type to be JSON.
  STRIP_OUTER_ARRAY = TRUE, -- Enables stripping of the outer JSON array, useful for JSON files that contain a single array of objects.
  COMPRESSION = 'AUTO'; -- Automatically detects and applies the optimal compression method for the files being loaded.