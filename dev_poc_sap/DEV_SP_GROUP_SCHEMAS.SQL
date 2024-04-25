-- Script: Create Grouped Table from Schema Information
-- Author: M'hamed Issam ED-DAOU
-- Description:
--   This stored procedure dynamically constructs and executes a SQL statement to create a new table
--   based on aggregated schema information from multiple tables ending in '_SCHEMA'. It is designed to
--   facilitate the future casting of column types for the SAP technical tables consolidating schema data into a single unified structure.
--   This script is particularly useful for environments where schema evolution tracking or schema-based
--   data consolidation is required.

CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_TABLE_SCHEMA.CREATE_GRPD_TABLE_FROM_QUERY(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  v_concatenated_sql STRING DEFAULT '''';
  v_create_table_sql STRING;

BEGIN
  -- Step 1: Retrieve concatenated SQL to build a union of select statements from all tables ending with "SCHEMA"
  -- This creates a single large SQL from multiple smaller SQLs, each selecting the table name, column type, and column name
  -- from one of the schema tables.
  SELECT LISTAGG(''SELECT '''''' || TABLE_NAME || '''''' AS source_table, COLUMN_TYPE AS type, COLUMN_NAME AS name FROM '' || TABLE_NAME, '' UNION ALL '') WITHIN GROUP (ORDER BY TABLE_NAME)
  INTO :v_concatenated_sql
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME LIKE ''%SCHEMA'';

  -- Step 2: Remove the trailing "UNION ALL" for the final SQL statement construction
  IF RIGHT(:v_concatenated_sql, 10) = '' UNION ALL '' THEN
    SET v_concatenated_sql = LEFT(:v_concatenated_sql, LENGTH(:v_concatenated_sql) - 10);
  END IF;

  -- Step 3: Construct the CREATE TABLE statement using the dynamically generated SQL statement
  -- This statement creates a new table with the consolidated schema information from multiple tables.
  SET v_create_table_sql = ''CREATE OR REPLACE TABLE '' || :TABLE_NAME || '' AS '' || :v_concatenated_sql;

  -- Step 4: Execute the create table statement
  EXECUTE IMMEDIATE :v_create_table_sql;

  -- Step 5: Return success message confirming table creation
  RETURN ''Table '' || :TABLE_NAME || '' created successfully.'';
END;
';

-- Example usage: Calling the procedure to create a table named 'DEV_DB_VISEO.SAP_BRONZE.RAW_SCHEMAS'
CALL DEV_DB_VISEO.SAP_TABLE_SCHEMA.CREATE_GRPD_TABLE_FROM_QUERY('DEV_DB_VISEO.SAP_BRONZE.RAW_SCHEMAS');