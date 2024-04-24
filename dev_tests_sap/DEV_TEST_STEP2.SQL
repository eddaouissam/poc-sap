select* from information_schema.tables
where table_name = 'RAW_SCHEMAS'
;


select * from DEV_DB_VISEO.SAP_BRONZE.RAW_SCHEMAS;

select * from adrct;


select DEV_DB_VISEO.SAP_SILVER* from DEV_DB_VISEO.SAP_BRONZE.RAW_SCHEMAS;



use database TEST;
use schema TEST.PUBLIC;

select * from adr6;

show columns in adr6;


-- TEST to cast one column full manual ;)
create or replace table adr6 as (
select
    *,
    to_timestamp(recordstamp, 'YYYY-MM-DD HH24:MI:SS.FF UTC') as temp
from adr6
);

select * from adr6;

alter table ADR6 drop column recordstamp;

alter table ADR6 rename column temp to recordstamp;




-- 

select distinct type from RAW_SCHEMAS;



select source_table from RAW_SCHEMAS
where type = 'NUMERIC';


-- Step 1: Retrieve schema mapping for all tables
WITH SchemaMapping AS (
    SELECT SOURCE_TABLE, NAME, TYPE
    FROM DEV_DB_VISEO.SAP_BRONZE.RAW_SCHEMAS
),

-- Step 2: Generate ALTER TABLE statements for each table
AllTables AS (
    SELECT DISTINCT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'DEV_DB_VISEO.SAP_RAW_DATA' -- replace with your schema name
),

AlterStatements AS (
    SELECT 
        CONCAT('ALTER TABLE ', at.TABLE_NAME, ' MODIFY COLUMN ', column_name, ' ', sm.TYPE) AS alter_statement
    FROM AllTables at
    JOIN INFORMATION_SCHEMA.COLUMNS c ON at.TABLE_NAME = c.TABLE_NAME
    JOIN SchemaMapping sm ON at.TABLE_NAME = sm.SOURCE_TABLE AND c.COLUMN_NAME = sm.NAME
)

-- Step 3: Execute ALTER TABLE statements
SELECT *
FROM AlterStatements;











----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE update_column_types()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  var errors = [];
  var query = "SELECT SOURCE_TABLE, NAME, TYPE FROM SAP_BRONZE.RAW_SCHEMAS;";
  var stmt = snowflake.createStatement({sqlText: query});
  var result = stmt.execute();

  while (result.next()) {
    var source_table = result.getColumnValue(1);
    var column_name = result.getColumnValue(2);
    var column_type = result.getColumnValue(3);

    // Adding logic to handle conversion
    try {
      if (column_type === 'STRING') {
        // Handle conversion from NUMBER to VARCHAR
        var temp_column_name = column_name + '_temp';
        var alter_sql = `ALTER TABLE SAP_RAW_DATA.${source_table} ADD COLUMN ${temp_column_name} VARCHAR;`;
        snowflake.createStatement({sqlText: alter_sql}).execute();

        var update_sql = `UPDATE SAP_RAW_DATA.${source_table} SET ${temp_column_name} = TO_VARCHAR(${column_name});`;
        snowflake.createStatement({sqlText: update_sql}).execute();

        var drop_sql = `ALTER TABLE SAP_RAW_DATA.${source_table} DROP COLUMN ${column_name};`;
        snowflake.createStatement({sqlText: drop_sql}).execute();

        var rename_sql = `ALTER TABLE SAP_RAW_DATA.${source_table} RENAME COLUMN ${temp_column_name} TO ${column_name};`;
        snowflake.createStatement({sqlText: rename_sql}).execute();
      } else {
        var alter_sql = `ALTER TABLE SAP_RAW_DATA.${source_table} ALTER COLUMN ${column_name} TYPE ${column_type};`;
        snowflake.createStatement({sqlText: alter_sql}).execute();
      }
    } catch (err) {
      errors.push(`Error updating ${source_table}.${column_name} to ${column_type}: ${err.message}`);
    }
  }

  if (errors.length > 0) {
    return "Errors encountered: " + errors.join(", ");
  }
  return "Column types updated successfully!";
$$;

CALL update_column_types();


---------------------------------------------------------
