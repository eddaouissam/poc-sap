CREATE OR REPLACE PROCEDURE create_tables()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var createStmt, loadStmt, schemaQuery, result;
  var errors = [];

  // Query to fetch schema definitions
  schemaQuery = "SELECT source_table, name, type FROM SAP_BRONZE.RAW_SCHEMAS ORDER BY source_table, name;";
  result = snowflake.execute({sqlText: schemaQuery});
  
  var currentTable = null;
  var sqlCreate = "";
  
  while (result.next()) {
    var source_table = result.getColumnValue(1);
    var columnName = result.getColumnValue(2);
    var columnType = result.getColumnValue(3);
    var tableName = source_table.replace('_SCHEMA', '');
    
    if (currentTable !== tableName) {
      if (currentTable !== null) {
        // Finish the previous table creation statement and execute it
        sqlCreate = sqlCreate.slice(0, -2) + ");"; // Remove the last comma and space, add closing parenthesis
        try {
          createStmt = snowflake.createStatement({sqlText: sqlCreate});
          createStmt.execute();
        } catch (err) {
          errors.push("Error creating table " + currentTable + ": " + err.message);
        }
      }
      // Start new CREATE TABLE statement
      sqlCreate = "CREATE OR REPLACE TABLE " + tableName + " (";
      currentTable = tableName;
    }
    sqlCreate += columnName + " " + columnType + ", ";
  }

  // Execute the last CREATE TABLE statement if any
  if (sqlCreate) {
    sqlCreate = sqlCreate.slice(0, -2) + ");"; // Finalize the last table creation
    try {
      createStmt = snowflake.createStatement({sqlText: sqlCreate});
      createStmt.execute();
    } catch (err) {
      errors.push("Error creating table " + currentTable + ": " + err.message);
    }
  }

  // Assuming loading can be done separately or similarly automated
  return errors.length > 0 ? "Errors: " + errors.join(", ") : "Tables created and data loaded successfully.";
$$;


CALL create_tables();
