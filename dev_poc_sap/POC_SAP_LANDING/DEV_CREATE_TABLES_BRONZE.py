"""
Script: Create Tables Dynamically from Schema Information
Author: M'hamed Issam ED-DAOU
Description:
  This script leverages Snowflake's Snowpark Python API to dynamically construct and execute SQL statements that create new tables based on schema information extracted from specified source tables ending with '_SCHEMA'.
  It iterates over schema definitions, ensures no duplicate columns are included in any single table, and handles exceptions during table creation. This approach is especially beneficial in environments where schema evolution needs to be tracked or where consolidated schema data is required for operations or analytics.
  The script assumes that the schema information includes the source table name, column name, and column type, and constructs tables accordingly, excluding any duplicate column names to ensure schema integrity.

Function:
  create_tables(session: Session):
    - Parameters: session (Snowflake.snowpark.Session): The session object through which all database operations are executed.
    - Returns: A string message indicating the success of the table creations or detailing any errors encountered.

Usage:
  To use this script, ensure that you have an active Snowpark session configured with appropriate permissions to execute SQL statements and create tables. Call the create_tables function with the session as its argument.

Note:
  This script is designed for use within a Snowflake environment that supports Python execution through Snowpark. 
  Modifications might be required based on specific database schema details or Snowflake account configurations.
  This script will be used for our POC, enabling us to create the SAP technical tables structure
"""

from snowflake.snowpark import Session

def create_tables(session):
    errors = []
    current_table = None
    sql_create = ""
    column_names = set()  # Used to track column names and avoid duplicates

    # Query to fetch schema definitions
    schema_query = "SELECT source_table, name, type FROM SAP_RAW_LANDING.GROUPED_SCHEMAS ORDER BY source_table, name;"
    result = session.sql(schema_query).collect()

    for row in result:
        source_table = row['SOURCE_TABLE']
        column_name = row['NAME']
        column_type = row['TYPE']
        table_name = 'B_'+source_table.replace('_SCHEMA', '')

        if current_table != table_name:
            if current_table is not None:
                # Finish the previous table creation statement and execute it
                sql_create = sql_create.rstrip(", ") + ");"  # Remove the last comma and space, add closing parenthesis
                try:
                    session.sql(sql_create).collect()
                except Exception as e:
                    errors.append(f"Error creating table {current_table}: {str(e)}")
                column_names.clear()  # Reset column tracking for the next table

            # Start new CREATE TABLE statement
            sql_create = f"DROP TABLE {table_name} "
            session.sql(sql_create).collect()

    # Return a message based on whether there were errors
    return "Errors: " + ", ".join(errors) if errors else "Tables created successfully."

