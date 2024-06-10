"""
 Script: Automated Table Structure Replicator BRONZE TO SILVER
    Author: M'hamed Issam ED-DAOU
    Description:
      This Snowpark Python script automates the process of creating tables in the SAP_SILVER schema based on the 
      structure of tables from the SAP_BRONZE schema. It excludes 'operation_flag' and 'is_deleted' columns to ensure 
      that the replicated tables in SAP_SILVER are optimized for analytics and reporting. This functionality is 
      crucial for environments where data management and structure consistency across different storage levels 
      (bronze to silver) are essential for streamlined operations and data integrity.
"""


from snowflake.snowpark import Session

def create_target_tables(session: Session):
    errors = []
    schema_query = r"""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'SAP_BRONZE' AND table_name LIKE 'B\\_%' ESCAPE '\\'
    """
    try:
        schema_data = session.sql(schema_query).collect()
        print("Schema query executed successfully, fetched columns:", len(schema_data))
    except Exception as e:
        return f"Failed to execute schema query: {str(e)}"

    # Initialize dictionary to hold table column data, excluding specific columns
    table_columns = {}

    # Organize columns by table, excluding 'operation_flag' and 'is_deleted'
    for row in schema_data:
        if row['COLUMN_NAME'] not in ['OPERATION_FLAG', 'IS_DELETED']:
            table = row['TABLE_NAME']
            column = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            if table not in table_columns:
                table_columns[table] = []
            table_columns[table].append(f"{column} {data_type}")

    # Iterate through tables and create corresponding S_ tables
    for b_table, columns in table_columns.items():
        s_table = 'S_' + b_table[2:]  # Assuming S_ follows B_ directly
        column_definitions = ', '.join(columns)
        create_table_sql = f"CREATE OR REPLACE TABLE SAP_SILVER.{s_table} ({column_definitions});"

        # Execute SQL to create table
        try:
            session.sql(create_table_sql).collect()
            print(f"Table {s_table} created successfully in SAP_SILVER.")
        except Exception as e:
            errors.append(f"Error creating table {s_table}: {str(e)}")

    # Return a message based on whether there were errors
    return "Errors: " + ", ".join(errors) if errors else "All tables created successfully."