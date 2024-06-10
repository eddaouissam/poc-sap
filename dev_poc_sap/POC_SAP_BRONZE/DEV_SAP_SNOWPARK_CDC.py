"""
Script: Automated Change Data Capture (CDC)
Author: M'hamed Issam ED-DAOU
Description:
  This Snowpark Python script automates the process of capturing changes in data from source tables and applying them to target tables, implementing a CDC (Change Data Capture) mechanism. 
  It identifies primary keys and updates records in the target table based on changes detected in the source table, ensuring data integrity and consistency between environments. 
  This functionality is essential for maintaining synchronized data across different stages of a data pipeline, facilitating real-time analytics and reporting.
"""

from snowflake.snowpark import Session
from typing import List, Tuple

def get_primary_keys(session: Session, table_name: str) -> List[str]:
    """
    Retrieves a list of primary key column names for a given table from the SAP_BRONZE schema,
    filtering out any non-primary key columns and any special include columns.

    Parameters:
    - session: The Snowpark session to use for SQL execution.
    - table_name: The name of the table for which to extract primary keys.

    Returns:
    - A list of strings representing the primary key column names.
    """
    # Normalize table name by removing 'B_' prefix and underscores for correct querying
    tabname = table_name.replace("B_", "").replace("_", "")
    primary_key_query = f"""
    SELECT fieldname
    FROM SAP_BRONZE.B_dd03l
    WHERE UPPER(tabname) = UPPER('{tabname}') AND keyflag = 'X' AND fieldname != '.INCLUDE'
    """
    # Execute the query and collect results
    primary_keys = [row['FIELDNAME'] for row in session.sql(primary_key_query).collect()]
    return primary_keys

def get_columns(session: Session, table_name: str) -> List[str]:
    """
    Retrieves the column names for a specified table, excluding 'OPERATION_FLAG' and 'IS_DELETED'.

    Parameters:
    - session: The Snowpark session to use.
    - table_name: The table from which to fetch column names.

    Returns:
    - A list of column names, excluding any operational or deletion flags.
    """
    excluded_columns = ['OPERATION_FLAG', 'IS_DELETED']  # Columns to exclude from the fetch
    column_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'SAP_BRONZE' AND table_name = '{table_name}'
    AND column_name NOT IN ({', '.join("'" + col + "'" for col in excluded_columns)})
    """
    # Execute the query and collect results
    columns = [row['COLUMN_NAME'] for row in session.sql(column_query).collect()]
    return columns

def build_join_condition(primary_keys: List[str]) -> str:
    """
    Enhances the join condition to combine primary keys with handling for NULL values.
    This ensures that each table join respects the primary key combinations, preventing issues like duplicates.
    """
    conditions = []
    for pk in primary_keys:
        condition = f"(T.{pk} = S.{pk} OR (T.{pk} IS NULL AND S.{pk} IS NULL))"
        conditions.append(condition)
    return " AND ".join(conditions)

def format_columns_for_sql(columns: List[str]) -> Tuple[str, str, str]:
    """
    Formats column names for use in SQL INSERT and UPDATE statements.

    Parameters:
    - columns: A list of column names.

    Returns:
    - A tuple containing strings for the insert columns, value placeholders, and update assignments.
    """
    insert_columns = ", ".join(columns)  # Format for INSERT clause
    values_columns = ", ".join([f"S.{col}" for col in columns])  # Format for VALUES clause
    update_columns = ", ".join([f"T.{col} = S.{col}" for col in columns])  # Format for UPDATE clause
    return (insert_columns, values_columns, update_columns)

def automate_cdc(session: Session) -> str:
    schema_query = r"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'SAP_BRONZE' AND table_name LIKE 'B\\_%' ESCAPE '\\' AND table_name != 'B_DD03L'
    """
    results_summary = []
    tables = session.sql(schema_query).collect()

    for table_info in tables:
        source_table = table_info['TABLE_NAME']
        target_table = 'S_' + source_table[2:]

        columns = get_columns(session, source_table)
        primary_keys = get_primary_keys(session, source_table)
        if not primary_keys:
             # Handle the scenario where no primary keys are found by copying everything
            copy_sql = f"""
            INSERT INTO SAP_SILVER.{target_table}
            SELECT * FROM SAP_BRONZE.{source_table}
            """
            try:
                session.sql(copy_sql).collect()
                results_summary.append(f"Complete table loaded for {target_table} due to no primary keys.")
            except Exception as e:
                results_summary.append(f"Failed to load data for {target_table}: {str(e)}")
            continue  # Skip to next table

        insert_columns, values_columns, update_columns = format_columns_for_sql(columns)
        primary_key_condition = build_join_condition(primary_keys)

        merge_sql = f"""
        MERGE INTO SAP_SILVER.{target_table} AS T
        USING (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(primary_keys)} ORDER BY recordstamp DESC) AS rn
                FROM SAP_BRONZE.{source_table}
            ) a
            WHERE rn = 1
        ) S ON {primary_key_condition}
        WHEN MATCHED AND S.operation_flag IN ('I', 'U') THEN
            UPDATE SET {update_columns}
        WHEN NOT MATCHED AND COALESCE(S.operation_flag, 'I') != 'D' THEN
            INSERT ({insert_columns}) VALUES ({values_columns})
        WHEN MATCHED AND S.operation_flag = 'D' THEN
            DELETE;
        """

        try:
            session.sql(merge_sql).collect()
            results_summary.append(f"Merge operation successful for {target_table}.")
        except Exception as e:
            results_summary.append(f"Failed to merge data for {target_table}: {str(e)}")

    return "\n".join(results_summary)
