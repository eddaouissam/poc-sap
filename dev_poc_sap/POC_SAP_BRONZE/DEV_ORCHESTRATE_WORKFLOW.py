"""
Script: Orchestrate Workflow
Author: M'hamed Issam ED-DAOU
Description:
  This script checks whether the SAP_SILVER tables exist and either creates them
  if they do not exist or proceeds to transform and merge data from SAP_BRONZE
  to SAP_SILVER using the respective stored procedures.
"""

from snowflake.snowpark import Session

def check_tables_exist(session: Session, schema: str) -> bool:
    """
    Check if any tables exist in the given schema.

    Parameters:
    session (snowflake.snowpark.Session): The session object for database interaction.
    schema (str): The schema to check for tables.

    Returns:
    bool: True if tables exist, False otherwise.
    """
    query = f"""
    SELECT COUNT(*) AS table_count
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}';
    """
    result = session.sql(query).collect()
    return result[0]['TABLE_COUNT'] > 0

def orchestrate_workflow(session: Session):
    """
    Orchestrates the workflow by checking for table existence and executing the appropriate stored procedures.
    
    Parameters:
    session (snowflake.snowpark.Session): The session object for database interaction.
    """
    silver_schema = 'SAP_SILVER'
    
    # Check if any tables exist in the SAP_SILVER schema
    if check_tables_exist(session, silver_schema):
        # If tables exist, proceed to transform and merge data
        session.sql("CALL DEV_DB_VISEO.SAP_BRONZE.SAP_MERGE();").collect()
    else:
        # If tables do not exist, create them first then proceed to transform and merge data
        session.sql("CALL DEV_DB_VISEO.SAP_BRONZE.CREATE_SILVER_SP();").collect()
        session.sql("CALL DEV_DB_VISEO.SAP_BRONZE.SAP_MERGE();").collect()