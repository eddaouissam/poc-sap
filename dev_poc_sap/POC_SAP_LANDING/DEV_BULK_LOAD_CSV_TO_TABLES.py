"""
Script: Automated Data Loader
Author: M'hamed Issam ED-DAOU
Description:
  This Snowpark Python script automates the process of loading data from specified files in a Snowflake stage into corresponding tables. 
  It accounts for variations in naming conventions (e.g., underscores in table names but not in file names) by normalizing the names during processing. 
  This functionality is crucial for environments with frequent data updates and where automated data loading needs to manage minor discrepancies in naming conventions between data storage (files) and database schema (tables).
  """

from snowflake.snowpark import Session

def load_data_automated(session: Session) -> str:
    """
    Automatically loads data from files in a specified stage into tables with corresponding names in Snowflake,
    ignoring any 'B_' prefixes in the table names.

    Parameters:
    session (snowflake.snowpark.Session): The session object for database interaction.

    Returns:
    str: A message indicating the overall status of the load operations.
    """
    stage_name = 'DEV_DB_VISEO.SAP_RAW_LANDING.SAP_CSV_FILES'
    file_format_name = 'DEV_DB_VISEO.SAP_RAW_LANDING.FF_CSV_SAP'
    target_schema = 'DEV_DB_VISEO.SAP_BRONZE'
    messages = []

    # Retrieve all relevant tables from the specified schema
    tables = session.sql(f"SHOW TABLES IN SCHEMA {target_schema};").collect()
    filtered_tables = [table for table in tables if table['name'].startswith('B_')]

    # Attempt to retrieve file list from the stage using the LIST command
    try:
        files_in_stage = session.sql(f"LIST @{stage_name};").collect()
        # Normalize file names by removing underscores and converting to uppercase, without file extension
        files = {file['name'].split('/')[-1].split('.')[0].replace('_', '').upper(): file['name'].split('/')[-1].split('.')[0] for file in files_in_stage}
    except Exception as e:
        return f"Error accessing stage {stage_name}: {str(e)}"

    # Attempt to load data for each table
    for table_info in tables:
        # Original table name with 'B_' prefix
        table_name_with_prefix = table_info['name']
        # Normalized table name without 'B_' prefix for file matching
        normalized_table_name = table_name_with_prefix[2:].replace('_', '').upper() if table_name_with_prefix.startswith('B_') else table_name_with_prefix.replace('_', '').upper()
        
        # Corresponding file name expected in stage
        if normalized_table_name in files:
            try:
                copy_into_sql = f"""
                COPY INTO {target_schema}.{table_name_with_prefix}
                FROM @{stage_name}/{files[normalized_table_name]}
                FILE_FORMAT = (FORMAT_NAME = '{file_format_name}')
                ON_ERROR = CONTINUE
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """
                session.sql(copy_into_sql).collect()
                messages.append(f"Data successfully loaded into {target_schema}.{table_name_with_prefix} from {files[normalized_table_name]}.")
            except Exception as e:
                messages.append(f"Error loading data into {target_schema}.{table_name_with_prefix}: {str(e)}")
        else:
            messages.append(f"No corresponding file found for table {target_schema}.{table_name_with_prefix}.")

    return "\n".join(messages)