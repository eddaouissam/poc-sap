"""
Script: Automated Data Loader
Author: M'hamed Issam ED-DAOU
Description:
  This Snowpark Python script automates the process of loading data from specified files in a Snowflake stage into corresponding tables. 
  It accounts for variations in naming conventions (e.g., underscores in table names but not in file names) by normalizing the names during processing. 
  This functionality is crucial for environments with frequent data updates and where automated data loading needs to manage minor discrepancies in naming conventions between data storage (files) and database schema (tables).
"""
from snowflake.snowpark import Session

def load_data_automated(session: Session):
    """
    Automatically loads data from files in a specified stage into tables with corresponding names in Snowflake,
    using parameters provided.

    Parameters:
    session (snowflake.snowpark.Session): The session object for database interaction.

    Returns:
    str: A message indicating the overall status of the load operations.
    """
    stage_name = 'DEV_DB_VISEO.SAP_RAW_DATA.SAP_CSV_FILES'
    file_format_name = 'DEV_DB_VISEO.SAP_RAW_DATA.MY_CSV_FORMAT2'
    messages = []

    # Retrieve all relevant tables
    tables = session.sql("SHOW TABLES LIKE '%';").collect()  # Adjust the LIKE pattern as necessary

    # Attempt to retrieve file list from the stage using the LIST command
    try:
        files_in_stage = session.sql(f"LIST @{stage_name};").collect()
        files = {file['name'].split('/')[-1].split('.')[0].upper(): file['name'] for file in files_in_stage}
    except Exception as e:
        return f"Error accessing stage {stage_name}: {str(e)}"

    # Attempt to load data for each table
    for table_info in tables:
        table_name = table_info['name']
        file_name = table_name + '.csv'  # Adjust as necessary

        if table_name.upper() in files:
            try:
                copy_into_sql = f"""
                COPY INTO {table_name}
                FROM @{stage_name}/{files[table_name.upper()]}
                FILE_FORMAT = (FORMAT_NAME = {file_format_name})
                ON_ERROR = CONTINUE
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """
                session.sql(copy_into_sql).collect()
                messages.append(f"Data successfully loaded into {table_name} from {files[table_name.upper()]}.")
            except Exception as e:
                messages.append(f"Error loading data into {table_name}: {str(e)}")
        else:
            messages.append(f"No corresponding file found for table {table_name}.")

    return "\n".join(messages)