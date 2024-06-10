"""
Script: Automated CDC Consistency Checker
Author: M'hamed Issam ED-DAOU
Description:
    This Snowpark Python script is designed to validate the Change Data Capture (CDC) process between 
    two schemas in Snowflake: SAP_BRONZE (source) and SAP_SILVER (target). The script verifies the integrity 
    and consistency of data transferred from the source to the target schema by comparing the row counts 
    of tables after applying CDC operations.

    This tool generates logs for each table it checks, offering insights into potential discrepancies 
    and confirming successful synchronization, thereby aiding in troubleshooting and auditing CDC processes.
"""

from snowflake.snowpark.session import Session

def get_primary_keys(session: Session, base_table_name: str) -> list:
    """ Fetch primary keys from B_dd03l for a given table """
    primary_keys_query = f"""
        SELECT fieldname
        FROM SAP_BRONZE.B_dd03l
        WHERE UPPER(tabname) = UPPER('{base_table_name}') AND keyflag = 'X' AND fieldname != '.INCLUDE'
    """
    try:
        return [row['FIELDNAME'] for row in session.sql(primary_keys_query).collect()]
    except Exception as e:
        print(f"Error fetching primary keys for {base_table_name}: {e}")
        return []
 
def build_and_execute_query(session: Session, bronze_table: str, silver_table: str, primary_keys: list) -> bool:
    """ Builds and executes the row count check query for given tables """
    if not primary_keys:
        print(f"No primary keys found for {bronze_table}. Cannot perform deduplication.")
        return False

    primary_key_string = ', '.join(primary_keys)
    latest_records_query = f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key_string} ORDER BY recordstamp DESC) AS rn
            FROM {bronze_table}
        ) a
        WHERE rn = 1
        AND COALESCE(a.operation_flag, 'I') != 'D'
    """
    try:
        latest_records_df = session.sql(latest_records_query)
        latest_records_count = latest_records_df.count()
        target_table_count = session.table(silver_table).count()

        print(f"Checking {bronze_table} against {silver_table}:")
        print(f"  Rows in latest records from {bronze_table}: {latest_records_count}")
        print(f"  Rows in {silver_table}: {target_table_count}")

        return latest_records_count == target_table_count
    except Exception as e:
        print(f"Failed to execute query or count rows for {bronze_table}: {e}")
        return False

def check_all_tables(session: Session):
    """ Main function to check all tables in SAP_BRONZE and compare with SAP_SILVER """
   # Track if all tables matched successfully
    tables = session.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'SAP_BRONZE' AND table_name LIKE 'B\\_%' AND table_name != 'B_DD03L'").collect()
    all_success = 'ALL SUCCESS !!!!'
    for table_info in tables:
        bronze_table = table_info['TABLE_NAME']
        silver_table = 'SAP_SILVER.S_' + bronze_table[2:]  # Strip 'B_' and prepend 'S_'
        base_table_name = bronze_table[2:]
        primary_keys = get_primary_keys(session, base_table_name)
        if primary_keys:
            if not build_and_execute_query(session, bronze_table, silver_table, primary_keys):
                print(f"Row counts DO NOT MATCH for {bronze_table} and {silver_table}.")
                all_success = 'UNFORTUNATE'
        else:
            print(f"Skipping {bronze_table} due to no primary keys or an error occurred.")

    return all_success
