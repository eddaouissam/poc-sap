# POC Setup Documentation

This repository contains scripts and configuration files for setting up and using the POC Framework with Snowflake. Below are the instructions for each script.

## Scripts

### FIVETRAN_SETUP.SQL
- **Purpose**: Establishes connection between Fivetran and Snowflake for data loading.
- **Note**: Used in the initial setup, not necessary for the demo.

### DEV_USER_SETUP.SQL
- **Purpose**: Sets up new development USER in Snowflake.
- **Usage**: Run this script in Snowflake for development user configuration.

### DEV_SNOWFLAKE_SETUP.SQL
- **Purpose**: Sets up the Snowflake environment (database, virtual warehouse, roles, schema...)
- **Usage**: 
  - Can be automatically triggered using `setup_snowflake_env.yml` in `.gitHub/workflows`.
  - Ensures automatic environment setup with GitHub Actions.

### upload_files.sh
- **Purpose**: Uploads CSV files from local system to `SAP_RAW_LANDING` schema in Snowflake.
- **Prerequisites**: Ensure the stage exists in Snowflake and `snowsql` is installed.
- **Usage**: Run the script from a local machine using bash: `bash upload_files.sh`.

## Notes
- Ensure necessary permissions and roles are set in Snowflake before executing the scripts.
- Verify required stages and configurations exist before using `upload_files.sh`.
