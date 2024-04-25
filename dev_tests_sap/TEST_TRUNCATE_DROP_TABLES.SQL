CREATE OR REPLACE PROCEDURE DEV_DB_VISEO.SAP_BRONZE.TRUNCATE_ALL_TABLES_EXCEPT_RAW_SCHEMAS("DB_SCHEMA_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    DB_NAME STRING;
    SCHEMA_NAME STRING;
    QUERY_RESULT RESULTSET;
    TRUNCATE_CMD STRING;
BEGIN
    -- Extract the database and schema names from the input
    DB_NAME := SPLIT_PART(:DB_SCHEMA_NAME, ''.'', 1);
    SCHEMA_NAME := SPLIT_PART(:DB_SCHEMA_NAME, ''.'', 2);

    -- Prepare the resultset query dynamically
    QUERY_RESULT := (SELECT table_name 
                     FROM information_schema.tables 
                     WHERE table_schema = :SCHEMA_NAME
                     AND table_catalog = :DB_NAME
                     AND table_type = ''BASE TABLE''
                     AND table_name != ''RAW_SCHEMAS'');

    -- Loop through the result set
    FOR QUERY_REC IN QUERY_RESULT DO
        -- Construct the TRUNCATE command
    TRUNCATE_CMD := ''TRUNCATE TABLE "'' || :DB_NAME || ''"."'' || :SCHEMA_NAME || ''"."'' || QUERY_REC.TABLE_NAME || ''"'';


        -- Execute TRUNCATE TABLE statement
        EXECUTE IMMEDIATE TRUNCATE_CMD;
    END FOR;

    -- Return success message
    RETURN ''All tables in '' || :DB_SCHEMA_NAME || '' have been truncated except RAW_SCHEMAS.'';
END;
';