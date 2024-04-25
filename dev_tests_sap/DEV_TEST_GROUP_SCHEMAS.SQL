
CREATE OR REPLACE PROCEDURE filterByRole(tableName VARCHAR, role VARCHAR)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'filter_by_role'
AS
$$
import pandas as pd
from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
  df = session.table(table_name)
  pd_df = df.to_pandas()
  data_df =pd_df[ pd_df["C_MKTSEGMENT"]==role]
  data_df = session.create_dataframe(data_df)
  return data_df
$$;

call filterByRole('SNOWFLAKE_SAMPLE_DATA.tpch_sf1.customer','HOUSEHOLD');


-- TEST TO PRESENT
CREATE OR REPLACE PROCEDURE testtest()
RETURNS STRING 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
def main(session: snowpark.Session):
 session.close()
$$;



call testtest();




-- TEST 2
CREATE OR REPLACE PROCEDURE abcde(table_name VARCHAR)
RETURNS TABLE() 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark

def main(session: snowpark.Session, table_name):
 df = session.table(table_name)
 return df
$$;



call abcde('ADR6_SCHEMA');



-- TEST 3

CREATE OR REPLACE PROCEDURE HAHA(table_name VARCHAR)
RETURNS TABLE() 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import pandas as pd

def main(session: snowpark.Session, table_name):
 df = session.table(table_name)
 pandasdf = df.to_pandas()
 data_df = session.create_dataframe(pandasdf)
 return data_df
$$;

call HAHA('ADR6_SCHEMA');


-- TEST 4 

CREATE OR REPLACE PROCEDURE TESTAGAIN(table_name VARCHAR)
RETURNS STRING 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
import pandas as pd

def main(session: snowpark.Session, table_name):
 df = session.table(table_name).collect()
 pandasdf = df.to_pandas()
 #data_df = session.create_dataframe(pandasdf)
 return df
$$;

call TESTAGAIN('ADR6_SCHEMA');




-- Query test to union
SELECT * FROM TEST.TEST.ADR_6_SCHEMA
UNION ALL
SELECT * FROM TEST.TEST.ADRC_SCHEMA;


-- ANOTHER ONE
SELECT LISTAGG('SELECT TYPE,NAME FROM ' || table_name, ' UNION ALL ') AS concatenated_sql
FROM information_schema.tables
WHERE table_schema = 'TEST'
AND table_name LIKE '%SCHEMA';



CREATE OR REPLACE TABLE TEST.PUBLIC.AGAIN AS SELECT TYPE, NAME FROM(

SELECT TYPE,NAME FROM ADRCT_SCHEMA UNION ALL SELECT TYPE,NAME FROM ADRC_SCHEMA UNION ALL SELECT TYPE,NAME FROM ADRT_SCHEMA UNION ALL SELECT TYPE,NAME FROM ADR_6_SCHEMA UNION ALL SELECT TYPE,NAME FROM AFKO_SCHEMA UNION ALL SELECT TYPE,NAME FROM AFPO_SCHEMA UNION ALL SELECT TYPE,NAME FROM ANKT_SCHEMA UNION ALL SELECT TYPE,NAME FROM BKPF_SCHEMA UNION ALL SELECT TYPE,NAME FROM BSEG_SCHEMA UNION ALL SELECT TYPE,NAME FROM BUT_000_SCHEMA UNION ALL SELECT TYPE,NAME FROM BUT_020_SCHEMA UNION ALL SELECT TYPE,NAME FROM CEPCT_SCHEMA UNION ALL SELECT TYPE,NAME FROM CEPC_HIER_SCHEMA UNION ALL SELECT TYPE,NAME FROM CKMLCR_SCHEMA UNION ALL SELECT TYPE,NAME FROM CSKS_HIER_SCHEMA UNION ALL SELECT TYPE,NAME FROM CSKS_SCHEMA UNION ALL SELECT TYPE,NAME FROM CSKT_SCHEMA UNION ALL SELECT TYPE,NAME FROM CURRENCY_CONVERSION_SCHEMA UNION ALL SELECT TYPE,NAME FROM DD_03_L_SCHEMA UNION ALL SELECT TYPE,NAME FROM DD_07_T_SCHEMA UNION ALL SELECT TYPE,NAME FROM EKBE_SCHEMA UNION ALL SELECT TYPE,NAME FROM EKET_SCHEMA UNION ALL SELECT TYPE,NAME FROM ANLA_SCHEMA UNION ALL SELECT TYPE,NAME FROM CEPC_SCHEMA UNION ALL SELECT TYPE,NAME FROM CURRENCY_DECIMAL_SCHEMA UNION ALL SELECT TYPE,NAME FROM EKKN_SCHEMA UNION ALL SELECT TYPE,NAME FROM EKKO_SCHEMA UNION ALL SELECT TYPE,NAME FROM AUFK_SCHEMA UNION ALL SELECT TYPE,NAME FROM CKMLHD_SCHEMA UNION ALL SELECT TYPE,NAME FROM EKES_SCHEMA

);
-- POSSIBILITE D OPTIMISATION


SELECT * FROM TEST.PUBLIC.AGAIN;


-- created generic
CREATE OR REPLACE PROCEDURE CREATE_TABLE_FROM_QUERY()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_concatenated_sql STRING;
  v_create_table_sql STRING;
BEGIN
  -- Retrieve concatenated SQL
  SELECT LISTAGG('SELECT TYPE, NAME FROM ' || table_name, ' UNION ALL ') 
  INTO v_concatenated_sql
  FROM information_schema.tables
  WHERE table_schema = 'TEST'
  AND table_name LIKE '%SCHEMA';

  -- Construct create table statement
  v_create_table_sql := 'CREATE OR REPLACE TABLE new_table AS ' || v_concatenated_sql;

  -- Execute create table statement
  EXECUTE IMMEDIATE v_create_table_sql;

  -- Return success message
  RETURN 'Table created successfully.';
END;
$$;



call CREATE_TABLE_FROM_QUERY();


select distinct * from new_table;



-- DONE (maybe)
CREATE OR REPLACE PROCEDURE CREATE_TABLE_FROM_QUERY(TABLE_NAME VARCHAR)
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_concatenated_sql STRING;
  v_create_table_sql STRING;
BEGIN
  -- Retrieve concatenated SQL
  SELECT LISTAGG('SELECT TYPE, NAME FROM ' || table_name, ' UNION ALL ') 
  INTO v_concatenated_sql
  FROM information_schema.tables
  WHERE table_name LIKE '%SCHEMA';

  -- Construct create table statement
  v_create_table_sql := 'CREATE OR REPLACE TABLE ' || TABLE_NAME || ' AS ' || v_concatenated_sql;

  -- Execute create table statement
  EXECUTE IMMEDIATE v_create_table_sql;

  -- Return success message
  RETURN 'Table ' || TABLE_NAME || ' created successfully.';
END;
$$;



select * from testtable;

call CREATE_TABLE_FROM_QUERY('TEST.PUBLIC.TESTTABLE');

select distinct * from testtable;


-- SP to create the table 

CREATE OR REPLACE PROCEDURE CREATE_GOOD_TABLE_FROM_QUERY(TABLE_NAME VARCHAR)
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
AS
$$
DECLARE
  v_concatenated_sql STRING;
  v_create_table_sql STRING;
BEGIN
  -- Retrieve concatenated SQL
  SELECT LISTAGG('SELECT ''' || table_name || ''' AS source_table, TYPE, NAME FROM ' || table_name, ' UNION ALL ') 
  INTO v_concatenated_sql
  FROM information_schema.tables
  WHERE table_name LIKE '%SCHEMA';

  -- Construct create table statement
  v_create_table_sql := 'CREATE OR REPLACE TABLE ' || TABLE_NAME || ' AS ' || v_concatenated_sql;

  -- Execute create table statement
  EXECUTE IMMEDIATE v_create_table_sql;

  -- Return success message
  RETURN 'Table ' || TABLE_NAME || ' created successfully.';
END;
$$;



select * from test.public.coucou;

call CREATE_GOOD_TABLE_FROM_QUERY('TEST.PUBLIC.COUCOU');

select * from test.public.coucou;


call CREATE_TABLE_FROM_QUERY('TEST.PUBLIC.TOCOMPARE');


TABLE_FROM_QUERY('TEST.PUBLIC.TESTTABLE');