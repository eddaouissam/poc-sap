
-- This SQL file creates the table currency_decimal and the function Currency_Decimal, in the GOLD layer

CREATE OR REPLACE TABLE DEV_DB_VISEO.SAP_GOLD.currency_decimal AS (
  SELECT DISTINCT
    tcurx.CURRKEY,
    CAST(POWER(10, 2 - COALESCE(tcurx.CURRDEC, 0)) AS NUMERIC) AS CURRFIX
  FROM
      FROM DEV_DB_VISEO.SAP_SILVER.s_tcurx AS tcurx );

CREATE OR REPLACE FUNCTION DEV_DB_VISEO.SAP_GOLD.Currency_Decimal(
  ip_curr STRING
) AS
((
  SELECT
    currdec
  FROM DEV_DB_VISEO.SAP_SILVER.s_tcurx
  WHERE currkey = ip_curr
))
;