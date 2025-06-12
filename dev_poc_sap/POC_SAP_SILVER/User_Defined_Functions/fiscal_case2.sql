CREATE OR REPLACE FUNCTION DEV_DB_VISEO.SAP_GOLD.FISCAL_CASE2("IP_MANDT" VARCHAR, "IP_PERIV" VARCHAR, "IP_DATE" DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS '
  SELECT
    ANY_VALUE(
      CONCAT(
        CAST(Bdatj AS INT) + CAST(Reljr AS INT),
        Poper
      )
    )
  FROM sap_silver.s_t009b
  WHERE
    Mandt = ip_mandt
    AND Periv = ip_periv
    AND Bdatj = TO_CHAR(EXTRACT(YEAR  FROM ip_date))
    AND Bumon = TO_CHAR(EXTRACT(MONTH FROM ip_date), ''00'')
';