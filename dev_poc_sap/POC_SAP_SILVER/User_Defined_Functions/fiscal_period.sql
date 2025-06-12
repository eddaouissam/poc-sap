CREATE OR REPLACE FUNCTION DEV_DB_VISEO.SAP_GOLD.FISCAL_PERIOD("IP_MANDT" VARCHAR, "IP_PERIV" VARCHAR, "IP_DATE" DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS '
  SELECT
    ANY_VALUE(
      CASE 
        WHEN COALESCE(LENGTH(Xkale), 0)
           + COALESCE(LENGTH(Xjabh), 0) = 0 THEN ''CASE3''
        WHEN Xkale IS NOT NULL THEN ''CASE1''
        ELSE ''CASE2''
      END
    )
  FROM sap_silver.s_t009
  WHERE Mandt = ip_mandt
    AND Periv  = ip_periv
  -- you can still LIMIT 1 if you like
';