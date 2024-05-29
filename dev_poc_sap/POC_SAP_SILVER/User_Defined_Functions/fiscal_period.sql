CREATE OR REPLACE FUNCTION sap_gold.Fiscal_Period (
  ip_mandt STRING, 
  ip_periv STRING, 
  ip_date DATE
) 
RETURNS STRING
LANGUAGE SQL
AS
$$
  SELECT 
    CASE 
      WHEN COALESCE(LENGTH(Xkale), 0) + COALESCE(LENGTH(Xjabh), 0) = 0 THEN 'CASE3'
      WHEN Xkale IS NOT NULL THEN 'CASE1'
      ELSE 'CASE2'
    END
  FROM sap_silver.s_t009
  WHERE Mandt = ip_mandt
    AND Periv = ip_periv
  LIMIT 1
$$;
