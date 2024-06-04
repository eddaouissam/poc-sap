CREATE OR REPLACE FUNCTION SAP_GOLD.Fiscal_Case2 (
  ip_mandt STRING, 
  ip_periv STRING, 
  ip_date DATE
) 
RETURNS STRING
LANGUAGE SQL
AS
$$
  SELECT
    CONCAT(CAST(Bdatj AS INT) + CAST(Reljr AS INT), Poper)
  FROM
    sap_silver.s_t009b
  WHERE
    Mandt = ip_mandt
    AND Periv = ip_periv
    AND Bdatj = TO_CHAR(EXTRACT(YEAR FROM ip_date))
    AND Bumon = TO_CHAR(EXTRACT(MONTH FROM ip_date), '00')
$$;
