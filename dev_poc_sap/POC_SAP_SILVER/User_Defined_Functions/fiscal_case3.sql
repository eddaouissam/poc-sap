CREATE OR REPLACE FUNCTION sap_gold.Fiscal_Case3 (
  ip_mandt STRING, 
  ip_periv STRING, 
  ip_date DATE
) 
RETURNS STRING
LANGUAGE SQL
AS
$$
  SELECT MIN(
    CONCAT(
      CAST(IF(Bdatj = '0000', CAST(EXTRACT(YEAR FROM ip_date) AS STRING), Bdatj) AS INT) + CAST(Reljr AS INT),
      Poper
    )
  )
  FROM sap_silver.s_t009b
  WHERE 
    Mandt = ip_mandt
    AND Periv = ip_periv
    AND CONCAT(
      IF(Bdatj = '0000', TO_CHAR(EXTRACT(YEAR FROM ip_date)), Bdatj),
      Bumon,
      Butag
    ) >= TO_CHAR(ip_date, 'YYYYMMDD')
$$;
