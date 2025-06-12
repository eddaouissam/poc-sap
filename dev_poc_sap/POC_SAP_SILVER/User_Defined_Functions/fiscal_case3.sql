CREATE OR REPLACE FUNCTION DEV_DB_VISEO.SAP_GOLD.FISCAL_CASE3("IP_MANDT" VARCHAR, "IP_PERIV" VARCHAR, "IP_DATE" DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS '
  SELECT
    MIN(
      CONCAT(
        CAST(
          IFF(
            Bdatj = ''0000'',
            CAST(EXTRACT(YEAR FROM ip_date) AS STRING),
            Bdatj
          ) AS INT
        ) + CAST(Reljr AS INT),
        Poper
      )
    )
  FROM sap_silver.s_t009b
  WHERE 
    Mandt = ip_mandt
    AND Periv = ip_periv
    AND CONCAT(
      IFF(
        Bdatj = ''0000'',
        TO_CHAR(EXTRACT(YEAR FROM ip_date)),
        Bdatj
      ),
      Bumon,
      Butag
    ) >= TO_CHAR(ip_date, ''YYYYMMDD'')
';