
CREATE OR REPLACE FUNCTION SAP_GOLD.Fiscal_Case2(
  ip_mandt STRING, ip_periv STRING, ip_date DATE
) AS 
((
    SELECT IF(
      COALESCE(LENGTH(Xkale), 0) + COALESCE(LENGTH(Xjabh), 0) = 0,
      'CASE3',
      IF(Xkale IS NOT NULL, 'CASE1', 'CASE2')
    )
    FROM
      sap_silver.s_t009
    WHERE
      Mandt = Ip_Mandt
      AND Periv = Ip_Periv
))