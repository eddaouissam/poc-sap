
-- This SQL file creates the table currency_conversion and the function Currency_Conversion, in the GOLD layer

CREATE TABLE IF NOT EXISTS sap_gold.currency_conversion
(
  mandt STRING,
  kurst STRING,
  fcurr STRING,
  tcurr STRING,
  ukurs NUMBER,
  start_date DATE,
  end_date DATE,
  conv_date DATE
);

CREATE OR REPLACE FUNCTION sap_gold.Currency_Conversion(
  ip_mandt STRING,
  ip_kurst STRING,
  ip_fcurr STRING,
  ip_tcurr STRING,
  ip_date DATE,
  ip_amount NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
  SELECT CASE
           WHEN ukurs < 0 THEN (1 / ABS(ukurs)) * ip_amount
           ELSE ukurs * ip_amount
         END
  FROM (
    SELECT
      mandt,
      kurst,
      fcurr,
      tcurr,
      ukurs,
      TO_DATE(99999999 - CAST(gdatu AS STRING), 'YYYYMMDD') AS start_date,
      COALESCE(
        LEAD(TO_DATE(99999999 - CAST(gdatu AS STRING), 'YYYYMMDD')) 
        OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC),
        DATEADD(year, 1000, TO_DATE(99999999 - CAST(gdatu AS STRING), 'YYYYMMDD'))
      ) AS end_date
    FROM sap_silver.s_tcurr
  ) AS sub
  WHERE mandt = ip_mandt
    AND kurst = ip_kurst
    AND fcurr = ip_fcurr
    AND tcurr = ip_tcurr
    AND ip_date BETWEEN start_date AND end_date;
$$;
