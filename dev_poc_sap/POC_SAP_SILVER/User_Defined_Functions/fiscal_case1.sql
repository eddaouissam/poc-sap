CREATE OR REPLACE FUNCTION sap_gold.Fiscal_Case1 (
  ip_mandt STRING, 
  ip_periv STRING, 
  ip_date DATE
) 
RETURNS STRING
LANGUAGE SQL
AS
$$
  SELECT
    CONCAT(EXTRACT(YEAR FROM ip_date), LPAD(TO_CHAR(EXTRACT(MONTH FROM ip_date)), 3, '0')) AS op_period
$$;