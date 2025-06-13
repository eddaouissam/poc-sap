SELECT
  PricingConditions.Client_MANDT,
  PricingConditions.NumberOfTheDocumentCondition_KNUMV,
  PricingConditions.ConditionItemNumber_KPOSN,
  MAX(CurrencyKey_WAERS) AS ConditionValueCurrencyKey_WAERS,
  MAX(Checkbox_KDATU) AS Checkbox_KDATU,
  SUM(
    IFF(
      PricingConditions.CalculationTypeForCondition_KRECH = 'C'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS ListPrice,
  SUM(
    IFF(
      PricingConditions.CalculationTypeForCondition_KRECH = 'C'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionType_KSCHL = 'PB00',
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS AdjustedPrice,
  SUM(
    IFF(
      PricingConditions.ConditionClass_KOAID = 'A'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS Discount,
  SUM(
    IFF(
      PricingConditions.ConditionForInterCompanyBilling_KFKIV = 'X'
        AND PricingConditions.ConditionClass_KOAID = 'B'
        AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
      PricingConditions.ConditionValue_KWERT,
    NULL)
  ) AS InterCompanyPrice
--##CORTEX-CUSTOMER If you prefer to use currency conversion, uncomment below and
--## uncomment currency_conversion in PricingConditions
-- MAX(TargetCurrency_TCURR) AS TargetCurrency_TCURR,
--  SUM(
--   IFF(
--     PricingConditions.CalculationTypeForCondition_KRECH = 'C'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS ListPriceInTargetCurrency,
-- SUM(
--   IFF(
--     PricingConditions.CalculationTypeForCondition_KRECH = 'C'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionType_KSCHL = 'PB00',
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS AdjustedPriceInTargetCurrency,
-- SUM(
--   IFF(
--     PricingConditions.ConditionClass_KOAID = 'A'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS DiscountInTargetCurrency,
-- SUM(
--   IFF(
--     PricingConditions.ConditionForInterCompanyBilling_KFKIV = 'X'
--       AND PricingConditions.ConditionClass_KOAID = 'B'
--       AND PricingConditions.ConditionIsInactive_KINAK IS NULL,
--     PricingConditions.ConditionValueInTargetCurrency_KWERT,
--   NULL)
-- ) AS InterCompanyPriceInTargetCurrency
FROM
  DEV_DB_VISEO.SAP_GOLD.PricingConditions AS PricingConditions
GROUP BY
  PricingConditions.Client_MANDT,
  PricingConditions.NumberOfTheDocumentCondition_KNUMV,
  PricingConditions.ConditionItemNumber_KPOSN