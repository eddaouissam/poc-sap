SELECT
  CurrencyConversion.MANDT AS Client_MANDT,
  CurrencyConversion.KURST AS ExchangeRateType_KURST,
  CurrencyConversion.FCURR AS FromCurrency_FCURR,
  CurrencyConversion.TCURR AS ToCurrency_TCURR,
  CurrencyConversion.UKURS AS ExchangeRate_UKURS,
  CurrencyConversion.start_date AS StartDate,
  CurrencyConversion.end_date AS EndDate,
  CurrencyConversion.conv_date AS ConvDate
FROM
  {{ ref('currency_conversion') }} AS CurrencyConversion
--## CORTEX-CUSTOMER: Uncomment the following code to use latest available
--exchange rate for reports if the currency conversion DAG is not working or setup
-- UNION ALL
-- SELECT
--   MANDT AS Client_MANDT,
--   KURST AS ExchangeRateType_KURST,
--   FCURR AS FromCurrency_FCURR,
--   TCURR AS ToCurrency_TCURR,
--   UKURS AS ExchangeRate_UKURS,
--   start_date AS StartDate,
--   CURRENT_DATE() AS EndDate,
--   ConvDate
-- FROM
--   {{ ref('currency_conversion') }},
--   UNNEST(GENERATE_DATE_ARRAY(
--       (SELECT DATE_ADD(MAX(conv_date), INTERVAL 1 DAY)
--         FROM {{ ref('currency_conversion') }}),
--       CURRENT_DATE())) AS ConvDate
-- WHERE
--   conv_date = (
--     SELECT
--       MAX(conv_date)
--     FROM
--       {{ ref('currency_conversion') }})
