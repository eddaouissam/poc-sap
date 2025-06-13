SELECT
  t148t.MANDT AS Client_MANDT,
  t148t.SPRAS AS LanguageKey_SPRAS,
  t148t.SOBKZ AS SpecialStockIndicator_SOBKZ,
  t148t.SOTXT AS DescriptionOfSpecialStock_SOTXT
FROM {{ source('silver_cdc_processed', 's_t148t') }} AS t148t
