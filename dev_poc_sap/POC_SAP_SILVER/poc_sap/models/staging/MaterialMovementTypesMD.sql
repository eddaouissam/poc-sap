SELECT
  t156t.MANDT AS Client_MANDT,
  t156t.SPRAS AS LanguageKey_SPRAS,
  t156t.BWART AS MovementType_BWART,
  t156t.SOBKZ AS SpecialStock_SOBKZ,
  t156t.KZBEW AS MovementIndicator_KZBEW,
  t156t.KZZUG AS ReceiptIndicator_KZZUG,
  t156t.KZVBR AS ConsumptionPosting_KZVBR,
  t156t.BTEXT AS MovementTypeText_BTEXT
FROM {{ source('silver_cdc_processed', 's_t156t') }} AS t156t
