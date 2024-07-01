SELECT
  TSPA.mandt AS Client_MANDT,
  TSPA.spart AS Division_SPART,
  TSPAT.spras AS LanguageKey_SPRAS,
  TSPAT.vtext AS DivisionName_VTEXT
FROM
  {{ source('silver_cdc_processed', 's_tspa') }} AS TSPA
LEFT JOIN
  {{ source('silver_cdc_processed', 's_tspat') }} AS TSPAT
  ON
    TSPA.MANDT = TSPAT.MANDT
    AND TSPA.SPART = TSPAT.SPART
