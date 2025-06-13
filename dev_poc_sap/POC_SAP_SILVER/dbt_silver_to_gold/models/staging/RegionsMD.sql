SELECT
  T005S.MANDT AS Client_MANDT,
  T005S.LAND1 AS CountryKey_LAND1,
  T005S.BLAND AS Region_BLAND,
  T005S.FPRCD AS ProvincialTaxCode_FPRCD,
  T005S.HERBL AS StateOfManufacture_HERBL
FROM
  {{ source('silver_cdc_processed', 's_t005s') }} AS T005S