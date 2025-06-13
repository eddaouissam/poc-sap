SELECT
 TVFST.MANDT AS Client_MANDT,
 TVFST.SPRAS AS LanguageKey_SPRAS,
 TVFST.FAKSP AS Block_FAKSP,
 TVFST.VTEXT AS BillingBlockReason_VTEXT,
FROM {{ source('silver_cdc_processed', 's_tvfst') }} AS TVFST
