SELECT t179.mandt AS Client_MANDT,
  t179.prodh AS Hierarchy_PRODH,
  t179.stufe AS Level_STUFE,
  t179t.spras AS Language_SPRAS,
  t179t.vtext AS Description_VTEXT
FROM {{ source('silver_cdc_processed', 's_t179') }} AS t179
INNER JOIN {{ source('silver_cdc_processed', 's_t179t') }} AS t179t
  ON t179.mandt = t179t.mandt AND t179.prodh = t179t.prodh
