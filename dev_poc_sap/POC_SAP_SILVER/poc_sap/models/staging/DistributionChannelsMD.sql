SELECT
  TVTW.MANDT AS Client_MANDT,
  TVTW.VTWEG AS DistributionChannel_VTWEG,
  TVTWT.SPRAS AS Language_SPRAS,
  TVTWT.VTEXT AS DistributionChannelName_VTEXT
FROM
  {{ source('silver_cdc_processed', 's_tvtw') }} AS TVTW
INNER JOIN
  {{ source('silver_cdc_processed', 's_tvtwt') }} AS TVTWT
  ON
    TVTW.MANDT = TVTWT.MANDT
    AND TVTW.VTWEG = TVTWT.VTWEG
