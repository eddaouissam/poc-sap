SELECT
  TVLST.MANDT AS Client_MANDT,
  TVLST.SPRAS AS LanguageKey_SPRAS,
  TVLST.LIFSP AS DefaultDeliveryBlock_LIFSP,
  TVLST.VTEXT AS DeliveryBlockReason_VTEXT,
FROM sap_silver.s_tvlst AS TVLST
