SELECT
  t157e.MANDT AS Client_MANDT,
  t157e.SPRAS AS LanguageKey_SPRAS,
  t157e.BWART AS MovementType_BWART,
  t157e.GRUND AS ReasonForMovement_GRUND,
  t157e.GRTXT AS ReasonForGoodsMovement_GRTXT
FROM sap_silver.s_t157e AS t157e
