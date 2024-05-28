SELECT
  tcurc.mandt AS Client_MANDT, tcurc.waers AS CurrencyCode_WAERS, tcurc.isocd AS CurrencyISO_ISOCD,
  tcurx.currdec AS CurrencyDecimals_CURRDEC, tcurt.spras AS Language,
  -- tcurt.ktext AS CurrShortText_KTEXT, tcurt.ltext AS CurrLongText_LTEXT
FROM sap_silver.s_tcurc AS tcurc
INNER JOIN
  sap_silver.s_tcurx AS tcurx ON tcurc.waers = tcurx.currkey
-- INNER JOIN
--   sap_silver.s_tcurt AS tcurt
--   ON tcurc.waers = tcurt.waers AND tcurc.mandt = tcurt.mandt
-- (table TCURT not found )