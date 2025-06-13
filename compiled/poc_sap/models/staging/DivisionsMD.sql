SELECT
  TSPA.mandt AS Client_MANDT,
  TSPA.spart AS Division_SPART,
  TSPAT.spras AS LanguageKey_SPRAS,
  TSPAT.vtext AS DivisionName_VTEXT
FROM
  DEV_DB_VISEO.sap_silver.s_tspa AS TSPA
LEFT JOIN
  DEV_DB_VISEO.sap_silver.s_tspat AS TSPAT
  ON
    TSPA.MANDT = TSPAT.MANDT
    AND TSPA.SPART = TSPAT.SPART