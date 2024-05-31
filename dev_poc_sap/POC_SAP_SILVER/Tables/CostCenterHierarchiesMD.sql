SELECT
  setheadert.MANDT AS Client_MANDT,
  setheadert.SETCLASS AS SetClass_SETCLASS,
  setheadert.SUBCLASS AS OrganizationalUnit_SUBCLASS,
  setheadert.SETNAME AS SetName_SETNAME,
  setheadert.LANGU AS LanguageKey_LANGU,
  setheadert.DESCRIPT AS ShortDescriptionOfSet_DESCRIPT
FROM
  sap_silver.s_setheadert AS setheadert
WHERE
  setclass = '0101'
