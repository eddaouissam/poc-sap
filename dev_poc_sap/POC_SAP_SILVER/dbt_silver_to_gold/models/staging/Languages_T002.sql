SELECT
  T002.SPRAS AS LanguageKey_SPRAS,
  T002.LASPEZ AS LanguageSpecifications_LASPEZ,
  T002.LAHQ AS DegreeOfTranslationOfLanguage_LAHQ,
  T002.LAISO AS TwoCharacterSapLanguageCode_LAISO
FROM {{ source('silver_cdc_processed', 's_t002') }} AS T002
