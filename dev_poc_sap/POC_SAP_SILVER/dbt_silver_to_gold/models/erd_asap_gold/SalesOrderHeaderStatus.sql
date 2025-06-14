SELECT
  vbuk.MANDT AS Client_MANDT,
  vbuk.VBELN AS SalesDocument_VBELN,
  vbuk.RFSTK AS ReferenceDocumentHeaderStatus_RFSTK,
  vbuk.RFGSK AS OverallReferenceStatusAllItems_RFGSK,
  vbuk.BESTK AS ConfirmationStatus_BESTK,
  vbuk.LFSTK AS DeliveryStatus_LFSTK,
  vbuk.LFGSK AS OverallDeliveryStatus_LFGSK,
  vbuk.WBSTK AS TotalGoodsMovementStatus_WBSTK,
  vbuk.FKSTK AS BillingStatus_FKSTK,
  vbuk.FKSAK AS OrderRelatedBillingStatusAllItems_FKSAK,
  vbuk.BUCHK AS PostingStatusOfBillingDocument_BUCHK,
  vbuk.ABSTK AS RejectionsStatus_ABSTK,
  vbuk.GBSTK AS OverallProcessingStatus_GBSTK,
  vbuk.KOSTK AS OverallPickingPutawayStatus_KOSTK,
  vbuk.LVSTK AS OverallStatusOfWarehouseManagementActivities_LVSTK,
  vbuk.UVALS AS IncompletionStatusAllItems_UVALS,
  vbuk.UVVLS AS DeliveryIncompletionStatusAllItems_UVVLS,
  vbuk.UVFAS AS BillingIncompletionStatusAllItems_UVFAS,
  vbuk.UVALL AS IncompletionStatusHeader_UVALL,
  vbuk.UVVLK AS DeliveryIncompletionStatusHeader_UVVLK,
  vbuk.UVFAK AS BillingIncompletionStatusHeader_UVFAK,
  vbuk.UVPRS AS PricingIncompletionStatusAllItems_UVPRS,
  vbuk.VBTYP AS DocumentCategory_VBTYP,
  vbuk.VBOBJ AS SdDocumentObject_VBOBJ,
  vbuk.AEDAT AS ChangedOn_AEDAT,
  vbuk.FKIVK AS BillingTotalsStatusForIntercompanyBilling_FKIVK,
  vbuk.RELIK AS InvoiceListStatusOfBillingDocument_RELIK,
  vbuk.UVK01 AS CustomerReserves1HeaderStatus_UVK01,
  vbuk.UVK02 AS CustomerReserves2HeaderStatus_UVK02,
  vbuk.UVK03 AS CustomerReserves3HeaderStatus_UVK03,
  vbuk.UVK04 AS CustmerReserves4HeaderStatus_UVK04,
  vbuk.UVK05 AS CustomerReserves5HeaderStatus_UVK05,
  vbuk.UVS01 AS CustomerReserves1SumOfAllItems_UVS01,
  vbuk.UVS02 AS CustomerReserves2SumOfAllItems_UVS02,
  vbuk.UVS03 AS CustomerReserves3SumOfAllItems_UVS03,
  vbuk.UVS04 AS CustomerReserves4SumOfAllItems_UVS04,
  vbuk.UVS05 AS CustomerReserves5SumOfAllItems_UVS05,
  vbuk.PKSTK AS OverallPackingStatusOfAllItems_PKSTK,
  vbuk.CMPSA AS StatusOfStaticCreditLimitCheck_CMPSA,
  vbuk.CMPSB AS StatusOfDynamicCreditLimitCheckInTheCreditHorizon_CMPSB,
  vbuk.CMPSC AS StatusOfCreditCheckAgainstMaximumDocumentValue_CMPSC,
  vbuk.CMPSD AS StatusOfCreditCheckAgainstTermsOfPayment_CMPSD,
  vbuk.CMPSE AS StatusOfCreditCheckAgainstCustomerReviewDate_CMPSE,
  vbuk.CMPSF AS StatusOfCreditCheckAgainstOpenItemsDue_CMPSF,
  vbuk.CMPSG AS StatusOfCreditCheckAgainstOldestOpenItems_CMPSG,
  vbuk.CMPSH AS StatusOfCreditCheckAgainstHighestDunningLevel_CMPSH,
  vbuk.CMPSI AS StatusOfCreditCheckAgainstFinancialDocument_CMPSI,
  vbuk.CMPSJ AS StatusOfCreditCheckAgainstExportCreditInsurance_CMPSJ,
  vbuk.CMPSK AS StatusOfCreditCheckAgainstPaymentCardAuthorization_CMPSK,
  vbuk.CMPSL AS StatusOfCreditCheckOfReserves4_CMPSL,
  vbuk.CMPS0 AS StatusOfCreditCheckForCustomerReserve1_CMPS0,
  vbuk.CMPS1 AS StatusOfCreditCheckForCustomerReserve2_CMPS1,
  vbuk.CMPS2 AS StatusOfCreditCheckForCustomerReserve3_CMPS2,
  vbuk.CMGST AS OverallStatusOfCreditChecks_CMGST,
  vbuk.TRSTA AS TransportationPlanningStatusHeader_TRSTA,
  vbuk.KOQUK AS StatusOfPickConfirmation_KOQUK,
  vbuk.COSTA AS ConfirmationStatusForAle_COSTA,
  vbuk.SAPRL AS SapRelease_SAPRL,
  vbuk.UVPAS AS TotalsIncompleteStatusForAllItemsPackaging_UVPAS,
  vbuk.UVPIS AS TotalsIncompleteStatusForAllItemsPicking_UVPIS,
  vbuk.UVWAS AS TotalIncompleteStatusOfAllItemsPostGoodsMovement_UVWAS,
  vbuk.UVPAK AS HeaderIncompleteStatusForPackaging_UVPAK,
  vbuk.UVPIK AS HeaderIncompleteStatusForPickingPutaway_UVPIK,
  vbuk.UVWAK AS PostHeaderIncompleteStatusForGoodsMovement_UVWAK,
  vbuk.UVGEK AS Unused_UVGEK,
  vbuk.CMPSM AS CreditCheckDataIsObsolete_CMPSM,
  vbuk.DCSTK AS DelayStatus_DCSTK,
  vbuk.VESTK AS HandlingUnitPlacedInStock_VESTK,
  vbuk.VLSTK AS DistributionStatusDecentralizedWarehouseProcessing_VLSTK,
  vbuk.RRSTA AS RevenueDeterminationStatus_RRSTA,
  vbuk.BLOCK AS IndicatorDocumentPreselectedForArchiving_BLOCK,
  vbuk.FSSTK AS BillingBlockStatus_FSSTK,
  vbuk.LSSTK AS OverallDeliveryBlockStatusAllItems_LSSTK,
  vbuk.SPSTG AS OverallBlockStatusHeader_SPSTG,
  vbuk.PDSTK AS PodStatusOnHeaderLevel_PDSTK,
  vbuk.FMSTK AS StatusFundsManagement_FMSTK,
  vbuk.MANEK AS ManualCompletionOfContract_MANEK,
  vbuk.SPE_TMPID AS TemporaryInboundDelivery_SPE_TMPID,
  vbuk.HDALL AS InboundDeliveryHeaderNotYetCompleteOnHold_HDALL,
  vbuk.HDALS AS AtLeastOneOfIdItemsNotYetCompleteOnHold_HDALS,
  vbuk.CMPS_CM AS StatusOfCreditCheckSapCreditManagement_CMPS_CM,
  vbuk.CMPS_TE AS StatusOfTechnicalErrorSapCreditManagement_CMPS_TE,
  vbuk.VBTYP_EXT AS ExtensionOfSdDocumentCategory_VBTYP_EXT,
  vbuk.FSH_AR_STAT_HDR AS OverallAllocationStatusSalesDocumentHeader_FSH_AR_STAT_HDR
FROM
  {{ source('silver_cdc_processed', 's_vbuk') }} AS vbuk