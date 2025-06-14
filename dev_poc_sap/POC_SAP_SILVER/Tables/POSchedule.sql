SELECT
  eket.MANDT AS Client_MANDT,
  eket.EBELN AS PurchasingDocumentNumber_EBELN,
  eket.EBELP AS ItemNumberOfPurchasingDocument_EBELP,
  eket.ETENR AS DeliveryScheduleLineCounter_ETENR,
  eket.EINDT AS ItemDeliveryDate_EINDT,
  eket.SLFDT AS StatisticsRelevantDeliveryDate_SLFDT,
  eket.LPEIN AS CategoryOfDeliveryDate_LPEIN,
  eket.MENGE AS ScheduledQuantity_MENGE,
  eket.AMENG AS PreviousQuantity__deliveryScheduleLines___AMENG,
  eket.WEMNG AS QuantityOfGoodsReceived_WEMNG,
  eket.WAMNG AS IssuedQuantity_WAMNG,
  eket.UZEIT AS DeliveryDateTimeSpot_UZEIT,
  eket.BANFN AS PurchaseRequisitionNumber_BANFN,
  eket.BNFPO AS ItemNumberOfPurchaseRequisition_BNFPO,
  eket.ESTKZ AS CreationIndicator__purchaseRequisitionscheduleLines___ESTKZ,
  eket.QUNUM AS NumberOfQuotaArrangement_QUNUM,
  eket.QUPOS AS QuotaArrangementItem_QUPOS,
  eket.MAHNZ AS NoOfRemindersexpeditersForScheduleLine_MAHNZ,
  eket.BEDAT AS OrderDateOfScheduleLine_BEDAT,
  eket.RSNUM AS NumberOfReservationdependentRequirements_RSNUM,
  eket.SERNR AS BomExplosionNumber_SERNR,
  eket.FIXKZ AS ScheduleLineIsfixed_FIXKZ,
  eket.GLMNG AS QuantityDelivered__stockTransfer___GLMNG,
  eket.DABMG AS QuantityReduced__mrp___DABMG,
  eket.CHARG AS BatchNumber_CHARG,
  eket.LICHA AS VendorBatchNumber_LICHA,
  eket.CHKOM AS Components_CHKOM,
  eket.VERID AS ProductionVersion_VERID,
  eket.ABART AS SchedulingAgreementReleaseType_ABART,
  eket.MNG02 AS CommittedQuantity_MNG02,
  eket.DAT01 AS CommittedDate_DAT01,
  eket.ALTDT AS PreviousDeliveryDate_ALTDT,
  eket.AULWE AS RouteSchedule_AULWE,
  eket.MBDAT AS MaterialAvailabilityDate_MBDAT,
  eket.MBUHR AS MaterialStagingTime_MBUHR,
  eket.LDDAT AS LoadingDate_LDDAT,
  eket.LDUHR AS LoadingTime__localTimeRelatingToAShippingPoint___LDUHR,
  eket.TDDAT AS TransportationPlanningDate_TDDAT,
  eket.TDUHR AS TranspPlanningTime__local_TDUHR,
  eket.WADAT AS GoodsIssueDate_WADAT,
  eket.WAUHR AS TimeOfGoodsIssue__local_RelatingToAPlant___WAUHR,
  eket.ELDAT AS GoodsReceiptEndDate_ELDAT,
  eket.ELUHR AS GoodsReceiptEndTime__local__ELUHR,
  eket.ANZSN AS NumberOfSerialNumbers_ANZSN,
  eket.NODISP AS Ind_ReservNotApplicableToMrpPurcReqNotCreated_NODISP,
  eket.GEO_ROUTE AS DescriptionOfAGeographicalRoute_GEO_ROUTE,
  eket.ROUTE_GTS AS RouteCodeForSapGlobalTradeServices_ROUTE_GTS,
  eket.GTS_IND AS GoodsTrafficType_GTS_IND,
  eket.TSP AS ForwardingAgent_TSP,
  eket.CD_LOCNO AS LocationNumberInApo_CD_LOCNO,
  eket.CD_LOCTYPE AS ApoLocationType_CD_LOCTYPE,
  eket.HANDOVERDATE AS HandoverDateAtTheHandoverLocation_HANDOVERDATE,
  eket.HANDOVERTIME AS HandoverTimeAtTheHandoverLocation_HANDOVERTIME,
  eket.FSH_RALLOC_QTY AS ArunRequirementAllocatedQuantity_FSH_RALLOC_QTY,
  eket.FSH_SALLOC_QTY AS AllocatedStockQuantity_FSH_SALLOC_QTY,
  eket.FSH_OS_ID AS OrderSchedulingGroupId_FSH_OS_ID,
  eket.KEY_ID AS UniqueNumberOfBudget_KEY_ID,
  eket.OTB_VALUE AS RequiredBudget_OTB_VALUE,
  eket.OTB_CURR AS OtbCurrency_OTB_CURR,
  eket.OTB_RES_VALUE AS ReservedBudgetForOtbRelevantPurchasingDocument_OTB_RES_VALUE,
  eket.OTB_SPEC_VALUE AS SpecialReleaseBudget_OTB_SPEC_VALUE,
  eket.SPR_RSN_PROFILE AS ReasonProfileForOtbSpecialRelease_SPR_RSN_PROFILE,
  eket.BUDG_TYPE AS BudgetType_BUDG_TYPE,
  eket.OTB_STATUS AS OtbCheckStatus_OTB_STATUS,
  eket.OTB_REASON AS ReasonIndicatorForOtbCheckStatus_OTB_REASON,
  eket.CHECK_TYPE AS TypeOfOtbCheck_CHECK_TYPE,
  eket.DL_ID AS DatelineId__guid___DL_ID,
  eket.HANDOVER_DATE AS TransferDate_HANDOVER_DATE,
  eket.NO_SCEM AS PurchaseOrderNotTransferredToScem_NO_SCEM,
  eket.DNG_DATE AS CreationDateOfReminderMessageRecord_DNG_DATE,
  eket.DNG_TIME AS CreationTimeOfReminderMessageRecord_DNG_TIME,
  eket.CNCL_ANCMNT_DONE AS CancellationThreatMade_CNCL_ANCMNT_DONE,
  eket.DATESHIFT_NUMBER AS NumberOfCurrentDateShifts_DATESHIFT_NUMBER,
  --##CORTEX-CUSTOMER Consider adding other dimensions from the calendar_date_dim table as per your requirement
  CalendarDateDimension_EINDT.CalYear AS YearOfItemDeliveryDate_EINDT,
  CalendarDateDimension_EINDT.CalMonth AS MonthOfItemDeliveryDate_EINDT,
  CalendarDateDimension_EINDT.CalWeek AS WeekOfItemDeliveryDate_EINDT,
  CalendarDateDimension_EINDT.CalQuarter AS QuarterOfItemDeliveryDate_EINDT,
  CalendarDateDimension_BEDAT.CalYear AS YearOfOrderDateOfScheduleLine_BEDAT,
  CalendarDateDimension_BEDAT.CalMonth AS MonthOfOrderDateOfScheduleLine_BEDAT,
  CalendarDateDimension_BEDAT.CalWeek AS WeekOfOrderDateOfScheduleLine_BEDAT,
  CalendarDateDimension_BEDAT.CalQuarter AS QuarterOfOrderDateOfScheduleLine_BEDAT
FROM
  sap_silver.s_eket AS eket
LEFT JOIN utils.calendar_date_dim AS CalendarDateDimension_EINDT
  ON CalendarDateDimension_EINDT.Date = eket.EINDT
LEFT JOIN utils.calendar_date_dim AS CalendarDateDimension_BEDAT
  ON CalendarDateDimension_BEDAT.Date = eket.BEDAT
