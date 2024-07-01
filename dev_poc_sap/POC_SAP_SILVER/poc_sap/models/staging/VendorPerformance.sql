WITH
  LanguageKey AS (
    SELECT LanguageKey_SPRAS
    FROM {{ ref('Languages_T002') }}
  ),
 
  CurrencyConversion AS (
    SELECT
      Client_MANDT, FromCurrency_FCURR, ToCurrency_TCURR, ConvDate, ExchangeRate_UKURS
    FROM {{ ref('CurrencyConversion') }}
    WHERE ExchangeRateType_KURST = 'M'
  ),

  PurchaseOrderScheduleLine AS (
    SELECT
      PurchaseOrders.Client_MANDT,
      PurchaseOrders.DocumentNumber_EBELN,
      PurchaseOrders.Item_EBELP,
      PurchaseOrders.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrders.PurchasingDocumentDate_BEDAT,
      PurchaseOrders.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrders.CurrencyKey_WAERS,
      PurchaseOrders.POQuantity_MENGE,
      PurchaseOrders.UoM_MEINS,
      PurchaseOrders.NetPrice_NETPR,
      PurchaseOrders.CreatedOn_AEDAT,
      PurchaseOrders.Status_STATU,
      PurchaseOrders.MaterialNumber_MATNR,
      PurchaseOrders.MaterialType_MTART,
      PurchaseOrders.MaterialGroup_MATKL,
      PurchaseOrders.PurchasingOrganization_EKORG,
      PurchaseOrders.PurchasingGroup_EKGRP,
      PurchaseOrders.VendorAccountNumber_LIFNR,
      PurchaseOrders.Company_BUKRS,
      PurchaseOrders.Plant_WERKS,
      PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrders.OverdeliveryToleranceLimit_UEBTO,
      POScheduleLine.ItemDeliveryDate_EINDT,
      POScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrders.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrders.WeekOfPurchasingDocumentDate_BEDAT,
      COALESCE(
        (PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS UnderdeliveryToleranceLimit,
      COALESCE(
        (PurchaseOrders.OverdeliveryToleranceLimit_UEBTO * PurchaseOrders.POQuantity_MENGE) / 100,
        0) AS OverdeliveryToleranceLimit
    FROM
      {{ ref('PurchaseDocuments') }} AS PurchaseOrders
    LEFT JOIN
      (
        SELECT
          Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP,
          MAX(ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
          MAX(OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT
        FROM {{ ref('POSchedule') }}
        GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
      ) AS POScheduleLine
      ON
        PurchaseOrders.Client_MANDT = POScheduleLine.Client_MANDT
        AND PurchaseOrders.DocumentNumber_EBELN = POScheduleLine.PurchasingDocumentNumber_EBELN
        AND PurchaseOrders.Item_EBELP = POScheduleLine.ItemNumberOfPurchasingDocument_EBELP
    WHERE PurchaseOrders.DocumentType_BSART IN ('NB', 'ENB')
      AND PurchaseOrders.ItemCategoryinPurchasingDocument_PSTYP != '2'
  ),

  PurchaseOrdersGoodsReceipt AS (
    SELECT
      PurchaseOrderScheduleLine.Client_MANDT,
      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
      PurchaseOrderScheduleLine.Item_EBELP,
      PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrderScheduleLine.CurrencyKey_WAERS,
      PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
      PurchaseOrderScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrderScheduleLine.POQuantity_MENGE,
      PurchaseOrderScheduleLine.UoM_MEINS,
      PurchaseOrderScheduleLine.NetPrice_NETPR,
      PurchaseOrderScheduleLine.CreatedOn_AEDAT,
      PurchaseOrderScheduleLine.Status_STATU,
      PurchaseOrderScheduleLine.MaterialNumber_MATNR,
      PurchaseOrderScheduleLine.MaterialType_MTART,
      PurchaseOrderScheduleLine.MaterialGroup_MATKL,
      PurchaseOrderScheduleLine.PurchasingOrganization_EKORG,
      PurchaseOrderScheduleLine.PurchasingGroup_EKGRP,
      PurchaseOrderScheduleLine.Company_BUKRS,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.VendorAccountNumber_LIFNR,
      PurchaseOrderScheduleLine.Plant_WERKS,
      PurchaseOrderScheduleLine.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.WeekOfPurchasingDocumentDate_BEDAT,
      POOrderHistory.AmountInLocalCurrency_DMBTR,
      POOrderHistory.CurrencyKey_WAERS AS POOrderHistoryCurrencyKey_WAERS,
      IFF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.PostingDateInTheDocument_BUDAT,
        NULL) AS PostingDateInTheDocument_BUDAT,
      IFF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ IS NULL,
        FALSE,
        TRUE
      ) AS IsDelivered,
      IFF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        COALESCE(
          DATEDIFF(DAY,
            IFF(
              POOrderHistory.MovementType__inventoryManagement___BWART = '101',
              MAX(POOrderHistory.PostingDateInTheDocument_BUDAT) OVER (
                PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                  PurchaseOrderScheduleLine.Item_EBELP),
              NULL),
            PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT),
          0),
        NULL) AS VendorCycleTimeInDays,
      IFF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        TRUE,
        FALSE) AS IsRejected,
      IFF(
        POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161'),
        POOrderHistory.Quantity_MENGE,
        0) AS RejectedQuantity,
      IFF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IFF(
          IFF(
            POOrderHistory.MovementType__inventoryManagement___BWART = '101',
            POOrderHistory.PostingDateInTheDocument_BUDAT,
            NULL) <= PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
          TRUE,
          FALSE),
        NULL) AS IsDeliveredOnTime,
      IFF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IFF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IFF(
            SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) >= PurchaseOrderScheduleLine.POQuantity_MENGE,
            TRUE,
            FALSE),
          IFF(
            SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) >= PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
            TRUE,
            FALSE)
          OR IFF(
            SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP) <= PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
        ),
        NULL) AS IsDeliveredInFull,
      IFF(
        PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X',
        IFF(
          PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL,
          IFF(
            PurchaseOrderScheduleLine.POQuantity_MENGE = SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP),
            TRUE,
            FALSE),
          IFF(
            SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP)
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + purchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
          OR IFF(
            SUM(
              IFF(
                POOrderHistory.MovementType__inventoryManagement___BWART = '101',
                POOrderHistory.Quantity_MENGE,
                (POOrderHistory.Quantity_MENGE * -1)
              )) OVER (
              PARTITION BY PurchaseOrderScheduleLine.Client_MANDT,
                PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                PurchaseOrderScheduleLine.Item_EBELP)
            BETWEEN PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
            AND PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
            TRUE,
            FALSE)
        ),
        NULL) AS IsGoodsReceiptAccurate,
      IFF(POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.AmountInLocalCurrency_DMBTR,
        (POOrderHistory.AmountInLocalCurrency_DMBTR * -1)
      ) AS GoodsReceiptAmountInSourceCurrency,
      IFF(
        POOrderHistory.MovementType__inventoryManagement___BWART = '101',
        POOrderHistory.Quantity_MENGE,
        (POOrderHistory.Quantity_MENGE * -1)
      ) AS GoodsReceiptQuantity
    FROM
      PurchaseOrderScheduleLine
    LEFT JOIN
      (
        SELECT
          Client_MANDT,
          PurchasingDocumentNumber_EBELN,
          ItemNumberOfPurchasingDocument_EBELP,
          MovementType__inventoryManagement___BWART,
          AmountInLocalCurrency_DMBTR,
          CurrencyKey_WAERS,
          PostingDateInTheDocument_BUDAT,
          Quantity_MENGE
        FROM {{ ref('PurchaseDocumentsHistory') }}
        WHERE TransactioneventType_VGABE = '1'
          AND MovementType__inventoryManagement___BWART IN ('101', '102', '161', '122')
      ) AS POOrderHistory
      ON
        PurchaseOrderScheduleLine.Client_MANDT = POOrderHistory.Client_MANDT
        AND PurchaseOrderScheduleLine.DocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
        AND PurchaseOrderScheduleLine.Item_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
  ),

  PurchaseDocuments AS (
    SELECT
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP,
      MAX(PurchaseOrdersGoodsReceipt.PurchasingDocumentDate_BEDAT) AS PurchasingDocumentDate_BEDAT,
      AVG(PurchaseOrdersGoodsReceipt.NetOrderValueinPOCurrency_NETWR) AS NetOrderValueinPOCurrency_NETWR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.CurrencyKey_WAERS) AS CurrencyKey_WAERS,
      MAX(PurchaseOrdersGoodsReceipt.ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
      MAX(PurchaseOrdersGoodsReceipt.OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.PostingDateInTheDocument_BUDAT) AS PostingDateInTheDocument_BUDAT,
      SUM(PurchaseOrdersGoodsReceipt.AmountInLocalCurrency_DMBTR) AS AmountInLocalCurrency_DMBTR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.POOrderHistoryCurrencyKey_WAERS) AS POOrderHistoryCurrencyKey_WAERS,
      AVG(PurchaseOrdersGoodsReceipt.POQuantity_MENGE) AS POQuantity_MENGE,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.UoM_MEINS) AS UoM_MEINS,
      AVG(PurchaseOrdersGoodsReceipt.NetPrice_NETPR) AS NetPrice_NETPR,
      MAX(PurchaseOrdersGoodsReceipt.CreatedOn_AEDAT) AS CreatedOn_AEDAT,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Status_STATU) AS Status_STATU,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialNumber_MATNR) AS MaterialNumber_MATNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialType_MTART) AS MaterialType_MTART,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.MaterialGroup_MATKL) AS MaterialGroup_MATKL,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingOrganization_EKORG) AS PurchasingOrganization_EKORG,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.PurchasingGroup_EKGRP) AS PurchasingGroup_EKGRP,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.VendorAccountNumber_LIFNR) AS VendorAccountNumber_LIFNR,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Company_BUKRS) AS Company_BUKRS,
      ANY_VALUE(PurchaseOrdersGoodsReceipt.Plant_WERKS) AS Plant_WERKS,
      BOOLAND_AGG(PurchaseOrdersGoodsReceipt.IsDelivered) AS IsDelivered,
      MAX(PurchaseOrdersGoodsReceipt.VendorCycleTimeInDays) AS VendorCycleTimeInDays,
      BOOLOR_AGG(PurchaseOrdersGoodsReceipt.IsRejected) AS IsRejected,
      SUM(PurchaseOrdersGoodsReceipt.RejectedQuantity) AS RejectedQuantity,
      BOOLAND_AGG(PurchaseOrdersGoodsReceipt.IsDeliveredOnTime) AS IsDeliveredOnTime,
      BOOLAND_AGG(PurchaseOrdersGoodsReceipt.IsDeliveredInFull) AS IsDeliveredInFull,
      BOOLAND_AGG(PurchaseOrdersGoodsReceipt.IsGoodsReceiptAccurate) AS IsGoodsReceiptAccurate,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptQuantity) AS GoodsReceiptQuantity,
      SUM(PurchaseOrdersGoodsReceipt.GoodsReceiptAmountInSourceCurrency) AS GoodsReceiptAmountInSourceCurrency,
      MAX(PurchaseOrdersGoodsReceipt.YearOfPurchasingDocumentDate_BEDAT) AS YearOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.MonthOfPurchasingDocumentDate_BEDAT) AS MonthOfPurchasingDocumentDate_BEDAT,
      MAX(PurchaseOrdersGoodsReceipt.WeekOfPurchasingDocumentDate_BEDAT) AS WeekOfPurchasingDocumentDate_BEDAT
    FROM PurchaseOrdersGoodsReceipt
    GROUP BY
      PurchaseOrdersGoodsReceipt.Client_MANDT,
      PurchaseOrdersGoodsReceipt.DocumentNumber_EBELN,
      PurchaseOrdersGoodsReceipt.Item_EBELP
  ),

  FiscalData AS (
    SELECT
      pd.Client_MANDT,
      pd.DocumentNumber_EBELN,
      pd.Item_EBELP,
      c.FiscalyearVariant_PERIV
    FROM PurchaseDocuments pd
    JOIN {{ ref('CompaniesMD') }} c
      ON pd.Client_MANDT = c.Client_MANDT
      AND pd.Company_BUKRS = c.CompanyCode_BUKRS
  ),

  PurchaseOrdersInvoiceReceipt AS (
    SELECT
      Client_MANDT,
      PurchasingDocumentNumber_EBELN,
      ItemNumberOfPurchasingDocument_EBELP,
      SUM(Quantity_MENGE) AS InvoiceQuantity,
      SUM(AmountInLocalCurrency_DMBTR) AS InvoiceAmountInSourceCurrency,
      MAX(PostingDateInTheDocument_BUDAT) AS InvoiceDate,
      MAX(YearOfPostingDateInTheDocument_BUDAT) AS YearOfInvoiceDate,
      MAX(MonthOfPostingDateInTheDocument_BUDAT) AS MonthOfInvoiceDate,
      MAX(WeekOfPostingDateInTheDocument_BUDAT) AS WeekOfInvoiceDate,
      COUNT(PurchasingDocumentNumber_EBELN) AS InvoiceCount
    FROM {{ ref('PurchaseDocumentsHistory') }}
    WHERE TransactioneventType_VGABE = '2'
    GROUP BY Client_MANDT, PurchasingDocumentNumber_EBELN, ItemNumberOfPurchasingDocument_EBELP
  )

SELECT
  pd.Client_MANDT,
  pd.DocumentNumber_EBELN,
  pd.Item_EBELP,
  pd.PurchasingDocumentDate_BEDAT,
  pd.NetOrderValueinPOCurrency_NETWR,
  pd.CurrencyKey_WAERS,
  pd.ItemDeliveryDate_EINDT,
  pd.OrderDateOfScheduleLine_BEDAT,
  pd.PostingDateInTheDocument_BUDAT,
  pd.AmountInLocalCurrency_DMBTR,
  pd.POOrderHistoryCurrencyKey_WAERS,
  pd.POQuantity_MENGE,
  pd.UoM_MEINS,
  pd.NetPrice_NETPR,
  pd.CreatedOn_AEDAT,
  pd.Status_STATU,
  pd.MaterialNumber_MATNR,
  pd.MaterialType_MTART,
  pd.MaterialGroup_MATKL,
  pd.PurchasingOrganization_EKORG,
  pd.PurchasingGroup_EKGRP,
  pd.VendorAccountNumber_LIFNR,
  pd.Company_BUKRS,
  pd.Plant_WERKS,
  pd.YearOfPurchasingDocumentDate_BEDAT,
  pd.MonthOfPurchasingDocumentDate_BEDAT,
  pd.WeekOfPurchasingDocumentDate_BEDAT,
  poi.InvoiceQuantity,
  poi.InvoiceAmountInSourceCurrency,
  poi.InvoiceDate,
  poi.YearOfInvoiceDate,
  poi.MonthOfInvoiceDate,
  poi.WeekOfInvoiceDate,
  poi.InvoiceCount,
  po.PurchasingOrganizationText_EKOTX,
  pg.PurchasingGroupText_EKNAM,
  v.CountryKey_LAND1,
  v.NAME1,
  c.CompanyText_BUTXT,
  c.FiscalyearVariant_PERIV,
  lk.LanguageKey_SPRAS,
  m.MaterialText_MAKTX,
  mt.DescriptionOfMaterialType_MTBEZ,
  pd.VendorCycleTimeInDays,
  pd.RejectedQuantity,
  pd.GoodsReceiptQuantity,
  pd.GoodsReceiptAmountInSourceCurrency,
  cc.ExchangeRate_UKURS,
  cc.ToCurrency_TCURR AS TargetCurrency_TCURR,
  pd.AmountInLocalCurrency_DMBTR * cc.ExchangeRate_UKURS AS AmountInTargetCurrency_DMBTR,
  pd.NetPrice_NETPR * cc.ExchangeRate_UKURS AS NetPriceInTargetCurrency_NETPR,
  pd.NetOrderValueinPOCurrency_NETWR * cc.ExchangeRate_UKURS AS NetOrderValueinTargetCurrency_NETWR,
  pd.GoodsReceiptAmountInSourceCurrency * cc.ExchangeRate_UKURS AS GoodsReceiptAmountInTargetCurrency,
  poi.InvoiceAmountInSourceCurrency * cc.ExchangeRate_UKURS AS InvoiceAmountInTargetCurrency,
  IFF(pd.IsDelivered, TRUE, FALSE) AS IsDelivered,
  IFF(pd.IsRejected, TRUE, FALSE) AS IsRejected,
  IFF(pd.IsDeliveredOnTime IS NULL, 'NotApplicable', IFF(pd.IsDeliveredOnTime, 'NotDelayed', 'Delayed')) AS VendorOnTimeDelivery,
  IFF(pd.IsDeliveredInFull IS NULL, 'NotApplicable', IFF(pd.IsDeliveredInFull, 'DeliveredInFull', 'NotDeliveredInFull')) AS VendorInFullDelivery,
  IFF(pd.IsDeliveredInFull IS NULL OR pd.IsDeliveredOnTime IS NULL, 'NotApplicable', IFF(pd.IsDeliveredInFull AND pd.IsDeliveredOnTime, 'OTIF', 'NotOTIF')) AS VendorOnTimeInFullDelivery,
  IFF(pd.IsGoodsReceiptAccurate IS NULL OR poi.InvoiceQuantity IS NULL, 'NotApplicable', IFF(pd.IsGoodsReceiptAccurate AND pd.POQuantity_MENGE = poi.InvoiceQuantity, 'AccurateInvoice', 'InaccurateInvoice')) AS VendorInvoiceAccuracy,
  IFF(pd.IsDelivered, 'NotApplicable', IFF(CURRENT_DATE() > pd.ItemDeliveryDate_EINDT, 'PastDue', 'Open')) AS PastDueOrOpenItems
FROM PurchaseDocuments pd
LEFT JOIN PurchaseOrdersInvoiceReceipt poi
  ON pd.Client_MANDT = poi.Client_MANDT
  AND pd.DocumentNumber_EBELN = poi.PurchasingDocumentNumber_EBELN
  AND pd.Item_EBELP = poi.ItemNumberOfPurchasingDocument_EBELP
LEFT JOIN CurrencyConversion cc
  ON pd.Client_MANDT = cc.Client_MANDT
  AND pd.CurrencyKey_WAERS = cc.FromCurrency_FCURR
  AND pd.PurchasingDocumentDate_BEDAT = cc.ConvDate
LEFT JOIN {{ ref('PurchasingOrganizationsMD') }} po
  ON pd.Client_MANDT = po.Client_MANDT
  AND pd.PurchasingOrganization_EKORG = po.PurchasingOrganization_EKORG
LEFT JOIN {{ ref('PurchasingGroupsMD') }} pg
  ON pd.Client_MANDT = pg.Client_MANDT
  AND pd.PurchasingGroup_EKGRP = pg.PurchasingGroup_EKGRP
LEFT JOIN {{ ref('VendorsMD') }} v
  ON pd.Client_MANDT = v.Client_MANDT
  AND pd.VendorAccountNumber_LIFNR = v.AccountNumberOfVendorOrCreditor_LIFNR
LEFT JOIN {{ ref('CompaniesMD') }} c
  ON pd.Client_MANDT = c.Client_MANDT
  AND pd.Company_BUKRS = c.CompanyCode_BUKRS
CROSS JOIN LanguageKey lk
LEFT JOIN {{ ref('MaterialsMD') }} m
  ON pd.Client_MANDT = m.Client_MANDT
  AND pd.MaterialNumber_MATNR = m.MaterialNumber_MATNR
  AND m.Language_SPRAS = lk.LanguageKey_SPRAS
LEFT JOIN {{ ref('MaterialTypesMD') }} mt
  ON pd.Client_MANDT = mt.Client_MANDT
  AND pd.MaterialType_MTART = mt.MaterialType_MTART
  AND mt.LanguageKey_SPRAS = lk.LanguageKey_SPRAS
LEFT JOIN FiscalData fd
  ON pd.Client_MANDT = fd.Client_MANDT
  AND pd.DocumentNumber_EBELN = fd.DocumentNumber_EBELN
  AND pd.Item_EBELP = fd.Item_EBELP