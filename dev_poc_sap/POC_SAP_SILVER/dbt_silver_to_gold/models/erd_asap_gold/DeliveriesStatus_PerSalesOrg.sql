SELECT del.Client_MANDT, del.Delivery_VBELN, del.SalesDistrict_BZIRK,
  del.ShippingPointreceivingPoint_VSTEL, del.SalesOrganization_VKORG,
  del.DeliveryType_LFART, del.CompleteDeliveryDefinedForEachSalesOrder_AUTLF,
  del.ActualQuantityDeliveredInStockkeepingUnits_LGMNG,
  del.BaseUnitOfMeasure_MEINS, del.LoadingDate_LDDAT, del.TransportationPlanningDate_TDDAT,
  del.DeliveryDate_LFDAT, del.UnloadingPoint_ABLAD, del.Incoterms__part1___INCO1,
  del.Incoterms__part2___INCO2, del.ExportIndicator_EXPKZ, del.Route_ROUTE,
  del.BillingBlockInSdDocument_FAKSK, del.DeliveryBlock_documentHeader_LIFSK,
  del.SdDocumentCategory_VBTYP, del.CustomerFactoryCalendar_KNFAK,
  del.ShippingConditions_VSBED, del.ShipToParty_KUNNR, del.SoldToParty_KUNAG,
  del.CustomerGroup_KDGRP, del.TotalWeight_BTGEW, del.NetWeight_NTGEW, del.WeightUnit_GEWEI,
  del.VolumeUnit_VOLEH, del.TotalNumberOfPackagesInDelivery_ANZPK,
  del.PickedItemsLocation_BEROT, del.TimeOfDelivery_LFUHR, del.LoadingPoint_LSTEL,
  del.SdDocumentCurrency_WAERK, del.ShippingProcessingTimeForTheEntireDocument_VBEAK,
  del.DateOfLastChange_AEDAT, del.DocumentDateInDocument_BLDAT,
  del.ReferenceDocumentNumber_XBLNR, del.Date__proofOfDelivery___PODAT,
  del.DeliveryItem_POSNR, del.MaterialNumber_MATNR, del.MaterialGroup_MATKL,
  del.Plant_WERKS, del.StorageLocation_LGORT,
  org.SalesOrgCurrency_WAERS, org.SalesOrgName_VTEXT, org.Country_LAND1,
  SD.Delivery_Status, org.Language_SPRAS, del.is_return,
  ( CASE SD.DeliveryStatus_LFSTA
    WHEN 'A' THEN 'Not Yet Processed'
    WHEN 'B' THEN 'Partially Processed'
    WHEN 'C' THEN 'Completely Processed'
    END ) AS Delivery_StatusItm
FROM {{ ref('Deliveries') }} AS del
LEFT OUTER JOIN {{ ref('SDStatus_Items') }} AS SD
  ON del.Delivery_VBELN = SD.SDDocumentNumber_VBELN
    AND del.DeliveryItem_POSNR = SD.ItemNumberOfTheSdDocument_POSNR
    AND del.Client_MANDT = SD.Client_MANDT
LEFT OUTER JOIN {{ ref('SalesOrganizationsMD') }} AS org
  ON del.Client_MANDT = org.Client_MANDT
    AND del.SalesOrganization_VKORG = org.SalesOrg_VKORG
