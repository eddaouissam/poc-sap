SELECT
  AccountingDocuments.Client_MANDT,
  AccountingDocuments.ExchangeRateType_KURST,
  AccountingDocuments.CompanyCode_BUKRS,
  CompaniesMD.CompanyText_BUTXT,
  AccountingDocuments.CustomerNumber_KUNNR,
  AccountingDocuments.FiscalYear_GJAHR,
  CustomersMD.NAME1_NAME1,
  CompaniesMD.Country_LAND1 AS Company_Country,
  CompaniesMD.CityName_ORT01 AS Company_City,
  CustomersMD.CountryKey_LAND1,
  CustomersMD.City_ORT01,
  AccountingDocuments.AccountingDocumentNumber_BELNR,
  AccountingDocuments.NumberOfLineItemWithinAccountingDocument_BUZEI,
  AccountingDocuments.CurrencyKey_WAERS,
  AccountingDocuments.LocalCurrency_HWAER,
  CompaniesMD.FiscalyearVariant_PERIV,
  IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE1',
    sap_gold.Fiscal_Case1(AccountingDocuments.Client_MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      AccountingDocuments.PostingDateInTheDocument_BUDAT),
    IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
        CompaniesMD.FiscalyearVariant_PERIV,
        AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE2',
      sap_gold.Fiscal_Case2(AccountingDocuments.Client_MANDT,
        CompaniesMD.FiscalyearVariant_PERIV,
        AccountingDocuments.PostingDateInTheDocument_BUDAT),
      IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT) = 'CASE3',
        sap_gold.Fiscal_Case3(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          AccountingDocuments.PostingDateInTheDocument_BUDAT),
        'DATA ISSUE'))) AS Period,
  IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      CURRENT_DATE()) = 'CASE1',
    sap_gold.Fiscal_Case1(AccountingDocuments.Client_MANDT,
      CompaniesMD.FiscalyearVariant_PERIV,
      CURRENT_DATE()),
    IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
        CompaniesMD.FiscalyearVariant_PERIV,
        CURRENT_DATE()) = 'CASE2',
      sap_gold.Fiscal_Case2(AccountingDocuments.Client_MANDT,
        CompaniesMD.FiscalyearVariant_PERIV,
        CURRENT_DATE()),
      IFF(sap_gold.Fiscal_Period(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()) = 'CASE3',
        sap_gold.Fiscal_Case3(AccountingDocuments.Client_MANDT,
          CompaniesMD.FiscalyearVariant_PERIV,
          CURRENT_DATE()),
        'DATA ISSUE'))) AS Current_Period,
  AccountingDocuments.AccountType_KOART,
  AccountingDocuments.PostingDateInTheDocument_BUDAT,
  AccountingDocuments.DocumentDateInDocument_BLDAT,
  AccountingDocuments.InvoiceToWhichTheTransactionBelongs_REBZG,
  AccountingDocuments.BillingDocument_VBELN,
  AccountingDocuments.WrittenOffAmount_DMBTR,
  AccountingDocuments.BadDebt_DMBTR,
  AccountingDocuments.netDueDateCalc AS NetDueDate,
  AccountingDocuments.sk2dtCalc AS CashDiscountDate1,
  AccountingDocuments.sk1dtCalc AS CashDiscountDate2,
  AccountingDocuments.OpenAndNotDue,
  AccountingDocuments.ClearedAfterDueDate,
  AccountingDocuments.ClearedOnOrBeforeDueDate,
  AccountingDocuments.OpenAndOverDue,
  AccountingDocuments.DoubtfulReceivables,
  AccountingDocuments.DaysInArrear,
  AccountingDocuments.AccountsReceivable,
  AccountingDocuments.Sales
FROM
  sap_gold.AccountingDocuments AS AccountingDocuments
LEFT JOIN
  sap_gold.CustomersMD AS CustomersMD
  ON
    AccountingDocuments.Client_MANDT = CustomersMD.Client_MANDT
    AND AccountingDocuments.CustomerNumber_KUNNR = CustomersMD.CustomerNumber_KUNNR
LEFT JOIN
  sap_gold.CompaniesMD AS CompaniesMD
  ON
    AccountingDocuments.Client_MANDT = CompaniesMD.Client_MANDT
    AND AccountingDocuments.CompanyCode_BUKRS = CompaniesMD.CompanyCode_BUKRS
WHERE AccountingDocuments.AccountType_KOART = 'D';
