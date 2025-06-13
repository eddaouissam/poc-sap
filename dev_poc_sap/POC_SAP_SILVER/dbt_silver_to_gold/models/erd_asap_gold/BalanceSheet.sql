-- The granularity of this query is:
-- Client, Company, ChartOfAccounts, HierarchyName, BusinessArea,
-- ProfitCenter, CostCenter, LedgerInGeneralLedgerAccounting, FiscalYear,
-- FiscalPeriod, Hierarchy Node, Language, TargetCurrency.

WITH
  LanguageKey AS (
    SELECT
      NULL AS LanguageKey_SPRAS
    FROM
      (SELECT 1 AS dummy_column WHERE 1 = 0) -- Simulates an empty table
  ),

  CurrencyConversion AS (
    SELECT
      NULL AS Client_MANDT,
      NULL AS periv,
      NULL AS CompanyCode_BUKRS,
      NULL AS FiscalYear,
      NULL AS FiscalPeriod,
      NULL AS FromCurrency_FCURR,
      NULL AS ToCurrency_TCURR,
      NULL AS ExchangeRate,
      NULL AS MaxExchangeRate,
      NULL AS AvgExchangeRate
    FROM
      (SELECT 1 AS dummy_column WHERE 1 = 0) -- Simulates an empty table
  ),

  ParentId AS (
    SELECT
      NULL AS Client,
      NULL AS CompanyCode,
      NULL AS Parent,
      NULL AS FinancialStatementItem
    FROM
      (SELECT 1 AS dummy_column WHERE 1 = 0) -- Simulates an empty table
  )

SELECT
  FSV.Client,
  FSV.CompanyCode,
  FSV.FiscalYear,
  FSV.FiscalPeriod,
  FSV.ChartOfAccounts,
  FSV.HierarchyName AS GLHierarchy,
  FSV.BusinessArea,
  FSV.LedgerInGeneralLedgerAccounting,
  FSV.ProfitCenter,
  FSV.CostCenter,
  FSV.Node AS GLNode,
  LanguageKey.LanguageKey_SPRAS,
  FSV.Parent AS GLParent,
  FSV.FinancialStatementItem AS GLFinancialItem,
  NULL AS GLNodeText,
  NULL AS GLParentText,
  FSV.Level AS GLLevel,
  FSV.FiscalQuarter AS FiscalQuarter,
  FSV.IsLeafNode AS GLIsLeafNode,
  FSV.CompanyText AS CompanyText,
  FSV.AmountInLocalCurrency AS AmountInLocalCurrency,
  NULL AS CumulativeAmountInLocalCurrency,
  FSV.CurrencyKey AS CurrencyKey,
  CurrencyConversion.ExchangeRate AS ExchangeRate,
  CurrencyConversion.MaxExchangeRate AS MaxExchangeRate,
  CurrencyConversion.AvgExchangeRate AS AvgExchangeRate,
  NULL AS AmountInTargetCurrency,
  NULL AS CumulativeAmountInTargetCurrency,
  CurrencyConversion.ToCurrency_TCURR AS TargetCurrency_TCURR
FROM
  (
    SELECT
      NULL AS Client,
      NULL AS CompanyCode,
      NULL AS FiscalYear,
      NULL AS FiscalPeriod,
      NULL AS FiscalQuarter,
      NULL AS ChartOfAccounts,
      NULL AS HierarchyName,
      NULL AS BusinessArea,
      NULL AS LedgerInGeneralLedgerAccounting,
      NULL AS ProfitCenter,
      NULL AS CostCenter,
      NULL AS Node,
      NULL AS Parent,
      NULL AS FinancialStatementItem,
      NULL AS Level,
      NULL AS IsLeafNode,
      NULL AS CompanyText,
      NULL AS AmountInLocalCurrency,
      NULL AS CurrencyKey
    FROM
      (SELECT 1 AS dummy_column WHERE 1 = 0) -- Simulates an empty table
  ) AS FSV

LEFT JOIN ParentId
  ON
    FSV.Client = ParentId.Client
    AND FSV.Parent = ParentId.Parent
    AND FSV.CompanyCode = ParentId.CompanyCode
LEFT JOIN CurrencyConversion
  ON
    FSV.Client = CurrencyConversion.Client_MANDT
    AND FSV.CompanyCode = CurrencyConversion.CompanyCode_BUKRS
    AND FSV.CurrencyKey = CurrencyConversion.FromCurrency_FCURR
    AND FSV.FiscalYear = CurrencyConversion.FiscalYear
    AND FSV.FiscalPeriod = CurrencyConversion.FiscalPeriod
CROSS JOIN LanguageKey