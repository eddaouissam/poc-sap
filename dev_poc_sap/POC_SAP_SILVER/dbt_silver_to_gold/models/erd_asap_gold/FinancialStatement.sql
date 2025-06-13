-- This query outputs FinancialStatement by Client, Company, ChartOfAccounts, HierarchyName,
-- BusinessArea, LedgerInGeneralLedgerAccounting, ProfitCenter, CostCenter, FiscalYear,
-- FiscalPeriod, Hierarchy Node.

WITH
  FSVGLAccounts AS (
    SELECT
      -- This query joins transaction data with hierarchy node at GLAccount level.
      NULL AS Client,
      NULL AS GeneralLedgerAccount,
      NULL AS ChartOfAccounts,
      NULL AS HierarchyName,
      NULL AS CompanyCode,
      NULL AS BusinessArea,
      NULL AS LedgerInGeneralLedgerAccounting,
      NULL AS ProfitCenter,
      NULL AS CostCenter,
      NULL AS BalanceSheetAccountIndicator,
      NULL AS PLAccountIndicator,
      NULL AS FiscalYear,
      NULL AS FiscalPeriod,
      NULL AS FiscalQuarter,
      NULL AS Parent,
      NULL AS Node,
      NULL AS FinancialStatementItem,
      NULL AS Level,
      NULL AS IsLeafNode,
      NULL AS AmountInLocalCurrency,
      NULL AS CurrencyKey,
      NULL AS CompanyText
    FROM
      (SELECT 1 AS dummy_column WHERE 1 = 0) AS AccountingDocument -- Simulates an empty table
    INNER JOIN
      (SELECT 1 AS dummy_column WHERE 1 = 0) AS FinancialStatementVersion -- Simulates an empty table
      ON
        AccountingDocument.dummy_column = FinancialStatementVersion.dummy_column
  )

SELECT
  -- This query outputs transaction data at hierarchy (fsv) node level.
  Client,
  CompanyCode,
  FiscalYear,
  FiscalPeriod,
  ChartOfAccounts,
  HierarchyName,
  BusinessArea,
  LedgerInGeneralLedgerAccounting,
  ProfitCenter,
  CostCenter,
  Node,
  ANY_VALUE(Parent) AS Parent,
  ANY_VALUE(FiscalQuarter) AS FiscalQuarter,
  ANY_VALUE(FinancialStatementItem) AS FinancialStatementItem,
  ANY_VALUE(Level) AS Level,
  ANY_VALUE(IsLeafNode) AS IsLeafNode,
  ANY_VALUE(BalanceSheetAccountIndicator) AS BalanceSheetAccountIndicator,
  ANY_VALUE(PLAccountIndicator) AS PLAccountIndicator,
  ANY_VALUE(CompanyText) AS CompanyText,
  ANY_VALUE(CurrencyKey) AS CurrencyKey,
  SUM(AmountInLocalCurrency) AS AmountInLocalCurrency,
  SUM(SUM(AmountInLocalCurrency)) OVER (
    PARTITION BY
      Client, CompanyCode, BusinessArea, LedgerInGeneralLedgerAccounting,
      ProfitCenter, CostCenter, Node
    ORDER BY FiscalYear ASC, FiscalPeriod ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS CumulativeAmountInLocalCurrency

FROM
  FSVGLAccounts

GROUP BY
  Client,
  CompanyCode,
  ChartOfAccounts,
  HierarchyName,
  BusinessArea,
  LedgerInGeneralLedgerAccounting,
  ProfitCenter,
  CostCenter,
  FiscalYear,
  FiscalPeriod,
  Node