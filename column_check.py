import requests
import pandas as pd

#API_KEY = 'GAVV7ZLEKJ7M6M42'
API_KEY = 'AYTOXEPEL94213GS'

SYMBOL = 'MSFT'

def get_financial_data(function):
    url = f'https://www.alphavantage.co/query?function={function}&symbol={SYMBOL}&apikey={API_KEY}'
    response = requests.get(url)
    return response.json()

# Get sample data
income_data = get_financial_data('INCOME_STATEMENT')
balance_data = get_financial_data('BALANCE_SHEET')
cash_flow_data = get_financial_data('CASH_FLOW')

# Create DataFrames
income_df = pd.DataFrame(income_data.get('quarterlyReports', []))
balance_df = pd.DataFrame(balance_data.get('quarterlyReports', []))
cash_flow_df = pd.DataFrame(cash_flow_data.get('quarterlyReports', []))

print("Income Statement Columns (with sample values):")
for col in income_df.columns:
    sample_value = income_df[col].iloc[0] if not income_df.empty else 'No data'
    print(f"Column: IS_{col}")
    print(f"Sample value: {sample_value}")
    print("-" * 50)

print("\nOriginal Income Statement column names for SQL:")
for col in income_df.columns:
    sql_col = f"is_{col.lower()}"
    print(f"{sql_col} NUMERIC(38,2),")

print("\nBalance Sheet Columns (with sample values):")
for col in balance_df.columns:
    sample_value = balance_df[col].iloc[0] if not balance_df.empty else 'No data'
    print(f"Column: BS_{col}")
    print(f"Sample value: {sample_value}")
    print("-" * 50)

print("\nOriginal Balance Sheet column names for SQL:")
for col in balance_df.columns:
    sql_col = f"bs_{col.lower()}"
    print(f"{sql_col} NUMERIC(38,2),")

print("\nCash Flow Columns (with sample values):")
for col in cash_flow_df.columns:
    sample_value = cash_flow_df[col].iloc[0] if not cash_flow_df.empty else 'No data'
    print(f"Column: CF_{col}")
    print(f"Sample value: {sample_value}")
    print("-" * 50)

print("\nOriginal Cash Flow column names for SQL:")
for col in cash_flow_df.columns:
    sql_col = f"cf_{col.lower()}"
    print(f"{sql_col} NUMERIC(38,2),")