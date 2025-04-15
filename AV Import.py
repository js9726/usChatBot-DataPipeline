import requests
import pandas as pd
from datetime import datetime
from supabase import create_client

# Alpha Vantage API configuration
#API_KEY = 'GAVV7ZLEKJ7M6M42'
API_KEY = 'AYTOXEPEL94213GS'

SYMBOL = 'MSFT'

# Supabase configuration
SUPABASE_URL = 'https://xxstiqtixhgyxwwkemjd.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inh4c3RpcXRpeGhneXh3d2tlbWpkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ2NDUxMTQsImV4cCI6MjA2MDIyMTExNH0.KP0O-Mqoa_IlDpGSm5LwaKzpYVKFyGn73bKkox6eCaQ'
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Function to get data from Alpha Vantage
def get_financial_data(function):
    url = f'https://www.alphavantage.co/query?function={function}&symbol={SYMBOL}&apikey={API_KEY}'
    response = requests.get(url)
    return response.json()

# Get all financial data
income_statement_data = get_financial_data('INCOME_STATEMENT')
balance_sheet_data = get_financial_data('BALANCE_SHEET')
cash_flow_data = get_financial_data('CASH_FLOW')

# Extract quarterly reports for each statement
income_quarterly = pd.DataFrame(income_statement_data.get('quarterlyReports', []))
balance_quarterly = pd.DataFrame(balance_sheet_data.get('quarterlyReports', []))
cash_flow_quarterly = pd.DataFrame(cash_flow_data.get('quarterlyReports', []))

# Add statement type prefix to column names to avoid duplicates
income_quarterly.columns = ['date' if col == 'fiscalDateEnding' else f'IS_{col}' for col in income_quarterly.columns]
balance_quarterly.columns = ['date' if col == 'fiscalDateEnding' else f'BS_{col}' for col in balance_quarterly.columns]
cash_flow_quarterly.columns = ['date' if col == 'fiscalDateEnding' else f'CF_{col}' for col in cash_flow_quarterly.columns]

# Merge all statements on the date column
combined_df = pd.merge(income_quarterly, balance_quarterly, on='date', how='outer')
combined_df = pd.merge(combined_df, cash_flow_quarterly, on='date', how='outer')

# Sort by date in descending order (most recent first)
combined_df = combined_df.sort_values('date', ascending=False)

# Convert numeric columns and handle large numbers
numeric_columns = combined_df.columns[1:]  # Skip the date column
for col in numeric_columns:
    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    # Replace NaN, infinite values, and None with 0
    combined_df[col] = combined_df[col].fillna(0)
    combined_df[col] = combined_df[col].replace([float('inf'), float('-inf')], 0)
    # Convert to regular Python float for JSON compatibility
    combined_df[col] = combined_df[col].astype(object)

# Convert DataFrame to records for Supabase
records = combined_df.to_dict('records')

# Convert all keys to lowercase in records
records = [{k.lower(): v for k, v in record.items()} for record in records]

# Upload to Supabase
try:
    # Clear existing data for this symbol
    supabase.table('financial_statements').delete().eq('symbol', SYMBOL).execute()
    
    # Add symbol column to each record
    for record in records:
        record['symbol'] = SYMBOL
    
    # Insert new data
    result = supabase.table('financial_statements').insert(records).execute()
    print(f"Successfully uploaded {len(records)} records to Supabase for {SYMBOL}")
except Exception as e:
    print(f"Error uploading to Supabase: {str(e)}")

    # cd "c:\Users\Administrator\OneDrive\Desktop\Investment AI\Master Code\usChatBot-DataPipeline"
    # python "AV Import.py"

