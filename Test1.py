import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_financial_data(ticker_symbol):
    # Create a Ticker object
    stock = yf.Ticker(ticker_symbol)
    
    # Get quarterly financial statements with history parameter
    income_stmt = stock.quarterly_financials
    balance_sheet = stock.quarterly_balance_sheet
    cash_flow = stock.quarterly_cashflow
    
    # Print available date range
    print(f"Available date range for {ticker_symbol}:")
    print(f"From: {min(income_stmt.columns)}")
    print(f"To: {max(income_stmt.columns)}")
    
    # Filter data for the period 2015-2025 Q1
    start_date = pd.to_datetime('2015-01-01')
    end_date = pd.to_datetime('2025-03-31')
    
    # Filter dates using boolean indexing
    income_stmt = income_stmt.loc[:, (income_stmt.columns >= start_date) & (income_stmt.columns <= end_date)]
    balance_sheet = balance_sheet.loc[:, (balance_sheet.columns >= start_date) & (balance_sheet.columns <= end_date)]
    cash_flow = cash_flow.loc[:, (cash_flow.columns >= start_date) & (cash_flow.columns <= end_date)]
    
    # Get key financial ratios
    info = stock.info
    
    # Save to Excel files
    with pd.ExcelWriter(f'{ticker_symbol}_quarterly_financial_reports_2015_2025Q1.xlsx') as writer:
        income_stmt.to_excel(writer, sheet_name='Quarterly Income Statement')
        balance_sheet.to_excel(writer, sheet_name='Quarterly Balance Sheet')
        cash_flow.to_excel(writer, sheet_name='Quarterly Cash Flow')
    
    return income_stmt, balance_sheet, cash_flow, info

# Example usage
if __name__ == "__main__":
    # List of stock symbols you want to analyze
    stocks = ['MSFT']
    
    for symbol in stocks:
        try:
            income, balance, cash, info = get_financial_data(symbol)
            print(f"\nQuarterly financial data for {symbol} has been saved to Excel")
            
            # Print some key metrics
            print(f"Market Cap: ${info.get('marketCap', 'N/A')}")
            print(f"PE Ratio: {info.get('trailingPE', 'N/A')}")
            print(f"Revenue Growth: {info.get('revenueGrowth', 'N/A')}")
            print(f"Profit Margin: {info.get('profitMargins', 'N/A')}")
            
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
# To run this code:
# 1. Save this file as Test1.py
# 2. Make sure you have the required libraries installed:
#    pip install yfinance pandas
# 3. Open terminal/command prompt in the file directory
# 4. Run: python Test1.py
