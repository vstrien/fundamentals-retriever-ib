import xml.etree.ElementTree as ET
import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib

load_dotenv()

server = os.getenv('FUNDAMENTAL_SQL_SERVER')
database= os.getenv('FUNDAMENTAL_SQL_DATABASE')
username = os.getenv('FUNDAMENTAL_SQL_LOGIN')
password = os.getenv('FUNDAMENTAL_SQL_PASSWORD')
driver= '{ODBC Driver 17 for SQL Server}'

params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# List all files in the 'export' directory
files = os.listdir('export')

files_needed = [
    {
        "ReportsFinStatements_balance_sheet_annual": "Balance Sheet (Annual)",
        "ReportsFinStatements_balance_sheet_interim": "Balance Sheet (Interim)",
        "ReportsFinStatements_cash_flow_annual": "Cash Flow (Annual)",
        "ReportsFinStatements_cash_flow_interim": "Cash Flow (Interim)",
        "ReportsFinStatements_financial_statement_column_mapping": "Column Mapping",
        "ReportsFinStatements_income_statement_annual": "Income Statement (Annual)",
        "ReportsFinStatements_income_statement_interim": "Income Statement (Interim)",
        "ReportsFinStatements_issues": "Stock Issues Financial Statements",
        "ReportsFinStatements_toplevel_info": "Toplevel Information",
        "ReportSnapshot_actuals_annual": "Actuals (Annual)",
        "ReportSnapshot_actuals_interim": "Actuals (Interim)",
        "ReportSnapshot_company_profile": "Company Profile",
        "ReportSnapshot_fiscal_year_estimates_annual": "Fiscal Year Estimates (Annual)",
        "ReportSnapshot_fiscal_year_estimates_interim": "Fiscal Year Estimates (Interim)",
        "ReportSnapshot_forecast_data": "Forecast Data",
        "ReportSnapshot_issues": "Stock Issues Snapshot",
        "ReportSnapshot_net_profit_estimates": "Net Profit Estimates",
        "ReportSnapshot_periods_annual": "Periods (Annual)",
        "ReportSnapshot_periods_interim": "Periods (Interim)",
        "ReportSnapshot_security_info": "Security Information",
    }
]
    
# Loop through all files
for file in files:
    if file.endswith('.parquet') and file.replace('.parquet', '') in files_needed[0].keys():
        
        # Read the parquet file
        df = pd.read_parquet(f'export/{file}')
        # Save the dataframe to the SQL database
        df.to_sql(file.replace('.parquet', ''), con=engine, if_exists='replace', index=False)

