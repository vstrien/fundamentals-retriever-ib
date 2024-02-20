import os
from datetime import datetime, timedelta
import nest_asyncio
import yfinance as yf

nest_asyncio.apply()
from ib_insync import *
ib = IB()

ib.connect('127.0.0.1', 7496, clientId = 3)

def get_fundamentals_for(stock_ticker, market = 'SMART', currency = 'USD', report = 'ReportsFinStatements'):
    contract = Stock(stock_ticker, market, currency)

    return ib.reqFundamentalData(contract, report)



# Request the company's fundamentals
# fundamentals = ib.reqFundamentalData(contract, 'ReportsFinStatements')

companies_to_get = ['TITN', 'BLBD', 'BMM', 'GOTU', 'SKY', 'CPRI', 'OC', 'ASTE', 'KGS', 'GPS', 'ONON', 'ATGE', 'SHLS', 'IIAC.U', 'DECK', 'U', 'HDB', 'PLOW', 'MFB', 'SCS', 'MUFG', 'MDC', 'POWL', 'JFB', 'HSBC', 'JL', 'ENR', 'MTW', 'HNI', 'ACDC', 'MTH', 'BLDR', 'INVE', 'ANF', 'EIN3', 'AEO', 'ATNY', 'PERY', 'LOPE', 'MAS', 'IVAC', 'RELL', 'SKX', 'CROX', 'AONE', 'ENVX', 'KBH', 'SIG', 'WFC', 'JPM', 'TMHC', 'BAC', 'OMX', 'RAA', 'KNL', 'VICR', 'CSL', 'MOV', 'ASO', 'IBN', 'OMP', 'URBN', 'HLX', 'MLKN', 'HUL', 'PRDO', 'LAUR', 'WMS', 'RES', 'BIRK']
reports_to_request = ['ReportsFinStatements', 'ReportSnapshot', 'RESC'] # ['ReportsFinSummary', 'ReportsOwnership', 'ReportSnapshot', 'ReportsFinStatements', 'RESC']

# Niet gevonden: BMM, IIAC.U, MFB, JFB, EIN3, ATNY, PERY, AONE, OMX, RAA, KNL, OMP, HUL

# Create folder "fundamentals" if it doesn't exist
if not os.path.exists('fundamentals'):
    os.makedirs('fundamentals')

for company in companies_to_get:

    # Create folder with company name if it doesn't exist
    # If it already exists, don't get the fundamentals.
    if not os.path.exists(f'./fundamentals/{company}'):
        os.makedirs(f'./fundamentals/{company}')
        
    for report in reports_to_request
        if not os.path.exists(f'./fundamentals/{company}/{report}.xml'):
            fund = get_fundamentals_for(stock_ticker=company, report=report)
        
            with open(f'./fundamentals/{company}/{report}.xml', 'w') as file:
                file.write(fund)

ib.disconnect()