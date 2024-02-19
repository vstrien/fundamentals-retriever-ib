import os
from datetime import datetime, timedelta
import nest_asyncio
import yfinance as yf

nest_asyncio.apply()
from ib_insync import *
ib = IB()

ib.connect('127.0.0.1', 7496, clientId = 3)

def get_fundamentals_for(stock_ticker, market = 'SMART', currency = 'USD'):
    contract = Stock(stock_ticker, market, currency)
    reportsToRequest = ['ReportsFinStatements', 'ReportSnapshot'] # ['ReportsFinSummary', 'ReportsOwnership', 'ReportSnapshot', 'ReportsFinStatements', 'RESC']
    reports = { report: ib.reqFundamentalData(contract, report) for report in reportsToRequest }
    return reports



# Request the company's fundamentals
# fundamentals = ib.reqFundamentalData(contract, 'ReportsFinStatements')

companies_to_get = ['TITN', 'BLBD', 'BMM', 'GOTU', 'SKY', 'CPRI', 'OC', 'ASTE', 'KGS', 'GPS', 'ONON', 'ATGE', 'SHLS', 'IIAC.U', 'DECK', 'U', 'HDB', 'PLOW', 'MFB', 'SCS', 'MUFG', 'MDC', 'POWL', 'JFB', 'HSBC', 'JL', 'ENR', 'MTW', 'HNI', 'ACDC', 'MTH', 'BLDR', 'INVE', 'ANF', 'EIN3', 'AEO', 'ATNY', 'PERY', 'LOPE', 'MAS', 'IVAC', 'RELL', 'SKX', 'CROX', 'AONE', 'ENVX', 'KBH', 'SIG', 'WFC', 'JPM', 'TMHC', 'BAC', 'OMX', 'RAA', 'KNL', 'VICR', 'CSL', 'MOV', 'ASO', 'IBN', 'OMP', 'URBN', 'HLX', 'MLKN', 'HUL', 'PRDO', 'LAUR', 'WMS', 'RES', 'BIRK']

# Create folder "fundamentals" if it doesn't exist
if not os.path.exists('fundamentals'):
    os.makedirs('fundamentals')

# Niet gevonden: BMM, IIAC.U, MFB, JFB, EIN3, ATNY, PERY, AONE, OMX, RAA, KNL, OMP, HUL

for company in companies_to_get:

    # Create folder with company name if it doesn't exist
    # If it already exists, don't get the fundamentals.
    if not os.path.exists(f'./fundamentals/{company}'):
        os.makedirs(f'./fundamentals/{company}')
        
        fund = get_fundamentals_for(company)
        
        for r in fund.keys():
            with open(f'./fundamentals/{company}/{r}.xml', 'w') as file:
                file.write(str(fund[r]))

ib.disconnect()