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

companies_to_get = ['JPM']

# Create folder "fundamentals" if it doesn't exist
if not os.path.exists('fundamentals'):
    os.makedirs('fundamentals')


for company in companies_to_get:
    fund = get_fundamentals_for(company)

    # Create folder with company name if it doesn't exist
    if not os.path.exists(f'./fundamentals/{company}'):
        os.makedirs(f'./fundamentals/{company}')

    for r in fund.keys():
        with open(f'./fundamentals/{company}/{r}.xml', 'w') as file:
            file.write(str(fund[r]))

ib.disconnect()