from datetime import datetime, timedelta
import nest_asyncio
import yfinance as yf

nest_asyncio.apply()
from ib_insync import *
ib = IB()

ib.connect('127.0.0.1', 7496, clientId = 3)

def get_fundamentals_for(stock_ticker, market = 'SMART', currency = 'USD'):
    contract = Stock(stock_ticker, market, currency)
    reportsToRequest = ['ReportsFinSummary', 'ReportsOwnership', 'ReportSnapshot', 'ReportsFinStatements', 'RESC', 'CalendarReport']
    reports = [ib.reqFundamentalData(contract, report) for report in reportsToRequest]
    return reports



# Get the company's most important competitors from Yahoo
print(get_competitors_for('JPM'))
# Request the company's fundamentals
# fundamentals = ib.reqFundamentalData(contract, 'ReportsFinStatements')


ib.disconnect()