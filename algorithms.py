
'''
Created on Feb 16, 2013

@author: akittredge
'''
from zipline.algorithm import TradingAlgorithm
from datetime import date, datetime, time
from fundamentals import price_to_earnings

DOW_TICKERS = ['MMM', 'AA', 'AXP', 'T', 'BAC', 'BA', 'CAT', 'CVX', 
               'CSCO', 'DD', 'XOM', 'GE', 'HPQ', 'HD', 'INTC', 'IBM', 
               'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'PFE', 'PG', 'KO',
               'TRV', 'UTX', 'UNH', 'VZ', 'WMT', 'DIS']

    

class BuyValueStocks(TradingAlgorithm):
    num_days_processed = 0
    def handle_data(self, data):
        print self.num_days_processed
        self.num_days_processed += 1
        for ticker in DOW_TICKERS:
            ticker_data = data[ticker]
            trading_date = ticker_data.datetime.to_datetime().date()
            pe = price_to_earnings(ticker=ticker, date_=trading_date, 
                                   price=ticker_data.price)
            if pe < 10:
                self.order(ticker, 1)
                self.buy = True
            else:
                self.order(ticker, 1)
                self.sell = True
            
if __name__ == '__main__':
    import requests_cache
    requests_cache.configure('fundamentals_cache')
    from zipline.utils.factory import load_from_yahoo
    from dateutil import tz
    utc = tz.gettz('UTC')
    period_start = datetime.combine(date(2010, 1, 1), time(tzinfo=utc))
    period_end = datetime.combine(date(2013, 1, 1), time(tzinfo=utc))
    data = load_from_yahoo(stocks=DOW_TICKERS, start=period_start, end=period_end)
    algo = BuyValueStocks()
    results = algo.run(data)