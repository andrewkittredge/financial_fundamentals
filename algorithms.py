
'''
Created on Feb 16, 2013

@author: akittredge
'''
from zipline.algorithm import TradingAlgorithm
from datetime import date, datetime, time
from fundamentals import SQLLiteMultiplesCache
from accounting_metrics import QuarterlyEPS
import logging
from ModelPortfolioBuilders import EqualWeights
from trade_generators import AlwaysTrades

DOW_TICKERS = ['MMM', 'AA', 'AXP', 'T', 'BAC', 'BA', 'CAT', 'CVX', 
               'CSCO', 'DD', 'XOM', 'GE', 'HPQ', 'HD', 'INTC', 'IBM', 
               'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'PFE', 'PG', 'KO',
               'TRV', 'UTX', 'UNH', 'VZ', 'WMT', 'DIS']

logger = logging.basicConfig(level=logging.DEBUG)

class BuyValueStocks(TradingAlgorithm):
    def __init__(self, *args, **kwargs):
        TradingAlgorithm.__init__(self, *args, **kwargs)
        self.multiples_cache = SQLLiteMultiplesCache()
        self.model_portfolio_builder = EqualWeights()
        self.builds_trades = AlwaysTrades()
        self.universe = DOW_TICKERS
        self.holdings = {}
        
    num_days_processed = 0
    def handle_data(self, data):
        print self.num_days_processed
        self.num_days_processed += 1
        inputs = []
        for ticker in DOW_TICKERS:
            ticker_data = data[ticker]
            trading_date = ticker_data.datetime.to_datetime().date()
            price = ticker_data.price
            earnings = self.multiples_cache.get(ticker=ticker, 
                                                date_=trading_date, 
                                                metric=QuarterlyEPS)
            if earnings < 0:
                earnings = .00001
            pe = price / earnings
            inputs.append(PortfolioInput(ticker=ticker, pe=pe, price=price))
            logging.debug('p/e for {} on {} is {}'.format(ticker,
                                                         trading_date,
                                                         pe))
        ordered_universe = [input_.ticker for input_ in 
                            sorted(inputs, key=lambda input_ : input_.pe)]
        security_prices = dict((input_.ticker, input_.price) for input_ in inputs)
        model_portfolio = self.model_portfolio_builder.build_portfolio(ordered_universe, 
                                                                       security_prices, 
                                                                       target_value=100000)
        trades = self.builds_trades.build_trades(self.holdings, model_portfolio)
        
        for ticker, trade in trades.iteritems():
            self.order(ticker, trade)
        self.holdings = model_portfolio
            
        print ordered_universe

from collections import namedtuple
PortfolioInput = namedtuple('PortfolioInut', ['ticker', 'pe', 'price'])

        
if __name__ == '__main__':
    import requests_cache
    requests_cache.configure('fundamentals_cache')
    from zipline.utils.factory import load_from_yahoo
    from dateutil import tz
    utc = tz.gettz('UTC')
    period_start = datetime.combine(date(2012, 1, 1), time(tzinfo=utc))
    period_end = datetime.combine(date(2012, 1, 10), time(tzinfo=utc))
    data = load_from_yahoo(stocks=DOW_TICKERS, start=period_start, end=period_end)
    algo = BuyValueStocks()
    results = algo.run(data)
    