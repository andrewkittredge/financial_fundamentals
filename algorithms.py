
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

logging.basicConfig(level=logging.DEBUG)

class BuyValueStocks(TradingAlgorithm):
    def __init__(self, universe=DOW_TICKERS, percent_cash=.05):
        TradingAlgorithm.__init__(self)
        self.multiples_cache = SQLLiteMultiplesCache()
        self.model_portfolio_builder = EqualWeights()
        self.builds_trades = AlwaysTrades()
        self.universe = universe
        
        self.percent_cash = percent_cash
        
    def handle_data(self, data):
        inputs = []
        prices = {}
        for ticker in self.universe:
            ticker_data = data[ticker]
            trading_date = ticker_data.datetime.to_datetime().date()
            
            price = ticker_data.price
            prices[ticker] = price
            earnings = self.multiples_cache.get(ticker=ticker, 
                                                date_=trading_date, 
                                                metric=QuarterlyEPS)


            pe = price / earnings if earnings > 0 else float('inf') # negative p/e is strange
            inputs.append(PortfolioInput(ticker=ticker, pe=pe, price=price))

        logging.debug('processing {}'.format(trading_date))
        ordered_universe = [input_.ticker for input_ in 
                            sorted(inputs, key=lambda input_ : input_.pe)]
        security_prices = dict((input_.ticker, input_.price) for input_ in inputs)
        
        target_value = self.portfolio['portfolio_value']
        model_portfolio = self.build_model_portfolio(ordered_universe,
                                                    security_prices, 
                                                    target_value)
        trades = self.builds_trades.build_trades(positions=self.portfolio['positions'], 
                                                 model_portfolio=model_portfolio)
        
        for ticker, trade in trades.iteritems():
            self.order(ticker, trade)

        
    def build_model_portfolio(self, ordered_universe,
                              security_prices, target_value):
        return self.model_portfolio_builder.build_portfolio(ordered_universe, 
                                                            security_prices, 
                                                            target_value)

from collections import namedtuple
PortfolioInput = namedtuple('PortfolioInput', ['ticker', 'pe', 'price'])


def run_algo():
    import requests_cache
    requests_cache.configure('fundamentals_cache')
    from zipline.utils.factory import load_from_yahoo
    from dateutil import tz
    utc = tz.gettz('UTC')
    period_start = datetime.combine(date(2012, 12, 1), time(tzinfo=utc))
    period_end = datetime.combine(date(2013, 1, 10), time(tzinfo=utc))
    test_tickers = ['AA', 'MMM', 'AXP']
    data = load_from_yahoo(stocks=test_tickers, 
                           start=period_start, 
                           end=period_end)

    algo = BuyValueStocks(percent_cash=.1,
                          universe=test_tickers)
    results = algo.run(data)
    return results
    
    
if __name__ == '__main__':
    run_algo()
    