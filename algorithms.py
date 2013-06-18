
'''
Created on Feb 16, 2013

@author: akittredge
'''
from zipline.algorithm import TradingAlgorithm
from datetime import date, datetime, time
from fundamentals import SQLLiteMultiplesCache, MissingData
from accounting_metrics import QuarterlyEPS
import logging
from ModelPortfolioBuilders import EqualWeights
from trade_generators import AlwaysTrades
import warnings
from math import isnan
from financial_fundamentals import edgar



logging.basicConfig(level=logging.DEBUG)

class BuyValueStocks(TradingAlgorithm):
    def __init__(self, universe, percent_cash=.05):
        TradingAlgorithm.__init__(self)
        self.multiples_cache = SQLLiteMultiplesCache()
        self.model_portfolio_builder = EqualWeights()
        self.builds_trades = AlwaysTrades()
        self.universe = universe
        
        self.percent_cash = percent_cash
        
    def handle_data(self, data):
        inputs = []
        prices = {}
        trading_date = self.datetime.date()
        for ticker in self.universe:
            ticker_data = data[ticker]
            
            price = ticker_data.price
            if isnan(price):
                warnings.warn('No price for {} on {}'.format(ticker,
                                                             trading_date))
                continue
            prices[ticker] = price
            try:
                earnings = self.multiples_cache.get(ticker=ticker, 
                                                date_=trading_date, 
                                                metric=QuarterlyEPS)
            except (MissingData, edgar.CouldNotFindCIK):
                warnings.warn('No data for {} on {}'.format(ticker, trading_date))
                continue


            pe = price / earnings if earnings > 0 else float('inf') # negative p/e is strange
            inputs.append(PortfolioInput(ticker=ticker, pe=pe, price=price))

        logging.debug('processing {}'.format(trading_date))
        ordered_universe = [input_.ticker for input_ in 
                            sorted(inputs, key=lambda input_ : input_.pe)]
        security_prices = dict((input_.ticker, input_.price) for input_ in inputs)
        
        target_value = self.portfolio['portfolio_value']
        int(target_value)
        model_portfolio = self.build_model_portfolio(ordered_universe,
                                                    security_prices, 
                                                    target_value)
        trades = self.builds_trades.build_trades(positions=self.portfolio['positions'], 
                                                 model_portfolio=model_portfolio)
        
        for ticker, trade in trades.iteritems():
            if abs(trade) > 0:
                self.order(ticker, trade)

        
    def build_model_portfolio(self, ordered_universe,
                              security_prices, target_value):
        return self.model_portfolio_builder.build_portfolio(ordered_universe, 
                                                            security_prices, 
                                                            target_value)

from collections import namedtuple
PortfolioInput = namedtuple('PortfolioInput', ['ticker', 'pe', 'price'])


def run_algo(start, end, tickers):
    import requests_cache
    requests_cache.configure('/tmp/fundamentals_cache')
    from zipline.utils.factory import load_from_yahoo
    from dateutil import tz
    utc = tz.gettz('UTC')
    period_start = datetime.combine(start, time(tzinfo=utc))
    period_end = datetime.combine(end, time(tzinfo=utc))
    data = load_from_yahoo(indexes={'SPX': '^GSPC'},
                           stocks=tickers, 
                           start=period_start, 
                           end=period_end)
    data['PEG'], _ = data['PEG'].align(data['SPX'], method='ffill')
    algo = BuyValueStocks(percent_cash=.1,
                          universe=tickers)
    results = algo.run(data)
    return data, results

import unittest
class TestsAlgorithmRunner(unittest.TestCase):
    def test_february(self):
        '''This was throwing an error.
        
        '''
        from indicies import DOW_TICKERS
        run_algo(start=date(2013, 2, 1), 
                 end=date(2013, 2, 27), 
                 tickers=DOW_TICKERS)
    
if __name__ == '__main__':
    from indicies import CLEANED_S_P_500_TICKERS
    data, results = run_algo(start=date(2010, 3, 11), 
                             end=date(2013, 3, 30), 
                             tickers=CLEANED_S_P_500_TICKERS)