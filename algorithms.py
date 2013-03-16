
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
from portfolio import LongOnlyPortfolio

DOW_TICKERS = ['MMM', 'AA', 'AXP', 'T', 'BAC', 'BA', 'CAT', 'CVX', 
               'CSCO', 'DD', 'XOM', 'GE', 'HPQ', 'HD', 'INTC', 'IBM', 
               'JNJ', 'JPM', 'MCD', 'MRK', 'MSFT', 'PFE', 'PG', 'KO',
               'TRV', 'UTX', 'UNH', 'VZ', 'WMT', 'DIS']

logging.basicConfig(level=logging.DEBUG)

class BuyValueStocks(TradingAlgorithm):
    def __init__(self, initial_portfolio, universe=DOW_TICKERS, percent_cash=.05):
        TradingAlgorithm.__init__(self)
        self.multiples_cache = SQLLiteMultiplesCache()
        self.model_portfolio_builder = EqualWeights()
        self.builds_trades = AlwaysTrades()
        self.universe = universe
        self.portfolio_ = initial_portfolio # One of our ancestors is using portfolio
        self.percent_cash = percent_cash
        
    num_days_processed = 0
    def handle_data(self, data):
        portfolio = self.portfolio_
        logging.debug('{} days processed'.format(self.num_days_processed))
        self.num_days_processed += 1
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
            #logging.debug('p/e for {} on {} is {}'.format(ticker,
                                                         #trading_date,
                                                         #pe))
        ordered_universe = [input_.ticker for input_ in 
                            sorted(inputs, key=lambda input_ : input_.pe)]
        security_prices = dict((input_.ticker, input_.price) for input_ in inputs)
        target_value = portfolio.nav(prices) * (1 - self.percent_cash)
        model_portfolio = self.model_portfolio_builder.build_portfolio(
                                                        ordered_universe, 
                                                        security_prices, 
                                                        target_value=target_value)
        trades = self.builds_trades.build_trades(portfolio, model_portfolio)
        
        for ticker, trade in trades.iteritems():
            self.order(ticker, trade)
            change_in_cash = abs(prices[ticker] * trade)
            if trade > 0:
                portfolio.buy(security=ticker, order_size=trade,
                              cost=change_in_cash)
            elif trade < 0:
                portfolio.sell(security=ticker, order_size=trade,
                               proceeds=change_in_cash)
        
            
        logging.info(portfolio)

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
    starting_portfolio = LongOnlyPortfolio(initial_cash=1000000)
    algo = BuyValueStocks(starting_portfolio)
    results = algo.run(data)
    