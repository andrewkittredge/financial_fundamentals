'''
Created on Sep 24, 2013

@author: akittredge
'''
from zipline.algorithm import TradingAlgorithm
from datetime import datetime
import pytz
from financial_fundamentals import sqlite_fundamentals_cache,\
    mongo_fundamentals_cache, mongo_price_cache
from financial_fundamentals.accounting_metrics import QuarterlyEPS
from financial_fundamentals import sqlite_price_cache
from financial_fundamentals.indicies import DOW_TICKERS, S_P_500_TICKERS
from zipline.transforms.batch_transform import batch_transform
import numpy as np
import pandas as pd
import scipy.integrate

class BuysLowSellsHigh(TradingAlgorithm):
    def initialize(self, earnings):
        @batch_transform
        def price_to_earnings(datapanel):
            # Wes McKinney would probably do this differently.
            p_e_ratio = datapanel.price / (earnings * 4) # assuming quarterly eps, 
            latest_date = p_e_ratio.first_valid_index()
            latest_p_e_ratios = p_e_ratio.T[latest_date]
            latest_p_e_ratios.name = 'p/e ratios on {}'.format(latest_date)
            return latest_p_e_ratios.copy()
        
        self.price_to_earnings_transform = price_to_earnings(refresh_period=1,
                                                             window_length=1)
        self.init = True
        
    def handle_data(self, data):
        p_e_ratios = self.price_to_earnings_transform.handle_data(data)
        unknowns = p_e_ratios[p_e_ratios.isnull()].fillna(0)
        p_e_ratios = p_e_ratios.dropna()
        p_e_ratios.sort(ascending=False)
        desired_port = self.portfolio_weights(sorted_universe=p_e_ratios)
        prices = pd.Series({item[0] : item[1]['price'] for item in data.iteritems()})
        if self.init:
            positions_value = self.portfolio.starting_cash
        else:
            positions_value = self.portfolio.positions_value + \
                                self.portfolio.cash
        current_position = pd.Series({item[0] : item[1]['amount'] for item in
                                       self.portfolio.positions.items()},
                                     index=p_e_ratios.index).fillna(0)
        self.rebalance_portfolio(desired_port=pd.concat([desired_port, unknowns]),
                                 prices=prices,
                                 positions_value=positions_value,
                                 current_amount=current_position)
        self.init = False
        
    def portfolio_weights(self, sorted_universe):
        '''the universe weighted by area of equal width intervals under a curve.''' 
        curve = lambda x : x # linear
        interval_width = 1. / sorted_universe.size
        interval_start = pd.Series(np.linspace(start=0, 
                                               stop=1, 
                                               num=sorted_universe.size, 
                                               endpoint=False), 
                                    index=sorted_universe.index)
        weight_func = lambda x : scipy.integrate.quad(func=curve, 
                                                      a=x, 
                                                      b=(x + interval_width)
                                                      )[0]
        portfolio_weight = interval_start.map(arg=weight_func)
        portfolio_weights_summed_to_one = (portfolio_weight * 
                                           (1 / portfolio_weight.sum()))
        return portfolio_weights_summed_to_one
    
    def rebalance_portfolio(self, desired_port, prices, 
                            positions_value, current_amount):
        '''after zipline.examples.olmar'''
        desired_amount = np.round(desired_port * positions_value / prices)
        self.last_desired_port = desired_port
        diff_amount = desired_amount - current_amount
        for stock, order_amount in diff_amount[diff_amount != 0].dropna().iteritems():
            self.order(sid=stock, amount=order_amount)
            
    
def buy_low_sell_high(start=datetime(2013, 6, 1, tzinfo=pytz.UTC),
                      end=datetime(2013, 9, 15, tzinfo=pytz.UTC),
                      metric=QuarterlyEPS,
                      fundamentals_cache=mongo_fundamentals_cache,
                      price_cache=mongo_price_cache,
                      stocks=S_P_500_TICKERS):
    earnings = fundamentals_cache(metric).load_from_cache(stocks=stocks,
                                                              start=start,
                                                              end=end)
    earnings[earnings < 0] = 0 # negative p/e's don't make sense.
    algo = BuysLowSellsHigh(earnings=earnings)
    prices = price_cache().load_from_cache(stocks=stocks, start=start, end=end)
    results = algo.run(prices)
    return results, algo


if __name__ == '__main__':
    buy_low_sell_high()