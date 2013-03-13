'''
Created on Mar 12, 2013

@author: akittredge
'''


class AlwaysTrades(object):
    @classmethod
    def build_trades(cls, holdings, model_portfolio):
        '''holdings and model_portfolio are dicts of ticker, number of shares
        pairs.
        
        '''
        securities_to_trade = set(holdings.keys() + model_portfolio.keys())
        trades = {}
        for ticker in securities_to_trade:
            change_in_position = model_portfolio.get(ticker, 0) -\
                                                 holdings.get(ticker, 0)
            trades[ticker] = change_in_position
        return trades
    
import unittest
class TestsAlwaysTrades(unittest.TestCase):
    def test_always_trades(self):
        model_portfolio = {'msft' : 100, 'ibm' : 300, 'amzn' : 400}
        holdings = {'msft' : 50, 'ibm' : 300, 'f' : 200}
        trades = AlwaysTrades.build_trades(holdings, model_portfolio)
        self.assertEqual(trades['msft'], 50)
        self.assertEqual(trades['ibm'], 0)
        self.assertEqual(trades['amzn'], 400)
        self.assertEqual(trades['f'], -200)