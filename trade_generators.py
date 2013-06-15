'''
Created on Mar 12, 2013

@author: akittredge
'''
from collections import namedtuple


class AlwaysTrades(object):
    @classmethod
    def build_trades(cls, positions, model_portfolio):
        '''positions and model_portfolio are dicts of ticker, number of shares
        pairs.
        
        '''
        securities_to_trade = set(positions.keys() + model_portfolio.keys())
        trades = {}
        for ticker in securities_to_trade:
            current_position = positions.get(ticker)
            current_shares = current_position.amount if current_position else 0
            desired_shares = model_portfolio.get(ticker, 0)
            change_in_position = desired_shares - current_shares
            # Don't trade fractional shares.
            
            trades[ticker] = int(round(change_in_position))
            
                
        return trades
    
import unittest
class TestsAlwaysTrades(unittest.TestCase):
    def test_always_trades(self):
        Position = namedtuple('position', ['amount'])
        model_portfolio = {'msft' : 100, 'ibm' : 300, 'amzn' : 400}
        holdings = {'msft' : Position(50), 
                    'ibm' : Position(300), 
                    'f' : Position(200)}
        trades = AlwaysTrades.build_trades(holdings, model_portfolio)
        self.assertEqual(trades['msft'], 50)
        self.assertEqual(trades['ibm'], 0)
        self.assertEqual(trades['amzn'], 400)
        self.assertEqual(trades['f'], -200)