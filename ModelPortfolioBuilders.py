'''
Created on Mar 6, 2013

@author: akittredge
'''
import unittest


class EqualWeights():
    '''Hold the desired number of positions.  An equal value of each.
    
    '''
    def __init__(self, percentage_of_universe_to_hold=.5):
        self.percentage_of_universe_to_hold = percentage_of_universe_to_hold
        
        
    def build_portfolio(self, ordered_universe, security_prices, target_value):
        '''Takes the universe ordered from highest expected return to lowest.
            Returns the model portfolio.
        '''
        size_of_universe = len(ordered_universe)
        num_positions_to_hold = int(round(size_of_universe * 
                                      self.percentage_of_universe_to_hold))
        desired_securities = ordered_universe[:num_positions_to_hold]
        value_of_each_position = target_value / num_positions_to_hold
        model_portfolio = {}
        for security in desired_securities:
            import numpy
            assert not numpy.isnan(security_prices[security])
            model_position = value_of_each_position / security_prices[security]
            model_portfolio[security] = model_position
        return model_portfolio
    
    
from collections import OrderedDict
class TestEqualWeightsPortfolio(unittest.TestCase):
    security_prices = OrderedDict([('best', 10), ('better', 20), 
                        ('good', 5), ('meh', 100), ('yikes', 2)])
    ordered_universe = security_prices.keys()

    def test_model_portfolio_attributes(self):
        percentage_of_universe_to_buy = .5
        target_portfolio_value = 10. * 10 ** 6
        num_positions_to_hold = int(round(percentage_of_universe_to_buy * 
                                    len(self.ordered_universe)))
        builder = EqualWeights()

        model_portfolio = builder.build_portfolio(self.ordered_universe, 
                                                  self.security_prices,
                                                  target_portfolio_value)
        self.assertEqual(len(model_portfolio), num_positions_to_hold)
        value_of_each_position = target_portfolio_value / num_positions_to_hold
        for ticker, price in self.security_prices.items()[:num_positions_to_hold]:
            model_position = model_portfolio[ticker]
            position_value = model_position * price
            self.assertAlmostEqual(position_value, value_of_each_position, delta=1.)