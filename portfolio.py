'''
Created on Mar 16, 2013

@author: akittredge
'''


class Portfolio(dict):
    '''Models a portfolio of securities.
    
    Long only.
    
    '''
    def __init__(self, initial_positions=(), initial_cash=1000):
        self.cash = initial_cash
        super(Portfolio, self).__init__(initial_positions)
        
    def nav(self, prices):
        '''net asset value.
        
        '''
        return sum(position * prices[security] for 
                   security, position in self.iteritems()) + self.cash
                   
    def buy(self, security, order_size, cost):
        assert cost > 0
        self.check_buy(cost)
        self[security] = self.get(security, 0) + order_size
        self.cash -= cost
        
    def sell(self, security, order_size, proceeds):
        assert proceeds > 0 and order_size > 0
        self.check_sell(security, order_size)
        self[security] = self.get(security, 0) - order_size 
        self.cash += proceeds
        
    def check_sell(self, security, order_size):
        pass
    
    def check_buy(self, cost):
        pass
    
    def compliance(self):
        pass
    
    def __str__(self, *args, **kwargs):
        return 'Cash {}: Positions {}'.format(self.cash, super(Portfolio, self).__str__())
        
class LongOnlyPortfolio(Portfolio):
    '''Enforces long-only constraint.
    
    '''
    def check_sell(self, security, order_size):
        assert self.get(security, 0) - order_size >= 0, \
                            'Long only constraint violated'
    
    def check_buy(self, cost):
        pass
        
    def compliance(self):
        assert self.cash >= 0
        
import unittest
class TestsPortfolio(unittest.TestCase):
    portfolio_type = Portfolio
    def setUp(self):
        portfolio = self.portfolio_type(initial_positions=[('msft', 100),
                                                           ('amzn', 100), 
                                                           ('ibm', 100)],
                          initial_cash=1000)
        prices = dict([('msft', 10.), ('amzn', 20.), ('ibm', 30.)])
        self.portfolio = portfolio
        self.prices = prices
    def test_portfolio_nav(self):
        self.assertEqual(self.portfolio.nav(self.prices), 7000.)
    
    def test_initial_value(self):
        portfolio = Portfolio(initial_cash=100.)
        self.assertEqual(portfolio.nav({}), 100)
        
    def test_sell(self):
        portfolio = self.portfolio
        security = 'msft'
        initial_position = portfolio[security]
        initial_cash = portfolio.cash
        order_size = 100
        proceeds = 1000
        portfolio.sell(security, order_size=order_size, proceeds=proceeds)
        self.assertEqual(portfolio.cash, initial_cash + proceeds)
        self.assertEqual(portfolio[security], initial_position - order_size)
        
    def test_buy(self):
        portfolio = self.portfolio
        security = 'msft'
        initial_position = portfolio[security]
        initial_cash = portfolio.cash
        order_size = 100
        cost = 10
        portfolio.buy(security, order_size, cost)
        self.assertEqual(portfolio.cash, initial_cash - cost)
        self.assertEqual(portfolio[security], initial_position + order_size)
        
        
class TestsLongOnlyPortfolio(TestsPortfolio):
    portfolio_type = LongOnlyPortfolio
    def test_long_only_assertion(self):
        try_short_sale = lambda : self.portfolio.sell(security='hog', 
                                                           order_size=1, 
                                                           proceeds=0.)
        self.assertRaises(AssertionError, try_short_sale)
        
    def test_compliance(self):
        self.portfolio.buy(security='hog', order_size=1, cost=1000000)
        self.assertRaises(AssertionError, self.portfolio.compliance)