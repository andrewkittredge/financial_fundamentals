'''
Created on Sep 20, 2013

@author: akittredge
'''
import unittest
import datetime
from financial_fundamentals.prices import _wrapped_get_data_yahoo
from financial_fundamentals.exceptions import ExternalRequestFailed


class YahooPricesTestCase(unittest.TestCase):
    def test_dates_too_close_together(self):
        start = datetime.datetime(2013, 9, 16)
        end = datetime.datetime(2013, 9, 19)
        self.assertRaises(ExternalRequestFailed,
                          lambda : _wrapped_get_data_yahoo(symbol='BMC', 
                                                           start=start, 
                                                           end=end))
        

        