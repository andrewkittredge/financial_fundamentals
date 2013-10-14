'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
from financial_fundamentals.accounting_metrics import AccountingMetricGetter,\
    QuarterlyEPS
from financial_fundamentals.edgar import HTMLEdgarDriver
import datetime

class TestAccountingMetricGetter(unittest.TestCase):
    def test_google(self):
        getter = AccountingMetricGetter(metric=QuarterlyEPS, 
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 1, 2)
        interval_start, earnings, interval_end = getter.get_data(symbol='goog', 
                                                                 date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 30))
        self.assertEqual(interval_end, datetime.date(2013, 4, 25))
        self.assertEqual(earnings, 6.53)
        
    def test_google_boundry(self):
        getter = AccountingMetricGetter(metric=QuarterlyEPS,
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 4, 25)  #Google filed on this date.
        interval_start, _, _ = getter.get_data(symbol='goog', date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 30))
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestAccountingMetricGetter('test_google_boundry'))
    unittest.TextTestRunner().run(suite)
    
        