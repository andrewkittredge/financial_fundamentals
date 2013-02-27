'''
Created on Jan 26, 2013

@author: akittredge
'''

from datetime import date
from edgar import filing_before
from accounting_metrics import QuarterlyEPS
import logging
import unittest

quarter_end_days = ((3, 31), (6, 30), (9, 30), (12, 31))
quarter_closes = lambda year : (date(year, month[0], month[1]) for month in 
                                quarter_end_days)

years_quarter_closes = lambda years : (quarter_close for year in years for 
                                       quarter_close in quarter_closes(year))

logger = logging.getLogger('fundamentals')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def price_to_earnings(ticker, date_, price):
    filing = filing_before(ticker=ticker, 
                           filing_type='10-Q', 
                           date_after=date_)
    return price /(  QuarterlyEPS.value(filing) * 4)

class TestsFundamentals(unittest.TestCase):
    def setUp(self):
        import requests_cache
        requests_cache.configure('fundamentals_cache_test')

    def test_pe(self):
        dec_28_2011_appl_pe = 14.55 # from http://ycharts.com/companies/AAPL/pe_ratio
        dec_28_2011_appl_price = 402.64
        metric_date = date(2011, 12, 28)
        computed_pe = price_to_earnings('aapl', metric_date, dec_28_2011_appl_price)
        self.assertAlmostEqual(computed_pe, dec_28_2011_appl_pe, delta=.1)