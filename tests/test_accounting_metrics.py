'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
from financial_fundamentals.accounting_metrics import AccountingMetricGetter, EPS,\
    BookValuePerShare
from financial_fundamentals.edgar import HTMLEdgarDriver
import datetime
from tests.infrastructure import turn_on_request_caching, TEST_DOCS_DIR
from financial_fundamentals.xbrl import XBRLDocument
import os
from financial_fundamentals.sec_filing import Filing

class TestAccountingMetricGetter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        turn_on_request_caching()
        
    def test_google(self):
        quarterly_eps = EPS.quarterly()
        getter = AccountingMetricGetter(metric=quarterly_eps, 
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 1, 2)
        interval_start, earnings, interval_end = getter.get_data(symbol='goog', 
                                                                 date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 31))
        self.assertEqual(interval_end, datetime.date(2013, 4, 25))
        self.assertEqual(earnings, 6.53)
        
    def test_google_boundry(self):
        quarterly_eps = EPS.quarterly()
        getter = AccountingMetricGetter(metric=quarterly_eps,
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 4, 25)  #Google filed on this date.
        interval_start, _, _ = getter.get_data(symbol='goog', date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 31))

class TestBookValuePerShare(unittest.TestCase):
    def test_appl(self):
        '''value computed from http://www.sec.gov/cgi-bin/viewer?action=view&cik=320193&accession_number=0001193125-13-022339&xbrl_type=v#.
        
        '''
        sec_value = 135.6
        doc_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
        xbrl_document = XBRLDocument.gets_XBRL_locally(file_path=doc_path)
        filing = Filing(filing_date=None, document=xbrl_document, next_filing=None)
        book_value_per_share = BookValuePerShare.value_from_filing(filing)
        self.assertAlmostEqual(book_value_per_share, sec_value, places=1)

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestBookValuePerShare('test_appl'))
    unittest.TextTestRunner().run(suite)
    
        