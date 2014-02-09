'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
from financial_fundamentals.accounting_metrics import BookValuePerShare
import financial_fundamentals.accounting_metrics as accounting_metrics
import datetime
from financial_fundamentals.xbrl import XBRLDocument
from financial_fundamentals.sec_filing import Filing
import tests
tests.turn_on_request_caching()
import pandas as pd

class TestEarningsPerShare(unittest.TestCase):       
    def test_google(self):
        required_data = pd.DataFrame(columns=['GOOG'], 
                                     index=pd.date_range('2012-10-31', '2013-4-25'))
        eps = accounting_metrics.earnings_per_share(required_data)
        verified_eps = 6.53
        self.assertEqual(eps.GOOG[datetime.date(2012, 10, 31)], verified_eps)
        self.assertEqual(eps.GOOG[datetime.date(2013, 4, 25)], verified_eps)


class TestBookValuePerShare(unittest.TestCase):
    def test_appl(self):
        '''value computed from http://www.sec.gov/cgi-bin/viewer?action=view&cik=320193&accession_number=0001193125-13-022339&xbrl_type=v#.
        
        '''
        sec_value = 135.6
        doc_path = tests.asset_file_path('aapl-20121229.xml')
        xbrl_document = XBRLDocument.gets_XBRL_locally(file_path=doc_path)
        filing = Filing(filing_date=None, document=xbrl_document, next_filing=None)
        book_value_per_share = BookValuePerShare.value_from_filing(filing)
        self.assertAlmostEqual(book_value_per_share, sec_value, places=1)
        
    def test_GOOG_shareholders_equity(self):
        sec_value = 994.77
        doc_path = tests.asset_file_path('goog-20120630.xml')
        xbrl_document = XBRLDocument.gets_XBRL_locally(file_path=doc_path)
        filing = Filing(filing_date=None, document=xbrl_document, next_filing=None)
        book_value_per_share = BookValuePerShare.value_from_filing(filing)
        self.assertAlmostEqual(book_value_per_share, sec_value, places=1)
