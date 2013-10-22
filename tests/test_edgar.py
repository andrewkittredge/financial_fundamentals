'''
Created on Sep 12, 2013

@author: akittredge
'''

import mock
import unittest
from datetime import date
from tests.infrastructure import turn_on_request_caching, TEST_DOCS_DIR
import os
from financial_fundamentals.edgar import HTMLEdgarDriver, XBRLNotAvailable,\
    Filing
import datetime
import urlparse

class TestsEdgar(unittest.TestCase):
    def setUp(self):
        turn_on_request_caching()

    def test_get_filiing(self):
        filing = HTMLEdgarDriver.get_filing(ticker='aapl', 
                                            filing_type='10-Q',
                                            date_after=date(2013, 1, 24)
                                            )
        self.assertEqual(urlparse.urlsplit(filing._document._xbrl_url).path.split('/')[-1],
                         'aapl-20120630.xml')

    def test_mmm(self):
        '''This was getting a text file instead of xml.
        
        '''
        filing = HTMLEdgarDriver.get_filing(ticker='MMM', 
                                                filing_type='10-Q',
                                                date_after=date(2010, 1, 04))
        self.assertTrue(filing._document._xbrl_url.endswith('.xml'))
        
    @mock.patch('requests.models.Response.text', new_callable=mock.PropertyMock)
    def test_ABBV(self, text):
        '''Test page with no 10-Q's, downloaded 2013-3-2, 
        ABBV had just been spun off or something.
        
        '''
        with open(os.path.join(TEST_DOCS_DIR, 'abbv_search_results.html')) as test_html:
            text.return_value = test_html.read()

        ticker = 'ABBV'
        self.assertFalse(list(HTMLEdgarDriver._get_document_page_urls(symbol=ticker, 
                                                                      filing_type='10-Q')
                              )
                         )
        filing_type = '10-Q'
        date_after = date(2013, 1, 2)
        
        finds_no_filings = lambda : HTMLEdgarDriver.get_filing(ticker, 
                                                               filing_type, 
                                                               date_after, 
                                                               )
        self.assertRaises(XBRLNotAvailable, finds_no_filings)

    def test_sort_order(self):
        filing_dates = [datetime.date(2012, 12, 1),
                        datetime.date(2012, 12, 3),
                        datetime.date(2012, 12, 15),
                        datetime.date(2012, 12, 5)]
        class TestDriver(HTMLEdgarDriver):
            @classmethod
            def _get_document_page_urls(cls, *args, **kwargs):
                return iter(filing_dates)
                
            @classmethod
            def _get_filing_from_document_page(cls, date):
                return Filing(filing_date=date, document=None)
            
            @classmethod
            def _get_filing_urls(cls, *args, **kwargs):
                return filing_dates
                
        driver = TestDriver
        filings = driver._get_sorted_filings(ticker=None, filing_type=None)
        self.assertEqual(len(filing_dates), len(filings))
        
        filing = driver.get_filing(ticker=None, 
                                   filing_type=None, 
                                   date_after=datetime.date(2012, 12, 2))
        self.assertEqual(filing.date, datetime.date(2012, 12, 1))
        with self.assertRaises(Exception):
            driver.get_filing(tickers=None, 
                              filing_type=None, 
                              date_after=datetime.date(2012, 12, 16))
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestsEdgar('test_sort_order'))
    unittest.TextTestRunner().run(suite)
    