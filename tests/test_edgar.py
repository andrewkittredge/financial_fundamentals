'''
Created on Sep 12, 2013

@author: akittredge
'''

import mock
import unittest
from datetime import date
from tests.infrastructure import turn_on_request_caching, TEST_DOCS_DIR
import os
from financial_fundamentals.edgar import HTMLEdgarDriver, Filing, filing_sort_key_func,\
    FilingNotAvailableForDate, NoFilingsNotAvailable
import datetime
import urlparse
import blist

class TestsEdgar(unittest.TestCase):
    def setUp(self):
        turn_on_request_caching()
        HTMLEdgarDriver._ticker_filings = {}

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
        self.assertRaises(NoFilingsNotAvailable, finds_no_filings)

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
            
    def test_JCP(self):
        '''was getting a non-xbrl doc back.'''
        document_page_that_failed = 'http://sec.gov/Archives/edgar/data/1166126/000116612613000041/0001166126-13-000041-index.htm'
        filing = HTMLEdgarDriver._get_filing_from_document_page(document_page_url=document_page_that_failed)
        self.assertEqual(filing._document._xbrl_url.split('/')[-1], 'jcp-20130504.xml')
        
    def test_xbrl_not_available_yet(self):
        '''Verify behavior when looking for a document prior to company's first XBRL filing.
        
        '''
        filing_date=date(2012, 11, 30)
        sorted_filings = blist.sortedlist(key=filing_sort_key_func)
        sorted_filings.add(Filing(filing_date=filing_date, document=None))
        edgar_driver = HTMLEdgarDriver
        get_sorted_filings_mock = mock.Mock()
        get_sorted_filings_mock.return_value = sorted_filings
        edgar_driver._get_sorted_filings = get_sorted_filings_mock
        exception = None
        try:
            edgar_driver.get_filing(ticker=None, filing_type=None, 
                                    date_after=filing_date - datetime.timedelta(days=1))
        except FilingNotAvailableForDate as e:
            exception = e
        self.assertIsInstance(exception, FilingNotAvailableForDate)
        self.assertEqual(exception.end, filing_date + datetime.timedelta(days=1))
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestsEdgar('test_xbrl_not_available_yet'))
    unittest.TextTestRunner().run(suite)
    