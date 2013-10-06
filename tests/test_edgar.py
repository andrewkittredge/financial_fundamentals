'''
Created on Sep 12, 2013

@author: akittredge
'''

import mock
import unittest
from datetime import date
from tests.infrastructure import turn_on_request_caching, TEST_DOCS_DIR
import os
from financial_fundamentals.edgar import EdgarHTMLDriver, XBRLNotAvailable

class TestsEdgar(unittest.TestCase):
    def setUp(self):
        turn_on_request_caching()

    def test_get_filiing(self):
        filing = EdgarHTMLDriver.get_filing(ticker='aapl', 
                                            filing_type='10-Q',
                                            date_after=date(2013, 1, 24)
                                            )
        self.assertEqual(filing.xbrl_url, 
                         'http://www.sec.gov/Archives/edgar/data/320193/000119312513022339/aapl-20121229.xml')

    def test_mmm(self):
        '''This was getting a text file instead of xml.
        
        '''
        filing = EdgarHTMLDriver.get_filing(ticker='MMM', 
                                                filing_type='10-Q',
                                                date_after=date(2010, 1, 04))
        self.assertTrue(filing.xbrl_url.endswith('.xml'))
        
    @mock.patch('requests.models.Response.text', new_callable=mock.PropertyMock)
    def test_ABBV(self, text):
        '''Test page with no 10-Q's, downloaded 2013-3-2, 
        ABBV had just been spun off or something.
        
        '''
        with open(os.path.join(TEST_DOCS_DIR, 'abbv_search_results.html')) as test_html:
            text.return_value = test_html.read()

        ticker = 'ABBV'
        self.assertFalse(list(EdgarHTMLDriver._get_filing_urls(ticker=ticker, 
                                                               filing_type='10-Q')
                              )
                         )
        filing_type = '10-Q'
        date_after = date(2013, 1, 2)
        
        finds_no_filings = lambda : EdgarHTMLDriver.get_filing(ticker, 
                                                               filing_type, 
                                                               date_after, 
                                                               )
        self.assertRaises(XBRLNotAvailable, finds_no_filings)

    def test_sort_order(self):
        pass