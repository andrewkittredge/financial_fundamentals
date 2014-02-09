'''
Created on Sep 12, 2013

@author: akittredge
'''

import mock
import unittest
from datetime import date
import tests
import os
import financial_fundamentals.edgar as edgar

import urlparse
tests.turn_on_request_caching()

class TestsEdgar(unittest.TestCase):
    def test_get_filing(self):
        filings = edgar.get_filings(symbol='aapl', filing_type='10-Q')
        filing = filings[filings.bisect(date(2013, 1, 23)) - 1]
        self.assertEqual(urlparse.urlsplit(filing._document._xbrl_url).path.split('/')[-1],
                         'aapl-20120630.xml')

    def test_mmm(self):
        '''This was getting a text file instead of xml.
        
        '''
        filings = edgar.get_filings(symbol='MMM', filing_type='10-Q')
        filing = filings[filings.bisect(date(2010, 1, 4))]
        self.assertTrue(filing._document._xbrl_url.endswith('.xml'))
        
    @mock.patch('requests.models.Response.text', new_callable=mock.PropertyMock)
    def test_ABBV(self, text):
        '''Test page with no 10-Q's, downloaded 2013-3-2, 
        ABBV had just been spun off or something.
        
        '''
        with open(tests.asset_file_path('abbv_search_results.html')) as test_html:
            text.return_value = test_html.read()

        ticker = 'ABBV'
        urls = edgar._get_document_page_urls(symbol=ticker, filing_type='10-Q')
        self.assertFalse(list(urls))
        filing_type = '10-Q'
        filings = edgar.get_filings(symbol=ticker, filing_type=filing_type)
        self.assertEqual(len(filings), 0)
        


    def test_JCP(self):
        '''was getting a non-xbrl doc back.'''
        document_page_that_failed = 'http://sec.gov/Archives/edgar/data/1166126/000116612613000041/0001166126-13-000041-index.htm'
        filing = edgar._get_filing_from_document_page(document_page_url=document_page_that_failed)
        self.assertEqual(filing._document._xbrl_url.split('/')[-1], 'jcp-20130504.xml')
        

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestsEdgar('test_xbrl_not_available_yet'))
    unittest.TextTestRunner().run(suite)
    