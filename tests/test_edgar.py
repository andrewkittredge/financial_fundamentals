'''
Created on Sep 12, 2013

@author: akittredge
'''

import mock
import unittest
from financial_fundamentals.edgar import populate_filing_urls_map,\
    _filing_url_before, get_document_urls, filing_before, NoFilingFound
from collections import defaultdict
from datetime import date

class TestsEdgar(unittest.TestCase):
    def setUp(self):
        from financial_fundamentals.test_infrastructure import turn_on_request_caching
        turn_on_request_caching()

    def test_populate_filing_url_map(self):
        test_map = defaultdict(dict)
        ticker = 'aapl'
        filing_type = '10-Q'
        populate_filing_urls_map(ticker=ticker, 
                                 filing_type=filing_type, 
                                 filing_url_map=test_map)
        self.assertEqual(test_map[ticker][(filing_type, date(2013, 1, 24))], 
                         'http://www.sec.gov/Archives/edgar/data/320193/000119312513022339/aapl-20121229.xml')
    
    def test_last_filing_before(self):
        test_map = defaultdict(dict)
        ticker = 'aapl'
        filing_type = '10-Q'
        populate_filing_urls_map(ticker,
                                 filing_type=filing_type,
                                 filing_url_map=test_map)
        date_after = date(2010, 7, 22)
        june_2010_filing_url = 'http://www.sec.gov/Archives/edgar/data/320193/000119312510162840/aapl-20100626.xml'
        interval_start, filing, interval_end = _filing_url_before(ticker, filing_type, date_after, filing_map=test_map)
        self.assertEqual(filing, june_2010_filing_url)
        self.assertEqual(interval_start, date(2010, 7, 21))
        self.assertEqual(interval_end, date(2011, 1, 19))

    def test_mmm(self):
        '''This was getting a text file instead of xml.
        
        '''
        _, filing_url, _ = _filing_url_before(ticker='MMM', 
                                              filing_type='10-Q',
                                              date_after=date(2010, 1, 04), 
                                              filing_map=defaultdict(dict))
        self.assertTrue(filing_url.endswith('.xml'))
        
    @mock.patch('requests.models.Response.text', new_callable=mock.PropertyMock)
    def test_ABBV(self, text):
        '''Test page with no 10-Q's, downloaded 2013-3-2, 
        ABBV had just been spun off or something.
        
        '''
        import os
        from financial_fundamentals.test_infrastructure import TEST_DOCS_DIR
        with open(os.path.join(TEST_DOCS_DIR, 'abbv_search_results.html')) as test_html:
            text.return_value = test_html.read()

        ticker = 'ABBV'
        self.assertFalse(list(get_document_urls(symbol=ticker, filing_type='10-Q')))
        filing_type = '10-Q'
        date_after = date(2013, 1, 2)
        
        finds_no_filings = lambda : filing_before(ticker, 
                                                  filing_type, 
                                                  date_after, 
                                                  filing_map=defaultdict(dict))
        self.assertRaises(NoFilingFound, finds_no_filings)
