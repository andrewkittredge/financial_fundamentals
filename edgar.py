'''
Created on Jan 26, 2013

@author: akittredge
'''
import requests
from BeautifulSoup import BeautifulSoup
import re
from datetime import date
import unittest

from xml.etree import cElementTree as ET
import logging
from urlparse import urljoin
from collections import defaultdict
from bisect import bisect_left
ticker_search_string = 'http://www.sec.gov/cgi-bin/browse-edgar?company=&match=&CIK={}&filenum=&State=&Country=&SIC=&owner=exclude&Find=Find+Companies&action=getcompany'

logger = logging.getLogger('edgar')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



def get_CIK(ticker):
    search_url = 'http://www.sec.gov/cgi-bin/browse-edgar?company=&match=&CIK={}&filenum=&State=&Country=&SIC=&owner=exclude&Find=Find+Companies&action=getcompany'.format(ticker)
    search_results_page = BeautifulSoup(requests.get(search_url).text)
    try:
        company_name = search_results_page.find('span', {'class' : 'companyName'}).a.text
    except AttributeError:
        if search_results_page.find('h1', text='No matching Ticker Symbol.'):
            raise CouldNotFindCIK('Edgar cannot match {}'.format(ticker))
        else:
            raise
    else:
        cik = re.search('(\d+)', company_name).group()
        return cik

class CouldNotFindCIK(Exception):
    pass

SEARCH_URL = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={filing_type}&dateb=&owner=exclude&count=100'
def get_document_urls(cik, filing_type):
    '''Get the edgar filing_type document pages for the CIK.
    
    '''
    search_url = SEARCH_URL.format(cik=cik, filing_type=filing_type)
    search_page = requests.get(search_url).text
    search_results_page = BeautifulSoup(search_page)
    xbrl_rows = [row for row in 
                 search_results_page.findAll('tr') if 
                 row.find(text=re.compile('Interactive Data'))]
    for xbrl_row in xbrl_rows:
        documents_page = xbrl_row.find('a', {'id' : 'documentsbutton'})['href']
        documents_url = 'http://sec.gov' + documents_page
        yield documents_url
    

def find_urls_on_search_page(documents_urls, ticker, filing_type, filing_url_map):
    for documents_url in documents_urls:
        filing_page = BeautifulSoup(requests.get(documents_url).text)
        period_of_report_elem = filing_page.find('div', text='Period of Report')
        filing_date = period_of_report_elem.findNext('div', {'class' : 'info'}).text
        filing_date = date(*map(int, filing_date.split('-')))
        type_tds = filing_page.findAll('td', text='EX-101.INS')
        for type_td in type_tds:
            try:
                xbrl_link = type_td.findPrevious('a', text=re.compile('\.xml$')).parent['href']
            except AttributeError:
                continue
            else:
                break
        filing_xbrl_url = urljoin('http://www.sec.gov', xbrl_link)
        filing_url_map[ticker][(filing_type, filing_date)] = filing_xbrl_url

class NoFilingsFound(Exception):
    pass

FILING_URLS = defaultdict(dict)
def populate_filing_urls_map(ticker, filing_type, filing_url_map=FILING_URLS):
    cik = get_CIK(ticker)
    documents_urls = get_document_urls(cik, filing_type)
    find_urls_on_search_page(documents_urls, ticker, filing_type, filing_url_map)
        
class XBRLNotAvailable(Exception):
    pass


def _filing_url_before(ticker, filing_type, date_after, filing_map=FILING_URLS):
    if ticker not in filing_map:
        populate_filing_urls_map(ticker, filing_type, filing_map)
    filing_dates = sorted(key[1] for key in filing_map[ticker])
    if not filing_dates:
        raise XBRLNotAvailable('No {}s found for {}'.format(filing_type, ticker))
    last_filing_before = filing_dates[bisect_left(filing_dates, date_after) - 1]
    return filing_map[ticker][(filing_type, last_filing_before)]

FILINGS_CACHE = {}
def filing_before(ticker, filing_type, date_after, filing_map=FILING_URLS):
    try:
        filing_url = _filing_url_before(ticker, filing_type, date_after, filing_map)
    except XBRLNotAvailable:
        raise NoFilingFound('No filing found for ticker {}'.format(ticker))
    return FILINGS_CACHE.setdefault(filing_url, 
                                    ET.fromstring(requests.get(filing_url).text))

class NoFilingFound(Exception):
    pass

import mock
class TestsEdgar(unittest.TestCase):
    def setUp(self):
        import requests_cache
        requests_cache.configure('fundamentals_cache_test')
    
    def test_get_CIK(self):
        apple_CIK = get_CIK('aapl')
        self.assertEqual(apple_CIK, '0000320193')
        
    def test_populate_filing_url_map(self):
        test_map = defaultdict(dict)
        ticker = 'aapl'
        filing_type = '10-Q'
        populate_filing_urls_map(ticker=ticker, 
                                 filing_type=filing_type, 
                                 filing_url_map=test_map)
        self.assertEqual(test_map[ticker][(filing_type, date(2012, 12, 29))], 
                         'http://www.sec.gov/Archives/edgar/data/320193/000119312513022339/aapl-20121229.xml')
    
    def test_last_filing_before(self):
        test_map = defaultdict(dict)
        ticker = 'aapl'
        filing_type = '10-Q'
        populate_filing_urls_map(ticker,
                                 filing_type=filing_type,
                                 filing_url_map=test_map)
        date_after = date(2010, 7, 1)
        june_2010_filing_url = 'http://www.sec.gov/Archives/edgar/data/320193/000119312510162840/aapl-20100626.xml'
        filing = _filing_url_before(ticker, filing_type, date_after, filing_map=test_map)
        self.assertEqual(filing, june_2010_filing_url)

    def test_mmm(self):
        '''This was getting a text file instead of xml.
        
        '''
        filing_url = _filing_url_before(ticker='MMM', filing_type='10-Q',
                          date_after=date(2010, 1, 04), filing_map=defaultdict(dict))
        self.assertTrue(filing_url.endswith('.xml'))
        
    @mock.patch('requests.models.Response.text', new_callable=mock.PropertyMock)
    def test_ABBV(self, text):
        '''Test page with no 10-Q's, downloaded 2013-3-2, 
        ABBV had just been spun off or something.
        
        '''
        with open('test_docs/abbv_search_results.html') as test_html:
            text.return_value = test_html.read()

        abbv_CIK = '0001551152'
        self.assertFalse(list(get_document_urls(cik=abbv_CIK, filing_type='10-Q')))
        ticker = 'ABBV'
        filing_type = '10-Q'
        date_after = date(2013, 1, 2)
        
        finds_no_filings = lambda : filing_before(ticker, 
                                                  filing_type, 
                                                  date_after, 
                                                  filing_map=defaultdict(dict))
        self.assertRaises(NoFilingFound, finds_no_filings)

    def test_PDCO_CIK(self):
        '''This failed.
        
        '''
        ticker = 'PDCO'
        cik = get_CIK(ticker)
        self.assertEqual(cik, '0000891024')
        
    def test_no_matching_ticker(self):
        '''SEC doesn't know about PPL for some reason.
        
        '''
        throws_missing_ticker = lambda : get_CIK('PPL')
        self.assertRaises(CouldNotFindCIK, throws_missing_ticker)
        
if __name__ == '__main__':
    print list(get_document_urls(cik='0001551152', filing_type='10-Q'))