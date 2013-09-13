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
from dateutil.relativedelta import relativedelta
from financial_fundamentals.exceptions import NoDataForStock

ticker_search_string = 'http://www.sec.gov/cgi-bin/browse-edgar?company=&match=&CIK={}&filenum=&State=&Country=&SIC=&owner=exclude&Find=Find+Companies&action=getcompany'

logger = logging.getLogger('edgar')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


SEARCH_URL = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={symbol}&type={filing_type}&dateb=&owner=exclude&count=100'
def get_document_urls(symbol, filing_type):
    '''Get the edgar filing_type document pages for the CIK.
    
    '''
    search_url = SEARCH_URL.format(symbol=symbol, filing_type=filing_type)
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
        period_of_report_elem = filing_page.find('div', text='Filing Date')
        try:
            filing_date = period_of_report_elem.findNext('div', {'class' : 'info'}).text
        except AttributeError:
            continue
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
    documents_urls = get_document_urls(symbol=ticker, filing_type=filing_type)
    find_urls_on_search_page(documents_urls, ticker, filing_type, filing_url_map)
        
class XBRLNotAvailable(Exception):
    pass

def _filing_url_before(ticker, filing_type, date_after, filing_map=FILING_URLS):
    '''Return the url for the XBRL bracketed by the dates it was 'in effect.'
    We want the filing that was submitted before the date_after, what you would have been
    trading based on.
    
    '''
    if ticker not in filing_map:
        populate_filing_urls_map(ticker, filing_type, filing_map)
    filing_dates = sorted(key[1] for key in filing_map[ticker])
    if not filing_dates:
        raise XBRLNotAvailable('No {}s found for {}'.format(filing_type, ticker))
    filing_date_index = bisect_left(filing_dates, date_after) - 1
    filing_date_before_date_requested = filing_dates[filing_date_index]
    if filing_date_index < len(filing_dates) - 1:
        filing_date_after_that = filing_dates[filing_date_index + 1]
    else:
        #Needless to say this is hack.  We could get the last non-xbrl filing date
        # from edgar.
        filing_date_after_that = filing_date_before_date_requested + relativedelta(months=3)
        
    return (filing_date_before_date_requested, 
            filing_map[ticker][(filing_type, filing_date_before_date_requested)], 
            filing_date_after_that)


def filing_before(ticker, filing_type, date_after, filing_map=FILING_URLS):
    try:
        interval_start, filing_url, interval_end = _filing_url_before(ticker, 
                                                                      filing_type, 
                                                                      date_after, 
                                                                      filing_map)
    except XBRLNotAvailable:
        raise NoFilingFound('No filing found for ticker {}'.format(ticker))
    filing_text = ET.fromstring(requests.get(filing_url).text)
    return interval_start, filing_text, interval_end

class NoFilingFound(NoDataForStock):
    '''No filings on Edgar for this stock.'''

if __name__ == '__main__':
    import datetime
    print filing_before(ticker='GOOG', filing_type='10-Q', date_after=datetime.date(2012, 12, 1))