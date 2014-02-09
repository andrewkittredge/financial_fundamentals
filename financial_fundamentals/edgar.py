'''
Created on Jan 26, 2013

@author: akittredge
'''
import requests
from BeautifulSoup import BeautifulSoup
import datetime
from urlparse import urljoin
import blist

import time
from requests.exceptions import ConnectionError
from financial_fundamentals.sec_filing import Filing
import re


def get_filings(symbol, filing_type):
    '''Get the last xbrl filed before date.
        Returns a Filing object, return None if there are no XBRL documents
        prior to the date.

        Step 1 Search for the ticker and filing type,
        generate the urls for the document pages that have interactive data/XBRL.
       Step 2 : Get the document pages, on each page find the url for the XBRL document.
        Return a blist sorted by filing date.
    '''

    filings = blist.sortedlist(key=_filing_sort_key_func)
    document_page_urls = _get_document_page_urls(symbol, filing_type)
    for url in document_page_urls:
        filing = _get_filing_from_document_page(url)
        filings.add(filing)
    for i in range(len(filings) - 1):
        filings[i].next_filing = filings[i + 1]
    return filings

SEARCH_URL = ('http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&'
              'CIK={symbol}&type={filing_type}&dateb=&owner=exclude&count=100')
def _get_document_page_urls(symbol, filing_type):
    '''Get the edgar filing document pages for the CIK.
    
    '''
    search_url = SEARCH_URL.format(symbol=symbol, filing_type=filing_type)
    search_results_page = get_edgar_soup(url=search_url)
    xbrl_rows = [row for row in 
                 search_results_page.findAll('tr') if 
                 row.find(text=re.compile('Interactive Data'))]
    for xbrl_row in xbrl_rows:
        documents_page = xbrl_row.find('a', {'id' : 'documentsbutton'})['href']
        documents_url = 'http://sec.gov' + documents_page
        yield documents_url

def _get_filing_from_document_page(document_page_url):
    '''Find the XBRL link on a page like 
    http://www.sec.gov/Archives/edgar/data/320193/000119312513300670/0001193125-13-300670-index.htm
    
    '''
    filing_page = get_edgar_soup(url=document_page_url)
    period_of_report_elem = filing_page.find('div', text='Filing Date')
    filing_date = period_of_report_elem.findNext('div', {'class' : 'info'}).text
    filing_date = datetime.date(*map(int, filing_date.split('-')))
    type_tds = filing_page.findAll('td', text='EX-101.INS')
    for type_td in type_tds:
        try:
            xbrl_link = type_td.findPrevious('a', text=re.compile('\.xml$')).parent['href']
        except AttributeError:
            continue
        else:
            if not re.match(pattern='\d\.xml$', string=xbrl_link):
                # we don't want files of the form 'jcp-20120504_def.xml'
                continue
            else:
                break
    xbrl_url = urljoin('http://www.sec.gov', xbrl_link)
    filing = Filing.from_xbrl_url(filing_date=filing_date, xbrl_url=xbrl_url)
    return filing

def _filing_sort_key_func(filing_or_date):
    if isinstance(filing_or_date, Filing):
        return filing_or_date.date
    elif isinstance(filing_or_date, datetime.datetime):
        return filing_or_date.date()
    else:
        return filing_or_date
    
def get_edgar_soup(url):
    response = get(url)
    return BeautifulSoup(response)

def get(url):
    '''requests.get wrapped in a backoff retry.
    
    '''
    wait = 0
    while wait < 5:
        try:
            return requests.get(url).text
        except ConnectionError:
            print 'ConnectionError, trying again in ', wait
            time.sleep(wait)
            wait += 1
    else:
        raise
