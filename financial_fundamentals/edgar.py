'''
Created on Jan 26, 2013

@author: akittredge
'''
import requests
from BeautifulSoup import BeautifulSoup
import re
from datetime import date
from urlparse import urljoin
import blist


SEARCH_URL = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={symbol}&type={filing_type}&dateb=&owner=exclude&count=100'        
class XBRLNotAvailable(Exception):
    pass

class Filing(object):
    '''wrap filings.'''
    def __init__(self, filing_date, xbrl_url=None):
        self.xbrl_url = xbrl_url
        self.filing_date = filing_date

    def __cmp__(self, other):
        return (self.filing_date - other.filing_date).days



class EdgarHTMLDriver(object):
    '''Get documents from Edgar by parsing the HTML.'''
    _ticker_filings = {}
    @classmethod
    def get_filing(cls, ticker, filing_type, date_after):
        '''Get the last xbrl filed before date.
            Returns a Filing object, return None if there are no XBRL documents
            prior to the date.
        '''
        filings = cls._ticker_filings.setdefault(ticker,
                                                 cls._get_filing_urls(ticker, 
                                                                      filing_type)
                                                 )
        if filings:
            search_filing = Filing(filing_date=date_after) # hack filing for bisecting
            filing_before_index = filings.bisect_right(search_filing) - 1
            if filing_before_index == -1:
                raise XBRLNotAvailable('date is before the first XBRL filing in Edgar.')
            return filings[filing_before_index]
        else:
            raise XBRLNotAvailable('No XBRL filings found.')

    @classmethod
    def _get_filing_urls(cls, ticker, filing_type):
        '''Step 1 Search for the ticker and filing type,
            generate the urls for the document pages that have interactive data/XBRL.
        Step 2 : Get the document pages, on each page find the url for the XBRL document.
            Return a blist sorted by filing date.
        '''
        filings = blist.sortedlist()
        document_page_urls = cls._get_document_page_urls(ticker, filing_type)
        for url in document_page_urls:
            filing = cls._get_filing_from_document_page(url)
            filings.add(filing)
        return filings

    @classmethod
    def _get_document_page_urls(cls, symbol, filing_type):
        '''Get the edgar filing document pages for the CIK.
        
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

    @classmethod
    def _get_filing_from_document_page(cls, document_page_url):
        '''Find the XBRL link on a page like 
        http://www.sec.gov/Archives/edgar/data/320193/000119312513300670/0001193125-13-300670-index.htm
        
        '''
        filing_page = BeautifulSoup(requests.get(document_page_url).text)
        period_of_report_elem = filing_page.find('div', text='Filing Date')
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
        xbrl_url = urljoin('http://www.sec.gov', xbrl_link)
        filing = Filing(filing_date=filing_date, xbrl_url=xbrl_url)
        return filing 

if __name__ == '__main__':
    import datetime
    print EdgarHTMLDriver.get_filing(ticker='GOOG', filing_type='10-Q', date_after=datetime.date(1960, 9, 29)).xbrl_url