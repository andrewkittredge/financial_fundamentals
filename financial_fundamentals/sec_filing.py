'''
Created on Oct 28, 2013

@author: akittredge
'''

from financial_fundamentals.xbrl import XBRLDocument
import datetime


class Filing(object):
    '''Wrap SEC filings, 10-Ks, 10-Qs.'''
    def __init__(self, filing_date, document, next_filing=None):
        self._document = document
        self.date = filing_date
        self.next_filing = next_filing

    def latest_metric_value(self, metric_params):
        return self._document.latest_metric_value(metric_params)
    
    @property
    def first_tradable_date(self):
        '''the day after this filing was made public.
        Assuming that filings are submitted after the close on the filing date.
        '''
        return self.date + datetime.timedelta(days=1)
        
    @property
    def last_tradable_date(self):
        return self.next_filing and self.next_filing.date

    @classmethod
    def from_xbrl_url(cls, filing_date, xbrl_url):
        '''constructor.'''
        document = XBRLDocument.gets_XBRL_from_edgar (xbrl_url=xbrl_url)
        return cls(filing_date=filing_date, document=document)
    
    def __repr__(self):
        return '{} - {}'.format(self.__class__, self.date)
