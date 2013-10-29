'''
Created on Oct 28, 2013

@author: akittredge
'''
import datetime
from financial_fundamentals.xbrl import XBRLDocument


class Filing(object):
    '''Wrap SEC filings, 10-Ks, 10-Qs.'''
    def __init__(self, filing_date, document, next_filing=None):
        self._document = document
        self.date = filing_date
        self.next_filing = next_filing

    @classmethod
    def key_func(cls, filing_or_date):
        #assert False
        if isinstance(filing_or_date, cls):
            return filing_or_date.date
        elif isinstance(filing_or_date, datetime.datetime):
            return filing_or_date.date()
        else:
            return filing_or_date
        
    def latest_metric_value(self, metric):
        return self._document.latest_metric_value(metric)

    @classmethod
    def from_xbrl_url(cls, filing_date, xbrl_url):
        document = XBRLDocument(xbrl_url=xbrl_url)
        return cls(filing_date=filing_date, document=document)
    
    def __repr__(self):
        return '{} - {}'.format(self.__class__, self.date)
