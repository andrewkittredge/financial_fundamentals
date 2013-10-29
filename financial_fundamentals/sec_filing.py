'''
Created on Oct 28, 2013

@author: akittredge
'''

from financial_fundamentals.xbrl import XBRLDocument


class Filing(object):
    '''Wrap SEC filings, 10-Ks, 10-Qs.'''
    def __init__(self, filing_date, document, next_filing=None):
        self._document = document
        self.date = filing_date
        self.next_filing = next_filing

    def latest_metric_value(self, metric):
        return self._document.latest_metric_value(metric)

    @classmethod
    def from_xbrl_url(cls, filing_date, xbrl_url):
        '''constructor.'''
        document = XBRLDocument(xbrl_url=xbrl_url)
        return cls(filing_date=filing_date, document=document)
    
    def __repr__(self):
        return '{} - {}'.format(self.__class__, self.date)
