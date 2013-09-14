'''
Created on Jan 26, 2013

@author: akittredge
'''

from financial_fundamentals.edgar import filing_before
import datetime



gaap_namespaces = ('http://fasb.org/us-gaap/2011-01-31',
                   'http://xbrl.us/us-gaap/2009-01-31',
                   'http://fasb.org/us-gaap/2012-01-31')

def _value_from_filing(filing, element_of_interest):
    for gaap_namespace in gaap_namespaces:
        element_value = filing.findtext('{{{}}}{}'.format(gaap_namespace,
                                                element_of_interest))
        if element_value:
            return float(element_value)


class EPS(object):
    element_of_interest = 'EarningsPerShareDiluted'
    @classmethod
    def value_from_filing(cls, filing):
        return _value_from_filing(filing, cls.element_of_interest)
    
    @classmethod
    def get_data(cls, symbol, date):
        return _get_data(cls, symbol, date)
    
    
def _get_data(metric, symbol, date):
    interval_start, filing_text, interval_end = filing_before(ticker=symbol,
                                                              filing_type=metric.filing_type,
                                                              date_after=date.date())
    interval_start = datetime.datetime(interval_start.year, 
                                       interval_start.month, 
                                       interval_start.day)
    interval_end = datetime.datetime(interval_end.year, 
                                     interval_end.month, 
                                     interval_end.day)
    return interval_start, metric.value_from_filing(filing_text), interval_end


class QuarterlyEPS(EPS):
    filing_type = '10-Q'
    metric_name = 'quarterly_eps'

class AnnualEPS(EPS):
    filing_type = '10-K'

class BookValuePerShare(object):
    shares_outstanding_element = 'WeightedAverageNumberOfSharesOutstandingBasic'
    @classmethod
    def value_from_filing(cls, filing):
        return cls._book_value(filing) / _value_from_filing(filing, 
                                                cls.shares_outstanding_element)
    @classmethod
    def _book_value(cls, filing):
        assets = cls._assets(filing)
        liabilities = cls._liabilities(filing)
        return assets - liabilities
    
    assets_element = 'Assets'
    @classmethod
    def _assets(cls, filing):
        return _value_from_filing(filing, cls.assets_element)
    
    liabilities_element = 'Liabilities'    
    @classmethod
    def _liabilities(cls, filing):
        return _value_from_filing(filing, cls.liabilities_element)
    