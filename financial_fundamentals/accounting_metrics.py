'''
Created on Jan 26, 2013

@author: akittredge
'''
import datetime
from financial_fundamentals.edgar import HTMLEdgarDriver
                      
class AccountingMetric(object):
    '''Parent class for accounting metrics.'''

class EPS(AccountingMetric):
    xbrl_tags = ['us-gaap:EarningsPerShareDiluted',
                 'us-gaap:EarningsPerShareBasicAndDiluted']

class QuarterlyEPS(EPS):
    filing_type = '10-Q'
    name = 'quarterly_eps'

class AnnualEPS(EPS):
    filing_type = '10-K'
#===============================================================================
# Book Value per-share is currently broken.
#
# class BookValuePerShare(object):
#     shares_outstanding_element = 'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic'
#     @classmethod
#     def value_from_filing(cls, filing):
#         return cls._book_value(filing) / _value_from_filing(filing, 
#                                                 cls.shares_outstanding_element)
#     @classmethod
#     def _book_value(cls, filing):
#         assets = cls._assets(filing)
#         liabilities = cls._liabilities(filing)
#         return assets - liabilities
#     
#     assets_element = 'Assets'
#     @classmethod
#     def _assets(cls, filing):
#         return _value_from_filing(filing, cls.assets_element)
#     
#     liabilities_element = 'Liabilities'    
#     @classmethod
#     def _liabilities(cls, filing):
#         return _value_from_filing(filing, cls.liabilities_element)
#     
#===============================================================================
    
class AccountingMetricGetter(object):
    '''Connect accounting metrics to sources of accounting metrics.
    
    '''
    def __init__(self, metric, filing_getter=HTMLEdgarDriver):
        self._metric = metric
        self._filing_getter = filing_getter
        self.metric_name  = self._metric.name
        
    def get_data(self, symbol, date):
        '''Return a metric bracketed by first trading day on which it would have been tradable
        and day of the next filing.
        
        '''
        date = datetime.date(date.year, date.month, date.day)
        filing = self._filing_getter.get_filing(ticker=symbol, 
                                                filing_type=self._metric.filing_type, 
                                                date_after=date)
        
        assert filing.date < date
        if filing.next_filing:
            assert date <= filing.next_filing.date
        return (filing.first_tradable_date, 
                filing.latest_metric_value(self._metric), 
                filing.last_tradable_date)