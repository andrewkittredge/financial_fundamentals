'''
Created on Jan 26, 2013

@author: akittredge
'''
import datetime
from financial_fundamentals.edgar import HTMLEdgarDriver
from financial_fundamentals.xbrl import XBRLMetricParams, DurationContext,\
    InstantContext
from financial_fundamentals.exceptions import NoDataForStockOnDate
                      
class AccountingMetric(object):
    '''Parent class for accounting metrics.'''
    def __init__(self, filing_type, name):
        self.filing_type = filing_type
        self.name = name
        
    @classmethod
    def _build_xbrl_params(cls, possible_tags):
        return XBRLMetricParams(possible_tags=possible_tags,
                                context_type=cls._context)
        
    @classmethod
    def _value_from_filing(cls, filing, possible_tags):
        xbrl_params = cls._build_xbrl_params(possible_tags)
        metric_value = filing.latest_metric_value(xbrl_params)
        return metric_value
    
    @classmethod
    def quarterly(cls):
        return cls(filing_type='10-Q', 
                   name=cls._name_template.format('quarterly'))

    @classmethod
    def annual(cls):
        return cls(filing_type='10-k',
                   name=cls._name_template.format('annual'))

class EPS(AccountingMetric):
    _name_template = '{}_eps'
    _possible_tags=['us-gaap:EarningsPerShareDiluted',
                    'us-gaap:EarningsPerShareBasicAndDiluted']
    _context = DurationContext
    @classmethod
    def value_from_filing(cls, filing):
        return cls._value_from_filing(filing, possible_tags=cls._possible_tags)


class BookValuePerShare(AccountingMetric):
    _name_template = '{}_book_value_per_share'
    _context = InstantContext
    _assets_tags = ['us-gaap:Assets']
    _liabilities_tags = ['us-gaap:Liabilities']
    _shares_outstanding_tags = ['dei:EntityCommonStockSharesOutstanding']
    @classmethod
    def value_from_filing(cls, filing):
        assets = cls._value_from_filing(filing, possible_tags=cls._assets_tags)
        liabilities = cls._value_from_filing(filing, possible_tags=cls._liabilities_tags)
        shares_outstanding = cls._value_from_filing(filing, possible_tags=cls._shares_outstanding_tags)
        try:               
            return (assets - liabilities) / shares_outstanding
        except ZeroDivisionError:
            raise NoDataForStockOnDate('0 shares outstanding.')

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

        metric_value = self._metric.value_from_filing(filing)
        return (filing.first_tradable_date,
                metric_value, 
                filing.last_tradable_date)
