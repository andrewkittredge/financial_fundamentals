'''
Created on Jan 26, 2013

@author: akittredge
'''

from financial_fundamentals.xbrl import XBRLMetricParams, DurationContext,\
    InstantContext
from financial_fundamentals.exceptions import ValueNotInFilingDocument, NoDataForStockForRange
import numpy as np
import vector_cache
                      
                      
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
    _stockholders_equity_tags = ['us-gaap:StockholdersEquity']
    @classmethod
    def value_from_filing(cls, filing):
        try:
            assets = cls._value_from_filing(filing, possible_tags=cls._assets_tags)
            liabilities = cls._value_from_filing(filing, possible_tags=cls._liabilities_tags)
        except ValueNotInFilingDocument:
            book_value = cls._value_from_filing(filing, possible_tags=cls._stockholders_equity_tags)
        else:
            book_value = assets - liabilities
        shares_outstanding = cls._value_from_filing(filing, possible_tags=cls._shares_outstanding_tags)
        try:               
            return book_value / shares_outstanding
        except ZeroDivisionError:
            return np.NaN

import pandas as pd
import financial_fundamentals.edgar as edgar

@vector_cache.vector_cache
def earnings_per_share(required_data):
    start, end = required_data.index[0], required_data.index[-1]
    for symbol, values in required_data.iteritems():
        filings = edgar.get_filings(symbol=symbol, 
                                    filing_type='10-Q')
        filings = filings[filings.bisect(start) - 1:filings.bisect(end)]
        for filing in filings:
            value = EPS.value_from_filing(filing)
            interval_start = filing.first_tradable_date
            interval_end = filing.next_filing and filing.next_filing.first_tradable_date
            values[interval_start:interval_end] = value
    return required_data


if __name__ == '__main__':
    import requests_cache
    requests_cache.install_cache(cache_name='edgar')
    required_data = pd.DataFrame(columns=['GOOG', 'YHOO'], 
                                 index=pd.date_range('2012-1-1', '2012-12-31'))
    eps = earnings_per_share(required_data)
    print eps