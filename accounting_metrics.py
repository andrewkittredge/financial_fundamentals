'''
Created on Jan 26, 2013

@author: akittredge
'''
import unittest
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
    def get_data(cls, symbol, data):
        return _get_data(cls, symbol, data)
    
    
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
    
class TestsXBRL(unittest.TestCase):
    test_filing_path = 'test_docs/aapl-20121229.xml'
    asset_test_value = 196088000000.
    liabilities_test_value = 68742000000.
    book_value_per_share_test_value = 135.6 # from http://www.gurufocus.com/term/Book%20Value%20Per%20Share/AAPL/Book%252BValue%252Bper%252BShare/Apple%2BInc
    def setUp(self):
        from xml.etree import cElementTree as ET
        with open(self.test_filing_path) as f:
            self.test_statement = ET.fromstring(f.read())
            
    def test_eps(self):
        quarterly_eps = QuarterlyEPS.value_from_filing(self.test_statement)
        self.assertEqual(quarterly_eps, 13.81)
        
    def test_asssets(self):
        self.assertEqual(BookValuePerShare._assets(self.test_statement), 
                         self.asset_test_value)
        
    def test_liabilities(self):
        self.assertEqual(BookValuePerShare._liabilities(self.test_statement), 
                         self.liabilities_test_value)
        
    def test_book_value(self):
        self.assertEqual(BookValuePerShare._book_value(self.test_statement),
                         self.asset_test_value - self.liabilities_test_value)
        
    def test_book_value_per_share(self):
        self.assertAlmostEqual(BookValuePerShare.value_from_filing(self.test_statement),
                               self.book_value_per_share_test_value,
                               delta=1.)