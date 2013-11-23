'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
from financial_fundamentals.accounting_metrics import AccountingMetricGetter, EPS,\
    BookValuePerShare
from financial_fundamentals.edgar import HTMLEdgarDriver,\
    FilingNotAvailableForDate, NoFilingsNotAvailable
import datetime
from tests.infrastructure import turn_on_request_caching, TEST_DOCS_DIR
from financial_fundamentals.xbrl import XBRLDocument
import os
from financial_fundamentals.sec_filing import Filing
import mock
from financial_fundamentals.exceptions import ValueNotInFilingDocument,\
    NoDataForStockForRange

class TestAccountingMetricGetter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        turn_on_request_caching()
        
    def test_google(self):
        quarterly_eps = EPS.quarterly()
        getter = AccountingMetricGetter(metric=quarterly_eps, 
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 1, 2)
        interval_start, earnings, interval_end = getter.get_data(symbol='goog', 
                                                                 date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 31))
        self.assertEqual(interval_end, datetime.date(2013, 4, 25))
        self.assertEqual(earnings, 6.53)
        
    def test_google_boundry(self):
        quarterly_eps = EPS.quarterly()
        getter = AccountingMetricGetter(metric=quarterly_eps,
                                        filing_getter=HTMLEdgarDriver)
        date = datetime.date(2013, 4, 25)  #Google filed on this date.
        interval_start, _, _ = getter.get_data(symbol='goog', date=date)
        self.assertEqual(interval_start, datetime.date(2012, 10, 31))

class TestBookValuePerShare(unittest.TestCase):
    def test_appl(self):
        '''value computed from http://www.sec.gov/cgi-bin/viewer?action=view&cik=320193&accession_number=0001193125-13-022339&xbrl_type=v#.
        
        '''
        sec_value = 135.6
        doc_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
        xbrl_document = XBRLDocument.gets_XBRL_locally(file_path=doc_path)
        filing = Filing(filing_date=None, document=xbrl_document, next_filing=None)
        book_value_per_share = BookValuePerShare.value_from_filing(filing)
        self.assertAlmostEqual(book_value_per_share, sec_value, places=1)
        
class TestExceptions(unittest.TestCase):
    def test_metric_not_in_filing(self):
        mock_filing_getter = mock.Mock()
        mock_filing = mock.Mock()
        mock_filing.date = datetime.date(2012, 11, 30)
        mock_filing.next_filing = None
        mock_filing_getter.get_filing.return_value = mock_filing
        mock_metric = mock.Mock()
        mock_metric.value_from_filing.side_effect = ValueNotInFilingDocument()
        getter = AccountingMetricGetter(metric=mock_metric,
                                        filing_getter=mock_filing_getter)
        with self.assertRaises(NoDataForStockForRange):
            getter.get_data(symbol='ABC', date=datetime.datetime(2012, 12, 1))
            
    def test_filing_not_available_until(self):
        mock_filing_getter = mock.Mock()
        end_date = datetime.date(2012, 12, 1)
        mock_filing_getter.get_filing.side_effect = FilingNotAvailableForDate(message='', 
                                                                              end=end_date)
        getter = AccountingMetricGetter(metric=mock.Mock(), filing_getter=mock_filing_getter)
        with self.assertRaises(NoDataForStockForRange) as cm:
            getter.get_data(symbol=None, date=datetime.date(2012, 12, 1))
        self.assertEqual(cm.exception.end, end_date)
        
    def test_no_filings_available(self):
        mock_filing_getter = mock.Mock()
        mock_filing_getter.get_filing.side_effect = NoFilingsNotAvailable()
        getter = AccountingMetricGetter(metric=mock.Mock(), filing_getter=mock_filing_getter)
        with self.assertRaises(NoDataForStockForRange) as cm:
            getter.get_data(symbol=None, date=datetime.date(2012, 12, 1))
        self.assertIsNone(cm.exception.start)
        self.assertIsNone(cm.exception.end)
            

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestExceptions('test_no_filings_available'))
    unittest.TextTestRunner().run(suite)
    
        