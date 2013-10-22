'''
Created on Oct 8, 2013

@author: akittredge
'''
import unittest
import xmltodict
import os
from tests.infrastructure import TEST_DOCS_DIR, turn_on_request_caching
from financial_fundamentals.xbrl import XBRLDocument
import datetime
import mock

class Test(unittest.TestCase):
    def setUp(self):
        turn_on_request_caching()
        test_filing_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
        with open(test_filing_path) as f:
            test_statement_xml_dict = xmltodict.parse(f.read())
            self.xbrl_dict = test_statement_xml_dict['xbrl']
        self.xbrl_doc = XBRLDocument(None)
        self.xbrl_doc._xbrl_dict_ = self.xbrl_dict

    def test_context_dates(self):
        context_id = 'eol_PE2035----1210-Q0013_STD_98_20111231_0'
        context = self.xbrl_doc.time_span_contexts_dict()[context_id]
        self.assertEqual(context.start_date, datetime.date(2011, 9, 25))
        self.assertEqual(context.end_date, datetime.date(2011, 12, 31))

    def test_document_downloading(self):
        doc = XBRLDocument('http://www.sec.gov/Archives/edgar/data/320193/000119312513022339/aapl-20121229.xml')
        self.assertDictEqual(doc._xbrl_dict, self.xbrl_dict)
        
    def test_get_most_recent_metric_value(self):
        metric = mock.Mock()
        metric.xbrl_tags = ['us-gaap:EarningsPerShareDiluted']
        self.assertEqual(self.xbrl_doc.latest_metric_value(metric),
                         13.81)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()