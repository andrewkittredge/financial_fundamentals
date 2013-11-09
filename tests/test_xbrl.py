'''
Created on Oct 8, 2013

@author: akittredge
'''
import unittest
import xmltodict
import os
from tests.infrastructure import TEST_DOCS_DIR, turn_on_request_caching
from financial_fundamentals.xbrl import XBRLDocument, InstantContext,\
    DurationContext, XBRLMetricParams
import datetime


class Test(unittest.TestCase):
    def setUp(self):
        test_filing_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
        self.xbrl_doc = XBRLDocument.gets_XBRL_locally(file_path=test_filing_path)

    def test_duration_context(self):
        context_id = 'eol_PE2035----1210-Q0013_STD_98_20111231_0'
        context = self.xbrl_doc.contexts(context_type=DurationContext)[context_id]
        self.assertEqual(context.start_date, datetime.date(2011, 9, 25))
        self.assertEqual(context.end_date, datetime.date(2011, 12, 31))
        
    def test_instant_context(self):
        context_id = 'eol_PE2035----1210-Q0013_STD_0_20120929_0_510142x511009_530627x510743'
        context = self.xbrl_doc.contexts(context_type=InstantContext)[context_id]
        self.assertEqual(context.instant, datetime.date(2012, 9, 29))

    def test_document_downloading(self):
        turn_on_request_caching()
        url = 'http://www.sec.gov/Archives/edgar/data/320193/000119312513022339/aapl-20121229.xml'
        test_filing_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
        with open(test_filing_path) as f:
            test_statement_xml_dict = xmltodict.parse(f.read())
            xbrl_dict = test_statement_xml_dict['xbrl']
        doc = XBRLDocument.gets_XBRL_from_edgar(xbrl_url=url)
        self.assertDictEqual(doc._xbrl_dict, xbrl_dict)
        
    def test_get_most_recent_metric_value(self):
        metric_params = XBRLMetricParams(possible_tags=['us-gaap:EarningsPerShareDiluted'], 
                                         context_type=DurationContext)
        self.assertEqual(self.xbrl_doc.latest_metric_value(metric_params=metric_params),
                         13.81)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()