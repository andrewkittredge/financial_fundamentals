'''
Created on Sep 12, 2013

@author: akittredge
'''

import os
import unittest
from financial_fundamentals.accounting_metrics import QuarterlyEPS,\
    BookValuePerShare
from tests.infrastructure import TEST_DOCS_DIR

class TestsXBRL(unittest.TestCase):
    test_filing_path = os.path.join(TEST_DOCS_DIR, 'aapl-20121229.xml')
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