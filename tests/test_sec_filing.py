'''
Created on Nov 1, 2013

@author: akittredge
'''
import unittest
from financial_fundamentals import sec_filing
import datetime


class TestSecFiling(unittest.TestCase):
    def test_dates(self):
        july_filing = sec_filing.Filing(filing_date=datetime.date(2012, 7, 15), 
                                        document=None)
        june_filing  = sec_filing.Filing(filing_date=datetime.date(2012, 6, 15), 
                                         document=None,
                                         next_filing=july_filing)
        self.assertEqual(june_filing.first_tradable_date, 
                         datetime.date(2012, 6, 16))
        self.assertEqual(june_filing.last_tradable_date,
                         july_filing.date)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()