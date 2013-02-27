'''
Created on Jan 26, 2013

@author: akittredge
'''
import unittest


gaap_namespaces = ('http://fasb.org/us-gaap/2011-01-31',
                   'http://xbrl.us/us-gaap/2009-01-31',
                   'http://fasb.org/us-gaap/2012-01-31')

    
class EPS(object):
    @staticmethod
    def value(filing):
        for gaap_namespace in gaap_namespaces:
            eps = filing.findtext('{{{}}}EarningsPerShareDiluted'.format(gaap_namespace))
            if eps:
                return float(eps)
    
class QuarterlyEPS(EPS):
    filing_type = '10-Q'
    
class AnnualEPS(EPS):
    filing_type = '10-K'
        

class TestsXBRL(unittest.TestCase):
    def setUp(self):
        from xml.etree import cElementTree as ET
        with open('test_docs/aapl-20121229.xml') as f:
            self.appl_statement = ET.fromstring(f.read())
            
    def test_eps(self):
        self.assertEqual(eps(self.appl_statement), 13.81)