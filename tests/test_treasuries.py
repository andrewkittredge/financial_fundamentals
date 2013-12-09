'''
Created on Dec 8, 2013

@author: akittredge
'''
import unittest
from financial_fundamentals import treasuries
from financial_fundamentals.vector_cache import VectorCache
import tests.test_mongo_drivers as mongo_test
from tests.infrastructure import turn_on_request_caching

class TestTreasuryCache(unittest.TestCase):
    def setUp(self):
        turn_on_request_caching()
        self.data_store, self.collection = mongo_test.build_data_store(collection_name='treasuries')
        self.cache = VectorCache(data_store=self.data_store)
        
    def test_successfull_get(self):
        print self.cache
        data = treasuries.get(maturities=['1year'], 
                              start='2012-12-1', 
                              end='2012-12-30',
                              cache=self.cache)
        pass
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()