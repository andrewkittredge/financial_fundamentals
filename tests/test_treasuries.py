'''
Created on Dec 8, 2013

@author: akittredge
'''
import unittest
from financial_fundamentals import treasuries
import  financial_fundamentals.vector_cache as ff_vc 
import tests.test_mongo_drivers as mongo_test
import mock
import assets.treasury_yields as ff_treasury_yields
import pandas as pd


class TestTreasuryCache(unittest.TestCase):
    def setUp(self):
        store, collection = mongo_test.build_data_store()
        ff_vc.get_data_store = lambda : store
    
    @mock.patch('zipline.data.treasuries.get_treasury_data', 
                return_value=ff_treasury_yields.get_treasury_data())
    def test_successfull_get(self, *args, **kwargs):
        required_data = pd.DataFrame(columns=['1year', '5year'], 
                                     index=pd.date_range('1997-1-3', '1997-1-6')
                                     )
        data = treasuries.get_yields(required_data=required_data)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()