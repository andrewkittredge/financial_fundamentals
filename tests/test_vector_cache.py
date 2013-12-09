'''
Created on Dec 3, 2013

@author: akittredge
'''


from financial_fundamentals.vector_cache import VectorCache
import mock
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_index_equal
from tests.test_mongo_drivers import MongoTestCase

class VectorCacheTestCase(MongoTestCase):
    collection_name = 'vector_cache_test'
    def setUp(self):
        MongoTestCase.setUp(self)
        self.cache = VectorCache(data_store_driver=self.data_store)
    def test_misses(self):
        index = ['a', 'b', 'c']
        mock_data_store = mock.Mock()
        mock_data_store.get.return_value = pd.DataFrame()
        cache = VectorCache(data_store_driver=mock_data_store)
        verification_df = pd.DataFrame({'a' : [1, 2, 3]}, index=index)
        mock_get_external = mock.Mock()
        mock_get_external.return_value = verification_df
        returned_value = cache.get(metric=None, 
                               identifiers=[None], 
                               index=index, 
                               get_external_data=mock_get_external)
        assert_frame_equal(returned_value, verification_df)
        
    def test_get_cache_data(self):
        pass
    
    def test_missing_dates(self):
        metric = 'price'
        test_data = {'d' : [1, 2, 3], 'e' : [4, 5, 6]}
        index = pd.date_range('2012-12-1', '2012-12-3')
        test_df = pd.DataFrame(test_data, index=index)
        test_df.index.name = 'date'
        self.data_store.set(metric=metric, data=test_df)
        longer_index = pd.date_range('2012-11-30', '2012-12-5')
        _, missing_data = self.cache._get_cache_data(metric=metric, 
                                                     identifiers=test_data.keys(), 
                                                     index=longer_index)
        assert_index_equal(missing_data['e'], pd.DatetimeIndex(['2012-11-30',
                                                                '2012-12-4',
                                                                '2012-12-5']))