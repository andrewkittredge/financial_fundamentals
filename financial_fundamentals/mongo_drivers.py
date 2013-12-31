'''
Created on Jul 28, 2013

@author: akittredge
'''
import pymongo


import financial_fundamentals.io.mongo as mongo

class MongoCache(object):
    def __init__(self, mongo_collection, metric):
        self._ensure_indexes(mongo_collection)
        self._collection = mongo_collection
        self._metric = metric
       
class MongoDataStore(object):
    def __init__(self, collection):
        self._collection = collection
        
    def __repr__(self):
        return '{}(collection={})'.format(self.__class__.__name__, 
                               self._collection.full_name)
        
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('date', pymongo.ASCENDING), 
                                 ('symbol', pymongo.ASCENDING)])
        collection.ensure_index('symbol')
        
    def get(self, metric, df):
        '''Populate a DataFrame.
        
        '''
        identifiers = list(df.columns)
        start, stop = df.index[0], df.index[-1]
        query = {'identifier' : {'$in' : identifiers},
                 metric : {'$exists' : True},
                 'date' : {'$gte' : start,
                           '$lte' : stop},
                 }
        store_data = mongo.read_frame(qry=query,
                                      columns=['date', metric],
                                      collection=self._collection,
                                      index_col='date')
        df.update(store_data)
        return df

    def set(self, metric, df):
        mongo.write_frame(metric=metric,
                          df=df,
                          collection=self._collection)
        

