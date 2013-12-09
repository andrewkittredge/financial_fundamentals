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
        return '{}({})'.format(self.__class__.__name__, 
                               self._collection.full_name)
        
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('date', pymongo.ASCENDING), 
                                 ('symbol', pymongo.ASCENDING)])
        collection.ensure_index('symbol')
        
    def get(self, identifier, metric, index):
        query = {'identifier' : identifier,
                 metric : {'$exists' : True},
                 'date' : {'$gte' : index[0],
                           '$lte' : index[-1]},
                 }
        df = mongo.read_frame(qry=query,
                              columns=['date', metric],
                              collection=self._collection,
                              index_col='date')
        df.rename(columns={metric : identifier}, inplace=True)
        return df

    def set(self, metric, data):
        mongo.write_frame(metric=metric, 
                          frame=data, 
                          collection=self._collection)

