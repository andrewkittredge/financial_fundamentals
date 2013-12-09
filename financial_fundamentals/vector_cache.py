'''
Created on Dec 3, 2013

@author: akittredge
'''
import pandas as pd


 
class VectorCache(object):
    def __init__(self, data_store):
        self._data_store = data_store

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               self._data_store)

    def get(self, metric, identifiers, index, get_external_data):
        data, misses = self._get_cache_data(metric=metric, 
                                            identifiers=identifiers, 
                                            index=index)
        if misses:
            new_data = get_external_data(misses)
            if not new_data.empty:
                self._data_store.set(metric=metric, data=new_data)
                data = data.merge(new_data, left_index=True, right_index=True)
        return data

    def _get_cache_data(self, metric, identifiers, index):
        data = pd.DataFrame(index=index)
        uncached_data = {}
        for identifier in identifiers:
            df = self._data_store.get(identifier=identifier,
                                             metric=metric,
                                             index=index)
            if df.empty:
                uncached_data[identifier] = index
            else:
                df.rename(columns={metric : identifier}, inplace=True)
                missing_dates = index - df.index
                if len(missing_dates) != 0:
                    uncached_data[identifier] = missing_dates
                data = data.merge(df, left_index=True, right_index=True)
        return data, uncached_data
