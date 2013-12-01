'''
Created on Nov 29, 2013

@author: akittredge
'''
import pandas as pd
from zipline.data.treasuries import get_treasury_data

class CachedTimeSeries(object):
    @classmethod
    def _get_set(cls, metric, identifiers, start, end, cache):
        data, missing_data = cache.get(metric=metric, 
                                       identifiers=identifiers, 
                                       start=start, 
                                       end=end)
        if missing_data:
            new_data = cls.get_new_data(required_data=missing_data)
            if new_data:
                cache.set(new_data)
                data = data + new_data
        return data
       
class Treasuries(CachedTimeSeries):
    @classmethod
    def get(cls, maturities, start, end, cache):
        return cls._get_set(metric='yield',
                            identifiers=maturities,
                            start=start,
                            end=end,
                            cache=cache)
        
    @classmethod
    def get_new_data(cls, required_data):
        new_data = pd.DataFrame()
        treasury_yield = pd.DataFrame(data=list(get_treasury_data()), 
                                      columns=['date'] + required_data.keys())
        treasury_yield.set_index('date')
        for maturity, index in required_data:
            new_data[maturity] = pd.Series(data=treasury_yield[maturity], index=index)
        return new_data
