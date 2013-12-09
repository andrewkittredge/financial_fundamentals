'''
Created on Nov 29, 2013

@author: akittredge
'''
import pandas as pd
import zipline.data.treasuries as zipline_treasuries

def get(maturities, start, end, cache):
    index = pd.date_range(start, end)
    return cache.get(metric='yield', 
                     identifiers=maturities, 
                     index=index,
                     get_external_data=_get_treasury_data)

def _get_treasury_data(required_data):
        new_data = pd.DataFrame()
        treasury_data = zipline_treasuries.get_treasury_data()
        columns = ['date'] + required_data.keys()
        treasury_yield = pd.DataFrame.from_records(data=treasury_data, 
                                                   columns=columns)
        treasury_yield.set_index('date')
        for maturity, index in required_data.iteritems():
            new_data.merge(treasury_yield)
            new_data[maturity] = pd.Series(data=treasury_yield[maturity],
                                           index=index)
        return new_data
