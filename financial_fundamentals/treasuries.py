'''
Created on Nov 29, 2013

@author: akittredge
'''
import pandas as pd
import zipline.data.treasuries as zipline_treasuries
from financial_fundamentals.vector_cache import vector_cache

@vector_cache(metric='yield')
def get_yields(required_data):
    '''
    required data is an empty? dataframe that will be populated.
    
    
    Returns a dataframe.
    '''
    treasury_data = zipline_treasuries.get_treasury_data()
    treasury_yield = pd.DataFrame.from_records(data=treasury_data)
    treasury_yield.set_index('date')
    required_data.update(treasury_yield)
    return required_data
