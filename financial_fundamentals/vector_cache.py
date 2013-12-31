'''
Created on Dec 3, 2013

@author: akittredge
'''

from functools import wraps
import pandas as pd


def get_data_store():
    pass

def vector_cache(metric):
    def decorator(f):
        @wraps(f)
        def wrapper(required_data):
            data_store = get_data_store()
            cached_data = data_store.get(metric=metric,
                                         df=required_data)
            missing_data = cached_data.isnull()
            if missing_data.any().any():
                required_data = pd.DataFrame(index=missing_data.index,
                                             columns=missing_data.columns)
                new_data = f(required_data=required_data)
                if new_data.any().any():
                    data_store.set(metric=metric, df=new_data)
                    cached_data.update(new_data)
            return cached_data
        return wrapper
    return decorator
