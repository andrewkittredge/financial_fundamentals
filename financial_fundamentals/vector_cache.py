'''
Created on Dec 3, 2013

@author: akittredge
'''

from functools import wraps



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
            if missing_data:
                new_data = f(missing_data)
                data_store.set(new_data)
                cached_data.update(new_data)
            return cached_data
        return wrapper
    return decorator


