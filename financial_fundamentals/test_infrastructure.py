'''
Created on Sep 4, 2013

@author: akittredge

'''

def turn_on_request_caching():
    import requests_cache
    import os
    requests_cache.configure(os.path.join(os.path.expanduser('~'), 
                                          'fundamentals_test_requests'))