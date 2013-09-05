'''
Created on Sep 4, 2013

@author: akittredge

'''

import os

def turn_on_request_caching():
    import requests_cache
    requests_cache.configure(os.path.join(os.path.expanduser('~'), 
                                          '.fundamentals_test_requests'))

import financial_fundamentals
TEST_DOCS_DIR = os.path.join(os.path.split(os.path.dirname(financial_fundamentals.__file__))[0], 
                             'docs', 'test') 