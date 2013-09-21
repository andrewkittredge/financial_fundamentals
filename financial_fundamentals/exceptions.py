'''
Created on Sep 8, 2013

@author: akittredge
'''

class NoDataForStock(Exception):
    '''Raise when the metric is not available for the stock.'''
    
class ExternalRequestFailed(Exception):
    '''Raised when a call to an external service fails.'''