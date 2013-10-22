'''
Created on Sep 8, 2013

@author: akittredge
'''

class NoDataForStock(Exception):
    '''Raise when the metric is not available for the stock.'''
    
class NoDataForStockOnDate(Exception):
    '''Raised when the metric is not available fo the stock on the requested date.'''
    
class ExternalRequestFailed(Exception):
    '''Raised when a call to an external service fails.'''
    