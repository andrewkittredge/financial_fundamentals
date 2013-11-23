'''
Created on Sep 8, 2013

@author: akittredge
'''

class NoDataForStock(Exception):
    '''Raise when the metric is not available for the stock.'''
    
class NoDataForStockOnDate(Exception):
    '''Raised when the metric is not available for the stock on the requested date.'''
    
class NoDataForStockForRange(Exception):
    '''Raised when the metric is not available for the stock between two dates.'''
    def __init__(self, start=None, end=None):
        super(NoDataForStockForRange, self).__init__()
        self.start, self.end = start, end
    
class ExternalRequestFailed(Exception):
    '''Raised when a call to an external service fails.'''

class ValueNotInFilingDocument(Exception):
    '''Raised when unable to extract an accounting metric from a filing document.'''
    