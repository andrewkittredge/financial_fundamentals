'''
Created on Oct 8, 2013

@author: akittredge
'''

import dateutil
import xmltodict
import requests
from financial_fundamentals.exceptions import NoDataForStockOnDate

class TimeSpanContext(object):
    '''Encapsulate a time span XBRL context.'''
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        
    def __repr__(self):
        return '{}(start_date={}, end_date={})'.format(self.__class__, 
                                                       self.start_date, 
                                                       self.end_date)
        
    @classmethod
    def from_period(cls, period):
        start_date = dateutil.parser.parse(_find_node(xml_dict=period, 
                                                      key='startDate')
                                           ).date()
        end_date = dateutil.parser.parse(_find_node(xml_dict=period, 
                                                    key='endDate')
                                         ).date()
        return cls(start_date, end_date)

class XBRLDocument(object):
    '''wrapper for XBRL documents, lazily downloads XBRL text.'''
    def __init__(self, xbrl_url):
        self._xbrl_url = xbrl_url
        self._xbrl_dict_ = None

    @property
    def _xbrl_dict(self):
        if not self._xbrl_dict_:
            doc_text = requests.get(self._xbrl_url).text
            xml_dict = xmltodict.parse(doc_text)
            self._xbrl_dict_ = _find_node(xml_dict, 'xbrl')
        return self._xbrl_dict_

    def time_span_contexts_dict(self):
        contexts = {}
        for context in _find_node(xml_dict=self._xbrl_dict, key='context'):
            try:
                period = _find_node(context, 'period')
                _find_node(xml_dict=period, key='startDate')
            except KeyError:
                continue
            else:
                contexts[context['@id']] = TimeSpanContext.from_period(period)
        return contexts

    def latest_metric_value(self, metric):
        context_dates = self.time_span_contexts_dict()
        for tag in metric.xbrl_tags:
            try:
                metric_nodes = self._xbrl_dict[tag]
            except KeyError:
                continue
            else:
                break
        else:
            raise MetricNodeNotFound('Did not find any of {} in the document @ '\
                                     .format(self.xbrl_tags, self._xbrl_url))
        metric_node = sorted(metric_nodes,
                             key=lambda value : context_dates[value['@contextRef']].start_date, 
                             reverse=True)[0]
        return float(metric_node['#text'])
    
class MetricNodeNotFound(NoDataForStockOnDate):
    pass

def _find_node(xml_dict, key):
    '''OMG I hate XML.'''
    try:
        return xml_dict[key]
    except KeyError:
        return xml_dict['xbrli:{}'.format(key)]
