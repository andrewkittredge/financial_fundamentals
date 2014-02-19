'''
Created on Oct 8, 2013

@author: akittredge
'''

import dateutil.parser
import xmltodict
from financial_fundamentals.exceptions import ValueNotInFilingDocument

class XBRLMetricParams(object):
    '''Bundle the parameters sufficient to extract a metric from an xbrl document.
    
    '''
    def __init__(self, possible_tags, context_type):
        self.possible_tags = possible_tags
        self.context_type = context_type

        
class DurationContext(object):
    '''Encapsulate a time span XBRL context.'''
    characteristic_key = 'startDate'
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    @property
    def sort_key(self):
        return self.start_date
    
    def __repr__(self):
        return '{}(start_date={}, end_date={})'.format(self.__class__, 
                                                       self.start_date, 
                                                       self.end_date)

    @classmethod
    def from_period(cls, period):
        start_node = XBRLDocument.find_node(xml_dict=period, key='startDate')
        start_date = dateutil.parser.parse(start_node).date()
        end_node = XBRLDocument.find_node(xml_dict=period, key='endDate')
        end_date = dateutil.parser.parse(end_node).date()
        return cls(start_date, end_date)

class InstantContext(object):
    characteristic_key = 'instant'
    def __init__(self, instant):
        self.instant = instant

    @property
    def sort_key(self):
        return self.instant
        
    def __repr__(self):
        return '{}(instant={}'.format(self.__class__, self.instant)
    
    @classmethod
    def from_period(cls, period):
        node = XBRLDocument.find_node(xml_dict=period, key='instant')
        instant = dateutil.parser.parse(node).date()
        return cls(instant=instant)

class XBRLDocument(object):
    '''wrapper for XBRL documents, lazily downloads XBRL text.'''
    def __init__(self, xbrl_url, gets_xbrl):
        self._xbrl_url = xbrl_url
        self._xbrl_dict_ = None
        self._contexts = {}
        self._get_xbrl = gets_xbrl
        
    @property
    def _xbrl_dict(self):
        if not self._xbrl_dict_:
            doc_text = self._get_xbrl(self._xbrl_url)
            xml_dict = xmltodict.parse(doc_text)
            self._xbrl_dict_ = self.find_node(xml_dict, 'xbrl')
        return self._xbrl_dict_

    def contexts(self, context_type):
        contexts = self._contexts.get(context_type, {})
        if not contexts:
            context_nodes = self.find_node(xml_dict=self._xbrl_dict, key='context')
            for context in context_nodes:
                try:
                    period = self.find_node(xml_dict=context, key='period')
                    self.find_node(xml_dict=period, key=context_type.characteristic_key)
                except KeyError:
                    continue
                else:
                    contexts[context['@id']] = context_type.from_period(period)
            self._contexts[context_type] = contexts
        return contexts

    def _latest_metric_value(self, possible_tags, contexts):
        '''metric_params is a list of possible xbrl tags.
        
        '''
        for tag in possible_tags:
            try:
                metric_nodes = self._xbrl_dict[tag]
            except KeyError:
                continue
            else:
                if type(metric_nodes) != list:
                    metric_nodes = [metric_nodes]
                break
        else:
            raise MetricNodeNotFound('Did not find any of {} in the document @ {}'\
                                     .format(possible_tags, self._xbrl_url))
        def key_func(value):
            context_ref_id = value['@contextRef']
            context = contexts[context_ref_id]
            return context.sort_key

        metric_node = sorted(metric_nodes,
                             key=key_func, 
                             reverse=True)[0]
        return float(metric_node['#text'])
    
    def latest_metric_value(self, metric_params):
        contexts = self.contexts(context_type=metric_params.context_type)
        return self._latest_metric_value(possible_tags=metric_params.possible_tags,
                                         contexts=contexts)

    @staticmethod
    def find_node(xml_dict, key):
        '''OMG I hate XML.'''
        try:
            return xml_dict[key]
        except KeyError:
            return xml_dict['xbrli:{}'.format(key)]
        
    @classmethod
    def gets_XBRL_from_edgar(cls, xbrl_url):
        from financial_fundamentals import edgar
        return cls(xbrl_url=xbrl_url, gets_xbrl=edgar.get)
    
    @classmethod
    def gets_XBRL_locally(cls, file_path):
        return cls(xbrl_url=file_path, 
                   gets_xbrl=lambda file_path : open(file_path).read())

class MetricNodeNotFound(ValueNotInFilingDocument):
    pass