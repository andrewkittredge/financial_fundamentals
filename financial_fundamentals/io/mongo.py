'''
Created on Dec 1, 2013

@author: akittredge

Follows pandas.io.sql
'''
import pandas as pd

def read_frame(qry, columns, collection, index_col=None):
    documents = collection.find(qry, fields=columns)
    result = pd.DataFrame.from_records(documents,
                                       columns=columns)
    if index_col and not result.empty:
        result.set_index(index_col, inplace=True)
    return result

def write_frame(metric, frame, collection):
    docs = []
    index_name = frame.index.name # date?
    assert index_name
    for column in frame:
        doc = ({'identifier' : column,
                index_name : index_value,
                metric : value} for index_value, value in frame[column].iteritems())
        docs.extend(doc)
    collection.insert(docs)