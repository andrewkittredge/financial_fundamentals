'''
Created on Oct 22, 2013

@author: akittredge


Getting accounting metrics slow.  Can we use map/reduce and EC2 for more fast?
'''


from mrjob.job import MRJob

import financial_fundamentals as ff
import datetime
import pandas as pd
import StringIO


class MRAccountingMetricGetter(MRJob):

    def mapper(self, _, line):
        symbol, start_date, end_date = line.split(',')
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        cache = ff.mongo_fundamentals_cache(metric=ff.accounting_metrics.EPS.quarterly())
        df = cache.load_from_cache(stocks=[symbol], start=start, end=end)
        yield None, df.to_json()
        

    def reducer(self, _, symbol_dataframes):
        aggregate = pd.DataFrame()
        for symbol_dataframe in symbol_dataframes:
            df = pd.read_json(symbol_dataframe)
            aggregate = aggregate.join(df, how='outer')
        yield _, aggregate.to_json()
        
def run_in_parallel():
    input_file = ('MSFT,2012-1-1,2012-12-31\n'
                  'GOOG,2012-1-1,2012-12-31\n'
                  'AAPL,2012-1-1,2012-12-31\n')
    s = StringIO.StringIO(input_file)
    mr_job = MRAccountingMetricGetter(args=['-rhadoop'])
    with mr_job.make_runner() as runner:
        runner._stdin = s
        runner.run()
        for line in runner.stream_output():
            print line    

if __name__ == '__main__':
    run_in_parallel()