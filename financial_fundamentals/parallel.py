'''
Created on Oct 22, 2013

@author: akittredge


Getting accounting metrics slow.  Can we use map/reducde and EC2 for more fast?
'''


from mrjob.job import MRJob

import financial_fundamentals as ff
import datetime


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        symbol, start_date, end_date = line.split(',')
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        getter = ff.accounting_metrics.AccountingMetricGetter(metric=ff.accounting_metrics.QuarterlyEPS, 
                                                              filing_getter=ff.edgar.HTMLEdgarDriver)
        interval_start, value, interval_end = getter.get_data(symbol, date)
        yield line, value


    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
