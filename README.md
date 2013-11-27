financial_fundamentals
======================

Cache prices from yahoo and accounting metrics from SEC filings.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).


	import pytz
	import datetime
	import financial_fundamentals as ff
	from financial_fundamentals.accounting_metrics import EPS
	from financial_fundamentals.indicies import DOW_TICKERS
	
	
	start = datetime.datetime(2009, 1, 1, tzinfo=pytz.UTC)
	end = datetime.datetime(2013, 11, 26, tzinfo=pytz.UTC)
	eps_cache = ff.sqlite_fundamentals_cache(metric=EPS.quarterly())
	price_cache = ff.sqlite_price_cache()
	args = {'stocks' : ['GOOG', 'YHOO', 'MSFT', 'IBM'], 'start' : start, 'end' : end}
	
	# The first load_from_cache calls will take a long time as data is downloaded from
	# yahoo and edgar.sec.gov, thereafter data will be loaded from cache.
	earnings_per_share = eps_cache.load_from_cache(**args)
	price = price_cache.load_from_cache(**args)
	price_to_earnings = price / (earnings_per_share * 4)
	price_to_earnings.plot()