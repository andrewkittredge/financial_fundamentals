financial_fundamentals
======================

Caching for accounting metrics from XBRL downloaded from the SEC's Edgar.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).


	import pytz
	import datetime
	from financial_fundamentals import sqlite_fundamentals_cache as fundamentals_cache, 
	from financial_fundamenalts import sqlite_price_cache as price_cache
	from financial_fundamentals.accounting_metrics import QuarterlyEPS
	from financial_fundamentals.indicies import CLEANED_S_P_500_TICKERS
	
	
	start = datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC)
	end = datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC)
	eps_cache = fundamentals_cache(metric=QuarterlyEPS)
	
	# The first load_from_cache calls will take a long time as data is downloaded from
	# yahoo and edgar.sec.gov, thereafter data will be loaded from cache.
	eps_df = eps_cache.load_from_cache(stocks=CLEANED_S_P_500_TICKERS, start=start, end=end)
	price_df = price_cache().load_from_cache(stocks=CLEANED_S_P_500_TICKERS, start=start, end=end)
	price_to_earnings_df = price_dataframe / eps_dataframe * 4