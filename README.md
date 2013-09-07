financial_fundamentals
======================

Caching for accounting metrics from XBRL downloaded from the SEC's Edgar.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).


	import pytz
	import datetime
	from financial_fundamentals import sqlite_fundamentals_cache as fundamentals_cache, sqlite_price_cache as price_cache
	from financial_fundamentals.accounting_metrics import QuarterlyEPS
	from financial_fundamentals.indicies import CLEANED_S_P_500_TICKERS
	start, end = datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC), datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC)
	eps_dataframe = fundamentals_cache(metric=QuarterlyEPS).load_from_cache(stocks=CLEANED_S_P_500_TICKERS, start=start, end=end)
	price_dataframe = price_cache().load_from_cache(stocks=CLEANED_S_P_500_TICKERS, start=start, end=end)
	price_to_earnings_dataframe = price_dataframe / eps_dataframe * 4