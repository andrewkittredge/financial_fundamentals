financial_fundamentals
======================

Cache prices from yahoo and accounting metrics from SEC filings.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).


	import pytz
	import datetime
	import financial_fundamentals as ff
	from financial_fundamentals.accounting_metrics import QuarterlyEPS
	from financial_fundamentals.indicies import S_P_500_TICKERS
	
	
	start = datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC)
	end = datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC)
	eps_cache = ff.sqlite_fundamentals_cache(metric=QuarterlyEPS)
	price_cache = ff.sqlite_price_cache()
	
	# The first load_from_cache calls will take a long time as data is downloaded from
	# yahoo and edgar.sec.gov, thereafter data will be loaded from cache.
	eps_df = eps_cache.load_from_cache(stocks=S_P_500_TICKERS, start=start, end=end)
	price_df = price_cache.load_from_cache(stocks=S_P_500_TICKERS, start=start, end=end)
	price_to_earnings_df = price_df / eps_df * 4