financial_fundamentals
======================

Find XBRL filings on the SEC's edgar and extract accounting metrics.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).
Caching is provided by my vector_cache package, https://github.com/andrewkittredge/vector_cache.



	import pandas as pd
	import financial_fundamentals as ff
	
	date_range = pd.date_range('2012-1-1', '2013-12-31')
	required_data = pd.DataFrame(columns=['MSFT', 'GOOG', 'YHOO', 'IBM'], index=date_range)

	eps = ff.accounting_metrics.earnings_per_share(required_data)
	print eps
	
Follow up:

I (Andrew) am working for [Calcbench](http://calcbench.com) the leading commercial XBRL shop.  I have written an API client for Calcbench that achieves the goals of financial_fundamentals, check it out at https://github.com/calcbench/python_api_client.  

The SEC's XBRL database is a wonderful, huge, source of fundamentals data; but making sense of it and correcting the errors is a massive project.  Calcbench is further towards XBRL mastery than anybody else, if you have legitimate need for the data in XBRL I would encourage you to consider Calcbench before embarking on a parsing adventure of your own.
