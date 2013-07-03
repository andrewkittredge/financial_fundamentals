financial_fundamentals
======================

Caching for accounting metrics from XBRL downloaded from the SEC's Edgar.
See the blog @ [http://andrewonfinance.blogspot.com/](http://andrewonfinance.blogspot.com/).



    from datetime import date
    from financial_fundamentals import fundamentals, accounting_metrics
    fundamentals_cache = fundamentals.SQLLiteMultiplesCache()
    fundamentals_cache.create_database()
    print fundamentals_cache.get(ticker='GOOG', date_=date(2012, 12, 31), metric=accounting_metrics.QuarterlyEPS)
