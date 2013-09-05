from setuptools import setup, find_packages

desc = '''\
financial_fundamentals
=========================
:Author: Andrew Kittredge
:Version: $Revision: 0.0.0
:Copyright: Andrew Kittredge
:License: Apache Version 2


financial_fundamentals caches financial data to speed alogirithm development.  It is developed with the zipline backtesting library in mind. Currently it caches prices downloaded from yahoo and two accounting metrics extracted from XBRL downloaded from the SEC's Edgar system.


'''
setup(name='financial_fundamentals',
      version='1',
      description='Caching for financial metrics.',
      long_description=desc,
      author='Andrew Kittredge',
      author_email='andrewlkittredge@gmail.com',
      license='Apache 2.0',
      packages=find_packages(),
      classifiers=[
	'Development Status :: 4 - Beta',
	'License :: OSI Approved :: Apache Software License',
	'Natural Language :: English',
	'Programming Language :: Python',
	'Programming Language :: Python :: 2.7',
	'Operating Audience :: Science/Research',
	'Intended Audience :: Science/Research',
	'Topic :: Office/Business :: Financial',
     	],	
      install_requires=[
	'numpy',
	'pytz',
	'requests_cache',
	'requests',
	'BeautifulSoup',
	'pymongo',
	'mock',
	'pandas',
	],
      url='https://github.com/andrewkittredge/financial_fundamentals',
)
