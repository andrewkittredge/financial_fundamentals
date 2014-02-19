from setuptools import setup, find_packages

version = '0.2.3'
desc = '''\
FinancialFundamentals
=========================
:Author: Andrew Kittredge
:Version: $Revision: {}
:Copyright: Andrew Kittredge
:License: Apache Version 2


FinancialFundamentals caches financial data to speed alogirithm development.  It is developed with the zipline backtesting library in mind. Currently it caches prices downloaded from yahoo and two accounting metrics extracted from XBRL downloaded from the SEC's Edgar system.

FinancialFundamentals is under active development, comments, suggestions, and bug reports are appreciated.

'''.format(version)

setup(name='FinancialFundamentals',
      version=version,
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
	'Operating System :: OS Independent',
	'Intended Audience :: Science/Research',
	'Topic :: Office/Business :: Financial',
     	],	
      install_requires=[
	'numpy',
	'pytz',
	'requests_cache',
	'requests',
	'BeautifulSoup',
	'mock',
	'pandas',
	'xmltodict',
	'blist',
	'python-dateutil==1.5',
	'vector_cache',
	],
      dependency_links=[
	'http://github.com/andrewkittredge/vector_cache/tarball/master#egg=vector_cache',
      ],
      url='https://github.com/andrewkittredge/financial_fundamentals',
)
