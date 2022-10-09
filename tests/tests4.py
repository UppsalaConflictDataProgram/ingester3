import pandas as pd

from ingester3.FuzzyCountry import FuzzyCountry

assert FuzzyCountry('The Ukraine').isoab == 'UKR'
assert FuzzyCountry('Russian Fed').isoab == 'RUS'
assert FuzzyCountry('United States').isoab == 'USA'
assert FuzzyCountry('Soviet').isoab == 'SUN'
assert FuzzyCountry('PRC').isoab == 'CHN'
assert FuzzyCountry('United Kingdom of Great Britain').isoab == 'GBR'

from ingester3.extensions import FuzzyCountryAccessor

df = pd.DataFrame({'cname':['Russian Fed','United States','Soviet','PRC','The Ukraine','Greece'],
             'values':[1,2 ,9,7,3,10]})

assert list(df.fuzzy_country.isoab('cname')) == ['RUS', 'USA', 'SUN', 'CHN', 'UKR', 'GRC']