from ingester3.FuzzyCountry import FuzzyCountry

assert FuzzyCountry('The Ukraine').isoab == 'UKR'
assert FuzzyCountry('Russian Fed').isoab == 'RUS'
assert FuzzyCountry('United States').isoab == 'USA'
assert FuzzyCountry('Soviet').isoab == 'SUN'
assert FuzzyCountry('PRC').isoab == 'CHN'
assert FuzzyCountry('United Kingdom of Great Britain').isoab == 'GBR'