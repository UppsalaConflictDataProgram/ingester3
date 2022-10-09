from __future__ import annotations

import pandas as pd

from .scratch import fetch_data, cache_manager
from diskcache import Cache
from .config import inner_cache_path
from functools import partial
from Levenshtein import jaro_winkler as jaro_winkler


inner_cache = Cache(inner_cache_path)

class FuzzyCountry:
    def __init__(self, name: str, precision: float = 0.8):
        self.name = name
        self.precision = precision if 0 <= precision <= 1 else 0.8
        self.base_countries = self.__find_stack(self.name, self.__get_haystack())

    @staticmethod
    def __clean_names(cname: str):
        """
        cname : Country names
        """
        punct = set('abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        stopwords = ['and', 'saint', 'st', 'people', 'is', 'islands', 'island', 'the']
        cname = cname.strip().lower()
        cname = "".join(filter(punct.__contains__, cname))
        cname = "".join([i for i in cname.split() if i not in stopwords])
        return cname

    @inner_cache.memoize(typed=True, expire=600000, tag="fuzzy_countries")
    def __get_haystack(self):
        base_countries = fetch_data(loa_table='fuzzy_names', columns=['name', 'isoab'])
        base_countries['search_pattern'] = base_countries.apply(lambda row: self.__clean_names(row['name']), axis=1)
        return base_countries

    @staticmethod
    @inner_cache.memoize(typed=True, expire=600000, tag="matches")
    def __find_stack(needle : str, haystack : pd.DataFrame):
        lookup_s = partial(jaro_winkler, s1=FuzzyCountry.__clean_names(needle))
        haystack['score'] = haystack.apply(lambda row: lookup_s(s2=row.search_pattern), axis=1)
        haystack = haystack.sort_values('score', ascending=False).reset_index(False)
        return haystack

    @property
    def isoab(self):
        if self.base_countries.score[0] > self.precision:
            return self.base_countries.isoab[0]
        return None

    def top_matches(self, k: int = 10):
        if k is None:
            k = 99999
        return self.base_countries.head(k)

    @property
    def predict_proba(self):
        return self.base_countries[self.base_countries.score >= self.precision]
