from .Priogrid import Priogrid
from .ViewsMonth import ViewsMonth
from .Country import Country
import pandas as pd
import numpy as np
import warnings
from functools import partial


def test(x):
    print (Priogrid(x))
    print (ViewsMonth(x))

@pd.api.extensions.register_dataframe_accessor("c")
class CAccessor:
    """
    A Pandas data-frame accessor for the ViEWS country class.
    Can be explored using df.c.* and pd.DataFrame.c.*
    Any dataframe having a c_id column can be automagically used with this accessor.
    """
    def __init__(self, pandas_obj):
        """
        Initializes the accessor and validates that it really is a priogrid df
        :param pandas_obj: A pandas DataFrame containing
        """

        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._obj.c_id = self._obj.c_id.astype('int')

    @staticmethod
    def _validate(obj):
        """
        :param obj: An Pandas DF containing a pg_id column
        :return: Nothing. Will crash w/ ValueError if invalid, per Pandas documentation
        """
        if "c_id" not in obj.columns:
            raise AttributeError("Must have a c_id column!")
        #mid = obj['c_id'].astype('int')
        obj.apply(lambda row: Country(row['c_id']).id, axis=1)


    @staticmethod
    def __soft_validate(row):
        """
        Soft-validate a df containing lat/lon values. Will produce a valid_latlon column to the existing dataframe
        :param df:
        :param lat_col:
        :param lon_col:
        :return:
        """
        try:
            _ = Country(row.c_id).name
            ok = True
        except ValueError:
            ok = False
        return ok

    @classmethod
    def soft_validate(cls, df):
        z = df.copy()
        z['valid_id'] = df.apply(CAccessor.__soft_validate, axis=1)
        return z['valid_id']

    @property
    def name(self):
        return self._obj.apply(lambda row: Country(row.c_id).name, axis=1)

    @property
    def gwcode(self):
        return self._obj.apply(lambda row: Country(row.c_id).gwcode, axis=1)

    @property
    def isoab(self):
        return self._obj.apply(lambda row: Country(row.c_id).isoab, axis=1)

    @property
    def isonum(self):
        return self._obj.apply(lambda row: Country(row.c_id).isonum, axis=1)

    @property
    def capname(self):
        return self._obj.apply(lambda row: Country(row.c_id).capname, axis=1)

    @property
    def caplat(self):
        return self._obj.apply(lambda row: Country(row.c_id).caplat, axis=1)

    @property
    def caplong(self):
        return self._obj.apply(lambda row: Country(row.c_id).caplong, axis=1)

    @property
    def in_africa(self):
        return self._obj.apply(lambda row: Country(row.c_id).in_africa, axis=1)

    @property
    def month_start(self):
        return self._obj.apply(lambda row: Country(row.c_id).month_start, axis=1)

    @property
    def month_end(self):
        return self._obj.apply(lambda row: Country(row.c_id).month_end, axis=1)



    @classmethod
    def from_iso(cls, df, iso_col='iso'):
        z = df.copy()
        z['c_id'] = df.apply(lambda row: Country.from_iso(iso=row[iso_col]).id, axis=1)
        return z

    @classmethod
    def from_gwcode(cls, df, gw_col='gwcode'):
        z = df.copy()
        z['c_id'] = df.apply(lambda row: Country.from_gwcode(gwcode=row[gw_col]).id, axis=1)
        return z


@pd.api.extensions.register_dataframe_accessor("pg")
class PgAccessor:
    """
    A Pandas (pd) data-frame accessor for the ViEWS priogrid (pg) class.
    Can be explored using df.pg.* and pd.DataFrame.pg.*
    Any dataframe having a pg_id column can be automagically used with this accessor.
    You can also construct one from latitude and longitude.
    """
    def __init__(self, pandas_obj):
        """
        Initializes the accessor and validates that it really is a priogrid df
        :param pandas_obj: A pandas DataFrame containing
        """
        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._obj.pg_id = self._obj.pg_id.astype('int')

    @staticmethod
    def _validate(obj):
        """
        :param obj: An Pandas DF containing a pg_id column
        :return: Nothing. Will crash w/ ValueError if invalid, per Pandas documentation
        """
        # print (obj.columns)
        if "pg_id" not in obj.columns:
            raise AttributeError("Must have a pg_id column!")
        mid = obj.pg_id.copy()
        if mid.dtypes != 'int':
            warnings.warn('pg_id is not an integer - will try to typecast in place!')
            mid = mid.astype('int')
        if mid.min() < 1:
            raise ValueError("Negative pg_id encountered!")
        if mid.max() > 259200:
            raise ValueError("pg_id out of bounds!")

    @property
    def is_unique(self):
        """
        Determines if the dataframe is pg-unique (contains only unique priogrids)
        :return: Will determine if the dataframe conatins unique pg_id values
        """
        if pd.unique(self._obj.pg_id).size == self._obj.pg_id.size:
            return True
        return False

    @property
    def lat(self):
        """
        Computes the latitude of the centroid of each dataframe row, per priogrid definitions.
        :return: A latitude in WGS-84 format (decimal degrees).
        """
        return self._obj.apply(lambda x: Priogrid(x.pg_id).lat, axis=1)

    @property
    def lon(self):
        """
        Computes the longitude of the centroid of each dataframe row, per priogrid definitions.
        :return: A longitude in WGS-84 format (decimal degrees)
        """
        return self._obj.apply(lambda x: Priogrid(x.pg_id).lon, axis=1)

    @classmethod
    def from_latlon(cls, df, lat_col='lat', lon_col='lon'):
        """
        Given an arbitrary dataframe containing two columns, on for latitude (lat) and one for longitude (lon)
        Will return a dataframe containing pg_ids in the pg_id column, that can be used with the df.pg accessor.
        Note: Will crash with ValueError if lat/lon is malformed or contain nulls.
        Use self.soft_validate_latlon to soft-validate the dataframe if your input can be malformed.
        :param df: A dataframe containing a latitutde and a longitude column in WGS84 (decimal degrees) format
        :param lat_col: The name of the Latitude column
        :param lon_col: The name of the Longitude column
        :return: A pg-class dataframe.
        """
        z = df.copy()
        z['pg_id'] = df.apply(lambda row: Priogrid.latlon2id(lat=row[lat_col], lon=row[lon_col]), axis=1)
        return z

    @staticmethod
    def __soft_validate_pg(row, lat_col, lon_col):
        try:
            _ = Priogrid.latlon2id(lat=row[lat_col], lon=row[lon_col])
            ok = True
        except ValueError:
            ok = False
        return ok

    @classmethod
    def soft_validate_latlon(cls, df, lat_col='lat', lon_col='lon'):
        """
        Soft-validate a df containing lat/lon values. Will produce a valid_latlon column to the existing dataframe
        :param df:
        :param lat_col:
        :param lon_col:
        :return:
        """
        z = df.copy()
        soft_validator = partial(PgAccessor.__soft_validate_pg, lat_col=lat_col, lon_col=lon_col)
        z['valid_latlon'] = z.apply(soft_validator, axis=1)
        return z


@pd.api.extensions.register_dataframe_accessor("m")
class MAccessor():
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._obj.month_id = self._obj.month_id.astype('int')

    @staticmethod
    def _validate(obj):
        # print (obj.columns)
        if "month_id" not in obj.columns:
            raise AttributeError("Must have a month_id column!")
        mid = obj.month_id.copy()
        if mid.dtypes != 'int':
            warnings.warn('month_id is not an integer - will try to typecast in place!')
            mid = mid.astype('int')
        if mid.min() < 1:
            raise ValueError("Negative month_id encountered")

    @property
    def is_unique(self):
        if pd.unique(self._obj.month_id).size == self._obj.month_id.size:
            return True
        return False

    @property
    def year(self):
        return self._obj.apply(lambda x: ViewsMonth(x.id).year, axis=1)

    @property
    def month(self):
        return self._obj.apply(lambda x: ViewsMonth(x.id).month, axis=1)

    @classmethod
    def from_year_month(cls, df, year_col='year', month_col='month'):
        z = df.copy()
        z['month_id'] = z.apply(lambda row: ViewsMonth.from_year_month(year=row[year_col],
                                                                       month=row[month_col]).id,
                                axis=1)
        return z

    @staticmethod
    def __soft_validate_month(row, year_col, month_col):
        try:
            _ = ViewsMonth.from_year_month(year=row[year_col], month=row[month_col]).id
            ok = True
        except ValueError:
            ok = False
        return ok

    @classmethod
    def soft_validate_year_month(cls, df, year_col='year', month_col='month'):
        z = df.copy()
        soft_validator = partial(MAccessor.__soft_validate_month, year_col=year_col, month_col=month_col)
        z['valid_year_month'] = z.apply(soft_validator, axis=1)
        return z


@pd.api.extensions.register_dataframe_accessor("pgm")
class PGMAccessor(PgAccessor, MAccessor):
    def __init__(self, pandas_obj):
        super().__init__(pandas_obj)

    @property
    def is_unique(self):
        uniques = self._obj[['pg_id', 'month_id']].drop_duplicates().shape[0]
        totals = self._obj.shape[0]
        if uniques == totals:
            return True
        return False

    @classmethod
    def from_year_month_latlon(cls, df, year_col='year', month_col='month', lat_col='lat', lon_col='lon'):
        z = df.copy()
        z = super().from_year_month(z, year_col=year_col, month_col=month_col)
        z = super().from_latlon(z, lat_col=lat_col, lon_col=lon_col)
        return z

    @classmethod
    def soft_validate_year_month_latlon(cls, df, year_col='year', month_col='month', lat_col='lat', lon_col='lon'):
        z = df.copy()
        z = super().soft_validate_year_month(z, year_col=year_col, month_col=month_col)
        z = super().soft_validate_latlon(z, lat_col=lat_col, lon_col=lon_col)
        z['valid_year_month_latlon'] = z.valid_year_month & z.valid_latlon
        return z

