from .Priogrid import Priogrid
from .ViewsMonth import ViewsMonth
from .Country import Country
from .scratch import fetch_ids, fetch_ids_df, cache_manager
import pandas as pd
import warnings
from functools import partial

pd.options.mode.chained_assignment = None

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
        cache_manager(False)
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
        """
        try:
            _ = Country(row.c_id).id
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
    def in_me(self):
        return self._obj.apply(lambda row: Country(row.c_id).in_me, axis=1)

    @property
    def month_start(self):
        return self._obj.apply(lambda row: Country(row.c_id).month_start, axis=1)

    @property
    def month_end(self):
        return self._obj.apply(lambda row: Country(row.c_id).month_end, axis=1)

    @classmethod
    def from_iso(cls, df, iso_col='iso', month_col=None):
        z = df.copy()
        if month_col is None:
            z['c_id'] = df.apply(lambda row: Country.from_iso(iso=row[iso_col]).id, axis=1)
        else:
            z['c_id'] = df.apply(lambda row: Country.from_iso(iso=row[iso_col], month_id=row[iso_col]).id, axis=1)
        return z

    @classmethod
    def from_gwcode(cls, df, gw_col='gwcode', month_col=None):
        z = df.copy()
        if month_col is None:
            z['c_id'] = df.apply(lambda row: Country.from_gwcode(gwcode=row[gw_col]).id, axis=1)
        else:
            z['c_id'] = df.apply(lambda row: Country.from_gwcode(gwcode=row[gw_col], month_id=row[gw_col]).id, axis=1)

        return z


    @classmethod
    def new_structure(cls):
        return pd.DataFrame(fetch_ids_df('country').rename(columns={'id': 'c_id'})[['c_id']])

    @classmethod
    def new_africa(cls):
        africa = CAccessor.new_structure()
        return africa[africa.c.in_africa]

    @classmethod
    def new_me(cls):
        me = CAccessor.new_structure()
        return me[me.c.in_me]


    def db_id(self):
        return self._obj

    def full_set(self):
        available_countries, _ = fetch_ids('country')
        av_c = set(available_countries)
        df_c = set(self._obj.c_id)
        return av_c == df_c

    def is_unique(self):
        if pd.unique(self._obj.c_id).size == self._obj.c_id.size:
            return True
        return False



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
        cache_manager(False)
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

    @classmethod
    def new_structure(cls):
        return pd.DataFrame(fetch_ids_df('priogrid').rename(columns={'id': 'pg_id'})[['pg_id']])

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


    @property
    def row(self):
        """
        Computes the row of the centroid of each dataframe row, per priogrid definitions.
        :return: A longitude in WGS-84 format (decimal degrees)
        """
        return self._obj.apply(lambda x: Priogrid(x.pg_id).row, axis=1)

    @property
    def col(self):
        """
        Computes the col of the centroid of each dataframe row, per priogrid definitions.
        :return: A longitude in WGS-84 format (decimal degrees)
        """
        return self._obj.apply(lambda x: Priogrid(x.pg_id).col, axis=1)

    def db_id(self):
        return self._obj

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

    def full_set(self, land_only=True):
        x = self._obj
        ctrl_grids = set(range(1, 259201))
        if land_only:
            ctrl_grids = set(i for i in fetch_ids('priogrid')[0])
        if set(x.pg_id) == ctrl_grids:
            return True
        return False

    def get_bbox(self, only_views_cells=False):
        test_square = self._obj
        min_row = test_square.pg.row.min()
        max_row = test_square.pg.row.max()
        min_col = test_square.pg.col.min()
        max_col = test_square.pg.col.max()
        square = []
        for row in range(min_row, max_row + 1):
            for col in range(min_col, max_col + 1):
                square += [Priogrid.from_row_col(row=row, col=col).id]
        square = set(square)
        if only_views_cells:
            views_cells, _ = fetch_ids('priogrid')
            square = set(views_cells).intersection(square)
        return square

    def is_bbox(self, only_views_cells=False):
        square = self.get_bbox(only_views_cells=only_views_cells)
        return square == set(self._obj.pg_id)

    def fill_bbox(self):
        return pd.DataFrame({'pg_id': list(self.get_bbox())}).merge(self._obj, how='left', on='pg_id')



@pd.api.extensions.register_dataframe_accessor("m")
class MAccessor():
    def __init__(self, pandas_obj):
        cache_manager(False)
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

    @classmethod
    def from_datetime(cls, df, datetime_col='datetime'):
        z = df.copy()
        z['temp_year_col'] = z[datetime_col].dt.year
        z['temp_month_col'] = z[datetime_col].dt.month
        z['month_id'] = z.apply(lambda row: ViewsMonth.from_year_month(year=row['temp_year_col'],
                                                                       month=row['temp_month_col']).id,
                                axis=1)
        del z["temp_year_col"]
        del z["temp_month_col"]
        return z


    def db_id(self):
        return self._obj

    def fill_panel_gaps(self):
        extent = pd.DataFrame({'month_id': range(self._obj.month_id.min(), self._obj.month_id.max() + 1)})
        extent = extent.merge(self._obj, how='left', on=['month_id'])
        return extent

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

    def full_set(self, max_month=None):
        if max_month is not None:
            if set(range(190, max(self._obj.month_id)+1)) != set(range(190, max_month+1)):
                return False
            else:
                return True
        else:
            av_months = set(fetch_ids('month')[0])
            if set(range(190,max(self._obj.month_id)+1)) != av_months:
                return False
            else:
                return True


@pd.api.extensions.register_dataframe_accessor("cy")
class CYAccessor(CAccessor):
    def __init__(self, pandas_obj):
        super().__init__(pandas_obj)

    @property
    def is_unique(self):
        uniques = self._obj[['c_id', 'year_id']].drop_duplicates().shape[0]
        totals = self._obj.shape[0]
        if uniques == totals:
            return True
        return False

    def is_panel(self):
        """
        Tests if the
        """
        test_square = self._obj.copy()
        min_year = test_square.year_id.min()
        max_year = test_square.year_id.max()
        for cid in test_square.c_id.unique():
            test_country = test_square[test_square.c_id == cid]
            if test_country.year_id.min() > min_year and test_country.year_id.min() > Country(cid).year_start:
                return False
            if test_country.year_id.max() < max_year and test_country.year_id.max() < Country(cid).year_end:
                return False
            if len(test_country.index) != max_year - min_year + 1:
                low = Country(cid).year_start if Country(cid).year_start > min_year else min_year
                high = Country(cid).year_end if Country(cid).year_end < max_year else max_year
                if high - low + 1 != len(test_country.index):
                    return False
        return True

    def is_complete_time_series(self, min_year=1980, max_year=2040):
        test_square = self._obj.copy()
        if not self.is_panel():
            return False
        if min_year != test_square.year_id.min() or max_year != test_square.year_id.max():
            return False
        return True

    def expand_country_year(self, min_year = None, max_year = None):
        if min_year is None:
            min_year = 1945
        if max_year is None:
            max_year = 2050
        test_square = self._obj.copy()
        extents = pd.DataFrame()
        for cid in test_square.c_id.unique():
            low = Country(cid).year_start if Country(cid).year_start > min_year else min_year
            high = Country(cid).year_end if Country(cid).year_end < max_year else max_year
            extents = extents.append(pd.DataFrame({'c_id': cid, 'year_id':range(low, high+1)}))
        extents = extents.merge(self._obj, how='left', on=['c_id', 'year_id'])
        #except KeyError:
        #    extents = extents.merge(self._obj, how='left', on=['c_id'])
        if 'cy_id' in self._obj:
            extents = CYAccessor.__db_id(extents)
        return extents


    def fill_panel_gaps(self):
        min_year = self._obj.year_id.min()
        max_year = self._obj.year_id.max()
        return self.expand_country_year(min_year=min_year,max_year=max_year)

    def fill_spatial_gaps(self):
        year_list = self._obj.year_id.unique()
        z = self.fill_panel_gaps()
        z = z[z.year_id.isin(year_list)]
        return z

    @classmethod
    def new_structure(cls, max_year=2050):
        structure = fetch_ids_df('country_year').rename(columns={'country_id':'c_id'})[['c_id','year_id']]
        return structure[structure.year_id <= max_year]

    @classmethod
    def new_africa(cls, max_year=2050):
        ids = CYAccessor.new_structure(max_year)
        africa = CAccessor.new_structure()
        africa = africa[africa.c.in_africa].c_id
        return ids[ids.c_id.isin(africa)]

    @classmethod
    def new_middle_east(cls,max_year=2050):
        ids = CYAccessor.new_structure(max_year)
        me = CAccessor.new_structure()
        me = me[me.c.in_me].c_id
        return ids[ids.c_id.isin(me)]

    @staticmethod
    def __db_id(z):
        db_ids = fetch_ids_df('country_year')[['id', 'country_id', 'year_id']]
        z['cy_id'] = z.merge(db_ids,
                             left_on=['c_id', 'year_id'],
                             right_on=['country_id', 'year_id'],
                             how='left').id
        return z

    def db_id(self):
        return CYAccessor.__db_id(self._obj)




@pd.api.extensions.register_dataframe_accessor("cm")
class CMAccessor(CAccessor, MAccessor):
    def __init__(self, pandas_obj):
        super().__init__(pandas_obj)

    @property
    def is_unique(self):
        uniques = self._obj[['c_id', 'month_id']].drop_duplicates().shape[0]
        totals = self._obj.shape[0]
        if uniques == totals:
            return True
        return False

    def is_panel(self):
        """
        Tests if the
        """
        test_square = self._obj.copy()
        min_month = test_square.month_id.min()
        max_month = test_square.month_id.max()

        for cid in test_square.c_id.unique():
            test_country = test_square[test_square.c_id == cid]
            if test_country.month_id.min() > min_month and test_country.month_id.min() > Country(cid).month_start:
                return False
            if test_country.month_id.max() < max_month and test_country.month_id.max() < Country(cid).month_end:
                return False
            if len(test_country.index) != max_month - min_month + 1:
                low = Country(cid).month_start if Country(cid).month_start > min_month else min_month
                high = Country(cid).month_end if Country(cid).month_end < max_month else max_month
                if high - low + 1 != len(test_country.index):
                    return False
        return True

    def is_complete_time_series(self, min_month=190, max_month=621):
        test_square = self._obj.copy()
        if not self.is_panel():
            return False
        if min_month != test_square.month_id.min() or max_month != test_square.month_id.max():
            return False
        return True

    def expand_country_months(self, min_month = None, max_month = None):
        if min_month is None:
            min_month = 1
        if max_month is None:
            max_month = 999
        test_square = self._obj.copy()
        extents = pd.DataFrame()
        for cid in test_square.c_id.unique():
            low = Country(cid).month_start if Country(cid).month_start > min_month else min_month
            high = Country(cid).month_end if Country(cid).month_end < max_month else max_month
            extents = extents.append(pd.DataFrame({'c_id': cid, 'month_id':range(low, high+1)}))
        extents = extents.merge(self._obj, how='left', on=['c_id', 'month_id'])
        #except KeyError:
        #    extents = extents.merge(self._obj, how='left', on=['c_id'])
        if 'cm_id' in self._obj:
            extents = CMAccessor.__db_id(extents)
        return extents

    def fill_panel_gaps(self):
        min_month = self._obj.month_id.min()
        max_month = self._obj.month_id.max()
        return self.expand_country_months(min_month=min_month,max_month=max_month)

    def fill_spatial_gaps(self):
        month_list = self._obj.month_id.unique()
        z = self.fill_panel_gaps()
        z = z[z.month_id.isin(month_list)]
        return z

    def is_complete_cross_section(self, in_africa=False, in_me=False):
        test_square = self._obj.copy()
        if not self.is_panel():
            return False
        av_c, _ = fetch_ids('country')
        min_month = test_square.month_id.min()
        max_month = test_square.month_id.max()
        av_c = [Country(i) for i in av_c]
        av_c = [i for i in av_c if min_month <= i.month_end and max_month >= i.month_start]
        subset_c = []
        if in_africa: subset_c += [i for i in av_c if i.in_africa]
        if in_me: subset_c += [i for i in av_c if i.in_me]
        if len(subset_c) == 0: subset_c = av_c
        subset_c = set(i.id for i in subset_c)

        if set(test_square.c_id.unique()) == subset_c:
            return True
        return False

    @classmethod
    def new_structure(cls, max_month=621):
        structure = fetch_ids_df('country_month').rename(columns={'country_id':'c_id'})[['c_id','month_id']]
        return structure[structure.month_id <= max_month]

    @classmethod
    def new_africa(cls):
        ids = CMAccessor.new_structure()
        africa = CAccessor.new_structure()
        africa = africa[africa.c.in_africa].c_id
        return ids[ids.c_id.isin(africa)]

    @classmethod
    def new_middle_east(cls):
        ids = CMAccessor.new_structure()
        me = CAccessor.new_structure()
        me = me[me.c.in_me].c_id
        return ids[ids.c_id.isin(me)]

    def full_set(self, in_africa=False, in_me=False, min_month=190, max_month=621):
        full_cs = self.is_complete_cross_section(in_africa=in_africa, in_me=in_me)
        full_ts = self.is_complete_time_series(min_month=min_month, max_month=max_month)
        return full_cs and full_ts

    @classmethod
    def from_datetime_gwcode(cls, df, datetime_col='datetime', gw_col='gwcode'):
        z = df.copy()
        z = super().from_datetime(z, datetime_col=datetime_col)
        z = super().from_gwcode(z, gw_col=gw_col, month_col='month_id')
        return z

    @classmethod
    def from_datetime_iso(cls, df, datetime_col='datetime', iso_col='iso'):
        z = df.copy()
        z = super().from_datetime(z, datetime_col=datetime_col)
        z = super().from_iso(z, iso_col=iso_col, month_col='month_id')
        return z


    @classmethod
    def from_year_month_gwcode(cls, df, year_col='year', month_col='month', gw_col='gwcode'):
        z = df.copy()
        z = super().from_year_month(z, year_col=year_col, month_col=month_col)
        z = super().from_gwcode(z, gw_col=gw_col, month_col='month_id')
        return z


    @classmethod
    def from_year_month_iso(cls, df, year_col='year', month_col='month', iso_col='iso'):
        z = df.copy()
        z = super().from_year_month(z, year_col=year_col, month_col=month_col)
        z = super().from_iso(z, iso_col=iso_col, month_col='month_id')
        return z

    @staticmethod
    def __db_id(z):
        #z = self._obj.copy()
        db_ids = fetch_ids_df('country_month')[['id', 'country_id', 'month_id']]
        z['cm_id'] = z.merge(db_ids,
                             left_on=['c_id', 'month_id'],
                             right_on=['country_id', 'month_id'],
                             how='left').id
        return z

    def db_id(self):
        return CMAccessor.__db_id(self._obj)

@pd.api.extensions.register_dataframe_accessor("pgm")
class PGMAccessor(PgAccessor, MAccessor):
    def __init__(self, pandas_obj):
        super().__init__(pandas_obj)

    @classmethod
    def new_structure(cls):
        return pd.DataFrame(fetch_ids_df('priogrid_month').rename(columns={'id': 'pg_id'})[['pg_id','month_id']])

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

    def full_set(self, land_only=True):
        pg_full_set = super(PgAccessor, self).full_set(land_only)
        return pg_full_set

    def is_panel(self):
        test_square = self._obj
        min_month = test_square.month_id.min()
        max_month = test_square.month_id.max()
        if len(test_square.month_id.unique()) != max_month - min_month + 1:
            return False
        first_panel = set(test_square[test_square.month_id == min_month].pg_id.unique())
        for month in test_square.month_id.unique():
            cur_panel = set(test_square[test_square.month_id == month].pg_id.unique())
            if first_panel != cur_panel:
                return False
        return True

    def is_complete_cross_section(self, only_views_cells=True):
        x = self._obj
        if not self.is_bbox(only_views_cells=only_views_cells):
            return False
        vc_len = len(fetch_ids('priogrid')[0]) if only_views_cells else 259200
        if len(x.pg_id.unique()) == vc_len:
            return True
        return False

    def is_complete_time_series(self, min_month=190, max_month=621):
        x = self._obj
        if not self.is_panel():
            return False
        if x.test_square.month_id.min() != min_month:
            return False
        if x.test_square.month_id.max() != max_month:
            return False
        return True


    def fill_panel_gaps(self, fill_value=None):
        extent1 = pd.DataFrame({'month_id': range(self._obj.month_id.min(), self._obj.month_id.max() + 1), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'pg_id': self._obj.pg_id.unique()})
        extent = extent1.merge(extent2, on='key')[['pg_id','month_id']]
        extent = extent.merge(self._obj, how='left', on=['pg_id','month_id'])
        if 'pgm_id' in self._obj:
            extent = PGMAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    def fill_spatial_gaps(self, fill_value=None):
        extent1 = pd.DataFrame({'pg_id': self._obj.pg_id.unique(), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'month_id': self._obj.month_id.unique()})
        extent = extent1.merge(extent2, on='key')[['pg_id','month_id']]
        extent = extent.merge(self._obj, how='left', on=['pg_id','month_id'])
        if 'pgm_id' in self._obj:
            extent = PGMAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    def fill_bbox(self, fill_value=None):
        extent1 = pd.DataFrame({'pg_id': list(self.get_bbox()), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'month_id': list(self._obj.month_id.unique())})
        extent3 = extent1.merge(extent2, on='key')[['pg_id','month_id']]
        #print(extent.head())
        extent = extent3.merge(self._obj, how='left', on=['pg_id','month_id'])
        #print(extent.head())
        if 'pgm_id' in self._obj:
            extent = PGMAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    @staticmethod
    def __db_id(z):
        db_ids = fetch_ids_df('priogrid_month')[['id', 'priogrid_gid', 'month_id']]
        z['pgm_id'] = z.merge(db_ids,
                              left_on=['pg_id', 'month_id'],
                              right_on=['priogrid_gid', 'month_id'],
                              how='left').id
        return z

    def db_id(self):
        return PGMAccessor.__db_id(self._obj)

    def trim_to_db_extent(self):
        try:
            del self._obj.pgm_id
        except AttributeError:
            pass
        z = self.db_id()
        z = z[z.pgm_id.notna()]
        z['pgm_id'] = z.pgm_id.astype('int64')
        return z

        """Special method to attach"""



@pd.api.extensions.register_dataframe_accessor("pgy")
class PGYAccessor(PgAccessor):
    def __init__(self, pandas_obj):
        super().__init__(pandas_obj)

    @classmethod
    def new_structure(cls):
        return pd.DataFrame(fetch_ids_df('priogrid_year').rename(columns={'id': 'pg_id'})[['pg_id','year_id']])

    @property
    def is_unique(self):
        uniques = self._obj[['pg_id', 'year_id']].drop_duplicates().shape[0]
        totals = self._obj.shape[0]
        if uniques == totals:
            return True
        return False

    def is_panel(self):
        test_square = self._obj
        min_year = test_square.year_id.min()
        max_year = test_square.year_id.max()
        if len(test_square.year_id.unique()) != max_year - min_year + 1:
            return False
        first_panel = set(test_square[test_square.year_id == min_year].pg_id.unique())
        for year in test_square.year_id.unique():
            cur_panel = set(test_square[test_square.year_id == year].pg_id.unique())
            if first_panel != cur_panel:
                return False
        return True

    def is_complete_cross_section(self, only_views_cells=True):
        x = self._obj
        if not self.is_bbox(only_views_cells=only_views_cells):
            return False
        vc_len = len(fetch_ids('priogrid')[0]) if only_views_cells else 259200
        if len(x.pg_id.unique()) == vc_len:
            return True
        return False

    def is_complete_time_series(self, min_year=1980, max_year=2050):
        x = self._obj
        if not self.is_panel():
            return False
        if x.test_square.year_id.min() != min_year:
            return False
        if x.test_square.year_id.max() != max_year:
            return False
        return True


    def fill_panel_gaps(self, fill_value=None):
        extent1 = pd.DataFrame({'year_id': range(self._obj.year_id.min(), self._obj.year_id.max() + 1), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'pg_id': self._obj.pg_id.unique()})
        extent = extent1.merge(extent2, on='key')[['pg_id','year_id']]
        extent = extent.merge(self._obj, how='left', on=['pg_id','year_id'])
        if 'pgy_id' in self._obj:
            extent = PGYAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    def fill_spatial_gaps(self, fill_value=None):
        extent1 = pd.DataFrame({'pg_id': self._obj.pg_id.unique(), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'year_id': self._obj.year_id.unique()})
        extent = extent1.merge(extent2, on='key')[['pg_id','year_id']]
        extent = extent.merge(self._obj, how='left', on=['pg_id','year_id'])
        if 'pgy_id' in self._obj:
            extent = PGYAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    def fill_bbox(self, fill_value=None):
        extent1 = pd.DataFrame({'pg_id': list(self.get_bbox()), 'key': 0})
        extent2 = pd.DataFrame({'key': 0, 'year_id': list(self._obj.year_id.unique())})
        extent3 = extent1.merge(extent2, on='key')[['pg_id','year_id']]
        #print(extent.head())
        extent = extent3.merge(self._obj, how='left', on=['pg_id','year_id'])
        #print(extent.head())
        if 'pgy_id' in self._obj:
            extent = PGYAccessor.__db_id(extent)
        if fill_value is not None:
            extent = extent.fillna(fill_value)
        return extent

    @staticmethod
    def __db_id(z):
        db_ids = fetch_ids_df('priogrid_year')[['id', 'priogrid_gid', 'year_id']]
        z['pgy_id'] = z.merge(db_ids, left_on=['pg_id', 'year_id'],
                              right_on=['priogrid_gid', 'year_id'],
                              how='left').id
        return z

    def db_id(self):
        return PGYAccessor.__db_id(self._obj)

    def trim_to_db_extent(self):
        try:
            del self._obj.pgy_id
        except AttributeError:
            pass
        z = self.db_id()
        z = z[z.pgy_id.notna()]
        z['pgy_id'] = z.pgy_id.astype('int64')
        return z
