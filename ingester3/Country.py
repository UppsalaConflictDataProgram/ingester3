from __future__ import annotations
from .scratch import fetch_ids, cache_manager, fetch_data
from .ViewsMonth import ViewsMonth
from diskcache import Cache
from .config import inner_cache_path

inner_cache = Cache(inner_cache_path)

class Country(object):
    def __init__(self, id: int):
        self.name = None
        self.gwcode = None
        self.isonum = None
        self.isoab = None
        self.capname = None
        self.caplat = None
        self.caplong = None
        self.in_africa = None
        self.in_me = None
        self.month_start = None
        self.month_end = None
        self.year_start = None
        self.year_end = None
        self.lat = None
        self.lon = None
        self.id = self.__validate_id(id) if id != 0 else None
        self.__populate_attributes() if id != 0 else None

    @staticmethod
    @inner_cache.memoize(typed=True, expire=600, tag="country_list")
    def __validate_id(id):
        id = int(id)
        available_countries, _ = fetch_ids('country')
        if id not in available_countries:
            raise ValueError("No such country exists!")
        return id

    def __repr__(self):
        return f'Country({self.id})'

    def __str__(self):
        return f'Country(id={self.id}) #=> ' \
               f'name:{self.name}, ' \
               f'gwcode:{self.gwcode}, ' \
               f'iso:{self.isoab}, ' \
               f'capital:{self.capname}, ' \
               f'in_africa:{self.in_africa}, ' \
               f'in_me:{self.in_me}'

    @staticmethod
    @inner_cache.memoize(typed=True, expire=600000, tag="country_info")
    def __fetch_descriptors():
        columns = ['id', 'name', 'gwcode', 'isonum', 'isoab', 'capname',
                   'caplat', 'caplong', 'in_africa', 'in_me',
                   'month_start', 'month_end', 'centroidlat', 'centroidlong', 'gwsyear', 'gweyear']
        descriptors = fetch_data(loa_table='country', columns=columns)
        return descriptors

    @staticmethod
    @inner_cache.memoize(typed=True, expire=600000, tag="country_priogrid")
    def __fetch_priogrid():
        columns = ['pg_id', 'c_id', 'year']
        priogrids = fetch_data(loa_table='pgy2cy', columns=columns)
        return priogrids

    @staticmethod
    @inner_cache.memoize(typed=True, expire=6000000, tag="country_neighbors")
    def __fetch_neighbors():
        columns = ['a_id', 'b_id', 'month_id']
        neighbors = fetch_data(loa_table='country_country_month_expanded', columns=columns)
        return neighbors

    #@inner_cache.memoize(typed=True, expire=600000, tag="country_pg_outer")
    def priogrids(self):
        from .Priogrid import Priogrid
        priogrids = Country.__fetch_priogrid()
        priogrids = priogrids[priogrids.c_id == self.id]#.pg_id.drop_duplicates()
        priogrids = priogrids[['pg_id']].drop_duplicates()
        priogrids = [Priogrid(i) for i in list(priogrids.pg_id)]
        return priogrids





    @inner_cache.memoize(typed=True, expire=600000, tag="country_neighbors_outer")
    def neighbors(self, month_id = None):
        """
        Returns the first-order neighbors of a given country at a certain timepoint
        If no month is issued
        :param month_id:
        :return:
        """
        neighbors = Country.__fetch_neighbors()
        neighbors = neighbors[neighbors.a_id == self.id]
        if month_id is not None:
            neighbors = neighbors[neighbors.month_id == month_id]
        neighbors = neighbors.b_id.unique()
        return [Country(i) for i in list(neighbors)]

    @staticmethod
    def __extid2id(name_value, name_var='isoab', month_id=None):
        """
        Translates an alternate identification system present in the ViEWS DB to ViEWS id
        :param name_value: The value of the id (e.g. SWE for the ISO system)
        :param name_var: The name of the system to use (e.g. isoab, cowcode or gwcode)
        :param month_id: a ViEWS Month id (as int)
        :return: A ViEWS id corresponding to the name_value in the name_var system for the given month
        Newest iteration of the country is returned if no month is given.
        """
        descriptors = Country.__fetch_descriptors()
        descriptors = descriptors[(descriptors[name_var] == name_value)]

        if month_id is None:
            try:
                x = int(descriptors.sort_values(by='month_end',
                                                ascending=False,
                                                ignore_index=True).loc[0].id)
                return x
            except ValueError:
                raise ValueError("Country does not exist")
        else:
            descriptors = descriptors[(descriptors.month_start <= month_id) &
                                      (descriptors.month_end >= month_id)]
        try:
            return int(max(descriptors.id))
        except ValueError:
            raise ValueError(f"Country with {name_var} = {name_value} does not exist at month {month_id}!")


    @staticmethod
    def iso2id(iso, month_id = None):
        iso = str(iso).strip().upper()
        return Country.__extid2id(iso, name_var='isoab', month_id=month_id)

    @staticmethod
    def gwcode2id(gwcode, month_id = None):
        gwcode = int(gwcode)
        return Country.__extid2id(gwcode, name_var='gwcode', month_id=month_id)

    @classmethod
    def from_iso(cls, iso, month_id = None):
        return cls(cls.iso2id(iso, month_id))

    @classmethod
    def from_gwcode(cls, gwcode, month_id = None):
        return cls(cls.gwcode2id(gwcode, month_id))

    @classmethod
    def from_priogrid(cls, pg_id, year=None):
        priogrids = cls.__fetch_priogrid()
        if year is None:
            year = ViewsMonth.now().year
        try:
            # Find the country for the given pg_id/year combo
            c = cls(priogrids[(priogrids.pg_id == int(pg_id)) & (priogrids.year == int(year))].c_id.values[0])
        except IndexError:
            # Country not found, give the country 0 which is terra nulius, international land.
            c = cls(0)
        return c

    def __populate_attributes(self):
        descriptors = self.__fetch_descriptors()
        descriptors = descriptors[descriptors.id == self.id].iloc[0]
        self.name = descriptors['name']
        self.gwcode = descriptors.gwcode
        self.isonum = descriptors.isonum
        self.isoab = descriptors.isoab
        self.capname = descriptors.capname
        self.caplat = descriptors.caplat
        self.caplong = descriptors.caplong
        self.in_africa = bool(descriptors.in_africa)
        self.in_me = bool(descriptors.in_me)
        self.month_start = int(descriptors.month_start)
        self.month_end = int(descriptors.month_end)
        self.year_start = int(descriptors.gwsyear)
        self.year_end = int(descriptors.gweyear)
        self.lat = descriptors.centroidlat
        self.lon = descriptors.centroidlong

    def __eq__(self, other):
        if isinstance(other, Country):
            return self.id == other.id



