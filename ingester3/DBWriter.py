from .config import source_db_path
from .scratch import fetch_columns, flash_fetch_definitions, cache_manager
from . import log
import sqlalchemy as sa
import warnings
import numpy as np
import pandas as pd
import psycopg2
import re
from dataclasses import dataclass
from uuid import uuid1
import datetime
from io import StringIO


@dataclass
class ColumnMapper:
    origin_name: str
    destination_name: str
    origin_type: type
    destination_type: type
    destination_table: str
    same_type: bool
    in_panel_wipe: bool = True
    out_panel_wipe: bool = False
    in_panel_zero: bool = True
    out_panel_zero: bool = False
    new_table: bool = False


class DBWriter(object):

    @log.log_ingester()
    def __print(self, *args, **kwargs):
        """
        This is a verbose printer with a few extra tricks involved.
        You can pass a msg kwarg that will be printed first, and a list of args and kwargs
        Args will be
        :param args: Arbitrary. If cannot be printed, will just be skipped.
        :param kwargs: Arbitrary. If cannot be printed, will just be skipped.
        :return:
        """
        if self.__verbose:

            try:
                print(kwargs['msg'])
                del(kwargs['msg'])
            except Exception:
                pass

            for arg in args:
                try:
                    print(arg)
                except Exception:
                    pass

            for kwarg in kwargs:
                try:
                    print(f'{kwarg} :',end=' ')
                    print(kwargs[kwarg])
                except Exception:
                    pass


    def __match_names(self, a, b):
        a1 = self.__matching_pattern.sub('', a).lower()
        a2 = self.__matching_pattern.sub('', b).lower()
        if a2 == a1:
            #print('XXX',a,b, '>>>',a1, a2)
            return True
        return False

    def __validate(self, pandas_obj, level):
        key_col_name = level+'_id'
        np.dtype('int_')

        if key_col_name not in pandas_obj.columns:
            raise KeyError(f'No {key_col_name} is not in the data!')

        if not(pandas_obj.shape[0] == pandas_obj[key_col_name].unique().shape[0]):
            raise KeyError(f'Key column : {key_col_name} is not unique!')

        if pandas_obj[key_col_name].dtype != np.dtype('int_'):
            raise KeyError(f'Key column : {key_col_name} is not an integer!')

        if pandas_obj[key_col_name].isnull().sum()>0:
            raise KeyError(f'Key column has nulls!')

    @log.log_ingester()
    def __init__(self, pandas_obj, level='cm',
                 in_panel_wipe: bool = True,
                 out_panel_wipe: bool = False,
                 in_panel_zero: bool = True,
                 out_panel_zero: bool = False,
                 verbose: bool = False
    ):

        self.RESERVED_WORDS = {'isocc', 'isoab', 'iso', 'isonum',
                               'gw', 'gwcode', 'gwnum', 'gwab',
                               'col', 'row', 'gid', 'lat', 'latitude', 'lon', 'long', 'longitude',
                               'country','priogrid','state','caplat', 'caplon','caplong','start_date','end_date',
                               'gwdate', 'capname', 'in_africa', 'in_me', 'month', 'start','end',
                               'month_start', 'month_end', 'centroidlat', 'centroidlong', 'gwsyear', 'gweyear'}

        self.__verbose = verbose

        self.__validate(pandas_obj, level)

        self.__matching_pattern =  re.compile('[\W]+')
        self.level = level.lower().strip()
        self.tablespace = self.__get_tablespace()
        self.engine = sa.create_engine(source_db_path)

        self.df = pandas_obj
        self.df.columns = self.df.columns.str.strip().str.lower()

        # Time and space extents
        self.time_extent = None
        self.space_extent = None
        self.tname_temp = f'tbl_{uuid1().hex}'

        # Recipe
        self.recipe = None

        # Master Settings for wiping
        self.in_panel_wipe = in_panel_wipe
        self.out_panel_wipe = out_panel_wipe
        self.in_panel_zero = in_panel_zero
        self.out_panel_zero = out_panel_zero

        # Set writer index
        other_ids = [i for i in list(self.df.columns) if (i != self.level+'_id' and '_id' in i)]
        self.df = self.df.drop(other_ids, axis=1)


    def __get_tablespace(self):
        table_spaces = {'cm': 'country_month',
                        'cy': 'country_year',
                        'pgy': 'priogrid_year',
                        'pgm': 'priogrid_month',
                        'm': 'month',
                        'y': 'year',
                        'pg': 'priogrid',
                        'c': 'country',
                        'a': 'actor',
                        'am': 'actor_month',
                        'ay': 'actor_year'}
        return table_spaces[self.level]

    @property
    def __time_extent_name(self):
        if self.level in ['am','cm','pgm']:
            return 'month_id'
        if self.level in ['ay','cy','pgy']:
            return 'year_id'
        return None

    @property
    def __space_extent_name(self):
        if self.level in ['am','ay','a']:
            return 'actor_id'
        if self.level in ['cm','cy','c']:
            return 'country_id'
        if self.level in ['pgm','pgy','pg']:
            return 'priogrid_id'
        return None

    def set_time_extents(self, time_list=None):
        self.time_extent = time_list

    def set_time_extents_min_max(self, time_min=100, time_max=600):
        self.time_extent = list(range(time_min,time_max+1))

    def set_space_extents(self, space_list=None):
        self.space_extent = space_list

    def __get_col_python_type(self, column):
        column = column.copy().reset_index(drop=True)
        col_type = type(None)
        i = 0
        while col_type == type(None) or i < len(column)-1:
            try:
                col_type = type(column[i].item())
            except AttributeError:
                col_type = type(column[i])
            i += 1
        return col_type

    def publish(self):
        tbl_colset = fetch_columns(self.tablespace)
        #print("(*):",tbl_colset)
        column_mappers = []
        for df_column in self.df.columns:
            column_match = None
            for tbl_column in tbl_colset:
                if self.__match_names(tbl_column['column_name'],df_column):
                    #print("*>",df_column)
                    origin_type = self.__get_col_python_type(self.df[df_column])
                    destination_type=tbl_column['type']
                    if origin_type == destination_type:
                        match_type = True
                    else:
                        match_type = False
                    column_match = ColumnMapper(
                        origin_name=df_column,
                        destination_name=tbl_column['column_name'],
                        origin_type=origin_type,
                        destination_type=destination_type,
                        destination_table=tbl_column['table'],
                        same_type=match_type,
                        new_table=False,
                        in_panel_wipe = self.in_panel_wipe,
                        out_panel_wipe = self.out_panel_wipe,
                        in_panel_zero = self.in_panel_zero,
                        out_panel_zero = self.out_panel_wipe

                    )
                    #print(column_match)
                    column_mappers.append(column_match)
                    break
            if column_match is None:
                origin_type = self.__get_col_python_type(self.df[df_column])
                column_match = ColumnMapper(
                        origin_name=df_column,
                        destination_name=self.__matching_pattern.sub('', df_column.lower()),
                        origin_type=origin_type,
                        destination_type=origin_type,
                        destination_table='NEW',
                        same_type=True,
                        new_table=True,
                        in_panel_wipe=self.in_panel_wipe,
                        out_panel_wipe=self.out_panel_wipe,
                        in_panel_zero=self.in_panel_zero,
                        out_panel_zero=self.out_panel_wipe
                )
                column_mappers.append(column_match)

        self.recipe = column_mappers
        self.__print(msg = "Recipes", recipe=self.recipe)

    def __rename_headers(self):
        for column in self.recipe:
            self.__print(msg="Renaming Column:", origin=column.origin_name, destination=column.destination_name)
            if column.origin_name != column.destination_name:
                #print ("CHG: ", column.origin_name , column.destination_name)
                self.df = self.df.rename({column.origin_name:column.destination_name},axis=1)
        self.__print(msg="The table to be shipped to DB is : ",df=self.df.head(3))

    def write_temp(self):
        if self.recipe is None:
            self.publish()

        self.__rename_headers()

        with self.engine.connect() as con:
            trans = con.begin()
            self.df.head(0).to_sql(name=self.tname_temp,
                                   schema='public',
                                   con=con,
                                   index=False,
                                   if_exists='replace')
            trans.commit()
        #, con = cnx, index = False)  # head(0) uses only the header
        # set index=False to avoid bringing the dataframe index in as a column

        raw_con = self.engine.raw_connection()  # assuming you set up cnx as above
        cur = raw_con.cursor()
        out = StringIO()
        self.df.to_csv(out, sep='|', header=False, index=False)
        out.seek(0)
        contents = out.getvalue()
        try_simple = False
        try:
            cur.copy_from(out, sep='|', size=134217728,
                      table = self.tname_temp,
                      null="")
            raw_con.commit()
        except psycopg2.errors.BadCopyFileFormat:
            try_simple = True
            self.__print(msg="Format is too complicated for a COPY fast write. Trying fallback alternative...")
        finally:
            cur.close()
            raw_con.close()
        if try_simple:
            with self.engine.connect() as con:
                self.df.to_sql(con=con, schema='public', name=self.tname_temp, if_exists="append", index=False)

        return 0

    def del_temp(self):
        with self.engine.connect() as con:
            trans = con.begin()
            con.execute(f"DROP TABLE IF EXISTS public.{self.tname_temp}")
            trans.commit()

    def __zero_type(self, type):
        ZERO = {int:0,
                float:0,
                str:"",
                datetime.datetime:"1970-01-01",
                bool:False}
        try:
            ret_type = ZERO[type]
        except KeyError:
            ret_type = None
        return ret_type

    def __make_coalesce(self, all_columns, to_coalesce):
        #print (f"?>{all_columns}\nP>>><<{to_coalesce}")
        subquery = []
        for column in all_columns:
            if column in to_coalesce:
                subquery += [f'COALESCE ("{column}", :{column}) as {column}']
            else:
                subquery += [column]
        return ",".join(subquery)

    def __inner_where_query(self, outside = False):

        bool_call = ''
        if outside:
            bool_call = 'NOT'

        inner_where_query = []
        if self.time_extent is not None and self.__time_extent_name is not None:
            inner_where_query += [f"base.{self.__time_extent_name} IN ({','.join([str(i) for i in self.time_extent])})"]

        if self.space_extent is not None and self.__space_extent_name is not None:
            inner_where_query += [f"base.{self.__space_extent_name} IN ({','.join([str(i) for i in self.space_extent])})"]

        if len(inner_where_query)>0:
            inner_where_query = f"WHERE {bool_call} ({' AND '.join(inner_where_query)})"
        else:
            inner_where_query = ''

        #print(inner_where_query)

        return inner_where_query

    def __get_zero_columns(self, which_columns, inside = True):

        key_types = flash_fetch_definitions(schema='public', table=self.tname_temp)

        if inside:
            zero_inside = [i.destination_name for i in self.recipe
                           if i.destination_name in set(which_columns) and i.in_panel_zero]
        else:
            zero_inside = [i.destination_name for i in self.recipe
                           if i.destination_name in set(which_columns) and i.out_panel_zero]

        types_zero_inside = {i['column_name']: self.__zero_type(i['type']) for i in key_types if
                             i['column_name'] in set(zero_inside)}

        return zero_inside, types_zero_inside



    def __tname_checker(self, tname, drop_table = False):

        #print('waite')

        not_allowed_tables = set([i['table'] for i in fetch_columns(self.tablespace)])

        if self.level not in self.tablespace[-4:]:
            tname = tname + '_' + self.level

        if tname in not_allowed_tables:
            if drop_table:
                warnings.warn(f"Table {tname} exists in table space, and will be wiped!")
            else:
                tname = self.tname_temp[0:9] + tname
                warnings.warn(f"Table {tname} exists in table space, and will not be wiped. "
                              f"Instead we will use a randomly generated prefix : {tname}")
        return tname

    def index_temp_table(self):
        sql = f"""
        ALTER TABLE public.{self.tname_temp} ADD PRIMARY KEY ({self.level}_id);
        CREATE INDEX {self.tname_temp}_idx ON public.{self.tname_temp}({self.level}_id);
        """
        with self.engine.connect() as con:
            trans = con.begin()
            con.execute(sql)
            trans.commit()

    def del_spurios_loaded_data(self):
        if self.time_extent is not None or self.space_extent is not None:
            sql = f"""
              DELETE FROM public.{self.tname_temp} WHERE {self.level}_id::bigint NOT IN (
             SELECT id
             FROM prod.{self.tablespace} base
             {self.__inner_where_query()} )
             """
            with self.engine.connect() as con:
                self.__print(msg="Query for spurious delete",spurious_sql=sql)
                trans = con.begin()
                con.execute(sql)
                trans.commit()

    def new_transfer(self, tname, drop_table = False):

        cache_manager(clear=False)

        if self.recipe is None:
            self.publish()

        new_table_ids = [i.destination_name for i in self.recipe
                         if i.new_table and '_id' not in i.destination_name]

        if len(new_table_ids) == 0:
            return 0

        tname = self.__tname_checker(tname, drop_table = drop_table)

        zero_inside, types_zero_inside =  self.__get_zero_columns(new_table_ids)
        zero_outside, types_zero_outside = self.__get_zero_columns(new_table_ids, False)

        inner_sql_bit = f'''
        (
        SELECT base.id AS {self.tablespace}_id,
               {self.__make_coalesce(new_table_ids,zero_inside)}
        FROM prod.{self.tablespace} base LEFT JOIN public.{self.tname_temp} nnew
        ON (base.id::bigint = nnew.{self.level}_id::bigint)
        {self.__inner_where_query()}
        )
        '''

        outer_sql_bit = f'''
        (
        SELECT base.id AS {self.tablespace}_id,
               {self.__make_coalesce(new_table_ids,zero_outside)}
        FROM prod.{self.tablespace} base LEFT JOIN public.{self.tname_temp} nnew
        ON (base.id::bigint = nnew.{self.level}_id::bigint)
        {self.__inner_where_query(outside=True)}
        )
        '''

        sql_sub_routine = inner_sql_bit
        if self.time_extent is not None or self.space_extent is not None:
            sql_sub_routine = inner_sql_bit + ' UNION ALL ' + outer_sql_bit

        sql_copy = f"""
        DROP TABLE IF EXISTS prod.{tname};
        CREATE TABLE prod.{tname} AS
        SELECT * FROM
         (
        {sql_sub_routine}
         ) merged_final WHERE merged_final.{self.tablespace}_id IS NOT NULL;
        """
        sql_copy = sa.text(sql_copy).bindparams(**types_zero_inside, **types_zero_outside)

        sql_reference = sa.text(f"""
        ALTER TABLE prod.{tname} ADD PRIMARY KEY ({self.tablespace}_id);
        ALTER TABLE prod.{tname} ADD  CONSTRAINT fk_{self.tablespace}_id
        FOREIGN KEY({self.tablespace}_id)
	  REFERENCES prod.{self.tablespace}(id);
        """)
        self.__print(msg = "Creating New Table using following SQL:", new_table_query = str(sql_copy))

        with self.engine.connect() as con:
            trans = con.begin()
            con.execute(sql_copy)
            con.execute(sql_reference)
            trans.commit()
            cache_manager(clear=True)

    def __get_db_type(self, column):
            type_fetcher = sa.text("""
            SELECT data_type FROM information_schema.columns WHERE
            table_schema='prod' AND
            table_name=:tname AND
            column_name ilike :colname""").bindparams(tname=column.destination_table,
                                                      colname=column.destination_name)
            with self.engine.connect() as con:
                try:
                    inner_type = con.execute(type_fetcher).fetchone()[0]
                    return inner_type
                except Exception:
                    return None

    def __subquery_cast_to_db_type(self, column):
        recast = self.__get_db_type(column)
        if recast is None:
            recast = ''
        else:
            recast = f'::{recast}'
        return recast


    def old_transfer(self):
        cache_manager(clear=False)

        if self.recipe is None:
            self.publish()

        old_table_id = [i for i in self.recipe
                         if not i.new_table and '_id' not in i.destination_name]
        old_table_id = [i for i in old_table_id if i.destination_name not in self.RESERVED_WORDS]

        for column in old_table_id:
            self.__print(msg="Working on column:",column=column)

            update_queries = []

            table_primary_key = f'{self.tablespace}_id'
            if (column.destination_table == self.tablespace):
                table_primary_key = 'id'

            wiper_update = f"UPDATE prod.{column.destination_table} SET {column.destination_name} = :null_zero " \
                           f"WHERE {table_primary_key} IN "

            if column.in_panel_wipe or column.in_panel_zero:
                inner_wipe = wiper_update + f"(SELECT id FROM prod.{self.tablespace} base " \
                                            f"{self.__inner_where_query()})"
                null_zero = self.__zero_type(column.destination_type) if column.in_panel_zero else None
                #print("INNER", null_zero, column.in_panel_wipe, column.in_panel_zero)
                update_queries += [sa.text(inner_wipe).bindparams(null_zero=null_zero)]

            if column.out_panel_wipe or column.out_panel_zero:
                out_wipe = wiper_update + f"(SELECT id FROM prod.{self.tablespace} base " \
                                          f"{self.__inner_where_query(outside=True)})"
                null_zero = self.__zero_type(column.destination_type) if column.out_panel_zero else None
                #print("OUTER", null_zero, column.out_panel_wipe, column.out_panel_zero)
                update_queries += [sa.text(out_wipe).bindparams(null_zero=null_zero)]

            recast_to = self.__subquery_cast_to_db_type(column)

            update_queries += [f"""UPDATE prod.{column.destination_table} dest
                              SET "{column.destination_name}" = base."{column.destination_name}"{recast_to}
                              FROM public.{self.tname_temp} base
                              WHERE dest."{table_primary_key}" = base."{self.level}_id" """]

            with self.engine.connect() as con:

                trans = con.begin()
                try:
                    for query in update_queries:
                        self.__print(msg="Updating under the following system", update_query=str(query))
                        con.execute(query)
                    trans.commit()
                except sa.exc.DataError:
                    warnings.warn(f"""
                    Warning: {column.destination_name} is of an incompatible type in the DB, i.e ({recast_to})
                    This column was not modified in the database.
                    Try casting it in Pandas or changing the DB column to a larger cast (e.g. TEXT).""")
                    trans.rollback()

    def transfer(self, tname='extension', drop_table_if_needed=False):
        self.__print(msg = "Initializing the temporary writer using the fast routines...")
        self.write_temp()
        self.__print("Indexing the temporary table...")
        self.index_temp_table()
        self.__print(msg = "Wiping Spurious Data...")
        self.del_spurios_loaded_data()
        self.__print(msg = "Creating New Table (If needed)...")
        self.new_transfer(tname, drop_table = drop_table_if_needed)
        self.__print(msg = "Updating Tables Already in DB...")
        self.old_transfer()
        self.__print(msg="Cleaning up...")
        self.del_temp()
        # Destruct the recipe so that the object forces itself recreated.
        self.recipe = None


