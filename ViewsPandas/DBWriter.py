from .config import source_db_path
from .scratch import fetch_columns, flash_fetch_definitions, cache_manager
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


    def __init__(self, pandas_obj, level='cm'):

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

        # Set writer index
        other_ids = [i for i in list(self.df.columns) if (i != 'cm_id' and '_id' in i)]
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
                        new_table=False
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
                        new_table=True
                )
                column_mappers.append(column_match)

        self.recipe = column_mappers

    def __rename_headers(self):
        for column in self.recipe:
            print("Test", column.origin_name , column.destination_name)
            if column.origin_name != column.destination_name:
                print ("CHG: ", column.origin_name , column.destination_name)
                self.df = self.df.rename({column.origin_name:column.destination_name},axis=1)
        print(self.df.head(3))

    def write_temp(self):
        if self.recipe is None:
            self.publish()

        self.__rename_headers()

        self.df.head(0).to_sql(name=self.tname_temp,
                               schema='loader',
                               con=self.engine,
                               index=False)
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
                      table = f'loader.{self.tname_temp}',
                      null="")
            raw_con.commit()
        except psycopg2.errors.BadCopyFileFormat:
            try_simple = True
            print("Format is too complicated for a COPY fast write. Trying fallback alternative...")
        finally:
            cur.close()
            raw_con.close()
        if try_simple:
            with self.engine.connect() as con:
                self.df.to_sql(con=con, schema='loader', name=self.tname_temp, if_exists="append", index=False)
        return 0

    def del_temp(self):
        with self.engine.connect() as con:
            trans = con.begin()
            con.execute(f"DROP TABLE IF EXISTS loader.{self.tname_temp}")
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

        return inner_where_query

    def __get_zero_columns(self, which_columns, inside = True):

        key_types = flash_fetch_definitions(schema='loader', table=self.tname_temp)

        if inside:
            zero_inside = [i.destination_name for i in self.recipe
                           if i.destination_name in set(which_columns) and i.in_panel_zero]
        else:
            zero_inside = [i.destination_name for i in self.recipe
                           if i.destination_name in set(which_columns) and i.out_panel_zero]

        types_zero_inside = {i['column_name']: self.__zero_type(i['type']) for i in key_types if
                             i['column_name'] in set(zero_inside)}

        return zero_inside, types_zero_inside


    def new_transfer(self, tname):

        cache_manager(clear=False)

        if self.recipe is None:
            self.publish()

        new_table_ids = [i.destination_name for i in self.recipe
                         if i.new_table and '_id' not in i.destination_name]

        if len(new_table_ids) == 0:
            return 0

        zero_inside, types_zero_inside =  self.__get_zero_columns(new_table_ids)
        zero_outside, types_zero_outside = self.__get_zero_columns(new_table_ids, False)

        sql_copy = sa.text(f"""
        DROP TABLE IF EXISTS prod.{tname};
        CREATE TABLE prod.{tname} AS 
        SELECT * FROM 
        ((
        SELECT base.id AS {self.tablespace}_id, 
               {self.__make_coalesce(new_table_ids,zero_inside)}
        FROM prod.{self.tablespace} base LEFT JOIN loader.{self.tname_temp} nnew
        ON (base.id::bigint = nnew.{self.level}_id::bigint)  
        {self.__inner_where_query()}
        )
        UNION ALL
        (
        SELECT base.id AS {self.tablespace}_id, 
               {self.__make_coalesce(new_table_ids,zero_outside)}
        FROM prod.{self.tablespace} base LEFT JOIN loader.{self.tname_temp} nnew
        ON (base.id::bigint = nnew.{self.level}_id::bigint) 
        {self.__inner_where_query(outside=True)}
        )) merged_final WHERE merged_final.{self.tablespace}_id IS NOT NULL;
        """).bindparams(**types_zero_inside, **types_zero_outside)

        sql_reference = sa.text(f"""
        ALTER TABLE prod.{tname} ADD PRIMARY KEY ({self.tablespace}_id);
        ALTER TABLE prod.{tname} ADD  CONSTRAINT fk_{self.tablespace}_id
        FOREIGN KEY({self.tablespace}_id)
	  REFERENCES prod.{self.tablespace}(id);
        """)
        print(sql_copy)

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

    def __subquery_cast_to_db_type(self,column):
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
        for column in old_table_id:
            print(column)

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
                              FROM loader.{self.tname_temp} base 
                              WHERE dest."{table_primary_key}" = base."{self.level}_id" """]

            with self.engine.connect() as con:
                print(update_queries)
                trans = con.begin()
                try:
                    for query in update_queries:
                        con.execute(query)
                    trans.commit()
                except sa.exc.DataError:
                    warnings.warn(f"""
                    Warning: {column.destination_name} is of an incompatible type in the DB, i.e ({recast_to})
                    This column was not modified in the database. 
                    Try casting it in Pandas or changing the DB column to a larger cast (e.g. TEXT).""")
                    trans.rollback()
