from .config import source_db_path
from .scratch import fetch_columns
import sqlalchemy as sa
import pandas as pd
import re
from dataclasses import dataclass
from uuid import uuid1
from io import StringIO


@dataclass
class ColumnMapper:
    origin_name: str
    destination_name: str
    origin_type: type
    destination_type: type
    destination_table: str
    same_type: bool
    new_table: bool = False

class DBWriter(object):

    def __match_names(self, a, b):
        a1 = self.__matching_pattern.sub('', a).lower()
        a2 = self.__matching_pattern.sub('', b).lower()
        if a2 == a1:
            #print('XXX',a,b, '>>>',a1, a2)
            return True
        return False

    def __init__(self, pandas_obj, level='cm'):
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

    def set_time_extents(self, time_list=None):
        self.time_extent = time_list

    def set_time_extents_min_max(self, time_min=100, time_max=600):
        self.time_extent = list(range(time_min,time_max+1))

    def set_space(self, space_list=None):
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
        print(tbl_colset)
        column_mappers = []
        for df_column in self.df.columns:
            column_match = None
            for tbl_column in tbl_colset:
                if self.__match_names(tbl_column['column_name'],df_column):
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
                        same_type=match_type
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



        return column_mappers

    def write_temp(self):
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
        out.seek(0)  # sets the pointer on the file object to the first line
        contents = out.getvalue()
        cur.copy_from(out, sep='|', escape='"'
                      table = f'loader.{self.tname_temp}',
                      null="")  ## copies the contents of the file object into the SQL cursor and sets null values to empty strings
        raw_con.commit()
        cur.close()
        raw_con.close()
        return 0


