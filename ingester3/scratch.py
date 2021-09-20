import sqlalchemy as sa
import warnings
import pandas as pd
from diskcache import Cache
from .config import source_db_path
from .config import source_cache_path

cache = Cache(source_cache_path)

#source_db = 'postgres://mihai@hermes:5432/fallback3'
views_engine = sa.create_engine(source_db_path)

meta = sa.MetaData(schema='prod', bind=views_engine)

def cache_manager(clear = False):
    """If the database timestamp is different from the local timestamp,
    flush the cache, and store a new cache timestamp cookie"""
    try:
        timestamp_table = sa.Table('update_stamp',
                                   sa.MetaData(),
                                   schema='prod_metadata',
                                   autoload=True,
                                   autoload_with=views_engine)
        query = sa.select([timestamp_table])
        with views_engine.connect() as conn:
            db_stamp = conn.execute(query).fetchone()[0]
    except sa.exc.OperationalError:
        db_stamp = 0
        warnings.warn("No database connection! Will try to use cache for read-only ops as much as I can")

    try:
        with open('timestamp.cache', mode='r') as f: local_stamp = int(f.read())
    except FileNotFoundError:
        local_stamp = 0

    if local_stamp+db_stamp == 0:
        raise ConnectionError("Cannot connect to the DB and you have NO working cache!")

    if (local_stamp < db_stamp) or (db_stamp > 0 and clear):
        cache.clear(retry=True)
        with open('timestamp.cache',mode='w') as f: f.write(str(db_stamp))

    #print (local_stamp, db_stamp)

@cache.memoize(typed=True, expire=None, tag='fetch_children')
def fetch_children(loa_table, views_engine = views_engine):
    views_leafs = sa.Table('leaf_tables',
                           sa.MetaData(),
                           schema='prod_metadata',
                           autoload=True,
                           autoload_with=views_engine)
    query = sa.select([views_leafs]).where(views_leafs.c.root_table == loa_table)
    with views_engine.connect() as conn:
        data = conn.execute(query)
        results = data.fetchall()
        data = [{'table': row[1], 'id': row[2], 'parent': row[3]} for row in results]
        id_name = 'id'
        data += [{'table': loa_table, 'id': id_name, 'parent': None}]
        return data


def flash_fetch_definitions(schema,table):
    mapper = []
    conn = views_engine.connect()
    new_table = sa.Table(table,
                         sa.MetaData(),
                         schema=schema,
                         autoload=True,
                         autoload_with=views_engine)
    inspector = sa.inspect(new_table)
    #print(">",inspector)
    for column in inspector.c:
        #print (column)
        try:
            col_type = column.type.python_type
        except NotImplementedError:
            col_type = None
        mapper += [{'column_name': column.name,
                    'sa_column': column,
                    'type': col_type}]
    conn.close()
    return mapper



@cache.memoize(typed=True, expire=None, tag='fetch_columns')
def fetch_columns(loa_table):
    tables = fetch_children(loa_table)
    conn = views_engine.connect()
    mapper = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        for table in tables:
            #print(table['table'])
            views_tables = sa.Table(table['table'],
                                    sa.MetaData(),
                                    schema='prod',
                                    autoload=True,
                                    autoload_with=views_engine)
            inspector = sa.inspect(views_tables)
            for column in inspector.c:
                try:
                    min_value = conn.execute(sa.func.min(column)).fetchone()[0]
                    max_value = conn.execute(sa.func.max(column)).fetchone()[0]
                except sa.exc.ProgrammingError:
                    min_value = None
                    max_value = None
                try:
                    mean_value = float(conn.execute(sa.func.avg()).fetchone()[0])
                except Exception:
                    mean_value = None
                try:
                    col_type = column.type.python_type
                except NotImplementedError:
                    col_type = None
                mapper += [{'table': table['table'],
                            'column_name': column.name,
                            'sa_column': column,
                            'type': col_type,
                            'pkey': column.primary_key,
                            'min_value': min_value,
                            'max_value': max_value,
                            'mean_value': mean_value}]
        conn.close()
    return mapper

@cache.memoize(typed=True, expire=None, tag='fetch_keys')
def fetch_keys(loa_table):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        views_tables = sa.Table(loa_table,
                                    sa.MetaData(),
                                    schema='prod',
                                    autoload=True,
                                    autoload_with=views_engine)
        inspector = sa.inspect(views_tables)
        #list_fk = list(inspector.foreign_key_constraints)
        primary_keys = [col for col in inspector.c if col.primary_key]
        foreign_keys = [col for col in inspector.c if len(col.foreign_keys) > 0]
        return primary_keys, foreign_keys


@cache.memoize(typed=True, expire=None, tag="fetch_id")
def fetch_ids(loa_table):
    primary_keys, foreign_keys = fetch_keys(loa_table)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa.exc.SAWarning)
        query_pk = sa.select(primary_keys)
        query_fk = sa.select(foreign_keys)
        if len(foreign_keys)>0:
            query_fk = query_fk.order_by(primary_keys[0])

        with views_engine.connect() as conn:
            pk_data = conn.execute(query_pk).fetchall()
            pk_data = [i[0] for i in pk_data]
            fk_data = conn.execute(query_fk).fetchall()
            return pk_data, fk_data

@cache.memoize(typed=True, expire=None, tag="fetch_ids_df")
def fetch_ids_df(loa_table):
    primary_keys, foreign_keys = fetch_keys(loa_table)
    keys = primary_keys+foreign_keys
    query_keys = sa.select(keys)
    with views_engine.connect() as conn:
        return pd.read_sql(query_keys, con=conn)


def fetch_data(loa_table, columns=None):
    if columns is None:
        return None
    columns = [columns] if isinstance(columns, str) else columns
    columns = set(i.lower() for i in set(columns.copy()))
    fetched = fetch_columns(loa_table)
    db_subset = [j for i in columns for j in fetched if j['column_name'] == i]
    if len(db_subset) == 0:
        raise KeyError("No columns with these names exist in database")
    if len(columns) > len(db_subset):
        found = set([i['column_name'] for i in db_subset])
        not_found =  set(columns) - set(found)
        warnings.warn(f'I could not find columns: {not_found}\nWill proceed with remaining columns: {found}')
    if len(columns) < len(db_subset):
        raise KeyError("Duplicate columns in database! Contact the db administrator!")

    pk, fk = fetch_keys(loa_table)
    base_table = pk[0].table

    children = fetch_children(loa_table)

    sa_columns = [i['sa_column'] for i in db_subset]
    sa_columns += set(pk + fk + sa_columns)
    read_columns = []
    read_tables = []
    read_relations = []
    for column in sa_columns:
        read_columns += [column.table.key + '.' + column.key]
        read_tables += [column.table.key] #if column.table.key != base_table else []
        for relation in children:
            if column.table.name == relation['table'] and relation['parent'] is not None:
                read_relations += [f"{column.table.key}.{relation['id']} = prod.{relation['parent']}.id"]


    col_side = ','.join(set(read_columns))
    from_side = ','.join(set(read_tables))
    where_side = ' AND '.join(set(read_relations))
    text_query = f'SELECT {col_side} FROM {from_side}'
    text_query = text_query + 'WHERE {where_side}' if len(where_side)>0 else text_query
    text_query = sa.text(text_query)
    with views_engine.connect() as con:
        #data_out = con.execute(text_query).fetchall()
        data_out = pd.read_sql(text_query,con)
        return data_out