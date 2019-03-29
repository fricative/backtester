import datetime
from dateutil.relativedelta import relativedelta
import io
from os import path, makedirs

import numpy as np
import pandas as pd
import pymysql

from data.config import PRICE_DATABASE, PRICE_DATABASE_USERNAME, \
    PRICE_DATABASE_PASSWORD, PRICE_SCHEMA, \
    FUNDAMENTAL_DATABASE, FUNDAMENTAL_DATABASE_USERNAME, \
    FUNDAMENTAL_DATBASE_PASSWORD, FUNDAMENTAL_SCHEMA


def get_data_folder():
    BASE_PATH = path.join(path.expanduser('~'), 'backtester_database')
    DATA_FOLDER = {
                    'price': path.join(BASE_PATH, 'price'),
                    'fundamental': path.join(BASE_PATH, 'fundamental'),
                    'benchmark': path.join(BASE_PATH, 'benchmark')
                  }
    for data_dir in DATA_FOLDER.values():
        if not path.isdir(data_dir):
            makedirs(data_dir)            
    return DATA_FOLDER


def load_benchmark(benchmark: str) -> pd.DataFrame:
    """
    benchmark: str of the index name. Make sure there is a csv file 
    with the name as the passed in benchmark existing in benchmark folder
    """
    benchmark_folder = get_data_folder()['benchmark']
    file_path = path.join(benchmark_folder, benchmark + '.csv')
    if not path.isfile(file_path):
        raise FileNotFoundError('%s.csv is not found at %s' % 
                (benchmark, benchmark_folder))
    benchmark_series = pd.read_csv(file_path, index_col=0, parse_dates=True)
    if benchmark_series.shape[1] > 1:
        if benchmark in benchmark_series.columns:
            benchmark_series = benchmark_series.loc[:, [benchmark]]
    else:
        benchmark_series.columns = [benchmark]
    return benchmark_series


def load_adjusted_price(fsym_id: str, start_date: datetime.date=None, 
        end_date: datetime.date=None, adjustment_method: str='f',
        period: str='10y') -> pd.DataFrame:
    """
    Function interface to retrieve adjusted stock price

    start_date: start date of retrieved dataframe date range
    end_date: end date of retrieved dataframe date range
    adjustment_method: 'f' or 'b' for forward or backward adjustment, default
            is forward adjustment in order to avoid look-ahead bias in
            backtesting.
    period: length of the time period to retrieve. End date use today. Will
            be ignored if start date is supplied.
    """

    if not adjustment_method in ('f', 'b'):
        raise ValueError('Unrecognized adjustment method %s' % adjustment_method)
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        time_range = {'D': lambda t: relativedelta(days=-t),
                      'W': lambda t: relativedelta(weeks=-t),
                      'M': lambda t: relativedelta(months=-t),
                      'Y': lambda t: relativedelta(years=-t)}
        periods = int(period[: -1])
        period_unit = period[-1].upper()
        start_date = end_date + time_range[period_unit](periods)

    connection = pymysql.connect(host=PRICE_DATABASE, user=PRICE_DATABASE_USERNAME, 
                        password=PRICE_DATABASE_PASSWORD, db=PRICE_SCHEMA)
    price_dataframe = _load_daily_price(fsym_id, connection, start_date, end_date)
    dividend_dataframe = _load_dividend(fsym_id, connection, start_date, end_date)
    split_dataframe = _load_split(fsym_id, connection, start_date, end_date)
    
    # backward adjustment follows Bloomberg convention
    dataframe = pd.merge(left=price_dataframe, right=dividend_dataframe, 
                left_on='date', right_on='dividend_exdate', how='left')
    dataframe = pd.merge(left=dataframe, right=split_dataframe, 
                left_on='date', right_on='split_exdate', how='left')
    dataframe.fillna(inplace=True, value={'dividend': 0, 'split_factor': 1})

    if adjustment_method == 'b':
        dataframe['dividend'] = dataframe['dividend'].shift(-1)
        dataframe['split_factor'] = dataframe['split_factor'].shift(-1)
        dataframe['cash_factor'] = dataframe['price'] - dataframe['dividend']
        dataframe['cash_factor'] /= dataframe['price']
        total_factor = list(dataframe['cash_factor'] * dataframe['split_factor'])[::-1]
        total_factor[0] = 1
        dataframe['total_factor'] = np.cumprod(total_factor)[::-1]
    else:
        dataframe['cash_factor'] = dataframe['dividend'] + dataframe['price']
        dataframe['cash_factor'] /= dataframe['price']
        total_factor = list(dataframe['cash_factor'] / dataframe['split_factor'])
        dataframe['total_factor'] = np.cumprod(total_factor)
    
    dataframe[fsym_id] = dataframe['price'] * dataframe['total_factor']
    dataframe.set_index('date', inplace=True)
    return dataframe.loc[:, [fsym_id]]


def _load_daily_price(fsym_id: str, db_connection: pymysql.Connection,
        start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    Function to retrieve unadjusted daily price
    """
    base_sql = """ SELECT p_date AS date, p_price AS price FROM 
                    eod.fp_v2_fp_basic_prices WHERE fsym_id = '%s' 
                    AND p_date >= '%s' AND p_date <= '%s' """
    sql = base_sql % (fsym_id, start_date, end_date)
    dataframe = pd.read_sql(sql=sql, con=db_connection)
    return dataframe


def _load_dividend(fsym_id: str, db_connection: pymysql.Connection,
        start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    Function to retrieve dividend
    """
    base_sql = """ SELECT p_divs_exdate AS dividend_exdate, p_divs_pd AS dividend 
                    FROM eod.fp_v2_fp_basic_dividends WHERE fsym_id = '%s' 
                    AND p_divs_exdate >= '%s' AND p_divs_exdate <= '%s' """
    sql = base_sql % (fsym_id, start_date, end_date)
    dataframe = pd.read_sql(sql=sql, con=db_connection)
    return dataframe


def _load_split(fsym_id: str, db_connection: pymysql.Connection,
        start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    Function to retrieve stock split
    """
    base_sql = """ SELECT p_split_date AS split_exdate, 
                    p_split_factor AS split_factor
                    FROM eod.fp_v2_fp_basic_splits WHERE fsym_id = '%s' 
                    AND p_split_date >= '%s' AND p_split_date <= '%s' """
    sql = base_sql % (fsym_id, start_date, end_date)
    dataframe = pd.read_sql(sql=sql, con=db_connection)
    return dataframe


def _get_field_table(fields: [str]) -> dict:
    """
    fields: a list of factset fields
    
    return a dictionary showing for each field, the list of 
            tables that contain this field
    """

    sql = 'SELECT table_name FROM fundamental.ff_v3_ff_metadata WHERE field_name=%s'
    result = {}
    conn = pymysql.connect(host=FUNDAMENTAL_DATABASE, user=FUNDAMENTAL_DATABASE_USERNAME,
                    password=FUNDAMENTAL_DATBASE_PASSWORD, db=FUNDAMENTAL_SCHEMA)
    with conn.cursor() as cursor:
        for field in fields:
            cursor.execute(sql, field)
            r = cursor.fetchall()
            result[field] = [item[0] for item in r]
    return result
        

def load_fundamental_dataframe(fsym_ids: [str], period: str, factset_fields: [str]):
    """
    fsym_ids: a list of factset security identifiers
    factset_fields: a list of factset fields to retrieve from factset 
            SQL database. Fields should only come from tables whose 
            table name contains '_basic_' or '_advanced_'.
    period: periodicity of built dataframe, 'a' for annual, 'q' for quarter,
            'sa' for semi-annual, 'ltm' for last twelve month
    
    returns a dictionary of "fsym_id: dataframe" key, value pair.
    """
    
    def table_selector(period: str, tables: [str]) -> str:
        func = lambda tables, period: [table for table in 
                    tables if period in table.split('_')]
        selector = {'q': func(tables, 'qf'), 'sa': func(tables, 'saf'),
                    'a': func(tables, 'af'), 'ltm': func(tables, 'ltm')}
        tables = selector[period]
        return tables[0] if tables else None

    field_tables = _get_field_table(factset_fields)
    field_table_map = {field: table_selector(period, tables) for 
                      field, tables in field_tables.items()}
                      
    table_field_map, table_dataframe = {}, {}
    for field, table in field_table_map.items():
        if table is None:
            err_msg = 'field %s is not found in tables of periodicity %s' % (field, period)
            raise ValueError(err_msg)
        table_field_map[table] = table_field_map.get(table, []) + [field]
    
    sql = 'SELECT %s FROM fundamental.%s t WHERE t.fsym_id in (%s)' 
    sql_fsym_id_str = ','.join(["'" + fsym_id + "'" for fsym_id in fsym_ids])
    conn = pymysql.connect(host=FUNDAMENTAL_DATABASE, 
                    user=FUNDAMENTAL_DATABASE_USERNAME, 
                    password=FUNDAMENTAL_DATBASE_PASSWORD, 
                    db=FUNDAMENTAL_SCHEMA)

    for table, fields in table_field_map.items():
        fields = list(set(['fsym_id', 'date'] + fields))
        fields_str = ','.join(fields)
        query = sql % (fields_str, 'ff_v3_' + table, sql_fsym_id_str)
        table_dataframe[table] = pd.read_sql(query, con=conn, 
                parse_dates=True, index_col=['date', 'fsym_id'])
    
    merged_dataframe = pd.concat(table_dataframe.values(), axis=1, join='outer')
    return merged_dataframe.sort_index()