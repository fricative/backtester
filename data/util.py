import datetime
from dateutil.relativedelta import relativedelta
import io
from os import path, makedirs

import numpy as np
import pandas as pd
import pymysql

from data.config import FUNDAMENTAL_DATAFRAME_BUCKET, PRICE_DATABASE, \
    PRICE_DATABASE_USERNAME, PRICE_DATABASE_PASSWORD, PRICE_SCHEMA


def get_data_folder():    
    DATA_FOLDER = {'price': path.join(path.join(path.expanduser('~'), 
                            'backtester_database'), 'price'),
                   'fundamental': path.join(path.join(path.expanduser('~'), 
                            'backtester_database'), 'fundamental')}
    for db_name, data_dir in DATA_FOLDER.items():
        if not path.isdir(data_dir):
            makedirs(data_dir)            
    return DATA_FOLDER


def load_fundamental_dataframe(fsym_id: str) -> pd.DataFrame:
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=FUNDAMENTAL_DATAFRAME_BUCKET, Key=fsym_id + '.csv')
    dataframe = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return dataframe


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



if __name__ == '__main__':
    df = load_adjusted_price('MH33D6-R', start_date=datetime.date(2010, 1, 1), adjustment_method='f')
    print(df)