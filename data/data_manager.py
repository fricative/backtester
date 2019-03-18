from datetime import date, timedelta
from dateutil.parser import parse
from hashlib import sha256
from os.path import join, isfile, isdir
from os import makedirs
from sys import stdout
from typing import List, Dict

import pandas as pd

from data.util import get_data_folder, load_adjusted_price, load_fundamental_dataframe
from data.config import FUNDAMENTAL_PUBLISH_DELAY


class DataManager:
  
    @staticmethod
    def create_price_dataframe(start_date: date, end_date: date, \
                universe: List[str], strategy):

        run_id = ' '.join([start_date.isoformat(), end_date.isoformat(), 
                        ''.join(universe)])
        file_key = sha256(run_id.encode()).hexdigest()
        file_path = join(get_data_folder()['price'], file_key + '.csv')
        
        if isfile(file_path):
            print('found existing cached price data')
            return pd.read_csv(file_path, index_col='date', parse_dates=['date'])

        dataframe = None
        for idx, ticker in enumerate(universe):
            df = load_adjusted_price(fsym_id=ticker, start_date=start_date,
                        end_date=end_date, adjustment_method='f')

            stdout.write('\rloaded [%d / %d] prices' % (idx + 1, len(universe)))
            stdout.flush()

            if idx == 0:
                dataframe = df
                continue
            dataframe = dataframe.join(df, how='outer')
        
        print()
        dataframe = dataframe.round(decimals=2)
        dataframe.index = pd.to_datetime(dataframe.index)
        dataframe.sort_index(inplace=True)
        dataframe.fillna(inplace=True, method='pad')
        dataframe.to_csv(file_path)
        return dataframe


    @staticmethod
    def create_fundamental_dataframe(start_date: date, end_date: date, \
                universe: List[str], strategy):
        
        required_fields = list(strategy.required_fields())
        # return empty dataframe if this is a pure price based strategy
        if len(required_fields) == 0:
            return  pd.DataFrame()      

        run_id = ' '.join([start_date.isoformat(), end_date.isoformat(), 
                ''.join(universe), ','.join(sorted(required_fields))])
        file_key = sha256(run_id.encode()).hexdigest()
        file_path = join(get_data_folder()['fundamental'], file_key + '.csv')

        if isfile(file_path):
            print('found existing cached fundamental data')
            return pd.read_csv(file_path, parse_dates=True, 
                    index_col=['date', 'fsym_id'])

        dataframe = load_fundamental_dataframe(fsym_ids=universe, 
                period='q', factset_fields=required_fields)
        dataframe.to_csv(file_path)
        return dataframe


    @staticmethod
    def data_prep(db_type):
        prep_function = {'fundamental': DataManager.create_fundamental_dataframe,
                         'price': DataManager.create_price_dataframe}
        return prep_function[db_type]


    def __init__(self, *args, **kwargs):
        self.DATAFRAMES = {}
        self.current_row_index = None


    def setup(self, start_date: date, end_date: date, 
            universe: List[str], strategy) -> None:
        for db_type, _ in get_data_folder().items():
            load_function = DataManager.data_prep(db_type)
            self.DATAFRAMES[db_type] = load_function(start_date=start_date,
                    end_date=end_date, universe=universe, strategy=strategy)
        

    def get_market_data(self, as_of_date: date) -> Dict:
        data = {}
        for db_type, dataframe in self.DATAFRAMES.items():
            if db_type == 'price':
                data[db_type] = dataframe[dataframe.index < as_of_date]
            elif db_type == 'fundamental':
                # due to the lag between fiscal period end and the
                # actual publish date of fundamental date we add a 
                # conservative lag here to avoid look ahead bias
                visible_date = as_of_date - timedelta(days=FUNDAMENTAL_PUBLISH_DELAY)
                data[db_type] = dataframe.loc[pd.IndexSlice[:visible_date,:]]
        return data
    

    def get_prices_for_date(self, as_of_date: date) -> pd.Series:
        price = self.DATAFRAMES['price']
        return price[price.index <= as_of_date].iloc[-1, :].squeeze()
        
    
    def get_ticker_price_for_date(self, ticker: str, as_of_date: date) -> float:
        return self.DATAFRAMES['price'][ticker].loc[as_of_date]