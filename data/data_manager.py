from datetime import date, timedelta
from dateutil.parser import parse
from hashlib import sha256
from os.path import join, isfile, isdir
from os import makedirs
from sys import stdout
from typing import List, Dict

import pandas as pd

from data.util import get_data_folder, load_adjusted_price
from data.config import FUNDAMENTAL_DATAFRAME_FOLDER, FUNDAMENTAL_PUBLISH_DELAY


class DataManager:
  
    @staticmethod
    def create_price_dataframe(start_date: date, end_date: date, \
                universe: List[str], file_path:str, strategy):
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
                universe: List[str], file_path: str, strategy):
        
        if not isdir(FUNDAMENTAL_DATAFRAME_FOLDER):
            makedirs(FUNDAMENTAL_DATAFRAME_FOLDER)
            raise FileNotFoundError("Fundamental dataframe folder: %s is empty" \
                    % FUNDAMENTAL_DATAFRAME_FOLDER)

        required_fields = strategy.required_fields()
        dataframes = []
        for ticker in universe:
            file_path = FUNDAMENTAL_DATAFRAME_FOLDER + ticker + '.csv'
            try:
                print(file_path)
                df = pd.read_csv(file_path, parse_dates=['date'], 
                    usecols=list(required_fields) + ['date'])
                df['ticker'] = ticker
                df.set_index(['date', 'ticker'], inplace=True)
                dataframes.append(df)
            except FileNotFoundError:
                print('No fundamental data found for %s' % ticker)
                raise

        dataframe = pd.concat(dataframes, axis=0)
        dataframe.sort_index(inplace=True)
        dataframe.to_csv(file_path)
        return dataframe


    @staticmethod
    def data_prep(db_type):
        prep_function = {'fundamental': DataManager.create_fundamental_dataframe,
                         'price': DataManager.create_price_dataframe}
        return prep_function[db_type]


    def __init__(self, *args, **kwargs):
        self.DATA_FOLDER = get_data_folder()
        self.DATAFRAMES = {}
        self.current_row_index = None


    def setup(self, start_date: date, end_date: date, 
            universe: List[str], strategy) -> None:
        run_id = ' '.join([start_date.isoformat(), 
                          end_date.isoformat(), 
                          ''.join(universe)])
        self.file_key = sha256(run_id.encode()).hexdigest()
        
        for db_type, db_dir in self.DATA_FOLDER.items():
            file_path = join(db_dir, self.file_key + '.csv')
            if not isfile(file_path):
                load_function = DataManager.data_prep(db_type)
                dataframe = load_function(start_date=start_date,
                            end_date=end_date, universe=universe,
                            strategy=strategy, file_path=file_path)
                self.DATAFRAMES[db_type] = dataframe
            else:
                print('Found existing %s, using cached data' % db_type)
                self.DATAFRAMES[db_type] = pd.read_csv(file_path, 
                    index_col='date', parse_dates=['date'])


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
    

    def get_price_for_date(self, as_of_date: date) -> pd.Series:
        price = self.DATAFRAMES['price']
        return price[price.index <= as_of_date].iloc[-1, :].squeeze()