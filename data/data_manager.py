from datetime import date
from hashlib import sha256
from os.path import join, isfile
from sys import stdout
from typing import List

import pandas as pd

from data.util import get_data_folder, load_adjusted_price


class DataManager:
  
    @staticmethod
    def create_price_dataframe(start_date: date, end_date: date, \
                universe: List[str]):
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
        dataframe.sort_index(inplace=True)
        dataframe.fillna(inplace=True, method='pad')
        return dataframe.round(decimals=2)


    @staticmethod
    def create_fundamental_dataframe(start_date: date, end_date: date, \
                universe: List[str]):
        return pd.DataFrame()


    @staticmethod
    def data_prep(db_type):
        prep_function = {'fundamental': DataManager.create_fundamental_dataframe,
                         'price': DataManager.create_price_dataframe}
        return prep_function[db_type]


    def __init__(self, *args, **kwargs):
        self.DATA_FOLDER = get_data_folder()
        self.DATAFRAMES = {}
        self.current_row_index = None
        super().__init__(*args, **kwargs)


    def setup(self, start_date: date, end_date: date, universe: list):

        run_id = ' '.join([start_date.isoformat(), 
                          end_date.isoformat(), 
                          ''.join(universe)])
        self.file_key = sha256(run_id.encode()).hexdigest()
        
        for db_type, db_dir in self.DATA_FOLDER.items():
            file_path = join(db_dir, self.file_key + '.csv')
            if not isfile(file_path):
                process_function = DataManager.data_prep(db_type)
                dataframe = process_function(start_date=start_date, end_date=end_date, universe=universe)
                dataframe.to_csv(file_path)
                self.DATAFRAMES[db_type] = dataframe
            else:
                print('Found existing %s, using cached data' % db_type)
                self.DATAFRAMES[db_type] = pd.read_csv(file_path)


    def as_of_date(self, current_date: date):
        data = {}
        for db_type, dataframe in self.DATAFRAMES.items():
            data[db_type] = dataframe[dataframe['date'] < str(current_date)]
        return data