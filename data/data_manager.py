from datetime import date
from hashlib import sha256
from os.path import join, isdir
from sys import stdout

import pandas as pd

from data.util import get_data_folder, load_adjusted_price


class DataManager:
  
    @staticmethod
    def create_price_dataframe(start_date: date, end_date: date, \
                universe: "list[str]"):
        dataframe = None
        for idx, ticker in enumerate(universe):
            df = load_adjusted_price(fsym_id=ticker, start_date=start_date,
                        end_date=end_date, adjustment_method='f')
            
            # TODO: deal with missing dates

            if idx == 0:
                dataframe = df
                continue

            dataframe = pd.merge(left=dataframe, right=df, how='outer', on='date')
            stdout.write('\rloaded [%d / %d] prices' % (idx + 1, len(universe)))
            stdout.flush()
        dataframe.sort_values(by='date', inplace=True)
        print()
        return dataframe


    @staticmethod
    def create_fundamental_dataframe(start_date: date, end_date: date, \
                universe: "list[str]"):
        return pd.DataFrame()


    @staticmethod
    def data_prep(db_type):
        prep_function = {'fundamental': DataManager.create_fundamental_dataframe,
                         'price': DataManager.create_price_dataframe}
        return prep_function[db_type]


    def __init__(self, *args, **kwargs):
        self.DATA_FOLDER = get_data_folder()
        self.DATAFRAMES = {}
        return super().__init__(*args, **kwargs)


    def setup(self, start_date: date, end_date: date, universe: list):
        
        run_id = ' '.join([start_date.isoformat(), 
                          end_date.isoformat(), 
                          ''.join(universe)])
        self.file_key = sha256(run_id.encode()).hexdigest()
        
        for db_type, db_dir in self.DATA_FOLDER.items():
            file_path = join(db_dir, run_id + '.csv')
            if not isdir(file_path):
                process_function = DataManager.data_prep(db_type)
                dataframe = process_function(start_date=start_date, end_date=end_date, universe=universe)
                dataframe.to_csv(file_path)
                self.DATAFRAMES[db_type] = dataframe
            else:
                print('Found existing %s' % db_type)
                self.DATAFRAMES[db_type] = pd.read_csv(file_path)