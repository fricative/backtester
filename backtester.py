from datetime import date
from hashlib import sha256
import logging

from data.data_manager import DataManager


class Backtester:

    def __init__(self, universe: "list[str]", start_date: date, 
                 end_date: date=None, *args, **kwargs):
        self.universe = sorted(universe)
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.data_manager = DataManager()
        return super().__init__(*args, **kwargs)

    def run(self):
        self.initialize()

    def initialize(self):
        self.data_manager.setup(self.start_date, self.end_date, self.universe)


if __name__ == '__main__':

    universe = ['MH33D6-R', "HTM0LK-R", "HKP8JK-R"]
    start_date = date(2010, 1, 1)
    bt = Backtester(universe=universe, start_date=start_date)
    bt.run()