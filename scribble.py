from datetime import date

from backtester import Backtester
from strategies.strategy import Strategy
from strategies.simple_pe import SimplePE


if __name__ == '__main__':

    universe = ['FBBGRD-R',
                'G4BQ9Y-R',
                'Q7DXN9-R']
    start_date = date(2018, 1, 1)
    bt = Backtester(universe=universe[:100], start_date=start_date)
    strategy = SimplePE()
    bt.run(strategy)
    print(bt.data_manager.DATAFRAMES['fundamental'])