from datetime import date

from backtester import Backtester
from strategies.strategy import Strategy
from strategies.simple_pe import SimplePE


if __name__ == '__main__':

    universe = ['HTM0LK-R',
                'KPK586-R',
                'MH33D6-R',
                'P8R3C2-R']
                
    start_date = date(2018, 1, 1)
    bt = Backtester(universe=universe[:100], start_date=start_date)
    strategy = SimplePE()
    bt.run(strategy)
    print(bt.data_manager.DATAFRAMES['fundamental'])