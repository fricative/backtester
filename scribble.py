from datetime import date

from backtester import Backtester
from strategy.strategy import Strategy


if __name__ == '__main__':

    universe = ['B00242-R',
                'B00484-R',
                'B0060V-R',
                'B007G7-R',
                'B00FG1-R',
                'B00FVF-R',
                'B00K1J-R',
                'B00M9Q-R',
                'B00N7M-R',
                'B00TPX-R',
                'MH33D6-R']
    start_date = date(2010, 1, 1)
    bt = Backtester(strategy=Strategy(), universe=universe[:100], start_date=start_date)
    bt.run()