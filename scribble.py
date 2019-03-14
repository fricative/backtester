from datetime import date

from backtester import Backtester
from strategy.strategy import Strategy


if __name__ == '__main__':

    universe = ['HTM0LK-R',
                'P8R3C2-R',
                'CSMTMQ-R',
                'MH33D6-R']
    start_date = date(2019, 3, 1)
    bt = Backtester(strategy=Strategy(), universe=universe[:100], start_date=start_date)
    bt.run()