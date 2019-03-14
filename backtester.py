from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
import logging
from typing import List, NewType

from data.data_manager import DataManager
from strategy.strategy import Strategy
from trade import Trade

Strategy = NewType('Strategy', Strategy)
Trade = NewType('Trade', Trade)

class Backtester:

    def __init__(self, strategy: Strategy, universe: List[str], 
                start_date: date, end_date: date=None, *args, **kwargs):
        self.universe = sorted(universe)
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.data_manager = DataManager()
        self.position = defaultdict(int)
        self.trades = []
        super().__init__(*args, **kwargs)


    def initialize(self):
        # make sure the number of dates earlier than backtest start 
        # date is enough to cover the strategy's required data window
        date_buffer = self.strategy.data_window_size * int(7 / 5) + 20
        data_start_date = self.start_date - timedelta(days=date_buffer)
        self.data_manager.setup(start_date=data_start_date, 
                end_date=self.end_date, universe=self.universe)
        self.position = defaultdict(int)
        self.trades = []


    def execute_trades(self, trades: List[Trade], trade_date: date):
        for trade in trades:
            trade.trade_date = trade_date
            self.position[trade.ticker] += trade.quantity
            self.trades.append(trade)


    def post_trade(self):
        pass
    

    def post_run(self):
        print('Completed backtest run')
        print('List of trades are: ')
        for trade in self.trades:
            print(str(trade))
            print('=====================')


    def run(self):

        self.initialize()

        current_date = self.start_date
        while current_date <= self.end_date:
            
            if current_date.isoweekday() in (6, 7):
                increment = 1 if current_date.isoweekday() == 7 else 2
                current_date += timedelta(days=increment)
                continue

            data = self.data_manager.as_of_date(current_date)
            trades = self.strategy.digest(data, current_date)
            self.execute_trades(trades, current_date)
            self.post_trade()

            # TODO: optimize based on strategy rebalancing frequency
            
            current_date += timedelta(days=1)

        self.post_run()