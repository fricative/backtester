from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
import logging
from typing import List, NewType

import pandas as pd

from data.data_manager import DataManager
from strategies.strategy import Strategy
from trade import Trade
from metrics_util import calculate_information_ratio, \
        calculate_max_drawdown, calculate_sharpe


Trade = NewType('Trade', Trade)


class Backtester:

    def __init__(self, universe: List[str], start_date: date, 
            end_date: date=None, cash: float=1000000.0, *args, **kwargs):
        self.universe = sorted(universe)
        self.cash = cash
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.current_date = None
        self.data_manager = DataManager()
        self.position = defaultdict(int)
        self.trades = []
        self.pnl = None


    def initialize(self, strategy):
        
        # make sure the number of dates earlier than backtest start 
        # date is enough to cover the strategy's required data window
        date_buffer = strategy.data_window_size * int(7 / 5) + 20
        data_start_date = self.start_date - timedelta(days=date_buffer)
        self.data_manager.setup(start_date=data_start_date, 
            strategy=strategy, end_date=self.end_date, universe=self.universe)
        
        self.current_date = pd.Timestamp(self.start_date)
        self.position = defaultdict(int)
        self.position['cash'] = self.cash
        self.trades = []
        self.mtm = pd.Series()


    def execute_trades(self, trades: List[Trade]):
        for trade in trades:
            trade.trade_date = self.current_date
            self.position[trade.ticker] += trade.quantity
            self.position['cash'] -= trade.price * trade.quantity
            self.trades.append(trade)


    def post_trade(self):
        mtm = 0
        prices = self.data_manager.get_price_for_date(self.current_date)
        for ticker, quantity in self.position.items():
            if quantity != 0:
                ticker_price = prices[ticker] if ticker != 'cash' else 1
                mtm += ticker_price * quantity
        mtm = pd.Series([round(mtm, 2)], index=[self.current_date])
        self.mtm = pd.concat([self.mtm, mtm])
       

    def post_run(self):
        print('Completed backtest run')
        print('List of trades are: ')
        for trade in self.trades:
            print(str(trade))
            print('=====================')
        print(self.mtm)
        print(calculate_sharpe(time_series=self.mtm, periodicity='1D',
                               is_return=False))
        print(calculate_max_drawdown(time_serie=self.mtm,
                               is_return=False))

    def run(self, strategy):
        self.initialize(strategy)
        while self.current_date.date() <= self.end_date:
            
            if self.current_date.isoweekday() in (6, 7):
                increment = 1 if self.current_date.isoweekday() == 7 else 2
                self.current_date += timedelta(days=increment)
                continue

            data = self.data_manager.get_market_data(as_of_date=self.current_date)
            trades = strategy.digest(data=data, current_date=self.current_date)
            self.execute_trades(trades)
            self.post_trade()

            # TODO: optimize based on strategy rebalancing frequency
            
            self.current_date += timedelta(days=1)

        self.post_run()