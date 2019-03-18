from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
import logging
from typing import List, NewType

import pandas as pd

from data.data_manager import DataManager
from strategies.strategy import Strategy
from order import Order
from trade import Trade
from metrics_util import calculate_information_ratio, \
        calculate_max_drawdown, calculate_sharpe


Order = NewType('Order', Order)
Trade = NewType('Trade', Trade)


class Backtester:

    def __init__(self, universe: List[str], start_date: date, 
            end_date: date=None, initial_cash: float=1000000.0, *args, **kwargs):
        self.universe = sorted(universe)
        self.initial_cash = initial_cash
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.current_date = None
        self.data_manager = DataManager()
        self.position = defaultdict(int)
        self.orders = {}
        self.trades = []


    def initialize(self, strategy):
        
        # make sure the number of dates earlier than backtest start 
        # date is enough to cover the strategy's required data window
        date_buffer = strategy.data_window_size * int(7 / 5) + 20
        data_start_date = self.start_date - timedelta(days=date_buffer)
        self.data_manager.setup(start_date=data_start_date, 
            strategy=strategy, end_date=self.end_date, universe=self.universe)
        
        self.current_date = pd.Timestamp(self.start_date)
        self.position = defaultdict(int)
        self.cash = self.initial_cash
        self.trades = []
        self.orders = {'pending': [], 'filled': [], 'cancelled': []}
        self.mtm = pd.Series()


    def execute_trades(self):
        unfilled_order = []
        for order in self.orders['pending']:

            if order.order_type == 'mkt':
                price = self.data_manager.get_ticker_price_for_date(ticker=order.ticker, 
                                as_of_date=self.current_date)
                trade = order.fill(price=price, trade_date=self.current_date)
                self.trades.append(trade)
                self.orders['filled'].append(order)
                self.position[order.ticker] += order.quantity
                self.cash -= trade.price * trade.quantity

            elif order.order_type == 'lmt':
                raise NotImplementedError('Limit order is NOT implemented yet')
            
        self.orders['pending'] = unfilled_order


    def post_trade(self):
        mtm = self.cash
        prices = self.data_manager.get_prices_for_date(self.current_date)
        for ticker, quantity in self.position.items():
            mtm  += prices[ticker] * quantity if quantity else 0
        mtm = pd.Series([round(mtm, 2)], index=[self.current_date])
        self.mtm = pd.concat([self.mtm, mtm])
       

    def post_run(self):
        self.max_drawdown = calculate_max_drawdown(time_serie=self.mtm,
                               is_return=False)
        self.sharpe = calculate_sharpe(time_series=self.mtm, 
                            periodicity='1D', is_return=False)
        print('Completed backtest run')
        for param, value in vars(self).items():
            print('%s: %s' % (param, str(value)))
        self.mtm.plot()


    def run(self, strategy):
        self.initialize(strategy)
        while self.current_date.date() <= self.end_date:
            
            if self.current_date.isoweekday() in (6, 7):
                increment = 1 if self.current_date.isoweekday() == 7 else 2
                self.current_date += timedelta(days=increment)
                continue

            data = self.data_manager.get_market_data(as_of_date=self.current_date)
            # fill any pending orders before passing data into strategy for digestion
            self.execute_trades()
            self.post_trade()

            orders = strategy.digest(data=data, current_date=self.current_date,
                            position=self.position)
            self.orders['pending'].extend(orders)

            # TODO: optimize based on strategy rebalancing frequency
            
            self.current_date += timedelta(days=1)

        self.post_run()