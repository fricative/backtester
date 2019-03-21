from collections import defaultdict
from datetime import date, timedelta
from hashlib import sha256
import logging
import time
from typing import List, Dict, NewType

import pandas as pd

from core.order import Order
from core.trade import Trade
from core.util import get_logger
from core.metrics_util import calculate_information_ratio, \
        calculate_max_drawdown, calculate_sharpe

from data.data_manager import DataManager
from strategies.strategy import Strategy

Order = NewType('Order', Order)
Trade = NewType('Trade', Trade)


class Engine:

    def __init__(self, universe: List[str], start_date: date, end_date: date=None, 
            initial_cash: float=1000000.0, logger=None, *args, **kwargs):
        self.universe = sorted(universe)
        self.initial_cash = initial_cash
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.current_date = None
        self.data_manager = DataManager()
        self.position = defaultdict(int)
        self.orders = {}
        self.trades = []
        self.logger = logger or get_logger('backtester engine', logging.INFO)


    def initialize(self, strategy, data: Dict[str, pd.DataFrame]=None):
        
        if data is None:
            # make sure the number of dates earlier than backtest start 
            # date is enough to cover the strategy's required data window
            date_buffer = strategy.data_window_size * int(7 / 5) + 20
            data_start_date = self.start_date - timedelta(days=date_buffer)
            self.data_manager.setup(start_date=data_start_date, strategy=strategy, 
                    end_date=self.end_date, universe=self.universe)
        else:
            self.data_manager.DATAFRAMES = data
        
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
                price = self.data_manager.get_ticker_price_for_date(
                        ticker=order.ticker, as_of_date=self.current_date)
                trade = order.fill(price=price, trade_date=self.current_date)
                self.trades.append(trade)
                self.orders['filled'].append(order)
                self.position[order.ticker] += order.quantity
                self.cash -= trade.price * trade.quantity
            elif order.order_type == 'lmt':
                raise NotImplementedError('limit order is NOT implemented yet')
        self.orders['pending'] = unfilled_order


    def post_trade(self):
        mtm = self.cash
        prices = self.data_manager.get_prices_for_date(self.current_date)
        for ticker, quantity in self.position.items():
            mtm  += prices[ticker] * quantity if quantity else 0
        mtm = pd.Series([round(mtm, 4)], index=[self.current_date])
        self.mtm = pd.concat([self.mtm, mtm])
       

    def post_run(self):
        self.max_drawdown = calculate_max_drawdown(
                time_serie=self.mtm, is_return=False)
        self.sharpe = calculate_sharpe(time_series=self.mtm, 
                periodicity='1D', is_return=False)
        self.logger.info('completed backtest run')
        self.run_duration = time.time() - self.run_start_time
        #for param, value in vars(self).items():
        #    print('%s: %s' % (param, str(value)))
        self.mtm.plot()


    def run(self, strategy, data: Dict[str, pd.DataFrame]=None):
        """
        strategy: strategy object that implements Strategy class
        data: dictionary of dataframe for each types of data a strategy
             needs. dictionary should at the minimum contain price dataframe stored
             under 'price' key. If multi-indexed fundamental dataframe is
             also provided, store under 'fundamental' key. This is optional.
             If data is not provided, data will be built from SQL database.
        """
        
        self.run_start_time = time.time()
        self.logger.info('start backtest run')
        self.initialize(strategy, data)

        if bool(len(pd.bdate_range(self.current_date, self.current_date))):
            self.current_date += pd.offsets.BDay()

        while self.current_date.date() <= self.end_date:

            data = self.data_manager.get_market_data(as_of_date=self.current_date)
            # fill any pending orders before passing data into strategy for digestion
            self.execute_trades()
            self.post_trade()

            new_orders = strategy.digest(data=data, 
                current_date=self.current_date, position=self.position)
            self.orders['pending'].extend(new_orders)

            # TODO: optimize based on strategy rebalancing frequency
            
            #self.logger.info('run strategy for %s', self.current_date.date())
            self.current_date += pd.offsets.BDay()

        self.post_run()