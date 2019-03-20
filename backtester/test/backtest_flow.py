from datetime import date
import unittest

import pandas as pd 

from core.engine import Engine
from core.order import Order
from strategies.strategy import Strategy


class TestStrategy(Strategy):
    
    def digest(self, data, current_date, position):
        orders = []
        price = data['price']
        lastest_price = price.iloc[-1, :]
        # only trade on these two dates
        if current_date.date() in (date(2010, 2, 2), date(2014, 1, 24)):
            for ticker in lastest_price.index:
                if lastest_price[ticker] > 100:
                    orders.append(Order(ticker, 10, current_date))
                elif lastest_price[ticker] < 100:
                    orders.append(Order(ticker, -10, current_date))
        return orders


class TestBackTestFlow(unittest.TestCase):
    
    def setUp(self):
        self.start_date = date(2010, 1, 1)
        self.end_date = date(2015, 12, 31)
        date_range = pd.bdate_range(self.start_date, self.end_date)
        prices = pd.DataFrame(index=date_range)
        prices['aapl'] = 100
        prices['googl'] = 100
        prices.loc[date(2010, 2, 2)] = [102, 98]
        prices.loc[date(2010, 2, 3)] = [90, 95]
        prices.loc[date(2014, 1, 24)] = [98, 102]
        prices.loc[date(2014, 1, 27)] = [99, 101]
        data = {'price': prices}
        self.bt = Engine(universe=['aapl', 'googl'], 
            start_date=self.start_date, end_date=self.end_date, 
            initial_cash=100000)
        self.strategy = TestStrategy()
        self.bt.run(self.strategy, data)

    def test_trade_generation(self):
        trades = self.bt.trades
        for trade in trades:
            print(str(trade))
        self.assertEqual(len(trades), 4)


if __name__ == '__main__':
    unittest.main()