from datetime import date

import pandas as pd

from core.trade import Trade

class Order:

    def __init__(self, ticker: str, quantity: int, order_date: date, 
                order_type='mkt', price: float=None):
        """
        order_type: accepts one of ['lmt', 'mkt'] order type
        price: when order_type is 'mkt', price is ignored
        """
        self.ticker = ticker
        self.order_type = order_type
        self.quantity = quantity    # use negative value to represent short
        self.price = price if order_type == 'lmt' else None
        self.order_date = self._next_business_date(order_date)
        self.trade_date = None
        self.status = 'pending'


    def _next_business_date(self, current_business_date: date):
        return (current_business_date + pd.offsets.BDay()).date()


    def fill(self, price, trade_date) -> Trade:
        self.trade_date = trade_date
        self.price = price
        self.status = 'filled'
        return Trade(price=self.price, quantity=self.quantity, 
                    ticker=self.ticker, trade_date=trade_date)


    def cancel(self) -> None:
        self.status = 'cancelled'


    def __str__(self):
        return ', '.join('%s: %s' % (key, val) for key, val in vars(self).items())