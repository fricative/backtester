from datetime import timedelta

import pandas as pd

from core.order import Order
from strategies.strategy import Strategy

class PriceWeightQuarterly(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def is_quarter_end_business_date(self, date):
        return date - timedelta(days=1) + pd.offsets.BQuarterEnd() == date

    def digest(self, data, current_date, position, cash):
        orders = []
        if self.is_quarter_end_business_date(current_date):
            portfolio_value = cash
            if position:
                portfolio_value += sum(shares * data['price'][ticker].iloc[-1] 
                                    for ticker, shares in position.items())
            total_prices = sum(data['price'][ticker].iloc[-1] for ticker in data['price'].columns)
            print('cash %s, portfolio value %s, target position value %s' % (cash, portfolio_value, total_prices))
            for ticker in data['price'].columns:
                target_shares = portfolio_value * data['price'][ticker].iloc[-1] / total_prices / data['price'][ticker].iloc[-1]
                new_order = Order(ticker=ticker, quantity=target_shares - position.get(ticker, 0), 
                                    order_date=current_date)
                orders.append(new_order)
        return orders    