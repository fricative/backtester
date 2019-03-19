from typing import List
from order import Order

from strategies.strategy import Strategy


class SampleStrategy(Strategy):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        

    def digest(self, data, current_date, position) -> List[Order]:
        trades = []
        price_dataframe = data['price']
        aapl = price_dataframe['MH33D6-R']
        if aapl.iloc[-1] / aapl.iloc[-2] > 1.03:
            trade = Order(ticker='MH33D6-R', quantity=1000, order_date=current_date)
            trades = [trade]
        elif aapl.iloc[-1] / aapl.iloc[-2] < .97:
            trade = Order(ticker='MH33D6-R', quantity=-1000, order_date=current_date)
            trades = [trade]
        return trades