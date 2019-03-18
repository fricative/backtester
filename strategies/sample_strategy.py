from typing import List
from trade import Trade

from strategies.strategy import Strategy


class SampleStrategy(Strategy):

    @staticmethod
    def required_fields():   return {}
        

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        

    def digest(self, data, current_date, position) -> List[Trade]:
        trades = []
        price_dataframe = data['price']
        aapl = price_dataframe['MH33D6-R']
        if aapl.iloc[-1] / aapl.iloc[-2] > 1.03:
            trade = Trade(ticker='MH33D6-R', order_type='mkt', quantity=1000, price=aapl.iloc[-1])
            trades = [trade]
        elif aapl.iloc[-1] / aapl.iloc[-2] < .97:
            trade = Trade(ticker='MH33D6-R', order_type='mkt', quantity=-1000, price=aapl.iloc[-1])
            trades = [trade]
        return trades