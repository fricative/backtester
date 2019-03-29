from collections import defaultdict
from datetime import date, timedelta
from typing import List, Dict
from core.order import Order
from strategies.strategy import Strategy


class SimpleMACD(Strategy):


    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)


    def digest(self, data: Dict, current_date: date, position: Dict, cash:float=None) -> List[Order]:
        price = data['price']
        trades = defaultdict(int)
        for ticker, qty in position.items():
            trades[ticker] = -qty
        
        ewma12 = price.ewm(span=12, adjust=False).mean()
        ewma26 = price.ewm(span=26, adjust=False).mean()
        macd = ewma12 - ewma26
        signal = macd.ewm(span=9, adjust=False).mean()
        diff = macd.iloc[-2:, :] - signal.iloc[-2:, :]
        cross = (diff.iloc[0, :] * diff.iloc[1, :]) < 0
        crossed_tickers = cross[cross]
        for ticker in crossed_tickers.index:
            if diff[ticker].iloc[-1] > 0:       # buy signal
                trades[ticker] += 1000
            else:                           # sell signal
                trades[ticker] -= 1000
        
        res = []
        for ticker, quantity in trades.items():
            if quantity:
                res.append(Order(ticker=ticker, order_type='mkt', 
                    quantity=quantity, order_date=current_date))
        return res
