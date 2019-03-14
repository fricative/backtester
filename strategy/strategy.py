from trade import Trade


class Strategy:

    def __init__(self, data_window_size: int=100, rebalance_freq: str=None):
        self.data_window_size = data_window_size
        self.rebalance_freq = rebalance_freq
        pass

    def digest(self, data, current_date):
        trades = []
        price_dataframe = data['price']
        aapl = price_dataframe['MH33D6-R']
        if aapl.iloc[-1] / aapl.iloc[-2] > 1.03:
            trade = Trade(ticker='MH33D6-R', order_type='mkt', quantity=10, price=aapl.iloc[-1])
            trades = [trade]
        elif aapl.iloc[-1] / aapl.iloc[-2] < .97:
            trade = Trade(ticker='MH33D6-R', order_type='mkt', quantity=-10, price=aapl.iloc[-1])
            trades = [trade]
        return trades