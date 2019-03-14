
class Trade:

    def __init__(self, ticker, order_type, quantity, price, date=None):
        self.ticker = ticker
        self.order_type = order_type,
        self.quantity = quantity    # use negative value to represent short
        self.price = price
        self.trade_date = date


    def __str__(self):
        return ', '.join('%s: %s' % (key, val) for key, val in vars(self).items())