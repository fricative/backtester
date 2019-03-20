from datetime import date

class Trade:

    def __init__(self, price: float, quantity: int, trade_date: date, *args, **kwargs):
        self.price = price
        self.quantity = quantity
        self.trade_date = trade_date

    def __str__(self):
        return ', '.join('%s: %s' % (key, val) for key, val in vars(self).items())