

class Trade:

    def __init__(self, price, quantity, trade_date, *args, **kwargs):
        self.price = price
        self.quantity = quantity
        self.trade_date = trade_date

    
    def __str__(self):
        return {'%s : %s' % (key, val) for key, val in vars(self).items()}