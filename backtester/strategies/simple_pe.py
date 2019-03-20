from strategies.strategy import Strategy
from strategies.strategy import Strategy
from core.order import Order


class SimplePE(Strategy):

    @staticmethod
    def required_fields():  return {'ff_pe'}


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    
    def digest(self, data, current_date, position):
        return []