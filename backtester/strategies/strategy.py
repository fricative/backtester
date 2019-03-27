from datetime import date
from typing import List, Dict
from core.order import Order


class Strategy:

    @staticmethod
    def required_fields():   return {}


    def __init__(self, data_window_size: int=365, 
                rebalance_freq: str=None, *args, **kwargs):
        self.data_window_size = data_window_size
        self.rebalance_freq = rebalance_freq


    def digest(self, data: Dict, current_date: date, position: Dict, cash: float) -> List[Order]:
        raise NotImplementedError('This is an interface class. Digest method is NOT implemented')