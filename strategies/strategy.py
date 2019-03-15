from typing import List
from trade import Trade


class Strategy:

    @staticmethod
    def required_fields():   return {}

    def __init__(self, data_window_size: int=365, 
                rebalance_freq: str=None, *args, **kwargs):
        self.data_window_size = data_window_size
        self.rebalance_freq = rebalance_freq


    def digest(self, data, current_date) -> List[Trade]:
        raise NotImplementedError('Digest method is NOT implemented')