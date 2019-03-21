from datetime import date

from core.engine import Engine
from strategies.strategy import Strategy
from strategies.simple_macd import SimpleMACD
from strategies.sample_strategy import SampleStrategy

if __name__ == '__main__':

    universe = ['HTM0LK-R',
                'KPK586-R',
                'MH33D6-R',
                'P8R3C2-R']
    
    start_date = date(2017, 1, 1)
    bt = Engine(universe=universe[:100], start_date=start_date)
    strategy = SimpleMACD()
    bt.run(strategy)
    