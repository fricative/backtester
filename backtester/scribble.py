from datetime import date

from core.engine import Engine
from strategies.equal_weight_quarterly import EqualWeightQuarterly
from strategies.price_weight_quarterly import PriceWeightQuarterly

from security_universe import top_1000_usa, random_four_stocks

if __name__ == '__main__':

    start_date = date(2011, 1, 12)
    end_date = date(2018, 12, 31)
    bt = Engine(universe=random_four_stocks, start_date=start_date, end_date=end_date)
    strategy = PriceWeightQuarterly()
    bt.run(strategy)