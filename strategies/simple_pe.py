from strategies.strategy import Strategy
from strategies.strategy import Strategy

class SimplePE(Strategy):

    @staticmethod
    def required_fields():  return {'ff_div_cf', 'ff_mkt_val', 'ff_free_ps_cf'}


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    
    def digest(self, data, current_date):
        print('================= ' + str(current_date))
        print(data['fundamental'].tail())
        return []