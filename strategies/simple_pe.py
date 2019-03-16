from strategies.strategy import Strategy
from strategies.strategy import Strategy

class SimplePE(Strategy):

    @staticmethod
    def required_fields():  return {'ff_roa', 'ff_receiv_tot',
        'ff_sga', 'ff_asset_turn', 'ff_liabs', 'ff_assets'}


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    
    def digest(self, data, current_date):
        return []