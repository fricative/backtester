import os.path

FUNDAMENTAL_DATAFRAME_BUCKET = 'fundamental-dataframe'
FUNDAMENTAL_DATAFRAME_FOLDER = os.path.join(os.path.expanduser('~'), 'backtester_database/fundamental_dataframe/')

PRICE_DATABASE = 'eod.cxoz74rfqq7f.ap-southeast-1.rds.amazonaws.com'
PRICE_DATABASE_USERNAME = 'ops'
PRICE_DATABASE_PASSWORD = 'argusops123'
PRICE_SCHEMA = 'eod'

FUNDAMENTAL_PUBLISH_DELAY = 60          # unit is days