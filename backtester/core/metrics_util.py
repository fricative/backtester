import numpy as np
import pandas as pd


DEFAULT_RISK_FREE_RATE = .03
ANNUALIZATION_FACTOR = {'D': 252, 'W': 52, 'M': 12, 'Q': 4, 'Y': 1}


def calculate_sharpe(time_series, periodicity: str, ticker: str=None, 
                        is_return: bool=False)-> float:
    """
    time_series: pd.Serie or pd.DataFrame containing the 
                 time serie to calculate sharpe for
    periodicity: the periodicity of the passed in time_serie, 
                 format is e.g. "1D", "1W", "2W", "1M", "1Q", "1Y"
         ticker: if passed in time_series is dataframe, ticker is 
                 the name of the column to calculate sharpe for
      is_return: whether the passed in time_series is already return
    """

    if not is_return:
        time_series = np.log(time_series / time_series.shift(1))

    period_unit = periodicity[-1].upper()
    periods = int(periodicity[:-1])
    compounding_periods = ANNUALIZATION_FACTOR[period_unit] / periods

    if isinstance(time_series, pd.DataFrame):
        if ticker is None:
            err_msg = 'Must specify which column to calculate sharpe ratio'
            raise AttributeError(err_msg)
        time_series = time_series[ticker]
    
    annualized_return = (1 + time_series.mean()) ** compounding_periods - 1
    standard_deviation = time_series.std() * np.sqrt(compounding_periods)
    if standard_deviation == 0:     
        return np.nan
        
    return (annualized_return - DEFAULT_RISK_FREE_RATE) / standard_deviation


def calculate_max_drawdown(time_serie: pd.Series, is_return: bool=False) -> float:
    """
    time_series: pd.Serie or pd.DataFrame containing the 
                 time serie to calculate max drawdown for
    is_return: whether the passed in time_series is already return
    """

    if is_return:
        time_serie = pd.concat([pd.Series([0]), time_serie]) + 1
        time_serie = np.multiply.accumulate(time_serie)

    cumulative_max = np.maximum.accumulate(time_serie)
    drawdown = (time_serie - cumulative_max) / cumulative_max
    return np.min(drawdown)
    

def calculate_information_ratio(time_series: pd.DataFrame, periodicity: str, 
        ticker: str, benchmark: str, is_return: bool=False) -> float:
    """
    time_series: pd.DataFrame containing the time serie to calculate IC for
    periodicity: the periodicity of the passed in time_serie, 
                 format is e.g. "1D", "1W", "2W", "1M", "1Q", "1Y"
         ticker: if passed in time_series is dataframe, ticker is 
                 the name of the column to calculate sharpe for
      benchmark: column name for benchmark ticker
      is_return: whether the passed in time_series is already return
    """
    
    if not is_return:
        time_series = np.log((time_series / time_series.shift(1)).iloc[1:, :])
    
    period_unit = periodicity[-1].upper()
    periods = int(periodicity[:-1])
    compounding_periods = ANNUALIZATION_FACTOR[period_unit] / periods

    active_return = time_series[ticker] - time_series[benchmark]
    return_std = active_return.std()
    mean_active_return = active_return.mean()
    annualized_excess_return = (1 + mean_active_return) ** compounding_periods - 1
    annualized_excess_risk = return_std * np.sqrt(compounding_periods)
    return annualized_excess_return / annualized_excess_risk