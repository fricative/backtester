from datetime import timedelta

import pandas as pd

class Calendar:

    def __init__(self, calendar, *args, **kwargs):
        """
        calendar: Pandas.DatetimeIndex
        """
        self.calendar = calendar.sort_values()
        self._next_business_date_lookup = {self.calendar[i]: self.calendar[i + 1] 
                for i in range(self.calendar.shape[0] - 1)}

    def is_business_date(self, date):
        return date in self._next_business_date_lookup

    def next_business_date(self, current_date):
        """
        Returns the subsequent business date after current date
        """
        if not current_date in self._next_business_date_lookup:
            raise ValueError('Calendar dose NOT cover provided current date %s' % current_date)
        return self._next_business_date_lookup[current_date]

    def is_week_end_business_date(self, date):
        return self.next_business_date(date).isoweekday() < date.isoweekday()

    def is_month_end_business_date(self, date):
        return self.next_business_date(date).month != date.month

    def is_quarter_end_business_date(self, date):
        return self.next_business_date(date).month != date.month and date.month in (3, 6, 9, 12)

    def is_semiannual_end_business_date(self, date):
        return self.next_business_date(date).month != date.month and date.month == 6

    def is_year_end_business_date(self, date):
        return self.next_business_date(date).year > date.year