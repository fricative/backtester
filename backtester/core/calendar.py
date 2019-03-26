

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