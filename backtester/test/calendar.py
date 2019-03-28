from datetime import date
import random
import unittest

from pandas.tseries.holiday import get_calendar
import pandas as pd

from core.calendar import Calendar


class TestCalendar(unittest.TestCase):
    
    def setUp(self):
        self.holiday_calendar = get_calendar('USFederalHolidayCalendar')
        self.time_index = pd.date_range(start='1/1/2013', end='12/31/2013',
                freq=pd.offsets.CDay(calendar=self.holiday_calendar))
        self.calendar = Calendar(self.time_index)

    def test_IsBusinessDate(self):
        self.assertTrue(self.calendar.is_business_date(pd.Timestamp(date(2013, 1, 2))))
        self.assertFalse(self.calendar.is_business_date(date(2013, 7, 4)))

    def test_NextBusinessDate(self):
        date_index = random.randrange(1, 200)
        current_date = self.time_index[date_index]
        next_date = self.time_index[date_index + 1]
        self.assertEqual(next_date, self.calendar.next_business_date(current_date))
        current_date = pd.Timestamp(date(2015, 1, 9))
        self.assertRaises(ValueError)

    def test_IsWeekEndBusinessDate(self):
        self.assertFalse(self.calendar.is_week_end_business_date(pd.Timestamp(date(2013, 1, 2))))
        self.assertTrue(self.calendar.is_week_end_business_date(pd.Timestamp(date(2013, 1, 4))))

    def test_IsMonthEndBusinessDate(self):
        self.assertFalse(self.calendar.is_month_end_business_date(pd.Timestamp(date(2013, 1, 28))))
        self.assertTrue(self.calendar.is_month_end_business_date(pd.Timestamp(date(2013, 1, 31))))

    def test_IsQuarterEndBusinessDate(self):
        self.assertFalse(self.calendar.is_quarter_end_business_date(pd.Timestamp(date(2013, 1, 31))))
        self.assertTrue(self.calendar.is_quarter_end_business_date(pd.Timestamp(date(2013, 3, 29))))



if __name__ == '__main__':
    unittest.main()