import os
import time
import pandas as pd
import sys
import holidays
from dateutil.relativedelta import relativedelta
from dateutil.easter import easter
from datetime import datetime, timedelta

from database_reference import get_new_holidays, write_holiday
from trade_timing import get_start_of_week
from constants import START_DAY_TRADING_HOUR
import settings

start_holidays = '2026-01-05'
extra_holidays = [
    ('2011-11-30 17:00', '2011-12-06 17:00', 'CME Close'),
    ('2025-01-08 17:00', '2025-01-09 17:00', 'National Day of Mourning'),
]
show_calculated_holidays = False

def get_good_friday(year):
    """
    Returns the date of Good Friday for a given year.
    """
    easter_sunday = easter(year)  # Get Easter Sunday date
    good_friday = easter_sunday - timedelta(days=2)  # Good Friday is 2 days before Easter
    return good_friday

def get_christmas_weeks(year):
    christmas_date = datetime(year=year,month=12,day=25)

    christmas_week = get_start_of_week(christmas_date)
    after_christmas_week = christmas_week + timedelta(days=14)
    before_christmas_week = christmas_week - timedelta(days=7)

    start_date = christmas_week
    end_date = after_christmas_week
    if year == 2023 or christmas_week.day == 25:
        start_date = before_christmas_week
        end_date = christmas_week + timedelta(days=7)

    end_date = end_date - timedelta(days=2)

    return (start_date,end_date)

def update_holidays():
    print(f'{datetime.now()}: Running update_holidays')

    start_check = pd.Timestamp(start_holidays)
    existing_holidays = get_new_holidays(settings.host, settings.strategies_database, settings.user, settings.password)
    if len(existing_holidays) > 0:
        start_check = max(start_check, existing_holidays["start"].max())    

    now_datetime = datetime.now()
    one_year_future = now_datetime + relativedelta(years=1)

    start_year = start_check.year
    end_year = one_year_future.year

    holiday_list = []

    for year in range(start_year, end_year + 1):
        year_holiday_dates = holidays.US(years=year)
        for (holiday_date, holiday_name) in year_holiday_dates.items():
            if (
                holiday_name.startswith('Veteran') or
                '(observed)' in holiday_name or
                (year < 2022 and holiday_name.startswith('Juneteenth')) or
                (year >= 2012 and holiday_name.startswith('Columbus'))
                ):
                continue
            holiday_list.append((holiday_date, holiday_name))
        
        good_friday = get_good_friday(year)
        holiday_list.append((good_friday, 'Good Friday'))

    holiday_list.sort()

    holidays_dates = []
    for year in range(start_year, end_year + 1):
        (christmas_week_start, christmas_week_end) = get_christmas_weeks(year)
        holidays_dates.append((christmas_week_start, christmas_week_end, 'Christmas Weeks'))
    for historical_holiday in extra_holidays:
        (historical_holiday_start, historical_holiday_end, historical_holiday_name) = historical_holiday
        historical_holiday_start = pd.Timestamp(historical_holiday_start)
        historical_holiday_end = pd.Timestamp(historical_holiday_end)
        holidays_dates.append((historical_holiday_start, historical_holiday_end, historical_holiday_name))

    for holiday_date, holiday_name in holiday_list:
        day_of_week = holiday_date.strftime("%A")  # Get full day name
        # print(f"Processing holiday: {holiday_date}, {day_of_week}, {holiday_name}")

        holiday_date = pd.Timestamp(holiday_date)

        start_holiday = None
        end_holiday = None
        # Good Friday also covers Easter Monday
        if holiday_name == 'Good Friday':
            start_holiday = holiday_date - timedelta(days=2)
            end_holiday = holiday_date + timedelta(days=3)
        elif day_of_week == 'Monday':
            start_holiday = holiday_date - timedelta(days=4)
            end_holiday = holiday_date + timedelta(days=1)
        elif day_of_week == 'Tuesday':
            start_holiday = holiday_date - timedelta(days=5)
            end_holiday = holiday_date
        elif day_of_week == 'Wednesday':
            start_holiday = holiday_date - timedelta(days=2)
            end_holiday = holiday_date + timedelta(days=1)
        elif day_of_week == 'Thursday':
            start_holiday = holiday_date - timedelta(days=2)
            end_holiday = holiday_date + timedelta(days=1)
        elif day_of_week == 'Friday':
            start_holiday = holiday_date - timedelta(days=1)
            end_holiday = holiday_date + timedelta(days=2)
        elif day_of_week == 'Saturday':
            start_holiday = holiday_date - timedelta(days=3)
            end_holiday = holiday_date + timedelta(days=1)
        elif day_of_week == 'Sunday':
            start_holiday = holiday_date - timedelta(days=3)
            end_holiday = holiday_date + timedelta(days=1)
        
        if start_holiday is not None:
            start_holiday = start_holiday.replace(hour=START_DAY_TRADING_HOUR, minute=0, second=0, microsecond=0) 
            end_holiday = end_holiday.replace(hour=START_DAY_TRADING_HOUR, minute=0, second=0, microsecond=0) 
            if start_holiday > start_check:
                holidays_dates.append((start_holiday, end_holiday, holiday_name))
            # print(f"Holiday,{holiday_date},{day_of_week},{holiday_name},{start_holiday},{end_holiday}")
    

    holidays_dates.sort(key=lambda x: x[0])    

    if show_calculated_holidays:
        for holiday in holidays_dates:
            (start_holiday, end_holiday, holiday_name) = holiday
            print(f"Holiday,{start_holiday},{end_holiday},{holiday_name}")

    existing_holidays = set(zip(existing_holidays["start"], existing_holidays["end"], existing_holidays["name"]))
    new_holidays = set((start_holiday, end_holiday, holiday_name) for start_holiday, end_holiday, holiday_name in holidays_dates)
    missing_holidays = new_holidays - existing_holidays
    for holiday in missing_holidays:        
        (start_holiday, end_holiday, holiday_name) = holiday
        print(f'{datetime.now()}: Added new holiday,{start_holiday},{end_holiday},{holiday_name}')
        write_holiday(start_holiday, end_holiday, holiday_name, settings.host, settings.strategies_database, settings.user, settings.password)

    print(f'{datetime.now()}: Finish update_holidays')


if __name__ == "__main__":
    update_holidays()
