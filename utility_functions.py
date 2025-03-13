
import os
import time
import math
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta

eastern_tz = pytz.timezone('US/Eastern')

start_trading_day_hour = 17

def calculate_trade_day(given_datetime):

    # Extract weekday (0=Monday, 6=Sunday) and hour from the datetime
    weekday = given_datetime.weekday()
    hour = given_datetime.hour
    
    if hour > start_trading_day_hour:
        weekday = (weekday + 1) % 7  # Shift to next weekday, wrapping around using modulo 7
    
    return weekday

def calculate_start_trade_day(given_datetime):
    
    hour = given_datetime.hour

    start_trade_day = given_datetime.replace(hour=start_trading_day_hour, minute=0, second=0, microsecond=0)
    
    if hour < start_trading_day_hour:
        start_trade_day = start_trade_day - timedelta(days=1)
    
    return start_trade_day

def calculate_hour_offset():
    localized_time = datetime.now(eastern_tz)
    utc_offset = localized_time.utcoffset()
    hour_offset = utc_offset.total_seconds() / 3600
    return hour_offset

def get_current_start_week_datetime():
    start_week = datetime.now()
    
    # 5 is Saturday, 6 is Sunday
    if start_week.weekday() >= 5 and start_week.hour < start_trading_day_hour:
        start_week = start_week + timedelta(weeks=1)

    days_to_subtract = (start_week.weekday() + 1) % 7  # 0 = Monday, 6 = Sunday
    start_week = start_week - timedelta(days=days_to_subtract)
    start_week = start_week.replace(hour=0, minute=0, second=0, microsecond=0)

    return start_week  
