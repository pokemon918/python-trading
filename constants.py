from enum import Enum
import json
import numpy as np

# 23 * 60
# Assumes standard futures instrument
MINUTES_PER_DAY = 1380 
# 23 * 60 * 5
MINUTES_PER_WEEK = 6900

DEFAULT_DATETIME = np.datetime64('2006-12-31 12:00')

START_DAY_TRADING_HOUR = 17
WEEKS_PER_YEAR = 52
DEFAULT_VOLUME = 0.000001

class DayOfWeek(Enum):
    Monday = 0
    Tuesday = 1
    Wednesday = 2
    Thursday = 3
    Friday = 4
    Saturday = 5
    Sunday = 6
TRADING_DAY_COUNT = 5

class IndicatorReset(Enum):
    Weekly = 0
    Daily = 1

class OHLC(Enum):
    Open = 0
    High = 1
    Low = 2
    Close = 3
    Volume = 4
    DateTime = 5
    Hour = 6
    DayOfWeek = 7
    Symbol = 8

class Session(Enum):
    All = 0
    Asia = 1
    London = 2
    US = 3

class Severity(Enum):
    Critical = 1
    Error = 2
    Warning = 3
    Info = 4

# Trading Decisions
DECISON_SHORT = -1
DECISON_FLAT = 0
DECISON_LONG = 1
DECISON_NONE = 98
DECISON_UNKNOWN = 99

DAILY_START_TRADING_HOURS = 18
DAILY_EXIT_HOURS_START = 16
DAILY_EXIT_HOURS_END = 17

DAILY_EXIT_HOURS_ASIA_START = 2
DAILY_EXIT_HOURS_ASIA_END = 17
DAILY_EXIT_HOURS_LONDON_EVENING_START = 18
DAILY_EXIT_HOURS_LONDON_EVENING_END = 23
DAILY_EXIT_HOURS_LONDON_MORNING_START = 0
DAILY_EXIT_HOURS_LONDON_MORNING_END = 1
DAILY_EXIT_HOURS_US_EVENING_START = 18
DAILY_EXIT_HOURS_US_EVENING_END = 23
DAILY_EXIT_HOURS_US_MORNING_START = 0
DAILY_EXIT_HOURS_US_MORNING_END = 8

DAILY_EXIT_HOURS_ASIA_FINAL_HOUR = 1
DAILY_EXIT_HOURS_LONDON_FINAL_HOUR = 8
DAILY_EXIT_HOURS_US_FINAL_HOUR = 15
DAILY_EXIT_MINUTES_END_SESSION = 55

DAILY_ENTRY_HOURS_ASIA_START = 18
DAILY_ENTRY_HOURS_ASIA_END = 2
DAILY_ENTRY_HOURS_LONDON_START = 2
DAILY_ENTRY_HOURS_LONDON_END = 9
DAILY_ENTRY_HOURS_US_START = 9
DAILY_ENTRY_HOURS_US_END = 16
DAILY_ENTRY_MINUTES_START_SESSION = 5

class BarTypes(Enum):
    Minute1 = 0

class BarMinutes(Enum):
    Minute1 = 1

bartype_minutes = {
    BarTypes.Minute1: BarMinutes.Minute1,
}

barminutes_type = {
    BarMinutes.Minute1: BarTypes.Minute1,
}

minutes_type = {
    BarMinutes.Minute1.value: BarTypes.Minute1,
}

# Integer Contants used within njit calculation of trade which can not use the ExitReason enum
EXIT_REASON_UNKNOWN = 0
EXIT_REASON_STOPLOSS = 1
EXIT_REASON_PROFIT_TARGET = 2
EXIT_REASON_TIMED_EXIT = 3
EXIT_REASON_MAX_LENGTH = 4
EXIT_REASON_NEXT_ENTRY = 5

class ExitReason(Enum):
    Unknown = 0
    Stoploss = 1
    ProfitTarget = 2
    TimedExit = 3
    MaxLength = 4
    NextEntry = 5

class enum_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value  # Return the enum value, not the object
        return super().default(obj)

def enum_decoder(obj):
    if 'session' in obj:
        obj['session'] = Session(obj['session'])  # Convert the integer value back to the Enum member
    return obj

GA_REPLAY_WORKER_COUNT = 16
GA_REPLAY_WORKER_POOL_COUNT = 10
GA_REPLAY_WORKER_RESET_GENERATIONS = 50

class Reoccurring_day(Enum):
    NotReoccurring = 0
    Everyday = 1
    Monday = 2
    Tuesday = 3
    Wednesday = 4
    Thursday = 5
    Friday = 6
    Saturday = 7
    Sunday = 8
