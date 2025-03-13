import pandas as pd
import numpy as np

from constants import BarTypes, Session, OHLC, DayOfWeek, TRADING_DAY_COUNT, MINUTES_PER_DAY, MINUTES_PER_WEEK, IndicatorReset, \
                    DAILY_EXIT_HOURS_START, DAILY_EXIT_HOURS_END, \
                    DAILY_EXIT_HOURS_ASIA_FINAL_HOUR, DAILY_EXIT_HOURS_LONDON_FINAL_HOUR, DAILY_EXIT_HOURS_US_FINAL_HOUR, DAILY_EXIT_MINUTES_END_SESSION, \
                    DAILY_ENTRY_HOURS_ASIA_START, DAILY_ENTRY_HOURS_ASIA_END, DAILY_ENTRY_HOURS_LONDON_START, DAILY_ENTRY_HOURS_LONDON_END, \
                    DAILY_ENTRY_HOURS_US_START, DAILY_ENTRY_HOURS_US_END, DAILY_ENTRY_MINUTES_START_SESSION
from database_reference import get_database_data, get_risk_events, get_holidays, get_historical_circuit_breakers

def get_start_of_week(datetime):
    start_week = datetime - pd.Timedelta(days=(datetime.weekday() + 1) % 7, hours=datetime.hour - 17)
    start_week = start_week.replace(minute=0, second=0, microsecond=0)
    return start_week

def create_before_timed_entries(exit_period, before_time_exits_minutes):

    before_timed_entries = np.ones(len(exit_period), dtype=bool)

    true_indices = np.where(exit_period)[0]

    for idx in true_indices:
        start_idx = max(0, idx - before_time_exits_minutes)
        before_timed_entries[start_idx:idx+1] = False

    del true_indices

    return before_timed_entries

def create_allowed_entries(bars, indicator_reset):
    
    period_count = bars.shape[2]
    allowed_entry_sessions = np.empty((period_count, len(Session)), dtype=object)
    allowed_entry_days = np.empty((period_count, TRADING_DAY_COUNT), dtype=object)

    allowed_days = create_allowed_days(bars, indicator_reset)
    allowed_sessions = create_session_entries(bars)

    for period_index in range(period_count):
        allowed_entry_sessions[period_index][Session.All.value] = allowed_sessions[period_index][Session.All.value]
        allowed_entry_sessions[period_index][Session.Asia.value] = allowed_sessions[period_index][Session.Asia.value]
        allowed_entry_sessions[period_index][Session.London.value] = allowed_sessions[period_index][Session.London.value]
        allowed_entry_sessions[period_index][Session.US.value] = allowed_sessions[period_index][Session.US.value]

        allowed_entry_days[period_index][DayOfWeek.Monday.value] = allowed_days[period_index][DayOfWeek.Monday.value]
        allowed_entry_days[period_index][DayOfWeek.Tuesday.value] = allowed_days[period_index][DayOfWeek.Tuesday.value]
        allowed_entry_days[period_index][DayOfWeek.Wednesday.value] = allowed_days[period_index][DayOfWeek.Wednesday.value]
        allowed_entry_days[period_index][DayOfWeek.Thursday.value] = allowed_days[period_index][DayOfWeek.Thursday.value]
        allowed_entry_days[period_index][DayOfWeek.Friday.value] = allowed_days[period_index][DayOfWeek.Friday.value]

    return allowed_entry_sessions, allowed_entry_days

def create_allowed_days(bars, indicator_reset):

    period_count = bars.shape[2]

    minutes_in_period = None
    if indicator_reset == IndicatorReset.Daily:
        minutes_in_period = MINUTES_PER_DAY
    elif indicator_reset == IndicatorReset.Weekly:
        minutes_in_period = MINUTES_PER_WEEK

    allowed_days = np.empty((period_count, len(DayOfWeek), minutes_in_period), dtype=bool)

    for period_index in range(period_count):
        allowed_days[period_index][DayOfWeek.Monday.value] = (bars[BarTypes.Minute1.value][OHLC.DayOfWeek.value][period_index] == DayOfWeek.Monday.value)
        allowed_days[period_index][DayOfWeek.Tuesday.value] = (bars[BarTypes.Minute1.value][OHLC.DayOfWeek.value][period_index] == DayOfWeek.Tuesday.value)
        allowed_days[period_index][DayOfWeek.Wednesday.value] = (bars[BarTypes.Minute1.value][OHLC.DayOfWeek.value][period_index] == DayOfWeek.Wednesday.value)
        allowed_days[period_index][DayOfWeek.Thursday.value] = (bars[BarTypes.Minute1.value][OHLC.DayOfWeek.value][period_index] == DayOfWeek.Thursday.value)
        allowed_days[period_index][DayOfWeek.Friday.value] = (bars[BarTypes.Minute1.value][OHLC.DayOfWeek.value][period_index] == DayOfWeek.Friday.value)
        
    return allowed_days

def create_session_entries(bars):

    period_count = bars.shape[2]
    session_entries = np.empty((period_count, len(Session)), dtype=object)

    for period_index in range(period_count):
        
        # Using or because this spans different days for All and Asia 
        session_entries[period_index][Session.All.value] = np.logical_or(
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] >= DAILY_ENTRY_HOURS_ASIA_START, 
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] < DAILY_ENTRY_HOURS_US_END)                
        session_entries[period_index][Session.Asia.value] = np.logical_or(
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] >= DAILY_ENTRY_HOURS_ASIA_START, 
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] < DAILY_ENTRY_HOURS_ASIA_END)
        session_entries[period_index][Session.London.value] = np.logical_and(
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] >= DAILY_ENTRY_HOURS_LONDON_START, 
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] < DAILY_ENTRY_HOURS_LONDON_END)
        session_entries[period_index][Session.US.value] = np.logical_and(
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] >= DAILY_ENTRY_HOURS_US_START, 
                                        bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] < DAILY_ENTRY_HOURS_US_END)
        
        for index in range(len(bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index])):
            datetime = bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index][index]
            if datetime.hour == DAILY_EXIT_HOURS_ASIA_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                session_entries[period_index][Session.Asia.value][index] = False
            elif datetime.hour == DAILY_EXIT_HOURS_LONDON_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                session_entries[period_index][Session.London.value][index] = False
            elif datetime.hour == DAILY_EXIT_HOURS_US_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                session_entries[period_index][Session.US.value][index] = False
                session_entries[period_index][Session.All.value][index] = False
            elif datetime.hour == DAILY_ENTRY_HOURS_ASIA_START and datetime.minute < DAILY_ENTRY_MINUTES_START_SESSION:
                session_entries[period_index][Session.Asia.value][index] = False
                session_entries[period_index][Session.All.value][index] = False
        
    return session_entries

def create_all_exits(host, database, user, password, bars, market, period_lookup, indicator_reset, verbose = False, verbose_path = '.'):
    
    all_exits = create_end_of_day_exits(bars)
    create_risk_events_exits(host, database, user, password, market, bars, all_exits, period_lookup, indicator_reset, verbose, verbose_path)
    create_holidays_exits(host, database, user, password, bars, all_exits, period_lookup, indicator_reset, verbose, verbose_path)
    create_circuit_breaker_exits(host, user, password, market, bars, all_exits, period_lookup, indicator_reset, verbose, verbose_path)
    create_session_end_exits(bars, all_exits)

    return all_exits

def create_end_of_day_exits(bars):    

    period_count = bars.shape[2]
    exits_end_day = np.empty(period_count, dtype=object)

    for period_index in range(period_count):
        period_exits_end_day = np.logical_or(
            bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] == DAILY_EXIT_HOURS_START,
            bars[BarTypes.Minute1.value][OHLC.Hour.value][period_index] == DAILY_EXIT_HOURS_END
        )
        exits_end_day[period_index] = period_exits_end_day

    return exits_end_day

def add_week_start(dataframe):
    dataframe['week_start'] = dataframe['start'].apply(
            lambda dt: dt - pd.Timedelta(days=(dt.weekday() + 1) % 7, hours=dt.hour - 17))
    dataframe['period'] = dataframe['week_start'].dt.to_period('W')

def add_day_start(dataframe):
    dataframe['day_start'] = dataframe['start'].apply(
        lambda dt: dt.replace(hour=18, minute=0, second=0, microsecond=0) if dt.hour >= 18 else 
        (dt - pd.Timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0))
    dataframe['period'] = dataframe['day_start'].dt.to_period('D')

def add_period_start(dataframe, indicator_reset):
    if indicator_reset == IndicatorReset.Daily:
        add_day_start(dataframe)
    elif indicator_reset == IndicatorReset.Weekly:
        add_week_start(dataframe)

def get_period_frequency(indicator_reset):
    if indicator_reset == IndicatorReset.Daily:
        return 'D'
    elif indicator_reset == IndicatorReset.Weekly:
        return 'W'
    return None

def create_risk_events_exits(host, database, user, password, market, bars, all_exits, period_lookup, indicator_reset, verbose = False, verbose_path = '.'):

    risk_events_df = get_risk_events(host, database, user, password, market)

    if risk_events_df is None:
        return

    if verbose:
        filename = f'{verbose_path}/risk_events.csv'
        risk_events_df.to_csv(filename, mode='w', index=False)
        print(f"Risk Events written to {filename}")

    add_period_start(risk_events_df, indicator_reset)
    period_frequency = get_period_frequency(indicator_reset)
    
    period_risk_events = {}
    for period_start, period_index in period_lookup.items():
        period_df = risk_events_df[risk_events_df['period'] == period_start.to_period(period_frequency)]
        period_risk_events[period_index] = period_df

    for period_index in range(len(all_exits)):
        if period_index in period_risk_events:
            for _, (event_code, start, end, adjusted_start, period) in period_risk_events[period_index].iterrows():            
                mask = (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] >= start) & (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] < end)
                all_exits[period_index][mask] = True

def create_holidays_exits(host, database, user, password, bars, all_exits, period_lookup, indicator_reset, verbose = False, verbose_path = '.'):
    holidays_df = get_holidays(host, database, user, password)

    if verbose:
        filename = f'{verbose_path}/holidays.csv'
        holidays_df.to_csv(filename, mode='w', index=False)
        print(f"Holidays written to {filename}")

    add_period_start(holidays_df, indicator_reset)
    period_frequency = get_period_frequency(indicator_reset)

    period_holidays = {}
    for period_start, period_index in period_lookup.items():
        period_df = holidays_df[holidays_df['period'] == period_start.to_period(period_frequency)]
        period_holidays[period_index] = period_df

    for period_index in range(len(all_exits)):
        if period_index in period_holidays:
            for _, (event_code, start, end, adjusted_start, period) in period_holidays[period_index].iterrows():        
                mask = (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] >= start) & (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] < end)
                all_exits[period_index][mask] = True

def create_circuit_breaker_exits(host, user, password, market, bars, all_exits, period_lookup, indicator_reset, verbose = False, verbose_path = '.'):
    circuit_breakers_df = get_historical_circuit_breakers(host, user, password)

    if verbose:
        filename = f'{verbose_path}/circuit_breakers.csv'
        circuit_breakers_df.to_csv(filename, mode='w', index=False)
        print(f"Circuit breakers written to {filename}") 

    add_period_start(circuit_breakers_df, indicator_reset)
    period_frequency = get_period_frequency(indicator_reset)

    period_circuit_breakers = {}
    for period_start, period_index in period_lookup.items():
        period_df = circuit_breakers_df[circuit_breakers_df['period'] == period_start.to_period(period_frequency)]
        period_circuit_breakers[period_index] = period_df   

    for period_index in range(len(all_exits)):
        if period_index in period_circuit_breakers:
            for _, (circuit_breaker_market, start, end, adjusted_start, period) in period_circuit_breakers[period_index].iterrows():        
                if market == circuit_breaker_market:
                    mask = (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] >= start) & (bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index] < end)
                    all_exits[period_index][mask] = True

def create_session_end_exits(bars, all_exits):
    period_count = bars.shape[2]    
    for period_index in range(period_count):
        for index in range(len(bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index])):
            datetime = bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index][index]        
            if datetime.hour == DAILY_EXIT_HOURS_ASIA_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                all_exits[period_index][index] = True
            elif datetime.hour == DAILY_EXIT_HOURS_LONDON_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                all_exits[period_index][index] = True
            elif datetime.hour == DAILY_EXIT_HOURS_US_FINAL_HOUR and datetime.minute >= DAILY_EXIT_MINUTES_END_SESSION:
                all_exits[period_index][index] = True

if __name__ == "__main__":

    start = '2022-01-02'
    end = '2024-11-10'    
    market = 'CL'
    
    host = "10.10.209.61"
    database = "bars"
    strategies_database = "strategies"
    user = "postgres"
    password = "savernake01"

    verbose = True
    verbose_path = '/home/storage/Data/Python'

    before_time_exits_minutes = 15
    indicator_reset = IndicatorReset.Daily

    holidays_df = get_holidays(host, strategies_database, user, password)
    
    bars, period_lookup, period_offsets, period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
        host, user, password, market, start, end, "all", holidays_df, indicator_reset)

    all_exits = create_all_exits(host, strategies_database, user, password, bars, market, period_lookup, indicator_reset, verbose = verbose, verbose_path = verbose_path)
    allowed_entry_sessions, allowed_entry_days = create_allowed_entries(bars, indicator_reset)

    for period_index in range(len(all_exits)):

        datetimes = bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index]

        df_exits = pd.DataFrame({
            'datetime': datetimes,
            'exit': all_exits[period_index]
        })

        filename = f"{verbose_path}/AllExits.{market}.{indicator_reset}.{period_index}.csv"        
        df_exits.to_csv(filename, index=False)
        print(f"Written {filename}")

        df_allowed_entries = pd.DataFrame({
            'datetime': datetimes,
            'All': allowed_entry_sessions[period_index][Session.All.value],
            'Asia': allowed_entry_sessions[period_index][Session.Asia.value],
            'London': allowed_entry_sessions[period_index][Session.London.value],
            'US': allowed_entry_sessions[period_index][Session.US.value],
            'Monday': allowed_entry_days[period_index][DayOfWeek.Monday.value],
            'Tuesday': allowed_entry_days[period_index][DayOfWeek.Tuesday.value],
            'Wednesday': allowed_entry_days[period_index][DayOfWeek.Wednesday.value],
            'Thursday': allowed_entry_days[period_index][DayOfWeek.Thursday.value],
            'Friday': allowed_entry_days[period_index][DayOfWeek.Friday.value]
        })

        filename = f"{verbose_path}/AllEntries.{market}.{indicator_reset}.{period_index}.csv"
        df_allowed_entries.to_csv(filename, index=False)
        print(f"Written {filename}")
