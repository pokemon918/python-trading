import pandas as pd
import time
import numpy as np

import psycopg2
from psycopg2.extras import execute_values

from constants import BarTypes, bartype_minutes, OHLC, DayOfWeek, DEFAULT_DATETIME, MINUTES_PER_WEEK, MINUTES_PER_DAY, IndicatorReset
from market_reference import market_disable_risk_events

def calculate_trade_day(datetimes):

    # Extract weekday (0=Monday, 6=Sunday) and hour from the datetime
    weekdays = datetimes.dt.weekday
    hours = datetimes.dt.hour
    
    # Shift the weekday if the hour is after 17
    shifted_weekdays = np.where(hours > 17, (weekdays + 1) % 7, weekdays)
    
    return shifted_weekdays

def get_database_data(host, user, password, market, start, end, tag, holidays_df, indicator_reset):
    
    print(f'Getting data from database for {market}')
    database_start_time = time.time()

    bars = None
    period_lookup = {}
    period_offsets = {}
    period_lengths = {}
    day_of_week_lookup = {}

    period_column = None
    period_length = None
    if indicator_reset == IndicatorReset.Daily:
        period_column = 'day'
        period_length = MINUTES_PER_DAY
    elif indicator_reset == IndicatorReset.Weekly:
        period_column = 'week'
        period_length = MINUTES_PER_WEEK
    period_start_column = f'{period_column}_start'

    for bar_type in BarTypes:
        bar_length = bartype_minutes[bar_type].value
        
        bartype_df = get_bars(host, user, password, market, start, end, bar_length)
        if bartype_df is None:
            return None, None, None, None, None, None, None

        allowed = np.ones(len(bartype_df), dtype=bool)
        for _, (event_code, holiday_start, holiday_end) in holidays_df.iterrows():
            # Bars are time stamped to their end of the bar and holidays are full days
            # So remove bars that start after the bar upto on the holiday end
            mask = (bartype_df.index > holiday_start) & (bartype_df.index <= holiday_end)
            allowed[mask] = False
            
        # Adjust the 'datetime' to the previous Sunday at 17:00 (5 PM)
        bartype_df['week_start'] = bartype_df['datetime'].apply(
            lambda dt: dt - pd.Timedelta(days=(dt.weekday() + 1) % 7, hours=dt.hour - 17)
        )
        bartype_df['week'] = bartype_df['week_start'].dt.to_period('W')
        bartype_df['trade_day'] = calculate_trade_day(bartype_df['datetime'])

        bartype_df['day_start'] = bartype_df['datetime'].apply(
            lambda dt: dt.replace(hour=18, minute=0, second=0, microsecond=0) if dt.hour >= 18 else 
            (dt - pd.Timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0))
        bartype_df['day'] = bartype_df['day_start'].dt.to_period('D')
        
        allowed_bars = bartype_df[allowed]
        period_counts = allowed_bars[period_column].value_counts()

        if bars is None:
            bars = np.empty((len(BarTypes), len(OHLC), len(period_counts)), dtype=object)

        period_bars = {period: group for period, group in allowed_bars.groupby(period_column)}

        cumulative_week_offsets = 0
        all_datetimes = allowed_bars['datetime'].values        
        all_closes = allowed_bars['close'].values

        period_index = 0  # Initialize a period index to keep track of the current period in the array
        for period, group in period_bars.items():
            period_start = group[period_start_column].iloc[0]
            week_of_day = group['trade_day'].values
            period_lookup[period_start] = period_index
            
            group_length = len(group)
            period_offsets[period_index] = cumulative_week_offsets
            period_lengths[period_index] = group_length
            cumulative_week_offsets += group_length
        
            bars[bar_type.value][OHLC.DateTime.value][period_index] = np.full(period_length, DEFAULT_DATETIME, dtype='object')
            bars[bar_type.value][OHLC.DateTime.value][period_index][:group_length] = group['datetime']
            bars[bar_type.value][OHLC.Open.value][period_index] = np.full(period_length, 0.0, dtype=np.float64)
            bars[bar_type.value][OHLC.Open.value][period_index][:group_length] = group['open'].to_numpy()
            bars[bar_type.value][OHLC.High.value][period_index] = np.full(period_length, 0.0, dtype=np.float64)
            bars[bar_type.value][OHLC.High.value][period_index][:group_length] = group['high'].to_numpy()
            bars[bar_type.value][OHLC.Low.value][period_index] = np.full(period_length, 0.0, dtype=np.float64)
            bars[bar_type.value][OHLC.Low.value][period_index][:group_length] = group['low'].to_numpy()
            bars[bar_type.value][OHLC.Close.value][period_index] = np.full(period_length, 0.0, dtype=np.float64)
            bars[bar_type.value][OHLC.Close.value][period_index][:group_length] = group['close'].to_numpy()
            bars[bar_type.value][OHLC.Volume.value][period_index] = np.full(period_length, 0.000001, dtype=np.float64)
            bars[bar_type.value][OHLC.Volume.value][period_index][:group_length] = group['volume'].to_numpy()
            bars[bar_type.value][OHLC.Hour.value][period_index] = np.full(period_length, 0, dtype=np.int64)
            bars[bar_type.value][OHLC.Hour.value][period_index][:group_length] = group['datetime'].dt.hour.to_numpy()
            bars[bar_type.value][OHLC.DayOfWeek.value][period_index] = np.full(period_length, DayOfWeek.Saturday.value, dtype=int)
            bars[bar_type.value][OHLC.DayOfWeek.value][period_index][:group_length] = [DayOfWeek(day).value for day in week_of_day]

            if indicator_reset == IndicatorReset.Daily:
                day_of_week_lookup[period_index] = bars[bar_type.value][OHLC.DayOfWeek.value][period_index][0]

            period_index += 1

    database_end_time = time.time()

    database_time = database_end_time - database_start_time
    print(f'Loaded {tag} data for {market} in {database_time} seconds')

    return bars, period_lookup, period_offsets, period_lengths, all_datetimes, all_closes, day_of_week_lookup

def get_bars(host, user, password, market, start, end, bar_length):

    try:
        conn = psycopg2.connect(
            host=host,
            database='bars',
            user=user,
            password=password,
        )

        cursor = conn.cursor()

        query = f"""
        SELECT "end" as datetime, symbol, open, high, low, close, volume
        FROM {market}
        WHERE start >= %s AND start < %s
        and size = %s
        ORDER BY start
        """

        cursor.execute(query, (start, end, bar_length))

        data = cursor.fetchall()

        cursor.close()
        conn.close()    

        df = pd.DataFrame(data, columns=["datetime", "symbol", "open", "high", "low", "close", "volume"])

        df['datetime'] = pd.to_datetime(df['datetime'])

        df.set_index('datetime', inplace=True, drop=False)

        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['high'] = pd.to_numeric(df['high'], errors='coerce')
        df['low'] = pd.to_numeric(df['low'], errors='coerce')
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

        return df
    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        return None

def bars_batch_insert(data, market, host, user, password):

    conn = psycopg2.connect(
        host=host,
        database='bars',
        user=user,
        password=password
    )

    data_columns = list(data)
    columns = ",".join(data_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in data_columns])) 
    insert_statement = "INSERT INTO {} ({}) {}".format(market, columns, values)

    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, insert_statement, data.values)
    conn.commit()
    cur.close()

def get_risk_events(host, database, user, password, market):

    if market in market_disable_risk_events and market_disable_risk_events[market]:
        return None

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    event_query = f"""
        SELECT event_code,
        (event_date - stop_before_event * INTERVAL '1 minute') AS START,
        (event_date + resume_after_event * INTERVAL '1 minute') AS END
        FROM risk_event_calendar 
        INNER JOIN risk_event_types ON
        risk_event_calendar.event_id = risk_event_types.event_id
        INNER JOIN risk_event_markets ON
        risk_event_markets.event_id = risk_event_calendar.event_id AND risk_event_markets.market = %s
    """

    cursor.execute(event_query, (market,))

    event_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(event_data, columns=["event_code", "start", "end"])
    df['start'] = pd.to_datetime(df['start'])
    df.set_index('start', inplace=True, drop=False)
    df['end'] = pd.to_datetime(df['end'])
    df.set_index('end', inplace=True, drop=False)

    return df

def get_historical_circuit_breakers(host, user, password):
    conn = psycopg2.connect(
        host=host,
        database='instrument_reference',
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT market, START, historical_circuit_breakers.end FROM historical_circuit_breakers
        """

    cursor.execute(query)

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["market", "start", "end"])
    df['start'] = pd.to_datetime(df['start'])
    df.set_index('start', inplace=True, drop=False)
    df['end'] = pd.to_datetime(df['end'])
    df.set_index('end', inplace=True, drop=False)

    return df

def get_instruments(host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT market, exchange, symbol, expiry, tick_size, start_date, end_date from instrument_reference
        """

    cursor.execute(query)

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["market", "exchange", "symbol", "expiry", "tick_size", "start_date", "end_date"])
    df['expiry'] = pd.to_datetime(df['expiry'])
    df['start_date'] = pd.to_datetime(df['start_date'])
    df.set_index('start_date', inplace=True, drop=False)
    df['end_date'] = pd.to_datetime(df['end_date'])
    df.set_index('end_date', inplace=True, drop=False)

    return df

def get_current_instruments(now, markets, host, strategies_database, user, password):

    markets_sql = ",".join([f"'{market}'" for market in markets])
    
    query = f"""
        SELECT instrument_reference.market, instrument_reference.exchange, instrument_reference.symbol, 
        instrument_reference.expiry, instrument_reference.tick_size, instrument_reference.start_date, 
        instrument_reference.end_date FROM instrument_reference 
        WHERE instrument_reference.market IN ({markets_sql}) AND 
        (start_date <= %s AND end_date > %s)
    """
    
    conn = psycopg2.connect(
        host=host,
        database=strategies_database,
        user=user,
        password=password,
    )
    
    cur = conn.cursor()
    cur.execute(query, (now, now))

    result = cur.fetchall()

    cur.close()
    conn.close()

    df = pd.DataFrame(result, columns=["market", "exchange", "symbol", "expiry", "tick_size", "start_date", "end_date"])
    df['expiry'] = pd.to_datetime(df['expiry'])
    df['start_date'] = pd.to_datetime(df['start_date'])
    df.set_index('start_date', inplace=True, drop=False)
    df['end_date'] = pd.to_datetime(df['end_date'])
    df.set_index('end_date', inplace=True, drop=False)

    return df

def get_last_price_datetime(market, host, user, password):

    conn = psycopg2.connect(
        host=host,
        database='bars',
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT max(start) from {market}
        """

    cursor.execute(query)

    result = cursor.fetchone()
    max_start_datetime = result[0] if result else None

    cursor.close()
    conn.close()

    return max_start_datetime

def get_main_contracts(host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT market, main_contract_month from main_contract_months
        """

    cursor.execute(query)

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["market", "main_contract_month"])

    return df

def write_instrument(market, exchange, symbol, expiry, tick_size, host, database, user, password):
    query = """
        INSERT INTO "instrument_reference" 
            ("market", "exchange", "symbol", "expiry", "tick_size")
            VALUES (%s, %s, %s, %s, %s);
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (market, exchange, symbol, expiry, tick_size))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def get_holidays(host, database, user, password):

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cursor = conn.cursor()

        query = f"""
            SELECT holidays.name, holidays.start, holidays.end FROM holidays
            """

        cursor.execute(query)

        market_data = cursor.fetchall()

        cursor.close()
        conn.close()

        df = pd.DataFrame(market_data, columns=["name", "start", "end"])
        df['start'] = pd.to_datetime(df['start'])
        df.set_index('start', inplace=True, drop=False)
        df['end'] = pd.to_datetime(df['end'])
        df.set_index('end', inplace=True, drop=False)

        return df
    
    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        return None

def write_holiday(start, end, name, host, database, user, password):
    query = """
        INSERT INTO "holidays" 
            ("start", "end", "name")
            VALUES (%s, %s, %s);
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (start, end, name))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def get_risk_event_types(host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT event_id, event_code from risk_event_types 
        """

    cursor.execute(query)
    data = cursor.fetchall()
    
    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=["event_id", "event_code"])
    
    return df

def write_new_event_type(event_id, event, markets, host, database, user, password):
    query_type = """
        INSERT INTO "risk_event_types" 
            ("event_id", "event_code", "stop_before_event", "resume_after_event")
            VALUES (%s, %s, 10, 10);
        """
    
    query_market = """
        INSERT INTO "risk_event_markets" 
            ("event_id", "market")
            VALUES (%s, %s);
        """

    try:
        with psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query_type, (event_id, event))
                
                # Prepare data for batch insert into risk_event_markets
                market_values = [(event_id, market) for market in markets]                
                # Batch insert for all markets
                cur.executemany(query_market, market_values)
                # Commit after all operations are successful
                conn.commit()

    except Exception as error:
        print(f"Error occurred: {error}")

def get_new_risk_event_dates(start_date, host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT event_id, event_date from risk_event_calendar where event_date >= %s
        """

    cursor.execute(query, (start_date,))
    data = cursor.fetchall()
    
    cursor.close()
    conn.close()

    df = pd.DataFrame(data, columns=["event_id", "event_date"])
    df['event_date'] = pd.to_datetime(df['event_date'])
    
    return df

def write_risk_event_date(event_id, event_date, host, database, user, password):
    query = """
        INSERT INTO "risk_event_calendar"
            ("event_id", "event_date")
            VALUES (%s, %s);
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()

        cur.execute(query, (event_id, event_date))

        conn.commit()

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def delete_risk_event_date(event_id, event_date, host, database, user, password):
    delete_query = """
        DELETE FROM "risk_event_calendar" where "event_id" = %s and "event_date" = %s;
        """

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
        )

        cur = conn.cursor()
        
        cur.execute(delete_query, (event_id, event_date))

        cur.close()
        conn.close()

    except Exception as error:
        print(f"Error occurred: {error}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def get_parser_config(source, bar_type, host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT start_index, end_index, open_index, high_index, low_index, close_index, 
        volume_index, calculate_forward_fill,
        name_start, name_end, name_open, name_high, name_low, name_close, name_volume
          from parser_config where source = %s and bar_type = %s
        """
    
    cursor.execute(query, (source, bar_type))

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["start_index", "end_index", "open_index", "high_index", "low_index", "close_index", 
                                            "volume_index", "calculate_forward_fill", "name_start", 
                                            "name_end", "name_open", "name_high", "name_low", "name_close", "name_volume"])
    return df

def get_broker_mappings(broker, host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT market, broker_market from broker_mappings where broker = %s
        """
    
    cursor.execute(query, (broker,))

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(market_data, columns=["market", "broker_market"])
    return df

def get_broker_connections(broker, host, database, user, password):

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
    )

    cursor = conn.cursor()

    query = f"""
        SELECT market from broker_connections where broker = %s
        """
    
    cursor.execute(query, (broker,))

    market_data = cursor.fetchall()

    cursor.close()
    conn.close()

    market_list = [row[0] for row in market_data]

    return market_list

def batch_upsert_bars(data, market, host, user, password):

    conn = psycopg2.connect(
        host=host,
        database='bars',
        user=user,
        password=password
    )

    data_columns = list(data)
    columns = ",".join(data_columns)
    placeholders = ",".join(["%s" for _ in data_columns])
    
    on_conflict = (
        "ON CONFLICT (size, start, end) DO UPDATE SET "
        "open = EXCLUDED.open, "
        "high = EXCLUDED.high, "
        "low = EXCLUDED.low, "
        "close = EXCLUDED.close, "
        "volume = EXCLUDED.volume, "
        "symbol = EXCLUDED.symbol"
    )
    
    insert_statement = (
        f"INSERT INTO {market} ({columns}) "
        f"VALUES ({placeholders}) {on_conflict}"
    )

    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, insert_statement, data.values)
    conn.commit()
    cur.close()

if __name__ == "__main__":   

    start = '2022-01-01'
    end = '2024-11-10'    
    market = 'HG'
    verbose_path = '/home/storage/Data/Python'

    host = "10.10.209.61"
    user = "postgres"
    strategies_database = "strategies"
    password = "savernake01"

    indicator_reset = IndicatorReset.Daily

    holidays_df = get_holidays(host, strategies_database, user, password)
    bars, period_lookup, period_offsets, period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
        host, user, password, market, start, end, "all", holidays_df, indicator_reset)
    
    filename = f'{verbose_path}/holidays.csv'
    holidays_df.to_csv(filename, mode='w', index=False)
    print(f"Holidays written to {filename}")    

    if indicator_reset == IndicatorReset.Daily:
        daily_filename = f'{verbose_path}/daily_index.csv'
        with open(daily_filename, 'w') as file:
            file.write('Index,Day\n')
            for key, value in day_of_week_lookup.items():
                file.write(f'{key},{value}\n')        
   
    for bar_type in BarTypes:
        bar_length = bartype_minutes[bar_type].value
        period_count = bars.shape[2]

        for period_index in range(period_count):
        
            bar_data = {
                    'datetime': bars[bar_type.value][OHLC.DateTime.value][period_index],
                    'open': bars[bar_type.value][OHLC.Open.value][period_index],
                    'high': bars[bar_type.value][OHLC.High.value][period_index],
                    'low': bars[bar_type.value][OHLC.Low.value][period_index],
                    'close': bars[bar_type.value][OHLC.Close.value][period_index],
                    'volume': bars[bar_type.value][OHLC.Volume.value][period_index],
                    'hour': bars[bar_type.value][OHLC.Hour.value][period_index],
                    'day': bars[bar_type.value][OHLC.DayOfWeek.value][period_index],
                    }
            bar_data_df = pd.DataFrame(bar_data, index=bars[bar_type.value][OHLC.DateTime.value][period_index])

            filename = f'{verbose_path}/bar_data.{bar_length}.{market}.{indicator_reset}.{period_index}.csv'
            bar_data_df.to_csv(filename, mode='w', index=True)
            print(f"Bar Data {bar_length} written to {filename}")
        