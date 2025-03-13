import redis
import os
import time
import math
import pandas as pd
import numpy as np
from threading import Timer
from datetime import datetime, timedelta

import json
import pytz
import sys
import ib_async
import asyncio

from constants import BarMinutes, BarTypes, OHLC, Severity
from database_health import write_activity
from database_reference import get_bars, get_holidays, get_current_instruments, get_broker_connections
from data_integrity import check_data_integrity
from utility_functions import get_current_start_week_datetime
from historical_data_parser import prepare_bars, convert_data
import settings

# IB Async Docs
# https://ib-api-reloaded.github.io/ib_async/api.html

# Add unique constraint to the bars database for newly added markets
# Ensure that there are no duplicates
# SELECT size, START, "end", COUNT(*) AS duplicate_count
# FROM cl
# GROUP BY size, start, "end"
# HAVING COUNT(*) > 1;

# Add the constraint
# ALTER TABLE cl ADD CONSTRAINT cl_unique_size_start_end UNIQUE (size, START, "end");

# Notes
# store live bars as a list, not a dataframe. Use market, BarType and data type. dataframes are inefficient to add row by row
# Create live bars
# Do not create the live bar that used partial data
# After the first created bar, then get historical data
# Combine historical data and live data into a dataframe. There will be a gap if started during trading hours
# Check data integrity to get list of errors
# Use IB historical data to get error/missing data
# Reduce IB historical data to only error/missing bars
# combine IB historical data to historical data and live bars
# convert combined historical database bars, historical IB bars and live bars into a list. 
# create redis cache
# append new bars to list and redis cache

save_activity = False
save_excel = True
save_excel_bars = True
save_excel_errors = True
save_excel_merged = True

broker_source_name = 'IB'
source_type = 'historical_1minute'

start_trading_day_hour = 17
eastern_tz = pytz.timezone('US/Eastern')

status_enabled = 'enabled'
status_processed_database_bars = 'processed_database_bars'
          
def subscribe_to_live_data(ib, unique_markets, contracts):
    for market in unique_markets:

        contract = contracts[market]
       
        # Second parameter is 
        # https://interactivebrokers.github.io/tws-api/tick_types.html
        # 375 = RT Trade Volume
        ib.reqMktData(contract, '375', False, False)  

def get_ib_contracts(unique_markets, instruments_df):

    contracts = {}
    market_lookup = {}
    symbol_lookup = {}
    for market in unique_markets:

        row = instruments_df.loc[instruments_df['market'] == 'GC']
        
        expiry_date = row['expiry'].iloc[0].strftime('%Y%m')
        exchange = row['exchange'].iloc[0]
        contract = ib_async.Future(symbol=market, exchange=exchange, lastTradeDateOrContractMonth=expiry_date, currency='USD')
        # contract = ib_async.Forex(market)

        contract.includeExpired = False

        ib.qualifyContracts(contract)
        
        contracts[market] = contract
        market_lookup[contract.localSymbol] = market
        symbol_lookup[market] = row['symbol'].iloc[0]
        print(f'{datetime.now()}: Contract {market}: {contracts[market]}. localSymbol={contract.localSymbol}')

    return contracts, market_lookup, symbol_lookup

def prepare_data(unique_markets):
    
    all_ticks = {}
    all_bars = {}
    all_database_bars = {}
    all_broker_bars = {}
    all_last_prices = {}
    all_volumes = {}
    all_status = {}

    for market in unique_markets:
        all_ticks[market] = {}
        all_volumes[market] = {}
        all_last_prices[market] = {}
        prepare_bars(all_bars, market)
        prepare_bars(all_database_bars, market)
        prepare_bars(all_broker_bars, market)        
        all_status[market] = {}
        all_status[market][status_enabled] = False
        all_status[market][status_processed_database_bars] = False

    return all_ticks, all_bars, all_database_bars, all_broker_bars, all_last_prices, all_volumes, all_status

def on_pending_tickers(tickers):

    current_timestamp = datetime.now()
    end_current_minute_timestamp = current_timestamp.replace(second=0, microsecond=0)
    end_current_minute_timestamp = end_current_minute_timestamp + timedelta(minutes=1)

    for ticker in tickers:
        if not math.isnan(ticker.bid) and not math.isnan(ticker.ask):
            symbol = ticker.contract.localSymbol
            market = market_lookup[symbol]
            if market in all_ticks:
                mid = (ticker.bid + ticker.ask) / 2

                if end_current_minute_timestamp not in all_ticks[market]:
                    all_ticks[market][end_current_minute_timestamp] = []

                all_ticks[market][end_current_minute_timestamp].append(mid)
                all_volumes[market][end_current_minute_timestamp] = ticker.rtTradeVolume
                # print(f'{datetime.now()},Tick,{market},{mid},{ticker.rtTradeVolume},{end_current_minute_timestamp}')

def schedule_next_minute():
    current_time = time.time()  # Current time in seconds since the epoch
    seconds_to_next_minute = 60 - (current_time % 60)  # Time left until the next full minute
    # Schedule the on_minute_end function to be called exactly at the next full minute
    Timer(seconds_to_next_minute, close_minute_bar).start()

def close_minute_bar():

    print(f'{datetime.now()}: close_minute_bar - Start')

    creation_time = datetime.now()
    # creation_time = creation_time + timedelta(hours=hour_offset)
    creation_string = creation_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    minute_timestamp = creation_time.replace(second=0, microsecond=0)
    ## Minute timestamps use the end time time. This has created the previous minute's bar
    minute_timestamp = pd.to_datetime(minute_timestamp)
        
    schedule_next_minute()    

    for market in all_ticks.keys():  
        process_market(market, creation_string, minute_timestamp)

    print(f'{datetime.now()}: close_minute_bar - Proccessed')

def write_bars(market, bars, minute_timestamp):

    data = []
    
    for bar_index in range(len(bars[market][BarTypes.Minute1.value][OHLC.DateTime.value])):
        bar_datetime = bars[market][BarTypes.Minute1.value][OHLC.DateTime.value][bar_index]
        open_price = bars[market][BarTypes.Minute1.value][OHLC.Open.value][bar_index]
        high_price = bars[market][BarTypes.Minute1.value][OHLC.High.value][bar_index]
        low_price = bars[market][BarTypes.Minute1.value][OHLC.Low.value][bar_index]
        close_price = bars[market][BarTypes.Minute1.value][OHLC.Close.value][bar_index]
        volume = bars[market][BarTypes.Minute1.value][OHLC.Volume.value][bar_index]
        data.append([bar_datetime, open_price, high_price, low_price, close_price, volume])

    minute_timestamp_string = minute_timestamp.strftime('%Y.%m.%d.%H.%M.%S')

    df = pd.DataFrame(data, columns=['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df.to_csv(f'bars_{market}_{minute_timestamp_string}.csv', index=False)
    print(f"Data for {market} written to CSV successfully.")
    
def process_market(market, creation_string, minute_timestamp):

    ticks = []
    if minute_timestamp in all_ticks[market]:
        ticks = all_ticks[market][minute_timestamp]

    previous_minute_timestamp = minute_timestamp - timedelta(minutes=1)

    last_price = 0.0
    if previous_minute_timestamp in all_last_prices[market]:
        last_price = all_last_prices[market][previous_minute_timestamp]
    previous_volume = 0.0
    if previous_minute_timestamp in all_volumes[market]:
        previous_volume = all_volumes[market][previous_minute_timestamp]
    current_volume = 0.0
    if minute_timestamp in all_volumes[market]:
        current_volume = all_volumes[market][minute_timestamp]

    open_price = last_price
    close_price = open_price
    high_price = open_price
    low_price = open_price
    volume = current_volume - previous_volume

    if len(ticks) > 0:
        close_price = ticks[-1]    
        high_price = max(t for t in ticks)
        low_price = min(t for t in ticks)
    
    all_last_prices[market][minute_timestamp] = close_price
    if minute_timestamp not in all_volumes[market]:
        all_volumes[market][minute_timestamp] = previous_volume

    print(f'{datetime.now()}: New Bar: {market},{minute_timestamp},{open_price},{high_price},{low_price},{close_price},{volume} Tick Count: {len(ticks)}')

    ticks.clear()

    # Do not append the first created bar because that was a partial minute
    if open_price != 0.0:

        all_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value].append(minute_timestamp)
        all_bars[market][BarTypes.Minute1.value][OHLC.Symbol.value].append(symbol_lookup[market])
        all_bars[market][BarTypes.Minute1.value][OHLC.Open.value].append(open_price)
        all_bars[market][BarTypes.Minute1.value][OHLC.High.value].append(high_price)
        all_bars[market][BarTypes.Minute1.value][OHLC.Low.value].append(low_price)
        all_bars[market][BarTypes.Minute1.value][OHLC.Close.value].append(close_price)
        all_bars[market][BarTypes.Minute1.value][OHLC.Volume.value].append(volume)

        if market in all_database_bars and not all_status[market][status_processed_database_bars]:
            joined_bars = join_bars(market, all_bars, all_database_bars)

            # write_bars(market, all_bars, minute_timestamp)

            # Delete the database bars after they have been processed
            del all_database_bars[market]
            all_status[market][status_processed_database_bars] = True
            
            start_week = get_current_start_week_datetime()
            current_minute = datetime.now().replace(second=0, microsecond=0)
            data_errors = check_data_integrity(market, start_week, current_minute, given_bars=joined_bars,
                                               save_excel=save_excel, 
                                               save_activity=save_activity,
                                               save_excel_bars=save_excel_bars,
                                               save_excel_errors=save_excel_errors,                                                
                                               save_excel_merged=save_excel_merged)
            
            get_broker_bars_from_exchange(contracts, market)

            all_status[market][status_enabled] = True
            print(f'{datetime.now()}: {market} is synced')

        print(f'{datetime.now()}: Latest bars for {market}')
        for key in [OHLC.DateTime.value, OHLC.Open.value, OHLC.High.value, OHLC.Low.value, OHLC.Close.value, OHLC.Volume.value]:
            last_5_values = all_bars[market][BarTypes.Minute1.value][key][-5:]
            print(f"{market} = {key}: {last_5_values}")        

def redis_store_all_bars(market):

    bar_count = len(all_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value])

    datetimes = all_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value]
    opens = all_bars[market][BarTypes.Minute1.value][OHLC.Open.value]
    highs = all_bars[market][BarTypes.Minute1.value][OHLC.High.value]
    lows = all_bars[market][BarTypes.Minute1.value][OHLC.Low.value]
    closes = all_bars[market][BarTypes.Minute1.value][OHLC.Close.value]
    volumes = all_bars[market][BarTypes.Minute1.value][OHLC.Volume.value]

    for index in range(bar_count):
        store_bar(market, 
                  datetimes[index], 
                  opens[index], 
                  highs[index], 
                  lows[index], 
                  closes[index], 
                  volumes[index])

def join_bars(market, all_bars, all_historical_bars):
    joined_data = []

    if market in all_historical_bars and BarTypes.Minute1.value in all_historical_bars[market]:
        historical_bars_data = all_historical_bars[market][BarTypes.Minute1.value]

        # TODO: Consider removing the timezone so everything is local tz native
        # bar_datetime = bar_datetime.tz_localize(None)        
        
        if len(historical_bars_data[OHLC.DateTime.value]) > 0:
            for i in range(len(historical_bars_data[OHLC.DateTime.value])):
                joined_data.append({
                    'datetime': historical_bars_data[OHLC.DateTime.value][i],
                    'symbol': historical_bars_data[OHLC.Symbol.value][i],
                    'open': historical_bars_data[OHLC.Open.value][i],
                    'high': historical_bars_data[OHLC.High.value][i],
                    'low': historical_bars_data[OHLC.Low.value][i],
                    'close': historical_bars_data[OHLC.Close.value][i],
                    'volume': historical_bars_data[OHLC.Volume.value][i]
                })
            
    if market in all_bars and BarTypes.Minute1.value in all_bars[market]:
        bars_data = all_bars[market][BarTypes.Minute1.value]
        
        if len(bars_data[OHLC.DateTime.value]) > 0:
            for i in range(len(bars_data[OHLC.DateTime.value])):
                joined_data.append({
                    'datetime': bars_data[OHLC.DateTime.value][i],
                    'symbol': bars_data[OHLC.Symbol.value][i],
                    'open': bars_data[OHLC.Open.value][i],
                    'high': bars_data[OHLC.High.value][i],
                    'low': bars_data[OHLC.Low.value][i],
                    'close': bars_data[OHLC.Close.value][i],
                    'volume': bars_data[OHLC.Volume.value][i]
                })
    
    if joined_data:        
        df = pd.DataFrame(joined_data)
    else:
        df = pd.DataFrame(columns=['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume'])

    if len(df) > 0:
        df = df.sort_values(by=['datetime'])
    
    return df

def get_database_bars(markets, start_week):

    next_week = start_week + timedelta(weeks=1)

    for market in markets:
        bartype_df = get_bars(settings.host, settings.user, settings.password, market, start_week, next_week, BarMinutes.Minute1.value)

        all_database_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value].extend(bartype_df["datetime"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.Symbol.value].extend(bartype_df["symbol"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.Open.value].extend(bartype_df["open"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.High.value].extend(bartype_df["high"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.Low.value].extend(bartype_df["low"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.Close.value].extend(bartype_df["close"])
        all_database_bars[market][BarTypes.Minute1.value][OHLC.Volume.value].extend(bartype_df["volume"])

        print(f'{datetime.now()}: Processed {len(all_database_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value])} historical bars for {market}')
        if len(all_database_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value]) > 0:
            for key in [OHLC.DateTime.value, OHLC.Open.value, OHLC.High.value, OHLC.Low.value, OHLC.Close.value, OHLC.Volume.value]:
                last_5_values = all_database_bars[market][BarTypes.Minute1.value][key][-5:]
                print(f"{market} = {key}: {last_5_values}")

def get_broker_bars_from_exchange(contracts, market):

    contract = contracts[market]

    broker_bars = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='1 W',
        barSizeSetting='1 min',
        whatToShow='TRADES',
        # whatToShow='MIDPOINT',
        formatDate=1,
        useRTH=False,
        keepUpToDate=False)
    
    bars_df = ib_async.util.df(broker_bars)    
    broker_bars = convert_data(bars_df, market, broker_source_name, source_type, 
                               settings.host, settings.strategies_database, settings.user, settings.password)
    all_broker_bars[market] = broker_bars[market]
    
    # Is this needed on Beeks server?
    # Remove timezone    
    # bars_df['date'] = bars_df['date'].dt.tz_localize(None)    

    print(f'{datetime.now()}: Processed {len(all_broker_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value])} historical bars for {market}')
    for key in [OHLC.DateTime.value, OHLC.Symbol.value, OHLC.Open.value, OHLC.High.value, OHLC.Low.value, OHLC.Close.value, OHLC.Volume.value]:
        last_5_values = all_broker_bars[market][BarTypes.Minute1.value][key][-5:]
        print(f"{market} = {key}: {last_5_values}")

def calculate_trade_day(given_datetime):

    # Extract weekday (0=Monday, 6=Sunday) and hour from the datetime
    weekday = given_datetime.weekday()
    hour = given_datetime.hour
    
    if hour > start_trading_day_hour:
        weekday = (weekday + 1) % 7  # Shift to next weekday, wrapping around using modulo 7
    
    return weekday

def calculate_hour_offset():
    localized_time = datetime.now(eastern_tz)
    utc_offset = localized_time.utcoffset()
    hour_offset = utc_offset.total_seconds() / 3600
    return hour_offset

def store_bar(redis_cache, market, bar_type, datetime, open, high, low, close, volume):
    """
    Append a new bar to the separate lists for a given market and publish an update.
    """
    redis_cache.rpush(f"{market}:{bar_type}:datetime", datetime)
    redis_cache.rpush(f"{market}:{bar_type}:open", open)
    redis_cache.rpush(f"{market}:{bar_type}:high", high)
    redis_cache.rpush(f"{market}:{bar_type}:low", low)
    redis_cache.rpush(f"{market}:{bar_type}:close", close)
    redis_cache.rpush(f"{market}:{bar_type}:volume", volume)
    
    # Publish an update notification on a channel specific to the market and bar_type
    redis_cache.publish(f"{market}:{bar_type}:update", "new_bar")

def create_redis_cache():

    # Connect to the Redis server
    redis_cache = redis.Redis(host='localhost', port=6379, db=0)
    return redis_cache

def check_markets(markets, instruments_df):
    markets_set = set(markets)
    instruments_df_markets_set = set(instruments_df["market"].unique())

    if not markets_set.issubset(instruments_df_markets_set):
        missing = markets_set - instruments_df_markets_set

        write_activity('FeedManager', datetime.now(), Severity.Critical, f'Missing current markets in instrument reference: {missing}',
                       False, save_activity, settings.host, settings.strategies_database, settings.user, settings.password)

    if instruments_df["market"].duplicated().any():
        duplicates = instruments_df["market"][instruments_df["market"].duplicated()].unique()

        write_activity('FeedManager', datetime.now(), Severity.Critical, f'Duplicate current instruments: {duplicates}',
                       False, save_activity, settings.host, settings.strategies_database, settings.user, settings.password)

async def main():
        
    # Run script
    asyncio.wait(3000)    

if __name__ == "__main__":

    print(f'{datetime.now()}: Starting Feed Manager')

    broker = 'IB'
    markets = get_broker_connections(broker, settings.host, settings.strategies_database, settings.user, settings.password)

    instruments_df = get_current_instruments(datetime.now(), markets, settings.host, settings.strategies_database, settings.user, settings.password)
    check_markets(markets, instruments_df)

    hour_offset = calculate_hour_offset()
    start_week = get_current_start_week_datetime()
    holidays_df = get_holidays(settings.host, settings.strategies_database, settings.user, settings.password)

    print(f'{datetime.now()}: Connecting')

    ib_async.util.startLoop()
    ib = ib_async.IB()
    ib.connect('127.0.0.1', 4002, clientId=1)

    redis_cache = create_redis_cache()    

    contracts, market_lookup, symbol_lookup = get_ib_contracts(markets, instruments_df)
    all_ticks, all_bars, all_database_bars, all_broker_bars, all_last_prices, all_volumes, all_status = prepare_data(markets)

    ib.pendingTickersEvent += on_pending_tickers

    print(f'{datetime.now()}: Subscribing to Live Data')
    subscribe_to_live_data(ib, markets, contracts)

    print(f'{datetime.now()}: Syncing Live Pricing')

    schedule_next_minute()

    ib.sleep(120)

    get_database_bars(markets, start_week)

    # Causes RuntimeWarning: coroutine 'wait' was never awaited
    # asyncio.run(main())    

    run_seconds = 600
    print(f'{datetime.now()}: Running for {run_seconds} seconds')    
    ib.sleep(run_seconds)
    print(f'{datetime.now()}: Run time has finished')    

    print(f'{datetime.now()}: Disconnecting')    

    # TODO: Need to stop timer

    for contract in contracts.values():        
        ib.cancelMktData(contract)    

    print(f'{datetime.now()}: Finish')

    ib.disconnect()

    print(f'{datetime.now()}: Disconnected')
