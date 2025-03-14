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
import logging
from functools import wraps
import traceback

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
          
CONNECTION_RETRY_DELAY = 10  # seconds
MAX_RECONNECTION_ATTEMPTS = 5
CONNECTION_TIMEOUT = 30  # seconds

class IBConnectionManager:
    def __init__(self, host='127.0.0.1', port=4002, client_id=1):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = None
        self.connected = False
        self.reconnection_attempts = 0
        self.last_connection_time = None
        self.markets_status = {}  # Track connection status for each market
        self.connection_status_channel = "connection_status"  # Redis channel for connection status

    def connect(self, redis_cache=None):
        """Establish connection to Interactive Brokers with retry logic"""
        if self.connected and self.ib is not None:
            return self.ib
            
        try:
            print(f'{datetime.now()}: Connecting to IB at {self.host}:{self.port}')
            print(f'{datetime.now()}: Please ensure TWS or IB Gateway is running and configured for API connections')
            
            ib_async.util.startLoop()
            self.ib = ib_async.IB()
            
            # Set up connection timeout
            connection_timeout = time.time() + CONNECTION_TIMEOUT
            
            # Connect with timeout handling
            connection_task = self.ib.connect(self.host, self.port, clientId=self.client_id)
            
            # Wait for connection with timeout
            while not self.ib.isConnected() and time.time() < connection_timeout:
                time.sleep(0.1)
                
            if not self.ib.isConnected():
                raise TimeoutError(f"Connection to IB timed out after {CONNECTION_TIMEOUT} seconds. Please verify that TWS/IB Gateway is running and API connections are enabled.")
                
            self.connected = True
            self.reconnection_attempts = 0
            self.last_connection_time = datetime.now()
            
            # Set up disconnect callback
            self.ib.disconnectedEvent += self._handle_disconnect
            
            # Publish connection status to Redis if available
            if redis_cache:
                try:
                    self._publish_connection_status(redis_cache, True)
                except Exception as e:
                    print(f'{datetime.now()}: Warning - Failed to publish to Redis: {str(e)}')
                
            print(f'{datetime.now()}: Successfully connected to IB')
            return self.ib
            
        except Exception as e:
            self.connected = False
            self.reconnection_attempts += 1
            error_msg = f"Failed to connect to IB: {str(e)}"
            print(f'{datetime.now()}: {error_msg}')
            
            # Log the error
            write_activity('FeedManager', datetime.now(), Severity.Critical, 
                          error_msg, False, save_activity, settings.host, 
                          settings.strategies_database, settings.user, settings.password)
            
            # Publish connection status to Redis if available
            if redis_cache:
                try:
                    self._publish_connection_status(redis_cache, False)
                except Exception as redis_error:
                    print(f'{datetime.now()}: Warning - Failed to publish to Redis: {str(redis_error)}')
                
            # Retry connection if under max attempts
            if self.reconnection_attempts < MAX_RECONNECTION_ATTEMPTS:
                print(f'{datetime.now()}: Retrying connection in {CONNECTION_RETRY_DELAY} seconds (attempt {self.reconnection_attempts}/{MAX_RECONNECTION_ATTEMPTS})')
                time.sleep(CONNECTION_RETRY_DELAY)
                return self.connect(redis_cache)
            else:
                raise ConnectionError(f"Failed to connect to IB after {MAX_RECONNECTION_ATTEMPTS} attempts")

    def _handle_disconnect(self):
        """Handle disconnection events from IB"""
        self.connected = False
        disconnect_time = datetime.now()
        time_since_last = (disconnect_time - self.last_connection_time).total_seconds() if self.last_connection_time else 0
        
        error_msg = f"Disconnected from IB at {disconnect_time}. Connection was active for {time_since_last} seconds"
        print(f'{datetime.now()}: {error_msg}')
        
        # Log the disconnection
        write_activity('FeedManager', disconnect_time, Severity.Warning, 
                      error_msg, False, save_activity, settings.host, 
                      settings.strategies_database, settings.user, settings.password)
        
        # Attempt to reconnect
        self.reconnect()

    def reconnect(self, redis_cache=None):
        """Attempt to reconnect to IB"""
        if self.connected:
            return self.ib
            
        # Reset connection state
        if self.ib:
            try:
                self.ib.disconnect()
            except:
                pass
            
        self.ib = None
        self.connected = False
        
        # Attempt to reconnect
        return self.connect(redis_cache)

    def disconnect(self):
        """Safely disconnect from IB"""
        if self.ib and self.connected:
            try:
                self.ib.disconnect()
                print(f'{datetime.now()}: Successfully disconnected from IB')
            except Exception as e:
                print(f'{datetime.now()}: Error during disconnection: {str(e)}')
            finally:
                self.connected = False
                self.ib = None

    def update_market_status(self, market, is_connected, redis_cache=None):
        """Update connection status for a specific market"""
        self.markets_status[market] = is_connected
        
        # Publish market-specific status to Redis if available
        if redis_cache:
            redis_cache.hset("market_connection_status", market, "1" if is_connected else "0")
            redis_cache.publish(f"market:{market}:connection_status", "connected" if is_connected else "disconnected")
            
        # Log status change
        status_msg = f"Market {market} connection status changed to {'connected' if is_connected else 'disconnected'}"
        print(f'{datetime.now()}: {status_msg}')
        
        severity = Severity.Info if is_connected else Severity.Warning
        write_activity('FeedManager', datetime.now(), severity, 
                      status_msg, False, save_activity, settings.host, 
                      settings.strategies_database, settings.user, settings.password)

    def _publish_connection_status(self, redis_cache, is_connected):
        """Publish overall connection status to Redis"""
        if redis_cache:
            try:
                status_message = json.dumps({
                    "connected": is_connected,
                    "timestamp": datetime.now().isoformat(),
                    "reconnection_attempts": self.reconnection_attempts
                })
                redis_cache.publish(self.connection_status_channel, status_message)
            except Exception as e:
                print(f'{datetime.now()}: Warning - Failed to publish to Redis: {str(e)}')

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
    try:
        # Connect to the Redis server
        redis_cache = redis.Redis(host='localhost', port=6379, db=0)
        # Test the connection
        redis_cache.ping()
        print(f'{datetime.now()}: Successfully connected to Redis')
        return redis_cache
    except redis.ConnectionError as e:
        print(f'{datetime.now()}: Warning - Redis connection failed: {str(e)}')
        print(f'{datetime.now()}: Feed Manager will continue without Redis functionality')
        return None

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

    # Create Redis cache with error handling
    redis_cache = create_redis_cache()

    # Create and connect the IB connection manager
    connection_manager = IBConnectionManager(host='127.0.0.1', port=4002, client_id=1)
    try:
        ib = connection_manager.connect(redis_cache)
        
        contracts, market_lookup, symbol_lookup = get_ib_contracts(markets, instruments_df)
        all_ticks, all_bars, all_database_bars, all_broker_bars, all_last_prices, all_volumes, all_status = prepare_data(markets)

        ib.pendingTickersEvent += on_pending_tickers

        print(f'{datetime.now()}: Subscribing to Live Data')
        subscribe_to_live_data(ib, markets, contracts)

        print(f'{datetime.now()}: Syncing Live Pricing')

        schedule_next_minute()

        ib.sleep(120)

        get_database_bars(markets, start_week)

        run_seconds = 600
        print(f'{datetime.now()}: Running for {run_seconds} seconds')    
        ib.sleep(run_seconds)
        print(f'{datetime.now()}: Run time has finished')    

    except ConnectionError as e:
        error_msg = f"Connection error: {str(e)}"
        print(f'{datetime.now()}: {error_msg}')
        print(f'{datetime.now()}: Please ensure TWS/IB Gateway is running and properly configured')
        write_activity('FeedManager', datetime.now(), Severity.Critical, 
                      error_msg, False, save_activity, settings.host, 
                      settings.strategies_database, settings.user, settings.password)
    except Exception as e:
        error_msg = f"Error in Feed Manager: {str(e)}\n{traceback.format_exc()}"
        print(f'{datetime.now()}: {error_msg}')
        write_activity('FeedManager', datetime.now(), Severity.Critical, 
                      error_msg, False, save_activity, settings.host, 
                      settings.strategies_database, settings.user, settings.password)
    finally:
        print(f'{datetime.now()}: Disconnecting')    

        # Cancel market data subscriptions
        if 'contracts' in locals() and 'ib' in locals() and connection_manager.connected:
            for contract in contracts.values():        
                try:
                    ib.cancelMktData(contract)
                except:
                    pass

        # Disconnect from IB
        if 'connection_manager' in locals():
            connection_manager.disconnect()

        print(f'{datetime.now()}: Finish')
