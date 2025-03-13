from datetime import datetime, timedelta
import pandas as pd
import ib_async

from constants import DEFAULT_VOLUME
from database_reference import get_holidays,  get_instruments, get_last_price_datetime, bars_batch_insert
from utility_functions import calculate_start_trade_day, calculate_hour_offset
import settings
    
def get_historical_data(symbol, contract, end_day, end_day_adjusted):    

    historical_bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_day_adjusted,
        durationStr='1 D',
        barSizeSetting='1 min',
        whatToShow='TRADES',
        # whatToShow='MIDPOINT',
        formatDate=1,
        useRTH=False,
        keepUpToDate=False)
    
    bars_df = ib_async.util.df(historical_bars)

    if bars_df is None or len(bars_df) == 0:
        return None

    # Remove timezone
    bars_df['date'] = bars_df['date'].dt.tz_localize(None)

    original_start_date = bars_df['date'].min()
    complete_datetime_range = pd.date_range(start=original_start_date, end=end_day, freq='1 min')

    complete_df = pd.DataFrame(complete_datetime_range, columns=['date'])
    bars_df = pd.merge(complete_df, bars_df, on='date', how='left')

    # FFill of OHLC taken from,
    # https://stackoverflow.com/questions/70878536/how-to-resample-ohlc-data-properly-in-pandas-custom-fill-method-per-column
    bars_df['open'] = bars_df['open'].fillna(bars_df['close'].ffill(),limit=1)
    bars_df['close'] = bars_df['close'].fillna(bars_df['open'].bfill(),limit=1)
    bars_df['open'] = bars_df['open'].fillna(bars_df['close'].ffill())
    bars_df['close'] = bars_df['close'].fillna(bars_df['close'].ffill())
    bars_df['high'] = bars_df['high'].fillna(bars_df[['open','close']].max(axis=1))
    bars_df['low'] = bars_df['low'].fillna((bars_df[['open','close']].min(axis=1)))
    bars_df.fillna(DEFAULT_VOLUME, inplace=True)

    bars_df = bars_df.rename(columns={'date': 'start'})    
    # end is a key word so use quotes
    bars_df[f'\"end\"'] = bars_df['start'] + timedelta(minutes=1)
    bars_df['size'] = 1
    bars_df['symbol'] = symbol
    bars_df.drop(columns=['average', 'barCount'], inplace=True)
    bars_df.loc[bars_df['volume'] == 0, 'volume'] = DEFAULT_VOLUME

    return bars_df

if __name__ == "__main__":   

    print(f'{datetime.now()}: Starting Pricing Update')

    # Get instrument reference data
    # For each market separately,
    # Get last day of available data
    # Get pricing for each trading day between last day and yesterday. One day at at time
    # Determine which instrument is used on that day from instrument reference
    # Get minutes with volume for that day
    # Write minutes to database
    # Save those minutes as a transaction so that other processes can not pick them up while writing

    all_instruments = get_instruments(settings.host, settings.strategies_database, settings.user, settings.password)
    holidays_df = get_holidays(settings.host, settings.strategies_database, settings.user, settings.password)

    hour_offset = calculate_hour_offset()
    markets = ['EU']

    current_date_time = datetime.now()
    end_price_day = calculate_start_trade_day(current_date_time)
    end_price_day = end_price_day - timedelta(hours=hour_offset)
    
    print(f'{datetime.now()}: Connecting')

    ib_async.util.startLoop()
    ib = ib_async.IB()
    ib.connect('10.10.209.70', 4002, clientId=1)

    for market in markets:
        print(f'{datetime.now()}: Running {market}')

        market_last_price_datetime = get_last_price_datetime(market, settings.host, settings.user, settings.password)
        current_price_day = calculate_start_trade_day(market_last_price_datetime)
        market_instruments_mask = all_instruments['market'] == market
        market_instruments = all_instruments[market_instruments_mask]        

        while True:
            current_price_day = current_price_day + timedelta(days=1)

            if current_price_day >= end_price_day:
                break

            # Do not process weekend trading days 
            if current_price_day.weekday() == 4 or current_price_day.weekday() == 5:
                continue

            end_day = current_price_day + timedelta(days=1)            
            end_day_adjusted = end_day - timedelta(hours=hour_offset)
            # The start time of the final minute of the day is 1 minute before the end
            end_day = end_day - timedelta(minutes=1)

            matching_row = market_instruments[(market_instruments['start_date'] <= current_price_day) & (market_instruments['end_date'] > current_price_day)]

            if len(matching_row) == 0:
                print(f'{datetime.now()}: Failed to find instrument for {market} on {current_price_day}')
                continue

            first_row = matching_row.iloc[0]

            exchange = first_row['exchange']
            expiry_date = first_row['expiry'].strftime('%Y%m')
            symbol = first_row['symbol']

            print(f'{datetime.now()}: Running {market} - {symbol} - {current_price_day.strftime('%Y-%m-%d')}')

        
            contract = ib_async.Future(symbol=market, exchange=exchange, lastTradeDateOrContractMonth=expiry_date, currency='USD')
            # contract = ib_async.Forex(market)

            contract.includeExpired = True

            ib.qualifyContracts(contract)

            print(f'{datetime.now()}: Running {market} - {current_price_day} = {contract}')

            daily_bars = get_historical_data(symbol, contract, end_day, end_day_adjusted)
            if daily_bars is not None:
                bars_batch_insert(daily_bars, market, settings.host, settings.user, settings.password)
                # print(f'daily_bars={daily_bars}')

    print(f'{datetime.now()}: Disconnecting')    

    ib.disconnect()

    print(f'{datetime.now()}: Finish')
