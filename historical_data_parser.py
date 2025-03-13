import pandas as pd
from datetime import datetime, timedelta

from constants import DEFAULT_VOLUME, BarTypes, OHLC
from database_reference import get_parser_config
from data_integrity import calculate_tradable_array, combine_bars

def prepare_bars(given_bars, market):

    given_bars[market] = {}
    given_bars[market][BarTypes.Minute1.value] = {}
    given_bars[market][BarTypes.Minute1.value][OHLC.DateTime.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.Symbol.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.Open.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.High.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.Low.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.Close.value] = []
    given_bars[market][BarTypes.Minute1.value][OHLC.Volume.value] = []

def convert_data(source_data, 
                 market, source_name, source_type,
                 host, database, user, password):
    
    config_df = get_parser_config(source_name, source_type, host, database, user, password)

    # For IB, this is date
    start_field = config_df['name_start'].iloc[0]
    end_field = config_df['name_end'].iloc[0]
    open_field = config_df['name_open'].iloc[0]
    high_field = config_df['name_high'].iloc[0]
    low_field = config_df['name_low'].iloc[0]
    close_field = config_df['name_close'].iloc[0]
    volume_field = config_df['name_volume'].iloc[0]

    bars = {}
    prepare_bars(bars, market)

    for row in source_data.itertuples():
        if start_field is not None:
            start_value = row[start_field]
        if end_field is not None:
            end_value = row[end_field]
        open_value = row[open_field]
        high_value = row[high_field]
        low_value = row[low_field]
        close_value = row[close_field]
        volume_value = row[volume_field]

        if end_field is None:
            end_value = start_value + timedelta(minutes=1)

        bars[market][BarTypes.Minute1.value][OHLC.DateTime.value].append(end_value)
        bars[market][BarTypes.Minute1.value][OHLC.Open.value].append(open_value)
        bars[market][BarTypes.Minute1.value][OHLC.High.value].append(high_value)
        bars[market][BarTypes.Minute1.value][OHLC.Low.value].append(low_value)
        bars[market][BarTypes.Minute1.value][OHLC.Close.value].append(close_value)
        bars[market][BarTypes.Minute1.value][OHLC.Volume.value].append(volume_value)

    return bars

def forward_fill_bars(existing_bars, market, start_datetime, end_datetime, 
                      host, database, user, password):    

    all_bars = calculate_tradable_array(market, start_datetime, end_datetime, 
                                        host, database, user, password)

    bars_df = combine_bars(all_bars, existing_bars)

    # FFill of OHLC taken from,
    # https://stackoverflow.com/questions/70878536/how-to-resample-ohlc-data-properly-in-pandas-custom-fill-method-per-column
    bars_df['open'] = bars_df['open'].fillna(bars_df['close'].ffill(),limit=1)
    bars_df['close'] = bars_df['close'].fillna(bars_df['open'].bfill(),limit=1)
    bars_df['open'] = bars_df['open'].fillna(bars_df['close'].ffill())
    bars_df['close'] = bars_df['close'].fillna(bars_df['close'].ffill())
    bars_df['high'] = bars_df['high'].fillna(bars_df[['open','close']].max(axis=1))
    bars_df['low'] = bars_df['low'].fillna((bars_df[['open','close']].min(axis=1)))
    bars_df.fillna(DEFAULT_VOLUME, inplace=True)

    bars_df.loc[bars_df['volume'] == 0, 'volume'] = DEFAULT_VOLUME

    return bars_df
