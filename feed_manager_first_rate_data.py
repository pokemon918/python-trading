import pandas as pd
import os
from datetime import datetime, timedelta

from utility_functions import calculate_start_trade_day
from database_reference import get_instruments, batch_upsert_bars
import settings

input_path = "/home/storage/Data/FirstRateData"
output_csv_path = '/home/storage/Data/Python'
write_csv = True
write_database = False

def process_errors_first_rate_data(market, error_bars):    

    print(f'{datetime.now()}: Starting on {market}')
    
    contract_data = get_instruments(settings.host, settings.strategies_database, settings.user, settings.password)

    headers = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    
    print(f'{datetime.now()}: Loading files')

    full_input_path = f'{input_path}/{market}'

    error_bar_times = set(error_bars.keys()) if error_bars else set()
    all_dataframes = []

    for filename in os.listdir(full_input_path):
        file_path = os.path.join(full_input_path, filename)
            
        if os.path.isfile(file_path):
            
            filename_split = filename.split('_')
            contract = filename_split[1]

            full_contract = f'{market}{contract}'

            matching_row = contract_data[contract_data['symbol'] == full_contract]            
            if not matching_row.empty:
                first_trading_datetime = matching_row['start_date'].values[0]
                last_trading_datetime = matching_row['end_date'].values[0]

                print(f'Processing {full_contract} = {first_trading_datetime} - {last_trading_datetime}')
                df = pd.read_csv(file_path, header=None, names=headers)
                df['Start'] = pd.to_datetime(df['DateTime'])

                # Copy is used here so that it is a dataframe and not a copy of a slice
                # Otherwise, it has the error of "A value is trying to be set on a copy of a slice from a DataFrame."
                # See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy            
                filtered_df = df[(df['Start'] >= first_trading_datetime) & (df['Start'] <= last_trading_datetime)].copy()
                
                if len(filtered_df) > 0:
                    # Pre-calculate the days list outside the loop
                    days = []
                    check_day = calculate_start_trade_day(pd.to_datetime(first_trading_datetime))
                    final_day = calculate_start_trade_day(pd.to_datetime(last_trading_datetime))
                    while(check_day < final_day):
                        days.append(check_day)
                        check_day = check_day + timedelta(days=1)
                    
                    # Pre-filter weekend periods
                    weekend_mask = (
                        # Condition 1: After 5 PM on Fridays
                        (filtered_df['Start'].dt.weekday == 4) & (filtered_df['Start'].dt.hour >= 17) |
                        # Condition 2: All day Saturday
                        (filtered_df['Start'].dt.weekday == 5) |
                        # Condition 3: Before 6 PM on Sunday
                        (filtered_df['Start'].dt.weekday == 6) & (filtered_df['Start'].dt.hour < 18)
                    )
                    filtered_df = filtered_df[~weekend_mask]
                    
                    # Process each day, but with optimizations
                    day_dataframes = []
                    
                    for i, day_start in enumerate(days):
                        day_end = day_start + timedelta(days=1)
                        
                        # Filter for current day
                        day_data = filtered_df[(filtered_df['Start'] >= day_start) & (filtered_df['Start'] < day_end)]
                        
                        if len(day_data) > 0:
                            # Create a copy only once for this day's processing
                            day_filtered_df = day_data.copy()
                            day_filtered_df.set_index('Start', inplace=True)
                            
                            fill_start_time = day_filtered_df.index.min()
                            
                            if not pd.isna(fill_start_time):
                                # Create full time range for the day
                                full_time_range = pd.date_range(start=fill_start_time, end=day_end, freq='1 min')
                                day_filtered_df = day_filtered_df.reindex(full_time_range)
                                
                                # Forward fill OHLC data
                                day_filtered_df.loc[:, 'Open'] = day_filtered_df['Open'].fillna(day_filtered_df['Close'].ffill(), limit=1)
                                day_filtered_df.loc[:, 'Close'] = day_filtered_df['Close'].fillna(day_filtered_df['Open'].bfill(), limit=1)
                                day_filtered_df.loc[:, 'Open'] = day_filtered_df['Open'].fillna(day_filtered_df['Close'].ffill())
                                day_filtered_df.loc[:, 'Close'] = day_filtered_df['Close'].fillna(day_filtered_df['Close'].ffill())
                                day_filtered_df.loc[:, 'High'] = day_filtered_df['High'].fillna(day_filtered_df[['Open','Close']].max(axis=1))
                                day_filtered_df.loc[:, 'Low'] = day_filtered_df['Low'].fillna(day_filtered_df[['Open','Close']].min(axis=1))
                                
                                day_filtered_df.fillna(0.000001, inplace=True)
                                day_filtered_df.reset_index(inplace=True)
                                day_filtered_df.rename(columns={'index': 'Start'}, inplace=True)
                                
                                # Add columns for output
                                day_filtered_df[f'\"end\"'] = day_filtered_df['Start'] + pd.Timedelta(minutes=1)
                                day_filtered_df['size'] = 1
                                day_filtered_df['symbol'] = full_contract
                                day_filtered_df = day_filtered_df.drop(columns=["DateTime"])
                                
                                # Filter for error bars - do this early to reduce size of dataframes
                                if error_bar_times:
                                    # Use set membership test which is much faster than .isin()
                                    mask = day_filtered_df['\"end\"'].apply(lambda x: x in error_bar_times)
                                    day_filtered_df = day_filtered_df[mask]
                                
                                if len(day_filtered_df) > 0:
                                    day_dataframes.append(day_filtered_df)
                    
                    # Combine all days for this file
                    if day_dataframes:
                        file_df = pd.concat(day_dataframes, ignore_index=True)
                        all_dataframes.append(file_df)

                        # check_filename = f'{output_csv_path}/ParsedBars.{market}.{full_contract}.{datetime.now().strftime("%Y.%m.%d.%H.%M")}.csv'
                        # file_df.to_csv(check_filename, mode='w', index=False)
                        # print(f"Parsed Bars written to {check_filename}")

                        if write_database:
                            batch_upsert_bars(file_df, market, settings.host, settings.user, settings.password)

    if write_csv:

        combined_df = pd.concat(all_dataframes, ignore_index=True)

        combined_df.rename(columns={'\"end\"': 'End'}, inplace=True)
        combined_df.rename(columns={'size': 'Size'}, inplace=True)
        combined_df.rename(columns={'symbol': 'Symbol'}, inplace=True)
        new_order = ['Symbol', 'Size', 'Start', 'End', 'Open', 'High', 'Low', 'Close', 'Volume']
        combined_df = combined_df[new_order]
        combined_df.sort_values("Start", inplace=True)

        bar_writeout_filename = f'{output_csv_path}/Bars.{market}.{datetime.now().strftime("%Y.%m.%d.%H.%M")}.csv'
        combined_df.to_csv(bar_writeout_filename, mode='w', index=False)
        print(f"Bars written to {bar_writeout_filename}")

    print(f'{datetime.now()}: Finished {market}')