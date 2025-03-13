import pandas as pd
from datetime import datetime, timedelta

import tradingeconomics as te

from database_reference import get_risk_event_types, write_new_event_type, get_new_risk_event_dates, write_risk_event_date, delete_risk_event_date
from utility_functions import calculate_hour_offset
import settings

trading_economics_token = '71F86767ED314EF:05DBBA491DD34CD'
high_importance = 3

key_events = [
('Non Farm Payrolls', 'Non Farm Payrolls', 'non farm payrolls', 'united states'),
('Fed Interest Rate Decision', 'Fed Interest Rate Decision', 'interest rate', 'united states'),
('BoE Interest Rate Decision', 'BoE Interest Rate Decision', 'interest rate', 'United Kingdom'),
('ECB Interest Rate Decision', 'ECB Interest Rate Decision', 'interest rate', 'euro area'),
('BoC Interest Rate Decision', 'BoC Interest Rate Decision', 'interest rate', 'Canada'),
('RBA Interest Rate Decision', 'RBA Interest Rate Decision', 'interest rate', 'Australia'),
('SNB Interest Rate Decision', 'SNB Interest Rate Decison', 'interest rate', 'Switzerland'),
('RBNZ Interest Rate Decision', 'Interest Rate Decision', 'interest rate', 'New Zealand'),
('CBRT Interest Rate Decision', 'TCMB Interest Rate Decision', 'interest rate', 'Turkey'),
('SARB Interest Rate Decision', 'Interest Rate Decision', 'interest rate', 'South Africa'),
('BdeM Interest Rate Decision', 'Interest Rate Decision', 'interest rate', 'Mexico'),
('API Crude Oil Stock Change', 'API Crude Oil Stock Change', 'API Crude Oil Stock Change', 'united states'),
('EIA Crude Oil Stocks Change', 'EIA Crude Oil Stocks Change', 'Crude Oil Stocks Change', 'united states'),
('EIA Natural Gas Storage', 'EIA Natural Gas Stocks Change', 'Natural Gas Stocks Change', 'united states'),
]

markets = ['ES', 'CL', 'NQ', 'PX', 'LE', 'EU', 'GC', 'SI', 'PL', 'PA', 'HG', 'ZC', 'ZS', 'ZM', 'ZW', 'ZL', 'HE', 'NG', 'RB', 'HO', 'BZ', 'NK', 'EM', 'AD', 'BP', 'NE', 'CD', 'JP', 'BR', 'GF', 'RT', 'TN', 'UB', 'ZB', 'ZF', 'ZN', 'KE', 'ZT' ]

write_new_events = True

def update_risk_events():
    print(f'{datetime.now()}: Running update_risk_events')    

    hour_offset = calculate_hour_offset()

    te.login(trading_economics_token)
    start_date = '2025-01-18'
    end_date = '2025-03-08'

    all_events = []

    for (name, allowed_event_name, indicator, country) in key_events:        
        named_events = te.getCalendarData(country=country, category=indicator, initDate = start_date, endDate = end_date, output_type='df')
        named_events = named_events[named_events['Event'] == allowed_event_name]
        named_events['Event'] = name
        all_events.append(named_events)

    high_important_events = te.getCalendarData(country='all', initDate = start_date, endDate = end_date, importance=high_importance, output_type='df')
    all_events.append(high_important_events)

    joined_events = pd.concat(all_events, axis=0)
    
    # Convert Date time a pandas datetime and adjust to EST
    joined_events['Date'] = pd.to_datetime(joined_events['Date'])
    joined_events['Date'] = joined_events['Date'] + timedelta(hours=hour_offset)

    # Remove any duplicates that were key events and also high important
    joined_events = joined_events.drop_duplicates(subset=['Event', 'Date'])

    if write_new_events:
        joined_events_filename = 'all_events.csv'
        joined_events.to_csv(joined_events_filename, index=False)

    all_existing_event_types = get_risk_event_types(settings.host, settings.strategies_database, settings.user, settings.password)
    risk_event_dates = get_new_risk_event_dates(start_date, settings.host, settings.strategies_database, settings.user, settings.password)

    # Merge the events DataFrame with existing event types to get the event_id
    merged_events = joined_events.merge(all_existing_event_types, how='left', left_on='Event', right_on='event_code')    

    # Check for new events to add
    for row_index in range(len(merged_events)):

        row = merged_events.iloc[row_index]

        row_event = row['Event']
        row_date = row['Date']
        row_event_id = row['event_id']

        # Remove all single and double quotes from the event names to avoid issues in the database
        row_event = row_event.replace("'", "").replace('"', "")

        if pd.isna(row_event_id):
            next_event_id = int(all_existing_event_types['event_id'].max()) + 1
            print(f"{datetime.now()}: {row_event} not found in existing events. Adding as event_id {next_event_id}")
                        
            write_new_event_type(next_event_id, row_event, markets, settings.host, settings.strategies_database, settings.user, settings.password)

            # Get the new risk events and the merge on event_code to handle the new event type
            all_existing_event_types = get_risk_event_types(settings.host, settings.strategies_database, settings.user, settings.password)
            merged_events = joined_events.merge(all_existing_event_types, how='left', left_on='Event', right_on='event_code')        
            row = merged_events.iloc[row_index]
            row_event_id = row['event_id']

        row_event_id = int(row_event_id)

        # Check if row_event_id and row_date exist in risk_event_dates
        matching_rows = risk_event_dates[(risk_event_dates['event_id'] == row_event_id) & (risk_event_dates['event_date'] == row_date)]
        if matching_rows.empty:
            print(f"{datetime.now()}: Event {row_event} with event_id {row_event_id} and date {row_date} does not exist in risk_event_dates.")
            write_risk_event_date(row_event_id, row_date, settings.host, settings.strategies_database, settings.user, settings.password)

    # Check for existing events to remove
    for row_index in range(len(risk_event_dates)):
        row = risk_event_dates.iloc[row_index]

        row_event_id = int(row['event_id'])
        row_event_date = row['event_date']        

        # Check if the row exists in merged_events
        matching_rows = merged_events[(merged_events['event_id'] == row_event_id) & (merged_events['Date'] == row_event_date)]
        if matching_rows.empty:
            print(f"{datetime.now()}: Risk event with event_id {row_event_id} and event_date {row_event_date} no longer exists")
            delete_risk_event_date(row_event_id, row_date, settings.host, settings.strategies_database, settings.user, settings.password)            
    
    print(f'{datetime.now()}: Finish update_risk_events')

if __name__ == "__main__":
    update_risk_events()
