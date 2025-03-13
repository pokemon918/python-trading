import os
import time
import pandas as pd
import sys
from datetime import datetime, timedelta
import ib_async

from database_reference import get_instruments, get_main_contracts, write_instrument
import settings

def convert_symbol(symbol, expiry):
    # Converts a futures symbol from single digit year format (eg. GCJ5) to two digit year format (eg. GCJ25)

    root = symbol[:-2]
    month_code = symbol[-2]
    year_last_two_digits = expiry.strftime('%y')
    new_symbol = f'{root}{month_code}{year_last_two_digits}'
    return new_symbol

def calculate_current_contract(existing_instruments, now_datetime):

    matching_rows = existing_instruments[(existing_instruments["start_date"].notnull()) & (existing_instruments["end_date"].isnull())]

    # Return the symbol if a match is found
    if not matching_rows.empty:
        row = matching_rows.iloc[0]
        return row["symbol"], row['expiry']

    return None, None  # Return None if no match is found

def process_market(ib, market, existing_instruments):

    print(f'{datetime.now()}: Processing {market}')

    now_datetime = datetime.now()
    two_months_future = now_datetime + timedelta(days=60)

    first_market_row = existing_instruments[existing_instruments['market'] == market]
    exchange = first_market_row.iloc[0]['exchange']

    current_symbol, current_expiry = calculate_current_contract(existing_instruments, now_datetime)

    market_contract = ib_async.Future(symbol=market, exchange=exchange)

    # Request available contracts
    contracts = ib.reqContractDetails(market_contract)

    # Print available contracts
    # for contract in contracts:
    #    print(f'contract={contract.contract},{contract.minTick}')

    volume_data = []

    # Loop through available contracts
    for contract_detail in contracts:
        contract = contract_detail.contract  # Get the contract object

        expiry = datetime.strptime(contract.lastTradeDateOrContractMonth, "%Y%m%d")

        # Only pick contracts that expiry soon
        if expiry > two_months_future:
            continue

        # Live contracts can not go backwards in expiry
        if current_expiry is not None and expiry < current_expiry:
            continue

        # Get historical market data (daily bar)
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',  # Empty means latest available data
            durationStr='1 D',  # Get 1 day of data
            barSizeSetting='1 day',  # Daily bar
            whatToShow='TRADES',  # Get trade data
            useRTH=True,  # Only regular trading hours
            formatDate=1
        )

        # Extract and store volume data
        if bars:
            new_symbol = convert_symbol(contract.localSymbol, expiry)

            volume_data.append({
                "conId": contract.conId,
                "symbol": new_symbol,
                "tick_size": contract_detail.minTick,
                "expiry": expiry,
                "exchange": contract.exchange,
                "date": bars[0].date,
                "volume": bars[0].volume
            })

    # Convert to DataFrame
    df = pd.DataFrame(volume_data)
    df = df.sort_values(by="volume", ascending=False)

    # Display results
    print(df)

    first_row = df.iloc[0]

    print(f'{datetime.now()}: Highest volume: {first_row['symbol']} {first_row['expiry']}')

if __name__ == "__main__":

    ib_async.util.startLoop()
    ib = ib_async.IB()
    # ib.connect('127.0.0.1', 4002, clientId=1)
    ib.connect('10.10.209.70', 4002, clientId=1)

    existing_instruments = get_instruments(settings.host, settings.strategies_database, settings.user, settings.password)
    unique_markets = existing_instruments['market'].unique()

    for market in unique_markets:
        process_market(ib, market, existing_instruments)

    print(f'{datetime.now()}: Finish')

    ib.disconnect()

    print(f'{datetime.now()}: Disconnected')
