import pandas as pd
import json
import multiprocessing

from constants import IndicatorReset, enum_decoder, BarTypes, OHLC
from scores import run_strategy
from database_reference import get_database_data, get_holidays
from database_strategies import get_strategy, get_strategy_returns, get_strategy_trades
from trade_timing import create_all_exits, create_allowed_entries
from market_reference import market_slippage, market_contract_size
import settings 

def calculate_strategies_for_market(market, market_strategies, strategy_jsons):

    slippage = market_slippage[market] / 200
    contract_size = market_contract_size[market]

    first_key = next(iter(market_strategies))
    first_strategy = json.loads(strategy_jsons[first_key], object_hook=enum_decoder)
    first_indicator_reset = first_strategy['indicator_reset']
    if isinstance(first_indicator_reset, int):
        indicator_reset = IndicatorReset(first_indicator_reset)

    holidays_df = get_holidays(settings.host, settings.user, settings.password)
    all_bars, all_period_lookup, all_period_offsets, all_period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
        settings.host, settings.user, settings.password, market, settings.start, settings.end, "all", holidays_df, indicator_reset)
    all_timed_exits = create_all_exits(settings.host, settings.user, settings.password, all_bars, market, all_period_lookup, indicator_reset)
    all_allowed_entry_sessions, all_allowed_entry_days = create_allowed_entries(all_bars, indicator_reset)

    for strategy_id in market_strategies:
        test_strategy = json.loads(strategy_jsons[strategy_id], object_hook=enum_decoder)

        if test_strategy['indicator_reset'] != first_indicator_reset:
            print(f'Skipping strategy because of different indicator_reset,{market},{strategy_id},{test_strategy}')    
            continue

        print(f'Running,{market},{strategy_id},{test_strategy}')

        returns_array, trade_entry_datetimes, trade_returns, trade_df, signal_counts, trade_indexes, best_profit, worst_loss, fail_strategy = run_strategy(
            test_strategy, market, True, 
            all_bars[BarTypes.Minute1.value][OHLC.DateTime.value],
            all_bars[BarTypes.Minute1.value][OHLC.Open.value],
            all_bars[BarTypes.Minute1.value][OHLC.High.value],
            all_bars[BarTypes.Minute1.value][OHLC.Low.value],
            all_bars[BarTypes.Minute1.value][OHLC.Close.value],
            all_bars[BarTypes.Minute1.value][OHLC.Volume.value],
            all_timed_exits,
            all_allowed_entry_days,
            all_allowed_entry_sessions,
            all_period_offsets, 
            all_period_lengths,
            all_period_lookup,
            all_datetimes,
            slippage)
                
        trades_filename = f'{settings.write_all_path}/strategy_{strategy_id}.trades.csv'
        trade_df.to_csv(trades_filename, mode='w', index=False)
        print(f"Trades written to {trades_filename}")

        returns_series = pd.Series(returns_array, index=all_datetimes)

        hourly_returns = returns_series.resample('1h').sum()
        cumulative_hourly_returns = hourly_returns.cumsum()

        custom_headers = [f'Strategy_{strategy_id}']
        hourly_returns_filename = f'{settings.write_all_path}/strategy_{strategy_id}.returns.csv'
        cumulative_hourly_returns.to_csv(hourly_returns_filename, mode='w', index=True, header=custom_headers)
        print(f"Hourly returns written to {hourly_returns_filename}")

def write_strategies_for_market(market, market_strategies):

    holidays_df = get_holidays(settings.host, settings.user, settings.password)
    all_bars, all_period_lookup, all_period_offsets, all_period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
        settings.host, settings.user, settings.password, market, settings.start, settings.end, "all", holidays_df, IndicatorReset.Weekly)
    datetimes_1minute = [pd.Timestamp(dt) for dt in all_datetimes]

    for strategy_id in market_strategies:

        strategy_returns = get_strategy_returns(strategy_id, datetimes_1minute, settings.host, settings.strategies_database, settings.user, settings.password)
        strategy_trades = get_strategy_trades(strategy_id, settings.host, settings.strategies_database, settings.user, settings.password)
                
        trades_filename = f'{settings.write_all_path}/strategy_{strategy_id}.trades.xlsx'
        strategy_trades.to_excel(trades_filename)
        print(f"Trades written to {trades_filename}")

        returns_series = pd.Series(strategy_returns, index=all_datetimes)

        hourly_returns = returns_series.resample('1h').sum()
        cumulative_hourly_returns = hourly_returns.cumsum()

        custom_headers = [f'Strategy_{strategy_id}']
        hourly_returns_filename = f'{settings.write_all_path}/strategy_{strategy_id}.returns.csv'
        cumulative_hourly_returns.to_csv(hourly_returns_filename, mode='w', index=True, header=custom_headers)
        print(f"Hourly returns written to {hourly_returns_filename}")

if __name__ == "__main__":

    # Ensure strategies use same indicator reset when use_database_strategy_data = False

    test_strategies = [2000]
    use_database_strategy_data = True

    market_strategies = {}
    strategy_jsons = {}    

    for test_strategy_id in test_strategies:
        (database_market, database_optimisation_date, database_parameters_json) = get_strategy(test_strategy_id, 
                                                                                           settings.host, settings.strategies_database, 
                                                                                           settings.user, settings.password)
        if database_market not in market_strategies:
            market_strategies[database_market] = []
        market_strategies[database_market].append(test_strategy_id)
        strategy_jsons[test_strategy_id] = database_parameters_json    

    print(f'Running {len(test_strategies)} strategies using {len(market_strategies)} markets')
        
    if use_database_strategy_data:
        with multiprocessing.Pool(processes=len(market_strategies)) as pool:
            pool.starmap(write_strategies_for_market,
                        [(market, market_strategies[market]) for market in market_strategies.keys()])
    else:
        with multiprocessing.Pool(processes=len(market_strategies)) as pool:
            pool.starmap(calculate_strategies_for_market,
                        [(market, market_strategies[market], strategy_jsons) for market in market_strategies.keys()])
        
    print(f'Finished')
                   