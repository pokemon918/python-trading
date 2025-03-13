import pandas as pd
import json
import multiprocessing
from datetime import datetime

from constants import IndicatorReset, enum_decoder, enum_encoder
from database_strategies import update_strategy_json, get_last_scores, delete_strategy_returns, delete_strategy_trades, get_latest_config
from database_reference import get_holidays
from market_reference import market_slippage, market_contract_size
from strategy_scoring import write_strategy, create_optimisation_dates, fetch_market_data
from strategy_ga import import_python_file
import settings 
       
def run_strategy_incremental(overwrite_all_scores, market, strategy_id, strategy_market, strategy_json, 
                             strategy_optimisation_date, last_score_date, write_trades_returns, stoploss_max,
                             min_sharpes_lookbacks, strategy_score_lookbacks,
                             mean_reverting_flag_requirements, mean_reverting_flag_limits):

    if market != strategy_market:
        return

    strategy = json.loads(strategy_json, object_hook=enum_decoder)

    # Logic to update the stoploss if the max stoploss has been changed
    if strategy['stoploss'] > stoploss_max:
        print(f'Updating {strategy_id}: Stoploss to {stoploss_max}')
        strategy['stoploss'] = stoploss_max
        overwrite_all_scores = True
        write_trades_returns = True
        parameters_json = json.dumps(strategy, indent=4, cls=enum_encoder)
        update_strategy_json(strategy_id, parameters_json, settings.host, settings.strategies_database, settings.user, settings.password)

    slippage = market_slippage[market] / 200
    contract_size = market_contract_size[market]
    all_period_count = all_bars.shape[2]
    
    start = last_score_date
    if overwrite_all_scores:
        start = strategy_optimisation_date
    start_pd = pd.Timestamp(start)
    optimisation_dates = create_optimisation_dates(start_pd, settings.end_pd)

    print(f'{datetime.now()}: Incrementing,{strategy_id},{start},{end}')

    if overwrite_all_scores and write_trades_returns:
        delete_strategy_returns(strategy_id, settings.host, settings.strategies_database, settings.user, settings.password)
        delete_strategy_trades(strategy_id, settings.host, settings.strategies_database, settings.user, settings.password)

    strategy_optimisation_date_pd = pd.Timestamp(strategy_optimisation_date)
    
    generation_time_ms, score_time_ms = write_strategy(strategy, 0, 0, 0, 0, 0, strategy_optimisation_date_pd, market, slippage, contract_size,
                    all_period_count, all_bars, all_allowed_entry_sessions, all_allowed_entry_days, 
                    all_period_offsets, all_period_lengths, all_day_of_week_lookup, all_datetimes, all_timed_exits,
                    all_period_lookup, optimisation_dates, settings.host, settings.strategies_database, settings.user, settings.password,
                    min_sharpes_lookbacks, strategy_score_lookbacks,
                    mean_reverting_flag_requirements, mean_reverting_flag_limits,
                    write_database = True, show_scores = False, 
                    insert_strategy_database = False, write_trades_returns = write_trades_returns, 
                    override_strategy_id = strategy_id)

    print(f'{datetime.now()}: Finished {strategy_id},{generation_time_ms:.2f},{score_time_ms:.2f}')        

if __name__ == "__main__":

    overwrite_all_scores = True
    override_strategies = [8914]
    override_markets = None
    write_trades_returns = True
    process_count = 64

    if overwrite_all_scores:
        print(f'{datetime.now()}: Starting Regeneration')
    else:
        print(f'{datetime.now()}: Starting Incremental')    

    config = get_latest_config(settings.config_type, 
                               settings.host, settings.strategies_database,
                               settings.user, settings.password)                   
    settings_config = import_python_file(config, 'settings')

    # Get all strategies and the date of their most recent score

    latest_scores = get_last_scores(settings.host, settings.strategies_database, settings.user, settings.password)
    end = pd.to_datetime(settings.end)

    # Filter the scores if they have no scores, or matching end date which may have more data to increment
    latest_scores = latest_scores[((latest_scores['score_datetime'].isnull()) | (latest_scores['score_datetime'] <= end))]

    if override_strategies is not None:
        latest_scores = latest_scores[latest_scores['strategy_id'].isin(override_strategies)]

    unique_markets = latest_scores['market'].unique()

    if override_markets is not None:
        unique_markets = override_markets

    holidays_df = get_holidays(settings.host, settings.strategies_database, settings.user, settings.password)

    for market in unique_markets:

        print(f'Running {market}')

        market_scores = latest_scores[latest_scores['market'] == market]
        unique_indicator_resets = market_scores['indicator_reset'].unique()

        # Run each indicator reset separately
        for indicator_reset in unique_indicator_resets:
            market_indicator_reset_scores = market_scores[market_scores['indicator_reset'] == indicator_reset]
            indicator_reset = IndicatorReset(indicator_reset)               

            (all_bars, all_period_lookup, all_period_offsets, all_period_lengths, all_datetimes, all_timed_exits, all_allowed_entry_sessions, all_allowed_entry_days, all_day_of_week_lookup) = fetch_market_data(market, 
                            settings.start, settings.end, "All", holidays_df, indicator_reset)

            with multiprocessing.Pool(processes=process_count) as pool:
                all_strategies_results = pool.starmap(run_strategy_incremental,
                                            [(overwrite_all_scores, market, row['strategy_id'], row['market'], row['strategy_json'], row['strategy_optimisation_date'], row['score_datetime'], write_trades_returns, 
                                            settings_config.stoploss_max, settings_config.min_sharpes_lookbacks, settings_config.strategy_score_lookbacks,
                                            settings_config.mean_reverting_flag_requirements, settings_config.mean_reverting_flag_limits)
                                            for _, row in market_indicator_reset_scores.iterrows()])

    print(f'Finished')
