import pandas as pd
import numpy as np
import json
import time
from numba import njit
from functools import reduce
import multiprocessing as mp

from constants import BarTypes, OHLC, DayOfWeek, MINUTES_PER_DAY, IndicatorReset, Session, enum_decoder, ExitReason, \
    EXIT_REASON_UNKNOWN, EXIT_REASON_STOPLOSS, EXIT_REASON_PROFIT_TARGET, \
    EXIT_REASON_TIMED_EXIT, EXIT_REASON_MAX_LENGTH, EXIT_REASON_NEXT_ENTRY, \
    DECISON_NONE, DECISON_FLAT, DECISON_LONG, DECISON_SHORT, DECISON_UNKNOWN, TRADING_DAY_COUNT, DEFAULT_DATETIME
from database_reference import get_holidays, get_database_data
from database_strategies import get_strategy
from trade_timing import create_all_exits, create_allowed_entries, create_before_timed_entries, get_start_of_week
from market_reference import market_slippage, market_contract_size
from indicator_registry import indicator_registry, calculate_max_lookback
from shared_memory import attach_shared_indicator_cache, detach_indicator_cache, close_shared_data, \
    attach_shared_bars, attach_shared_timed_exits, attach_shared_allowed_sessions, attach_shared_allowed_days
import settings

def create_trade_df(trades, all_datetimes, trade_entry_datetimes_np, trade_returns_np):
    trade_df = pd.DataFrame(
        {
            "Direction": np.array([trade[0] for trade in trades]),
            "Entry DateTime": trade_entry_datetimes_np,
            "Exit DateTime": np.array([all_datetimes[trade[1]] for trade in trades]),
            "Entry Price": np.array([trade[2] for trade in trades]),
            "Exit Price": np.array([trade[3] for trade in trades]),
            "Return": trade_returns_np,
            "Reason": np.array([trade[4] for trade in trades]),
            "Profit Target": np.array([trade[5] for trade in trades]),
            "Stoploss": np.array([trade[6] for trade in trades]),
            "Entry Price Before Slippage": np.array([trade[7] for trade in trades]),
            "Exit Price Before Slippage": np.array([trade[8] for trade in trades]),
        }
    )
    trade_df["Direction"] = trade_df["Direction"].replace({1: "Long", -1: "Short"})

    return trade_df


# Function that can backtest a strategy
def run_strategy(strategy, market, calculate_trade_dataframe,
                 bars_datetime, bars_open, bars_high, bars_low, bars_close, bars_volume,
                 timed_exits, allowed_entry_days, allowed_entry_sessions,
                 period_offsets, period_lengths, all_datetimes, slippage,
                 period_count,
                 pid=0,
                 allowed_minutes_per_period=0,
                 indicator_cache_lookup=None,
                 pool=None,
                 buffer_returns_array=None,
                 limit_trade_count=0,
                 write_strategy_trace=False):
    trade_entry_datetimes = []
    trade_returns = []
    returns_array = buffer_returns_array
    if buffer_returns_array is None:
        returns_array = np.zeros(len(all_datetimes), dtype=np.float64)

    # Calculates all trades and produces returns and optionally trade list
    trades, signal_counts, profit_target_counts, stoploss_counts, trade_indexes, best_profit, worst_loss, fail_strategy = calculate_trades(
        strategy, market, 
        bars_datetime, bars_open, bars_high, bars_low, bars_close, bars_volume,
        timed_exits, allowed_entry_days, allowed_entry_sessions,
        period_offsets, period_lengths, slippage,
        returns_array, trade_entry_datetimes, trade_returns, limit_trade_count, calculate_trade_dataframe,
        indicator_cache_lookup, write_strategy_trace, all_datetimes, pool,
        pid, allowed_minutes_per_period, period_count)

    trade_entry_datetimes_np = np.fromiter(trade_entry_datetimes, dtype=object)
    trade_returns_np = np.fromiter(trade_returns, dtype=np.float64)

    trade_df = None
    if calculate_trade_dataframe:
        trade_df = create_trade_df(trades, all_datetimes, trade_entry_datetimes_np, trade_returns_np)
        del trades

    del trade_entry_datetimes, trade_returns

    return returns_array, trade_entry_datetimes_np, trade_returns_np, trade_df, signal_counts, profit_target_counts, stoploss_counts, trade_indexes, best_profit, worst_loss, fail_strategy

def calculate_entries(strategy, market,
                      bars_open, bars_high, bars_low, bars_close, bars_volume,
                      timed_exits, allowed_entry_days, allowed_entry_sessions,
                      period_index, period_length, allowed_entry_session_index, allowed_entry_day_indexes,
                      indicator_cache_lookup, write_strategy_trace,
                      pid, allowed_minutes_per_period, period_count):
    indicators_cache_long = None
    indicators_cache_short = None
    if indicator_cache_lookup is not None:
        (indicators_cache_long, indicators_cache_short, indicator_long_shared_memory,
         indicator_short_shared_memory) = attach_shared_indicator_cache(
            pid, market, period_count, allowed_minutes_per_period)
    
    using_shared_memory = False
    # Ensure that these variables are kept to keep the shared memory variables alive
    bars_open_shared_memory = None
    bars_high_shared_memory = None
    bars_low_shared_memory = None
    bars_close_shared_memory = None
    bars_volume_shared_memory = None
    timed_exits_shared_memory = None
    allowed_entry_sessions_shared_memory = None
    allowed_entry_days_shared_memory = None

    signal_trace = None
    if write_strategy_trace:
        signal_trace = []

    if bars_open is None:
        using_shared_memory = True
        (bars_open, bars_open_shared_memory) = attach_shared_bars(pid, market, period_count, OHLC.Open.value)
        (bars_high, bars_high_shared_memory) = attach_shared_bars(pid, market, period_count, OHLC.High.value)
        (bars_low, bars_low_shared_memory) = attach_shared_bars(pid, market, period_count, OHLC.Low.value)
        (bars_close, bars_close_shared_memory) = attach_shared_bars(pid, market, period_count, OHLC.Close.value)
        (bars_volume, bars_volume_shared_memory) = attach_shared_bars(pid, market, period_count, OHLC.Volume.value)
        (timed_exits, timed_exits_shared_memory) = attach_shared_timed_exits(pid, market, period_count)
        (allowed_entry_sessions, allowed_entry_sessions_shared_memory) = attach_shared_allowed_sessions(pid, market, period_count)
        (allowed_entry_days, allowed_entry_days_shared_memory) = attach_shared_allowed_days(pid, market, period_count)

    period_exits = timed_exits[period_index]

    day_entries = None
    if len(allowed_entry_day_indexes) == TRADING_DAY_COUNT:
        # if strategy can trade every day of the week, then only consider the session
        day_session_entries = allowed_entry_sessions[period_index][allowed_entry_session_index]
    else:
        # Combine which days of the week can be traded on
        day_entries = reduce(np.logical_or, [allowed_entry_days[period_index][i] for i in allowed_entry_day_indexes])
        # Combine with which sessions can be traded on
        day_session_entries = np.logical_and(allowed_entry_sessions[period_index][allowed_entry_session_index], day_entries)

    day_session_entries_without_exits = np.logical_and(day_session_entries, np.logical_not(period_exits))

    allowed_entries_before_timed_exits = None
    if 'max_trade_length' in strategy:
        allowed_entries_before_timed_exits = create_before_timed_entries(period_exits, strategy['max_trade_length'])
        allowed_entries = np.logical_and(allowed_entries_before_timed_exits, day_session_entries_without_exits)
    else:
        allowed_entries = day_session_entries_without_exits

    # Can not enter for the first minutes of length max lookback of the indicators
    strategy_max_lookback = min(calculate_max_lookback(strategy), len(allowed_entries))
    if strategy_max_lookback > 0:
        allowed_entries[:strategy_max_lookback] = False

    if write_strategy_trace:
        signal_trace.append(allowed_entries)

    long_signals = np.empty((len(strategy['indicators']), len(bars_open[period_index])), dtype=bool)
    short_signals = np.empty((len(strategy['indicators']), len(bars_open[period_index])), dtype=bool)

    array_index = 0
    for indicator_name, params in strategy['indicators']:
        long_indicator_signals = None
        short_indicator_signals = None

        if indicator_cache_lookup is not None:
            indicator_parameters = f'{indicator_name},{params}'
            indicator_index = indicator_cache_lookup.get(indicator_parameters, None)
            del indicator_parameters
            if indicator_index is not None:
                long_indicator_signals = indicators_cache_long[indicator_index][period_index]
                short_indicator_signals = indicators_cache_short[indicator_index][period_index]

        if long_indicator_signals is None:
            long_indicator_signals, short_indicator_signals = indicator_registry[indicator_name](
                bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params)

        if len(long_indicator_signals) != len(short_indicator_signals):
            raise Exception(
                f'Indicator had different length long and short signals. Indicator was {indicator_name},{params} for {strategy}')

        long_signals[array_index] = long_indicator_signals
        short_signals[array_index] = short_indicator_signals

        if write_strategy_trace:
            signal_trace.append(long_indicator_signals)
            signal_trace.append(short_indicator_signals)

        array_index += 1

    combined_entries = calculate_strategy_decisions(allowed_entries, long_signals, short_signals, period_length)
    entry_indices = np.where(combined_entries != 0)[0]
    signal_count = np.count_nonzero(combined_entries)
    decisions = {}
    for entry_index in entry_indices:
        decisions[entry_index] = combined_entries[entry_index]

    if write_strategy_trace:
        signal_trace.append(combined_entries)
        calculate_period_strategy_trace(strategy, bars, signal_trace, period_index)

    del long_signals, short_signals, day_entries, day_session_entries, allowed_entries_before_timed_exits, day_session_entries_without_exits, combined_entries

    # Clean up process
    if indicator_cache_lookup is not None:
        detach_indicator_cache(indicator_long_shared_memory, indicator_short_shared_memory)
    if using_shared_memory:
        close_shared_data(bars_open_shared_memory)
        close_shared_data(bars_high_shared_memory)
        close_shared_data(bars_low_shared_memory)
        close_shared_data(bars_close_shared_memory)
        close_shared_data(bars_volume_shared_memory)
        close_shared_data(timed_exits_shared_memory)
        close_shared_data(allowed_entry_sessions_shared_memory)
        close_shared_data(allowed_entry_days_shared_memory)

    return (entry_indices, decisions, signal_count)


@njit(cache=True)
def calculate_strategy_decisions(allowed_entries, long_signals, short_signals, period_length):
    indicator_count = len(long_signals)

    decisions = np.zeros(period_length, dtype=np.int32)
    
    check_indicator_decisions = False    

    previous_decision = DECISON_FLAT

    for minute_index in range(period_length):
        if check_indicator_decisions:
            
            all_indicators_flat = True
            for indicator_index in range(indicator_count):
                if long_signals[indicator_index][minute_index] == True or short_signals[indicator_index][minute_index] == True:
                    all_indicators_flat = False
                    break
            
            if all_indicators_flat:
                check_indicator_decisions = False

        # Recheck check_indicator_decisions since it could have been updated on this minute_index
        if check_indicator_decisions:
            previous_decision = DECISON_FLAT
            continue        

        if allowed_entries[minute_index] == False:
            previous_decision = DECISON_FLAT
            continue

        current_decision = DECISON_NONE

        for indicator_index in range(indicator_count):

            if long_signals[indicator_index][minute_index] == False and short_signals[indicator_index][
                minute_index] == False:
                current_decision = DECISON_FLAT
                break
            elif long_signals[indicator_index][minute_index] == True and short_signals[indicator_index][
                minute_index] == False:
                if current_decision == DECISON_NONE or current_decision == DECISON_LONG or current_decision == DECISON_UNKNOWN:
                    current_decision = DECISON_LONG
                else:
                    current_decision = DECISON_FLAT
                    break
            elif long_signals[indicator_index][minute_index] == False and short_signals[indicator_index][
                minute_index] == True:
                if current_decision == DECISON_NONE or current_decision == DECISON_SHORT or current_decision == DECISON_UNKNOWN:
                    current_decision = DECISON_SHORT
                else:
                    current_decision = DECISON_FLAT
                    break
            elif long_signals[indicator_index][minute_index] == True and short_signals[indicator_index][
                minute_index] == True:
                if current_decision == DECISON_NONE:
                    current_decision = DECISON_UNKNOWN

        if current_decision == DECISON_FLAT:
            continue

        if current_decision == DECISON_UNKNOWN:
            current_decision = DECISON_FLAT

        if current_decision == previous_decision:
            if current_decision == DECISON_LONG or current_decision == DECISON_SHORT:
                current_decision = DECISON_FLAT
        else:
            previous_decision = current_decision

        decisions[minute_index] = current_decision
        if current_decision == DECISON_LONG or current_decision == DECISON_SHORT:
            check_indicator_decisions = True

    return decisions


def calculate_allowed_entry_day_lookup(strategy):
    allowed_days_lookup = {}
    if strategy['monday'] == 1:
        allowed_days_lookup[DayOfWeek.Monday.value] = True
    if strategy['tuesday'] == 1:
        allowed_days_lookup[DayOfWeek.Tuesday.value] = True
    if strategy['wednesday'] == 1:
        allowed_days_lookup[DayOfWeek.Wednesday.value] = True
    if strategy['thursday'] == 1:
        allowed_days_lookup[DayOfWeek.Thursday.value] = True
    if strategy['friday'] == 1:
        allowed_days_lookup[DayOfWeek.Friday.value] = True

    return allowed_days_lookup


def calculate_allowed_entry_day_indexes(strategy):
    allowed_entry_day_indexes = []
    if strategy['monday'] == 1:
        allowed_entry_day_indexes.append(DayOfWeek.Monday.value)
    if strategy['tuesday'] == 1:
        allowed_entry_day_indexes.append(DayOfWeek.Tuesday.value)
    if strategy['wednesday'] == 1:
        allowed_entry_day_indexes.append(DayOfWeek.Wednesday.value)
    if strategy['thursday'] == 1:
        allowed_entry_day_indexes.append(DayOfWeek.Thursday.value)
    if strategy['friday'] == 1:
        allowed_entry_day_indexes.append(DayOfWeek.Friday.value)

    return allowed_entry_day_indexes


def calculate_period_strategy_trace(strategy, bars, signal_trace, period_index):
    # strategy trace shows bar information, with indicator decisions

    bar_data = {
        'datetime': bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index],
        'open': bars[BarTypes.Minute1.value][OHLC.Open.value][period_index],
        'high': bars[BarTypes.Minute1.value][OHLC.High.value][period_index],
        'low': bars[BarTypes.Minute1.value][OHLC.Low.value][period_index],
        'close': bars[BarTypes.Minute1.value][OHLC.Close.value][period_index],
    }
    datetimes = pd.to_datetime(bars[BarTypes.Minute1.value][OHLC.DateTime.value][period_index])
    bar_data_df = pd.DataFrame(bar_data, index=datetimes)

    signal_trace_headers = []
    signal_trace_headers.append(f'Allowed Entry')
    for indicator_name, params in strategy['indicators']:
        indicator_parameters = f'{indicator_name},{params}'
        signal_trace_headers.append(f'{indicator_parameters}.Long')
        signal_trace_headers.append(f'{indicator_parameters}.Short')

    signal_trace_headers.append(f'Entry Decision')
    
    signal_trace_df = pd.DataFrame(list(zip(*signal_trace)), columns=signal_trace_headers)
    signal_trace_df.index = bar_data_df.index
    signal_trace_df["Entry Decision"] = signal_trace_df["Entry Decision"].replace({1: "Long", 0: "Flat", -1: "Short"})

    combined_df = pd.concat([bar_data_df, signal_trace_df], axis=1)

    combined_df['Parameter.max_trade_length'] = strategy['max_trade_length']
    combined_df['Parameter.stoploss'] = strategy['stoploss']
    combined_df['Parameter.profit_target'] = strategy['profit_target']

    write_all_strategy_trace(combined_df, period_index)

def write_all_strategy_trace(combined_df, period_index):

    initial_columns = ['datetime', 'open', 'high', 'low', 'close', \
                       'Parameter.max_trade_length', 'Parameter.stoploss', \
                       'Parameter.profit_target']

    decision_columns = ['Entry Decision']

    long_columns = [col for col in combined_df.columns if col.endswith('.Long')]
    short_columns = [col for col in combined_df.columns if col.endswith('.Short')]

    new_column_order = initial_columns + long_columns + short_columns + decision_columns
    df_rearranged = combined_df[new_column_order]

    filename = f'{settings.write_all_path}/strategy_{period_index}.trace.csv'
    df_rearranged.to_csv(filename, mode='w', index=False)
    # print(f"Strategy trace written to {filename}")


def calculate_trades(strategy, market, 
                     bars_datetime, bars_open, bars_high, bars_low, bars_close, bars_volume,
                     timed_exits, allowed_entry_days, allowed_entry_sessions,
                     period_offsets, period_lengths, slippage,
                     returns_array, trade_entry_datetimes, trade_returns, limit_trade_count, calculate_trade_dataframe,
                     indicator_cache_lookup, write_strategy_trace, all_datetimes, pool,
                     pid, allowed_minutes_per_period, period_count):
    # Calculates the results of all trades

    session = strategy['session']
    if isinstance(session, int):
        session = Session(session)

    allowed_entry_session_index = session.value
    allowed_entry_day_indexes = calculate_allowed_entry_day_indexes(strategy)
    
    trades = None
    if calculate_trade_dataframe:
        trades = []
    if write_strategy_trace:
        all_strategy_traces = []

    signal_counts = {}
    signal_count_cumulative = 0
    profit_target_counts = {}
    profit_target_count_cumulative = 0
    stoploss_counts = {}
    stoploss_count_cumulative = 0
    trade_indexes = []

    fail_strategy = False
    worst_loss = 0.0
    best_profit = 0.0

    take_every_signal = False
    if 'take_every_signal' in strategy and strategy['take_every_signal']:
        take_every_signal = True

    max_trade_length = 0
    if 'max_trade_length' in strategy:
        max_trade_length = strategy['max_trade_length']

    one_trade_per_week = False
    if 'one_trade_per_week' in strategy:
        one_trade_per_week = strategy['one_trade_per_week']
    last_processed_start_of_week = None

    indicator_reset = strategy['indicator_reset']
    if isinstance(indicator_reset, int):
        indicator_reset = IndicatorReset(indicator_reset)

    period_results = np.empty(period_count, dtype=object)
    if pool is None:
        for period_index in range(period_count):
            entry_indices, decisions, signal_count = calculate_entries(strategy, market,
                                                                       bars_open, bars_high, bars_low, bars_close, bars_volume,
                                                                       timed_exits, allowed_entry_days, allowed_entry_sessions,
                                                                       period_index,
                                                                       period_lengths[period_index],
                                                                       allowed_entry_session_index,
                                                                       allowed_entry_day_indexes,
                                                                       indicator_cache_lookup, write_strategy_trace,
                                                                       pid, allowed_minutes_per_period, period_count)
            period_results[period_index] = (entry_indices, decisions, signal_count)
    else:
        # Pass None for all bars, timed exits and entries to force pool to load from shared memory
        all_period_results = pool.starmap(calculate_entries,
                                          [(strategy, market, 
                                            None, None, None, None, None,
                                            None, None, None,
                                            period_index, period_lengths[period_index], allowed_entry_session_index,
                                            allowed_entry_day_indexes,
                                            indicator_cache_lookup, write_strategy_trace,
                                            pid, allowed_minutes_per_period, period_count
                                            ) for period_index in range(period_count)])
        for period_index, result in enumerate(all_period_results):
            period_results[period_index] = result

            # Processes each period which could be day or week
    for period_index in range(period_count):
        period_exits = timed_exits[period_index]

        if indicator_reset == IndicatorReset.Daily:           
            if last_processed_start_of_week is not None:
                # For daily, skip the day if the strategy has already traded once this week and is only allowed to trade once per week
                current_start_of_week = get_start_of_week(
                    bars_datetime[period_index][0])
                if last_processed_start_of_week == current_start_of_week:
                    continue

        (entry_indices, decisions, signal_count) = period_results[period_index]

        # Track the count of signals by first date in the period
        signal_count_cumulative += signal_count
        signal_counts[bars_datetime[period_index][0]] = signal_count_cumulative
        profit_target_counts[bars_datetime[period_index][0]] = profit_target_count_cumulative
        stoploss_counts[bars_datetime[period_index][0]] = stoploss_count_cumulative

        # Initalise last used exit_index
        last_exit_index = -1

        # Process all allowed entries
        for trade_index in range(len(entry_indices)):
            entry_index = entry_indices[trade_index]

            # Can not enter before the previous exit
            if entry_index < last_exit_index:
                continue

            trade_entry_datetime = bars_datetime[period_index][entry_index]            

            # Do not process any padding
            if trade_entry_datetime == DEFAULT_DATETIME:
                break

            # Calculate the results of the trade
            (trade_return, reason_value, exit_index, all_exit_index, is_profit_target, is_stoploss, next_entry_index,
             entry_price,
             exit_price, profit_target_price, stop_loss_price) = calculate_trade(
                bars_open[period_index],
                bars_high[period_index],
                bars_low[period_index],
                bars_close[period_index],
                returns_array,
                entry_index,
                decisions[entry_index],
                max_trade_length,
                strategy['stoploss'],
                strategy['profit_target'],
                take_every_signal,
                slippage,
                period_exits,
                period_offsets[period_index],
                trade_index,
                entry_indices)

            # Track profit targets and stoplosses used in particular scores
            profit_target_count_cumulative += is_profit_target
            stoploss_count_cumulative += is_stoploss
            if trade_return < worst_loss:
                worst_loss = trade_return
            if trade_return > best_profit:
                best_profit = trade_return
            trade_indexes.append((period_index, entry_price, next_entry_index, exit_index))

            last_exit_index = exit_index

            # Record the trade datetime and return, with optionally the full trade details
            trade_entry_datetimes.append(trade_entry_datetime)
            trade_returns.append(trade_return)
            if calculate_trade_dataframe:
                reason = ExitReason(reason_value)
                entry_price_before_slippage = bars_open[period_index][
                    next_entry_index]
                exit_price_before_slippage = bars_close[period_index][exit_index]
                if reason == ExitReason.Stoploss:
                    exit_price_before_slippage = stop_loss_price
                elif reason == ExitReason.ProfitTarget:
                    exit_price_before_slippage = profit_target_price
                trade_details = (decisions[entry_index], all_exit_index, entry_price, exit_price, reason.name,
                                 profit_target_price, stop_loss_price, entry_price_before_slippage,
                                 exit_price_before_slippage)
                trades.append(trade_details)

            if limit_trade_count > 0 and len(trade_entry_datetimes) >= limit_trade_count:
                # Exceeded limit on allowed number of trades which is treated as a failure
                fail_strategy = True
                break

            if one_trade_per_week:
                if indicator_reset == IndicatorReset.Daily:
                    last_processed_start_of_week = get_start_of_week(bars_datetime[period_index][0])

                # For both daily and weekly reset, always break from processing further trades on the same period
                break

        if fail_strategy:
            break

    if fail_strategy:
        trade_entry_datetimes.clear()
        trade_returns.clear()

    del period_results

    return trades, signal_counts, profit_target_counts, stoploss_counts, trade_indexes, best_profit, worst_loss, fail_strategy


@njit(cache=True)
def calculate_trade(
        bars_open, bars_high, bars_low, bars_close, returns_array,
        entry_index, direction, max_trade_length, stop_loss, profit_target, take_every_signal,
        slippage, period_exits, period_offset,
        trade_index, entry_indices):
    # Calculate prices based on slippage
    if direction == 1:
        entry_price = bars_open[entry_index + 1] * (1 + slippage)
        stop_loss_price = bars_open[entry_index + 1] * (1 - stop_loss)
        profit_target_price = bars_open[entry_index + 1] * (1 + profit_target)
    else:
        entry_price = bars_open[entry_index + 1] * (1 - slippage)
        stop_loss_price = bars_open[entry_index + 1] * (1 + stop_loss)
        profit_target_price = bars_open[entry_index + 1] * (1 - profit_target)

    max_exit_index = np.inf
    if max_trade_length > 0:
        max_possible_trade_exit_index = entry_index + np.minimum(max_trade_length, len(bars_open) - entry_index - 1)
        max_exit_index = max_possible_trade_exit_index
    else:
        # Max possible length of a trade is a day. Use this value without max_trade_length to find the trade exit
        max_possible_trade_exit_index = entry_index + np.minimum(MINUTES_PER_DAY, len(bars_open) - entry_index - 1)

    # Get the slice of highs and lows to decide when stop and profit target would get hit
    lows = bars_low[entry_index + 1:max_possible_trade_exit_index + 1]
    highs = bars_high[entry_index + 1:max_possible_trade_exit_index + 1]

    stop_loss_hit = -1
    profit_target_hit = -1

    if direction == 1:
        stop_loss_check = lows <= stop_loss_price
        if np.any(stop_loss_check):
            stop_loss_hit = np.argmax(lows <= stop_loss_price)
        profit_target_check = highs >= profit_target_price
        if np.any(profit_target_check):
            profit_target_hit = np.argmax(profit_target_check)
    else:
        stop_loss_check = highs >= stop_loss_price
        if np.any(stop_loss_check):
            stop_loss_hit = np.argmax(stop_loss_check)
        profit_target_check = lows <= profit_target_price
        if np.any(profit_target_check):
            profit_target_hit = np.argmax(profit_target_check)

    # Calculate the index of when the profit target and stop are hit
    stop_loss_exit = (entry_index + 1 + stop_loss_hit) if stop_loss_hit >= 0 else np.inf
    profit_target_exit = (entry_index + 1 + profit_target_hit) if profit_target_hit >= 0 else np.inf

    # Calculate the index of when the timed event would trigger
    check_exits = period_exits[entry_index:max_possible_trade_exit_index + 1]
    check_exit_index = -1
    if np.any(check_exits):
        check_exit_index = np.argmax(check_exits)
    timed_exit = (entry_index + check_exit_index) if check_exit_index >= 0 else np.inf

    subsequent_entry_index = np.inf
    if take_every_signal and trade_index + 1 < len(entry_indices):
        subsequent_entry_index = entry_indices[trade_index + 1]

        # The exit is the first exit of all possible exit types
    exit_index = int(min(stop_loss_exit, profit_target_exit, max_exit_index, timed_exit, subsequent_entry_index))

    is_profit_target = 0
    is_stoploss = 0

    reason = EXIT_REASON_UNKNOWN
    # Determine exit price
    if exit_index == stop_loss_exit:
        # Prefer hitting stoploss first
        exit_price = stop_loss_price - (stop_loss_price * slippage) if direction == 1 else stop_loss_price + (
                    stop_loss_price * slippage)
        reason = EXIT_REASON_STOPLOSS
        is_stoploss = 1
    elif exit_index == profit_target_exit:
        # Prefer hitting profit target next since the others are end of minute
        exit_price = profit_target_price - (
                    profit_target_price * slippage) if direction == 1 else profit_target_price + (
                    profit_target_price * slippage)
        reason = EXIT_REASON_PROFIT_TARGET
        is_profit_target = 1
    elif exit_index == timed_exit:
        exit_price = bars_close[exit_index] - (bars_close[exit_index] * slippage) if direction == 1 else bars_close[
                                                                                                             exit_index] + (
                                                                                                                     bars_close[
                                                                                                                         exit_index] * slippage)
        reason = EXIT_REASON_TIMED_EXIT
    elif exit_index == max_exit_index:
        exit_price = bars_close[exit_index] - (bars_close[exit_index] * slippage) if direction == 1 else bars_close[
                                                                                                             exit_index] + (
                                                                                                                     bars_close[
                                                                                                                         exit_index] * slippage)
        reason = EXIT_REASON_MAX_LENGTH
    else:
        exit_price = bars_close[exit_index] - (bars_close[exit_index] * slippage) if direction == 1 else bars_close[
                                                                                                             exit_index] + (
                                                                                                                     bars_close[
                                                                                                                         exit_index] * slippage)
        reason = EXIT_REASON_NEXT_ENTRY

    trade_return = ((exit_price - entry_price) * direction) / entry_price

    all_entry_index = entry_index + period_offset
    all_exit_index = exit_index + period_offset

    # Calculate the P&L return for all minutes using close to close
    returns_array[all_entry_index + 1:all_exit_index + 1] += (
                                                                     bars_close[
                                                                     entry_index + 1:exit_index + 1] - bars_close[
                                                                                                       entry_index:exit_index]
                                                             ) * direction / entry_price
    # Adjust the first minute's return by the difference of the previous close and the entry price
    returns_array[all_entry_index + 1] -= (entry_price - bars_close[entry_index]) * direction / entry_price
    # Adjust the last minute's return by the difference of the close and the exit price
    returns_array[all_exit_index] -= (bars_close[exit_index] - exit_price) * direction / entry_price

    return trade_return, reason, exit_index, all_exit_index, is_profit_target, is_stoploss, entry_index + 1, entry_price, exit_price, profit_target_price, stop_loss_price


def write_outputs(strategy_id, trade_df, returns_array, all_datetimes):
    trades_filename = f'{settings.write_all_path}/strategy_{strategy_id}.trades.csv'
    trade_df.to_csv(trades_filename, mode='w', index=False)
    print(f"Trades written to {trades_filename}")

    returns_series = pd.Series(returns_array, index=all_datetimes)
    minutely_returns_filename = f'{settings.write_all_path}/strategy_{strategy_id}.returns.minutes.csv'
    returns_series.to_csv(minutely_returns_filename, mode='w', index=True)
    print(f"Minutely returns written to {minutely_returns_filename}")

    hourly_returns = returns_series.resample('1h').sum()
    cumulative_hourly_returns = hourly_returns.cumsum()

    custom_headers = [f'Strategy_{strategy_id}']
    hourly_returns_filename = f'{settings.write_all_path}/strategy_{strategy_id}.returns.csv'
    cumulative_hourly_returns.to_csv(hourly_returns_filename, mode='w', index=True, header=custom_headers)
    print(f"Hourly returns written to {hourly_returns_filename}")


if __name__ == "__main__":

    strategy_id = 8792
    write_strategy_trace = True

    # Load from database
    (market, optimisation_date, strategy_json) = get_strategy(
        strategy_id, settings.host, settings.strategies_database,
        settings.user, settings.password)

    # Fixed Strategy
    # market = 'CL'
    # strategy_json = """
    # {
    #     "stoploss": 0.038427251050666446,
    #     "profit_target": 0.03911091520410605,
    #     "session": 3,
    #     "max_trade_length": 150,
    #     "monday": 1,
    #     "tuesday": 0,
    #     "wednesday": 1,
    #     "thursday": 1,
    #     "friday": 0,
    #     "indicators": [
    #         [
    #             "AROONOSC_Against",
    #             {
    #                 "bar_type": 1,
    #                 "timeperiod": 562
    #             }
    #         ],
    #         [
    #             "STOCH_Against",
    #             {
    #                 "bar_type": 1,
    #                 "fastk_period": 203,
    #                 "slowk_period": 248,
    #                 "slowk_matype": 5,
    #                 "slowd_period": 24,
    #                 "slowd_matype": 5
    #             }
    #         ],
    #         [
    #             "APO_Against",
    #             {
    #                 "bar_type": 1,
    #                 "fastperiod": 615,
    #                 "slowperiod": 32
    #             }
    #         ],
    #         [
    #             "MACDEXT_With",
    #             {
    #                 "bar_type": 1,
    #                 "fastperiod": 893,
    #                 "slowperiod": 968,
    #                 "signalperiod": 137,
    #                 "fastmatype": 3,
    #                 "slowmatype": 7,
    #                 "signalmatype": 7
    #             }
    #         ],
    #         [
    #             "WCLPRICE_With",
    #             {
    #                 "bar_type": 1
    #             }
    #         ]
    #     ]
    # }
    # """

    strategy = json.loads(strategy_json, object_hook=enum_decoder)
    print(f'Loading {market}')

    slippage = market_slippage[market] / 200
    contract_size = market_contract_size[market]

    indicator_reset = strategy['indicator_reset']
    if isinstance(indicator_reset, int):
        indicator_reset = IndicatorReset(indicator_reset)

    holidays_df = get_holidays(settings.host, settings.strategies_database, settings.user, settings.password)
    bars, period_lookup, period_offsets, period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
        settings.host, settings.user, settings.password, market, settings.start, settings.end, "all", holidays_df,
        indicator_reset)
    timed_exits = create_all_exits(settings.host, settings.strategies_database, settings.user, settings.password, bars, market, period_lookup,
                                   indicator_reset)
    allowed_entry_sessions, allowed_entry_days = create_allowed_entries(bars, indicator_reset)
    period_count = bars.shape[2]

    pool_size = min(settings.population_size, mp.cpu_count())
    # pool = mp.Pool(processes=pool_size)
    pool = None

    start_backtest_time = time.time()
    (returns_array, trade_entry_datetimes, trade_returns, trade_df, signal_counts,
     profit_target_counts, stoploss_counts, trade_indexes, best_profit, worst_loss, fail_strategy) = run_strategy(
        strategy, market, True, 
        bars[BarTypes.Minute1.value][OHLC.DateTime.value],
        bars[BarTypes.Minute1.value][OHLC.Open.value],
        bars[BarTypes.Minute1.value][OHLC.High.value],
        bars[BarTypes.Minute1.value][OHLC.Low.value],
        bars[BarTypes.Minute1.value][OHLC.Close.value],
        bars[BarTypes.Minute1.value][OHLC.Volume.value],
        timed_exits,
        allowed_entry_days,
        allowed_entry_sessions,
        period_offsets,
        period_lengths,
        all_datetimes,
        slippage,
        period_count,
        write_strategy_trace=write_strategy_trace, 
        pool=pool)
    end_backtest_time = time.time()

    write_outputs(strategy_id, trade_df, returns_array, all_datetimes)

    backtester_time_ms = (end_backtest_time - start_backtest_time) * 1000
    print(f"Backtester Time: {backtester_time_ms:.2f}ms")

    if pool != None:
        pool.close()
        pool.join()

    print(f'Finished')
