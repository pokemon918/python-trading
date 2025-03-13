import datetime

import pandas as pd
from market_reference import market_slippage
from constants import IndicatorReset, Session

markets = ['CL', 'ES', 'GC', 'NQ', 'EU']
# markets = ['CL', 'ES', 'GC', 'EU']
# markets = ['GC']
start = '2008-01-01'
end = '2025-01-19'
# optimisation_date = '2023-01-01'
optimisation_date = '2022-01-02'

## StrategyGA
indicator_count = 5
population_size = 100
generations = 5000
gens_without_improvement_stop = 100
initial_score_target = 0.0
initial_score_gen_target = 10000
min_score_writeout = 0.0
reset_pool_generations = 10

cross_probability = 0.2
mutation_probability = 0.8
mutation_parameter_probability = 0.2
mutation_parameter_adjust_probability = 0.2
mutation_parameter_adjust_percentage = 0.1
mutation_indicator_add_probability = 0.2
mutation_indicator_remove_probability = 0.2
mutation_indicator_swap_probability = 0.5

# Enhanced adaptive mutation parameters
adaptive_mutation_enabled = False  # Enable/disable adaptive mutation behavior
ptsl_failure_boost = 3.0  # Boost mutation rates when PTSL fails
ptsl_adjustment_percentage = 0.25  # Higher adjustment percentage for PTSL params
ptsl_critical_failure_threshold = 5  # After this many consecutive failures, try dramatic changes

aar_target = 0.1
use_prediction = False
requirement_prediction = 0.0
limit_trade_count = 1500

requirements = {
    'min_sharpes0': 0.8,
    'mst0_52_trade_to_signal_percentage': 0.0,
    # 'min_sharpes0_104': 1.0,
    # 'min_sharpes0_4': 0.0,
    'tradable_weeks_rate0': 0.75,
    'profit_target_over_stoploss': 1.0,
    'trade_win_over_loss0': 1.0,
    'trade_win_over_loss52': 1.0,
    'profit_target_percentage0': 0.0, # To ensure it has hit its stops and targets before
    'stop_loss_percentage0': 0.01, # To ensure that stops aren't too far away from price movements
    'profit_target_stop_loss_percentage0': 0.0,
    # 'aart0': 0.1,
    # 'max_trade_length': 50,
    # 'edge_better_than_random0': 0.08,
    'indicator_reset': IndicatorReset.Daily,
}
market_requirements = {}
# market_requirements['ES'] = {
#     'min_sharpes0': 1.0,
# }
# market_requirements['NQ'] = {
#     'min_sharpes0': 0.8,
# }
market_requirements['GC'] = {
    'min_sharpes0_4': 0.0,
}
market_requirements['CL'] = {
    'min_sharpes0_104': 1.0,
}

limits = {
    'profit_target_over_stoploss': 1.5,
    'trade_win_over_loss0': 1.5,
    'trade_win_over_loss52': 1.5,
    'cost_percentage0': 0.05,
    'indicator_count': 5,
    'trade_count0': 1500,
    # 'weeks_after_optimisation': 50,
}
market_limits = {}
# market_limits['CL'] = {
#     'cost_percentage0': 0.02,
# }
# market_limits['ES'] = {
#     'cost_percentage0': 0.02,
# }
# market_limits['NQ'] = {
#     'max_trade_length': 150,
# }

underscore_multipliers = {
    'tradable_weeks_rate0': 1000000,
    'aart0': 1, # AAR Target is 0.1 so AART jumps from 0.1 to 1.0. This makes improving this score 0.1x as useful as the others
    'tawal': 1,
    'profit_target_over_stoploss': 1,
    'profit_target_percentage0': 1,
    'stop_loss_percentage0': 1,
    'profit_target_stop_loss_percentage0': 1,
    'trade_win_over_loss0': 1,
    'trade_win_over_loss52': 1,
    'trade_to_signal_percentage0': 1,
    'trade_count0': 1,
    'edge_better_than_random0': 1,
}

mean_reverting_flag_requirements = {
    'min_sharpes0': 1.0,
    'min_sharpes520': 0.0,
    'min_sharpes208': 0.0,
    'min_sharpes156': 0.0,
    'min_sharpes104': 0.0,
}
mean_reverting_flag_limits = {
    'min_sharpes13': 0.5,
}

min_sharpes_lookbacks = [0, 520, 208, 156, 104, 52]
edge_requirement = 0.05

use_random_optimisation_date = True

strategy_ga_cache_indicator_count_requirement = 10
strategy_ga_cache_size = 1000

strategy_score_lookbacks = [0, 520, 208, 156, 104, 52, 26, 13, 8, 4]

## Strategy Settings
stoploss_min = 0.0003
stoploss_max = 0.05
profit_target_min = 0.0003
profit_target_max = 0.05
max_trade_lengths = ([5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
                      95, 100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160,
                      165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215, 220, 225, 230,
                      235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290, 295, 300])
select_one_trade_day = False

strategy_options = {
    'stoploss': (stoploss_min, stoploss_max),
    'profit_target': (profit_target_min, profit_target_max),
    'session': [Session.All],
    'indicator_reset': IndicatorReset.Daily,
    # 'indicator_reset': [IndicatorReset.Daily, IndicatorReset.Weekly],
    'max_trade_length': max_trade_lengths,
    'take_every_signal': True,
    'one_trade_per_week': False,
    'monday': (0, 1),
    'tuesday': (0, 1),
    'wednesday': (0, 1),
    'thursday': (0, 1),
    'friday': (0, 1),
}

## Portfolio Settings
global_leverage_limit = 10
market_leverage_limit = {
    'CD': 1.0,
    'CL': 1.0,
    'ES': 1.0,
    'EU': 1.0,
    'GC': 1.0,
    'HG': 1.0,
    'NG': 1.0,
    'NQ': 1.0,
    'ZB': 1.0,
    'ZS': 1.0,
    'JP': 1.0,
}
account_size = 1000000
portfolio_max_correlation = 0.2
all_max_weight = True

## PoC
ranking_score = 'score'
# ranking_score = 'min_sharpes0'
portfolio_name = 'futures'
model_name = 'ai_model'
# creation_date_start = '2025-02-10 21:58'  # Repeated pattern change
creation_date_start = None # eg '2025-01-01 00:00' or None
creation_date_end = None
write_poc_trades = True
write_poc_returns = True
write_all_weightings = True
write_all_path = f'.'
data_process_count = 8
build_process_count = 8
strategy_cache_size = 2000

start_pd = pd.Timestamp(start)
end_pd = pd.Timestamp(end)
optimisation_date_pd = pd.Timestamp(optimisation_date)
original_optimisation_date_pd = pd.Timestamp(optimisation_date)
tawal_requirements = {}
for market in markets:
    tawal_requirements[market] = (market_slippage[market] / 100.0) / edge_requirement

config_type = 'standard'
host = "10.10.209.61"
strategies_database = "strategies"
user = "postgres"
password = "savernake01"

# Remote IP address
host = "185.28.89.199"
