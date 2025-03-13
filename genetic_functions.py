import random
import numpy as np
import math
import time

from constants import BarMinutes, IndicatorReset, BarTypes, OHLC
from database_reference import get_database_data, get_holidays
from indicator_registry import indicator_registry, indicator_options, update_indicator_registry_lookbacks
import settings

def random_parameter(parameter, low, high, options, strategy_options):
    if parameter == "bar_type":
        return random.choice(list(BarMinutes)).value
    elif parameter == "session":
        return random.choice(strategy_options['session'])
    elif parameter == "day_of_week":
        return random.choice(strategy_options['day_of_week'])
    elif options is not None:
        return random.choice(options)
    elif isinstance(low, int) and isinstance(high, int):
        return random.randint(low, high)
    elif isinstance(low, float) and isinstance(high, float):
        return random.uniform(low, high)
    return 0

def adjust_parameter(parameter, low, high, options, current_value, adjust_percentage, strategy_options):

    if parameter == "bar_type":
        return random.choice(list(BarMinutes)).value
    elif parameter == "session":
        return random.choice(strategy_options['session'])
    elif parameter == "day_of_week":
        return random.choice(strategy_options['day_of_week'])
    elif options is not None:
        return random.choice(options)

    lower_bound = max(low, current_value - (current_value * adjust_percentage))
    upper_bound = min(high, current_value + (current_value * adjust_percentage))
    range = upper_bound - lower_bound
    if lower_bound < 0:
        lower_bound = max(low, current_value + (range * adjust_percentage))
    if upper_bound < 0:        
        upper_bound = min(high, current_value - (range * adjust_percentage))
        
    if isinstance(low, int) and isinstance(high, int):
        return random.randint(int(math.floor(lower_bound)), int(math.ceil(upper_bound)))
    elif isinstance(lower_bound, float) and isinstance(upper_bound, float):
        return random.uniform(lower_bound, upper_bound)
    
    return 0

def generate_strategy_parameters(strategy_options):

    parameters = {}

    for parameter, value in strategy_options.items():
        if isinstance(value, list):
            parameters[parameter] = random_parameter(parameter, 0, 0, value, strategy_options)
        elif isinstance(value, tuple):
            parameters[parameter] = random_parameter(parameter, value[0], value[1], None, strategy_options)
        else:
            parameters[parameter] = value

    return parameters

def generate_indicator_parameters(indicator_name):

    parameters = {}
    for parameter, value in indicator_options[indicator_name].items():
        if isinstance(value, list):
            parameters[parameter] = random_parameter(parameter, 0, 0, value, None)
        elif isinstance(value, tuple):
            parameters[parameter] = random_parameter(parameter, value[0], value[1], None, None)
    
    return parameters

def select_mutation_parameter(strategy_parameters):

    mutable_options = []
    for strategy_parameter, strategy_parameter_value in strategy_parameters.items():
        ## Can not change day_of_week or session
        if strategy_parameter == 'day_of_week' or strategy_parameter == 'session' or \
            strategy_parameter == 'monday' or strategy_parameter == 'tuesday' or \
            strategy_parameter == 'wednesday' or strategy_parameter == 'thursday' or strategy_parameter == 'friday' or \
            strategy_parameter == 'take_every_signal' or strategy_parameter == 'one_trade_per_week' or strategy_parameter == 'indicator_reset':
            continue

        if strategy_parameter == 'indicators':
            for indicator_index, (indicator_name, indicator_parameters) in enumerate(strategy_parameter_value):
                for param_name, param_value in indicator_parameters.items():

                    ## Only using 1 minute bars so need to mutate the bartype
                    if param_name == 'bartype':
                        continue

                    mutable_options.append((indicator_index, indicator_name, param_name))
        else:
            mutable_options.append(('strategy', 'strategy', strategy_parameter))

    return random.choice(mutable_options)

def decide_range_stoploss(mutation_range_min, mutation_range_max, 
                          strategy_parameters, given_market_requirements, given_market_limits):
    existing_profit_target = strategy_parameters['profit_target']
    existing_stoploss = strategy_parameters['stoploss']

    requirement_profit_target_over_stoploss = given_market_requirements['profit_target_over_stoploss']
    limit_profit_target_over_stoploss = given_market_limits['profit_target_over_stoploss']

    stoploss_range_min = existing_profit_target / limit_profit_target_over_stoploss
    stoploss_range_max = existing_profit_target / requirement_profit_target_over_stoploss

    calculated_mutation_range_min = max(mutation_range_min, stoploss_range_min)
    calculated_mutation_range_max = min(mutation_range_max, stoploss_range_max)

    inclusive_mutation_range_min = min(calculated_mutation_range_min, existing_stoploss)
    inclusive_mutation_range_max = max(calculated_mutation_range_max, existing_stoploss)    

    return inclusive_mutation_range_min, inclusive_mutation_range_max

def decide_range_profit_target(mutation_range_min, mutation_range_max, 
                               strategy_parameters, given_market_requirements, given_market_limits):
    
    existing_profit_target = strategy_parameters['profit_target']
    existing_stoploss = strategy_parameters['stoploss']

    requirement_profit_target_over_stoploss = given_market_requirements['profit_target_over_stoploss']
    limit_profit_target_over_stoploss = given_market_limits['profit_target_over_stoploss']

    profit_target_range_min = existing_stoploss / (1.0 / requirement_profit_target_over_stoploss)
    profit_target_range_max = existing_stoploss / (1.0 / limit_profit_target_over_stoploss)

    calculated_mutation_range_min = max(mutation_range_min, profit_target_range_min)
    calculated_mutation_range_max = min(mutation_range_max, profit_target_range_max)

    inclusive_mutation_range_min = min(calculated_mutation_range_min, existing_profit_target)
    inclusive_mutation_range_max = max(calculated_mutation_range_max, existing_profit_target)    

    return inclusive_mutation_range_min, inclusive_mutation_range_max

def mutation_parameter(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                       given_market_requirements, given_market_limits):
    
    (mutation_source, indicator_name, mutation_parameter_name) = select_mutation_parameter(strategy_parameters)

    if mutation_source == 'strategy':
        mutation_range = strategy_options[mutation_parameter_name]

        mutation_range_min = mutation_range[0]
        mutation_range_max = mutation_range[1]

        if mutation_parameter_name == 'stoploss' and \
            'profit_target_over_stoploss' in given_market_requirements and \
            'profit_target_over_stoploss' in given_market_limits:
            mutation_range_min, mutation_range_max = decide_range_stoploss(mutation_range_min, mutation_range_max, 
                          strategy_parameters, given_market_requirements, given_market_limits)            
        elif mutation_parameter_name == 'profit_target' and \
            'profit_target_over_stoploss' in given_market_requirements and \
            'profit_target_over_stoploss' in given_market_limits:
            mutation_range_min, mutation_range_max = decide_range_profit_target(mutation_range_min, mutation_range_max, 
                          strategy_parameters, given_market_requirements, given_market_limits)

        strategy_parameters[mutation_parameter_name] = random_parameter(mutation_parameter_name, 
                                                                        mutation_range_min, mutation_range_max, None, settings.strategy_options)
    else:
        mutation_range = indicator_options[indicator_name][mutation_parameter_name]        
        indicator_index = int(mutation_source)
        if isinstance(mutation_range, list):
            strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name] = random_parameter(
                                mutation_parameter_name, mutation_range[0], mutation_range[1], mutation_range, settings.strategy_options)
        else:
            strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name] = random_parameter(
                                mutation_parameter_name, mutation_range[0], mutation_range[1], None, settings.strategy_options)

def mutation_parameter_adjust(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                              given_market_requirements, given_market_limits):

    (mutation_source, indicator_name, mutation_parameter_name) = select_mutation_parameter(strategy_parameters)

    if mutation_source == 'strategy':
        mutation_range = strategy_options[mutation_parameter_name]
        if isinstance(mutation_range, list):
            strategy_parameters[mutation_parameter_name] = adjust_parameter(mutation_parameter_name, mutation_range[0], mutation_range[1], mutation_range,
                                                                            strategy_parameters[mutation_parameter_name],
                                                                            mutation_parameter_adjust_percentage,
                                                                            strategy_options)
        else:        

            mutation_range_min = mutation_range[0]
            mutation_range_max = mutation_range[1]

            if mutation_parameter_name == 'stoploss' and \
                'profit_target_over_stoploss' in given_market_requirements and \
                'profit_target_over_stoploss' in given_market_limits:
                mutation_range_min, mutation_range_max = decide_range_stoploss(mutation_range_min, mutation_range_max, 
                            strategy_parameters, given_market_requirements, given_market_limits)
            elif mutation_parameter_name == 'profit_target' and \
                'profit_target_over_stoploss' in given_market_requirements and \
                'profit_target_over_stoploss' in given_market_limits:
                mutation_range_min, mutation_range_max = decide_range_profit_target(mutation_range_min, mutation_range_max, 
                            strategy_parameters, given_market_requirements, given_market_limits)

            strategy_parameters[mutation_parameter_name] = adjust_parameter(mutation_parameter_name, mutation_range_min, mutation_range_max, None,
                                                                            strategy_parameters[mutation_parameter_name],
                                                                            mutation_parameter_adjust_percentage,
                                                                            strategy_options)
    else:
        mutation_range = indicator_options[indicator_name][mutation_parameter_name]        
        indicator_index = int(mutation_source)                
        if isinstance(mutation_range, list):
            strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name] = adjust_parameter(mutation_parameter_name, mutation_range[0], mutation_range[1],
                                                                                                              mutation_range,
                                                                                                              strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name],
                                                                                                              mutation_parameter_adjust_percentage,
                                                                                                              strategy_options)
        else:        
            strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name] = adjust_parameter(mutation_parameter_name, mutation_range[0], mutation_range[1],
                                                                                                              None,
                                                                                                              strategy_parameters['indicators'][indicator_index][1][mutation_parameter_name],
                                                                                                              mutation_parameter_adjust_percentage,
                                                                                                              strategy_options)
            
def mutation_indicator_add(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage,
                           given_market_requirements, given_market_limits):
    indicator_name = random.choice(list(indicator_registry.keys()))
    indicator_parameters = generate_indicator_parameters(indicator_name)
    strategy_parameters['indicators'].append((indicator_name, indicator_parameters))

def mutation_indicator_remove(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                              given_market_requirements, given_market_limits):
    indicator_count = len(strategy_parameters['indicators'])
    remove_indicator_index = random.randint(0, indicator_count - 1)
    strategy_parameters['indicators'].pop(remove_indicator_index)    

def mutation_indicator_swap(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                            given_market_requirements, given_market_limits):
    mutation_indicator_remove(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                              given_market_requirements, given_market_limits)
    mutation_indicator_add(strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                           given_market_requirements, given_market_limits)

mutation_functions = {
    "mutation_parameter": mutation_parameter,
    "mutation_parameter_adjust": mutation_parameter_adjust,
    "mutation_indicator_add": mutation_indicator_add,
    "mutation_indicator_remove": mutation_indicator_remove,
    "mutation_indicator_swap": mutation_indicator_swap
}

def custom_mutation(
        strategy_parameters, 
        indicator_count,
        strategy_options, 
        mutation_parameter_adjust_percentage,
        mutation_parameter_probability, 
        mutation_parameter_adjust_probability,
        mutation_indicator_add_probability, 
        mutation_indicator_remove_probability, 
        mutation_indicator_swap_probability,
        given_market_requirements,
        given_market_limits
        ):
    if len(strategy_parameters['indicators']) == 0:
        return strategy_parameters,

    probabilities = [
        mutation_parameter_probability,         
        mutation_parameter_adjust_probability,
        mutation_indicator_swap_probability
    ]

    mutation_options = [
        "mutation_parameter", 
        "mutation_parameter_adjust",
        "mutation_indicator_swap"
    ]

    if len(strategy_parameters['indicators']) < indicator_count:
        probabilities.append(mutation_indicator_add_probability)
        mutation_options.append("mutation_indicator_add")
    if len(strategy_parameters['indicators']) > 1:
        probabilities.append(mutation_indicator_remove_probability)
        mutation_options.append("mutation_indicator_remove")

    total_probability = sum(probabilities)
    normalized_probabilities = [p / total_probability for p in probabilities]

    selected_mutation = np.random.choice(mutation_options, p=normalized_probabilities)
    mutation_functions[selected_mutation](strategy_parameters, strategy_options, mutation_parameter_adjust_percentage, 
                                          given_market_requirements, given_market_limits)
    
    return strategy_parameters,

def custom_crossover(ind1, ind2):

    min_indicator_count = min(len(ind1['indicators']), len(ind2['indicators']))
    if min_indicator_count > 1: # Need more than 1 indicator otherwise you can swap the main indicator
    
        crossover_index = random.randint(0, min_indicator_count - 1)
        swap = ind2['indicators'][crossover_index]
        ind2['indicators'][crossover_index] = ind1['indicators'][crossover_index]
        ind1['indicators'][crossover_index] = swap

    return ind1, ind2

### Indicator Testing ###
if __name__ == "__main__":        

    indicator_names = ['ChaikinVol_With']
    run_count = 10

    market = 'GC'
    start = '2022-01-02'
    end = '2024-01-07'

    indicator_reset = IndicatorReset.Daily
    
    holidays_df = get_holidays(settings.host, settings.strategies_database, settings.user, settings.password)
    test_bars, period_lookup, period_offsets, period_lengths, all_datetimes, all_closes, day_of_week_lookup = get_database_data(
            settings.host, settings.user, settings.password, market, start, end, "test", holidays_df, indicator_reset)
    
    update_indicator_registry_lookbacks(indicator_reset)

    bars_open = test_bars[BarTypes.Minute1.value][OHLC.Open.value]
    bars_high = test_bars[BarTypes.Minute1.value][OHLC.High.value]
    bars_low = test_bars[BarTypes.Minute1.value][OHLC.Low.value]
    bars_close = test_bars[BarTypes.Minute1.value][OHLC.Close.value]
    bars_volume = test_bars[BarTypes.Minute1.value][OHLC.Volume.value]

    for indicator_name in indicator_names:
        for counter in range(run_count):
            indicator_parameters = generate_indicator_parameters(indicator_name)

            bar_count = len(all_datetimes)
            long_signal_count = 0
            short_signal_count = 0

            period_count = test_bars.shape[2]

            start_indicator_generation_time = time.time()                
            for period_index in range(period_count):                
                long_signal, short_signal = indicator_registry[indicator_name](
                    bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, indicator_parameters)
                long_signal_count += np.sum(long_signal)
                short_signal_count += np.sum(short_signal)                
            end_indicator_generation_time = time.time()

            long_rate =  long_signal_count / bar_count
            short_rate = short_signal_count / bar_count

            indicator_time_ms = (end_indicator_generation_time - start_indicator_generation_time) * 1000
            print(f"{indicator_name} Test {counter+1}: Time: {indicator_time_ms:.2f}ms LongRate: {long_rate:.2f} ShortRate: {short_rate:.2f} - {indicator_parameters}")
