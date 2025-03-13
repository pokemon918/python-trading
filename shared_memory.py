import numpy as np

from constants import Session, TRADING_DAY_COUNT
from multiprocessing import shared_memory, resource_tracker

import settings

NP_SHARED_NAME_BARS = 'shared_bars'
NP_SHARED_NAME_TIMED_EXITS = 'shared_timed_exits'
NP_SHARED_NAME_ALLOWED_DAYS = 'shared_allowed_days'
NP_SHARED_NAME_ALLOWED_SESSIONS = 'shared_allowed_sessions'
NP_SHARED_NAME_DATETIMES = 'shared_datetimes'
NP_SHARED_NAME_INDICATOR_CACHE = 'shared_indicator_cache'
NP_SHARED_NAME_STRATEGY = 'shared_strategy'

# Global list to track shared memory references to help clean up
shared_memory_refs = []
shared_strategy_refs = {}


def handle_termination_signal(signum, frame):
    cleanup_shared_memory()
    exit(0)


# Shared Memory Helper Functions
def create_shared_array(arr, name):
    shape = arr.shape
    dtype = arr.dtype
    shm = shared_memory.SharedMemory(create=True, size=arr.nbytes, name=name)
    shared_array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    np.copyto(shared_array, arr)
    # Track the shared memory reference for clean up
    shared_memory_refs.append(shm)
    return (shm, shared_array)


def allocate_shared_array(shape, dtype, name):
    dtype_np = np.dtype(dtype).itemsize
    nbytes = np.prod(shape) * dtype_np
    shm = shared_memory.SharedMemory(create=True, size=nbytes, name=name)
    shared_array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    shared_memory_refs.append(shm)
    return (shm, shared_array)


def cleanup_memory_ref(memory_ref):
    try:
        memory_ref.close()
        memory_ref.unlink()
    except OSError as e:
        if e.errno != 2:  # errno 2 corresponds to "No such file or directory" because it has already been removed
            print(f"Error cleaning shared memory {memory_ref.name}: {e}")
    except Exception as e:
        print(f"Error cleaning shared memory {memory_ref.name}: {e}")

def disable_tracking_shared_memory_in_resource_tracker():
    """Monkey-patch multiprocessing.resource_tracker so SharedMemory won't be tracked

    This function with more details are available at: https://bugs.python.org/issue38119
    """

    def fix_register(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.register(name, rtype)
    resource_tracker.register = fix_register

    def fix_unregister(name, rtype):
        if rtype == "shared_memory":
            return
        return resource_tracker._resource_tracker.unregister(name, rtype)
    resource_tracker.unregister = fix_unregister

    if "shared_memory" in resource_tracker._CLEANUP_FUNCS:
        del resource_tracker._CLEANUP_FUNCS["shared_memory"]

def cleanup_shared_memory():
    cleanup_given_shared_memory(shared_memory_refs)


def cleanup_given_shared_memory(memory_refs):
    for memory_ref in memory_refs:
        cleanup_memory_ref(memory_ref)


def cleanup_shared_strategy_memory():
    for memory_ref in shared_strategy_refs.values():
        cleanup_memory_ref(memory_ref)

def create_shared_bars(pid, market, bars, tag):
    
    shared_bars = {}

    name = f'{pid}_{NP_SHARED_NAME_BARS}_{market}_{tag}'
    array = bars
    create_shared_array(array, name)
    shared_bars[name] = array.shape    

    return shared_bars

def create_shared_datetimes(pid, market, datetimes):
    
    shared_datetimes = {}

    name = f'{pid}_{NP_SHARED_NAME_DATETIMES}_{market}'
    array = datetimes
    create_shared_array(array, name)
    shared_datetimes[name] = array.shape    

    return shared_datetimes

def create_shared_timed_exits(pid, market, timed_exits):
    shared_timed_exits = {}

    name = f'{pid}_{NP_SHARED_NAME_TIMED_EXITS}_{market}'
    array = timed_exits
    create_shared_array(array, name)
    shared_timed_exits[name] = array.shape    

    return shared_timed_exits

def create_shared_allowed_days(pid, market, allowed_days):
    shared_allowed_days = {}

    name = f'{pid}_{NP_SHARED_NAME_ALLOWED_DAYS}_{market}'
    array = allowed_days
    create_shared_array(array, name)
    shared_allowed_days[name] = array.shape

    return shared_allowed_days

def create_shared_allowed_sessions(pid, market, allowed_sessions):
    shared_allowed_sessions = {}

    name = f'{pid}_{NP_SHARED_NAME_ALLOWED_SESSIONS}_{market}'
    array = allowed_sessions
    create_shared_array(array, name)
    shared_allowed_sessions[name] = array.shape

    return shared_allowed_sessions

def create_shared_indicator_cache(pid, market, shape, dtype):
    shared_indicator_cache = {}

    long_name = f'{pid}_{NP_SHARED_NAME_INDICATOR_CACHE}_{market}_long'
    (shm_long, shared_array_long) = allocate_shared_array(shape, dtype, long_name)
    shared_indicator_cache[long_name] = shape
    short_name = f'{pid}_{NP_SHARED_NAME_INDICATOR_CACHE}_{market}_short'
    (shm_short, shared_array_short) = allocate_shared_array(shape, dtype, short_name)
    shared_indicator_cache[short_name] = shape

    return (shared_indicator_cache, shm_long, shared_array_long, shm_short, shared_array_short)


def attach_shared_strategy(pid, strategy_id, minute_bar_count):
    name = f'{pid}_{NP_SHARED_NAME_STRATEGY}_{strategy_id}'
    shape = (minute_bar_count,)

    shm_data = shared_memory.SharedMemory(name=name)
    array = np.ndarray(shape, dtype='float64', buffer=shm_data.buf)
    strategy_returns = array

    return (strategy_returns, shm_data)


def allocate_shared_strategy(pid, strategy_id, minute_bar_count, all_shared_strategy_names, all_shared_strategies):
    name = f'{pid}_{NP_SHARED_NAME_STRATEGY}_{strategy_id}'
    shape = (minute_bar_count,)
    nbytes = np.prod(shape) * np.dtype(np.float64).itemsize

    shm = shared_memory.SharedMemory(create=True, size=nbytes, name=name)
    shared_strategy_refs[name] = shm
    all_shared_strategy_names[strategy_id] = name
    all_shared_strategies[name] = shm


def attach_shared_indicator_cache(pid, market, period_count, allowed_minutes_per_period):
    shape = (settings.strategy_ga_cache_size, period_count, allowed_minutes_per_period)

    name_long = f'{pid}_{NP_SHARED_NAME_INDICATOR_CACHE}_{market}_long'
    shm_data_long = shared_memory.SharedMemory(name=name_long)
    array_long = np.ndarray(shape, dtype=bool, buffer=shm_data_long.buf)
    indicators_long = array_long

    name_short = f'{pid}_{NP_SHARED_NAME_INDICATOR_CACHE}_{market}_short'
    shm_data_short = shared_memory.SharedMemory(name=name_short)
    array_short = np.ndarray(shape, dtype=bool, buffer=shm_data_short.buf)
    indicators_short = array_short

    return (indicators_long, indicators_short, shm_data_long, shm_data_short)

def attach_shared_bars(pid, market, week_count, tag):
    
    bars_shared_memory = {}

    name = f'{pid}_{NP_SHARED_NAME_BARS}_{market}_{tag}'
    shape = (week_count, )

    shm_data = shared_memory.SharedMemory(name=name)
    array = np.ndarray(shape, dtype=object, buffer=shm_data.buf)
    bars = array
    bars_shared_memory[name] = shm_data

    return (bars, bars_shared_memory)

def attach_shared_datetimes(pid, market, datetime_count):
    
    datetimes_shared_memory = {}

    name = f'{pid}_{NP_SHARED_NAME_DATETIMES}_{market}'
    shape = (datetime_count)

    shm_data = shared_memory.SharedMemory(name=name)
    array = np.ndarray(shape, dtype='datetime64[ns]', buffer=shm_data.buf)
    datetimes = array
    datetimes_shared_memory[name] = shm_data

    return (datetimes, datetimes_shared_memory)

def attach_shared_timed_exits(pid, market, week_count):
    
    timed_exits_shared_memory = {}

    name = f'{pid}_{NP_SHARED_NAME_TIMED_EXITS}_{market}'
    shape = (week_count, )

    shm_data = shared_memory.SharedMemory(name=name)    
    array = np.ndarray(shape, dtype=object, buffer=shm_data.buf)
    timed_exits = array
    timed_exits_shared_memory[name] = shm_data

    return (timed_exits, timed_exits_shared_memory)

def attach_shared_allowed_sessions(pid, market, week_count):

    allowed_entries_shared_memory = {}
    
    name = f'{pid}_{NP_SHARED_NAME_ALLOWED_SESSIONS}_{market}'
    shape = (week_count, len(Session), )

    shm_data = shared_memory.SharedMemory(name=name)
    array = np.ndarray(shape, dtype=object, buffer=shm_data.buf)
    allowed_entries = array
    allowed_entries_shared_memory[name] = shm_data

    return (allowed_entries, allowed_entries_shared_memory)

def attach_shared_allowed_days(pid, market, week_count):

    allowed_entries_shared_memory = {}
    
    name = f'{pid}_{NP_SHARED_NAME_ALLOWED_DAYS}_{market}'
    shape = (week_count, TRADING_DAY_COUNT, )

    shm_data = shared_memory.SharedMemory(name=name)
    array = np.ndarray(shape, dtype=object, buffer=shm_data.buf)
    allowed_entries = array
    allowed_entries_shared_memory[name] = shm_data

    return (allowed_entries, allowed_entries_shared_memory)

def release_shared_strategy(pid, strategy_id):
    name = f'{pid}_{NP_SHARED_NAME_STRATEGY}_{strategy_id}'
    release_shared_name(name)


def release_shared_name(name):
    try:
        shm = shared_memory.SharedMemory(name=name)
        shm.close()
        shm.unlink()
    except FileNotFoundError:
        # The shared memory has already been released
        pass
    except Exception as e:
        print(f"Error releasing shared memory {name}: {e}")


def release_shared_data(data):
    for name in data.keys():
        release_shared_name(name)


def release_shared_strategies(data):
    for shm in data.values():
        shm.close()
        shm.unlink()


def close_shared_data(data):
    for shared_memory in data.values():
        shared_memory.close()


def detach_indicator_cache(indicator_long_shared_memory, indicator_short_shared_memory):
    indicator_long_shared_memory.close()
    indicator_short_shared_memory.close()
