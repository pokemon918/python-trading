import vectorbtpro as vbt
import numpy as np
import pandas as pd
import talib as talib

from constants import BarMinutes, minutes_type, IndicatorReset, MINUTES_PER_DAY, MINUTES_PER_WEEK

indicator_registry = {}
indicator_registry_max_lookback = {}
indicator_options = {}
indicator_lookback_options = {}

def register_indicator(name, logic):
    indicator_registry[name] = logic

def register_indicator_max_lookback(name, logic):
    indicator_registry_max_lookback[name] = logic

def register_indicator_lookback(name, lookback_parameter):
    if name not in indicator_lookback_options:
        indicator_lookback_options[name] = []
    indicator_lookback_options[name].append(lookback_parameter)

## Handling for updating min and max lookbacks

min_lookback = None
max_lookback = None

def update_indicator_registry_lookbacks(indicator_reset):
    # Updates the min/max lookbacks in the indicator registry based on the reset type

    min_lookback = 2
    max_lookback = None

    if indicator_reset == IndicatorReset.Daily:
        max_lookback = MINUTES_PER_DAY
    elif indicator_reset == IndicatorReset.Weekly:
        max_lookback = MINUTES_PER_WEEK

    update_indicator_lookbacks(min_lookback, max_lookback)

def update_indicator_lookbacks(new_min_lookback, new_max_lookback):
    min_lookback = new_min_lookback
    max_lookback = new_max_lookback

    for name in indicator_lookback_options:
        for lookback_parameter in indicator_lookback_options[name]:
            indicator_options[name][lookback_parameter] = (new_min_lookback, new_max_lookback)

def calculate_max_lookback(strategy):
    strategy_max_lookback = 0

    for indicator_name, params in strategy['indicators']:
        max_lookback = indicator_registry_max_lookback[indicator_name](params)
        if max_lookback > strategy_max_lookback:
            strategy_max_lookback = max_lookback

    return strategy_max_lookback

### Functions to calculate the max lookback of all lookback parameters

def max_lookback_only_lookback(params):
    return params['lookback']

def max_lookback_fast_slow_signal(params):
    return max(params['fast'], params['slow'], params['signal'])

def max_lookback_only_timeperiod(params):
    return params['timeperiod']

def max_lookback_none(params):
    return 0

def max_lookback_fastperiod_slowperiod(params):
    return max(params['fastperiod'], params['slowperiod'])

def max_lookback_fastperiod_slowperiod_signalperiod(params):
    return max(params['fastperiod'], params['slowperiod'], params['signalperiod'])

def max_lookback_fastk_period_slowk_period_slowd_period(params):
    return max(params['fastk_period'], params['slowk_period'], params['slowd_period'])

def max_lookback_fastk_period_fastd_period(params):
    return max(params['fastk_period'], params['fastd_period'])

def max_lookback_timeperiod1_timeperiod2_timeperiod3(params):
    return max(params['timeperiod1'], params['timeperiod2'], params['timeperiod3'])

# Create a specific max lookback function for MAVP parameters
def max_lookback_minperiod_maxperiod(params):
    return max(params['minperiod'], params['maxperiod'])

def max_lookback_timeperiod_fastk_fastd(params):
    return max(params['timeperiod'], params['fastk_period'], params['fastd_period'])


### Indicators

### VectorBT ###

# SMA
def ma_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    wtype = params['wtype']
    ma = vbt.MA.run(bars_close[period_index], window=lookback, wtype=wtype)
    return bars_close[period_index] > ma, bars_close[period_index] < ma

indicator_options["MA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("MA_With", ma_with_logic)
register_indicator_max_lookback("MA_With", max_lookback_only_lookback)
register_indicator_lookback("MA_With", "lookback")

def ma_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    wtype = params['wtype']
    ma = vbt.MA.run(bars_close[period_index], window=lookback, wtype=wtype)
    return bars_close[period_index] < ma, bars_close[period_index] > ma

indicator_options["MA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("MA_Against", ma_against_logic)
register_indicator_max_lookback("MA_Against", max_lookback_only_lookback)
register_indicator_lookback("MA_Against", "lookback")

# MACD
def macd_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    fast = params['fast']
    slow = params['slow']
    signal = params['signal']
    wtype = params['wtype']
    macd_wtype = params['macd_wtype']
    signal_wtype = params['signal_wtype']
    bar_type = minutes_type[params['bar_type']]
    macd = vbt.MACD.run(bars_close[period_index], fast_window=fast, slow_window=slow, signal_window=signal, wtype=wtype, macd_wtype=macd_wtype, signal_wtype=signal_wtype)
    return macd.macd > macd.signal, macd.macd < macd.signal


indicator_options["MACD_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fast": (min_lookback, max_lookback),
    "slow": (min_lookback, max_lookback),
    "signal": (min_lookback, max_lookback),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
    "macd_wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
    "signal_wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("MACD_With", macd_with_logic)
register_indicator_max_lookback("MACD_With", max_lookback_fast_slow_signal)
register_indicator_lookback("MACD_With", "fast")
register_indicator_lookback("MACD_With", "slow")
register_indicator_lookback("MACD_With", "signal")


def macd_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    fast = params['fast']
    slow = params['slow']
    signal = params['signal']
    wtype = params['wtype']
    macd_wtype = params['macd_wtype']
    signal_wtype = params['signal_wtype']
    bar_type = minutes_type[params['bar_type']]
    macd = vbt.MACD.run(bars_close[period_index], fast_window=fast, slow_window=slow, signal_window=signal, wtype=wtype, macd_wtype=macd_wtype, signal_wtype=signal_wtype)
    return macd.macd < macd.signal, macd.macd > macd.signal

indicator_options["MACD_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fast": (min_lookback, max_lookback),
    "slow": (min_lookback, max_lookback),
    "signal": (min_lookback, max_lookback),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
    "macd_wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
    "signal_wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("MACD_Against", macd_against_logic)
register_indicator_max_lookback("MACD_Against", max_lookback_fast_slow_signal)
register_indicator_lookback("MACD_Against", "fast")
register_indicator_lookback("MACD_Against", "slow")
register_indicator_lookback("MACD_Against", "signal")


# RSI
def rsi_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    wtype = params['wtype']
    rsi = vbt.RSI.run(bars_close[period_index], window=lookback, wtype=wtype)
    return rsi > upper_threshold, rsi < lower_threshold


indicator_options["RSI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("RSI_With", rsi_with_logic)
register_indicator_max_lookback("RSI_With", max_lookback_only_lookback)
register_indicator_lookback("RSI_With", "lookback")


def rsi_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    wtype = params['wtype']
    rsi = vbt.RSI.run(bars_close[period_index], window=lookback, wtype=wtype)
    return rsi < lower_threshold, rsi > upper_threshold


indicator_options["RSI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("RSI_Against", rsi_against_logic)
register_indicator_max_lookback("RSI_Against", max_lookback_only_lookback)
register_indicator_lookback("RSI_Against", "lookback")


# ATR
def atr_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    wtype = params['wtype']
    atr = vbt.ATR.run(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], window=lookback, wtype=wtype
    )
    return atr > params['threshold'], atr > params['threshold']


indicator_options["ATR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("ATR_With", atr_with_logic)
register_indicator_max_lookback("ATR_With", max_lookback_only_lookback)
register_indicator_lookback("ATR_With", "lookback")


def atr_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    lookback = params['lookback']
    bar_type = minutes_type[params['bar_type']]
    wtype = params['wtype']
    atr = vbt.ATR.run(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], window=lookback, wtype=wtype
    )
    return atr < params['threshold'], atr < params['threshold']


indicator_options["ATR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),
    "wtype": ["Simple", "Weighted", "Exp", "Wilder", "Vidya"],
}
register_indicator("ATR_Against", atr_against_logic)
register_indicator_max_lookback("ATR_Against", max_lookback_only_lookback)
register_indicator_lookback("ATR_Against", "lookback")


### TA-Lib Indicators ###

# SMA (Simple Moving Average)
def sma_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    sma = talib.SMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > sma, bars_close[period_index] < sma


indicator_options["SMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("SMA_With", sma_with_logic)
register_indicator_max_lookback("SMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("SMA_With", "timeperiod")


def sma_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    sma = talib.SMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < sma, bars_close[period_index] > sma


indicator_options["SMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("SMA_Against", sma_against_logic)
register_indicator_max_lookback("SMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("SMA_Against", "timeperiod")


# EMA (Exponential Moving Average)
def ema_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    ema = talib.EMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > ema, bars_close[period_index] < ema


indicator_options["EMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("EMA_With", ema_with_logic)
register_indicator_max_lookback("EMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("EMA_With", "timeperiod")


def ema_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    ema = talib.EMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < ema, bars_close[period_index] > ema


indicator_options["EMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("EMA_Against", ema_against_logic)
register_indicator_max_lookback("EMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("EMA_Against", "timeperiod")


# RSI (Relative Strength Index)
def rsi_ta_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    rsi = talib.RSI(bars_close[period_index], timeperiod=timeperiod)
    return rsi > upper_threshold, rsi < lower_threshold


indicator_options["RSI_TA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
}
register_indicator("RSI_TA_With", rsi_ta_with_logic)
register_indicator_max_lookback("RSI_TA_With", max_lookback_only_timeperiod)
register_indicator_lookback("RSI_TA_With", "timeperiod")


def rsi_ta_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    rsi = talib.RSI(bars_close[period_index], timeperiod=timeperiod)
    return rsi < lower_threshold, rsi > upper_threshold


indicator_options["RSI_TA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
}
register_indicator("RSI_TA_Against", rsi_ta_against_logic)
register_indicator_max_lookback("RSI_TA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("RSI_TA_Against", "timeperiod")


# ATR (Average True Range)
def atr_threshold_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    threshold = params['threshold']
    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return atr > threshold, atr < threshold


indicator_options["ATR_Threshold_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),
}
register_indicator("ATR_Threshold_With", atr_threshold_with_logic)
register_indicator_max_lookback("ATR_Threshold_With", max_lookback_only_timeperiod)
register_indicator_lookback("ATR_Threshold_With", "timeperiod")


def atr_threshold_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    threshold = params['threshold']
    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return atr < threshold, atr > threshold


indicator_options["ATR_Threshold_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),
}
register_indicator("ATR_Threshold_Against", atr_threshold_against_logic)
register_indicator_max_lookback("ATR_Threshold_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ATR_Threshold_Against", "timeperiod")


# ATR Breakout
def atr_breakout_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    multiplier = params['multiplier']

    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    close_prices = bars_close[period_index]

    # Calculate ATR bands - using np.roll() instead of .shift()
    previous_close = np.roll(close_prices, 1)
    # Set the first element to the same as the first close price to avoid invalid references
    previous_close[0] = close_prices[0]

    upper_band = previous_close + (atr * multiplier)
    lower_band = previous_close - (atr * multiplier)

    # Breakout signals
    long_signal = close_prices > upper_band
    short_signal = close_prices < lower_band

    return long_signal, short_signal


indicator_options["ATR_Breakout_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (1.0, 3.0),
}
register_indicator("ATR_Breakout_With", atr_breakout_with_logic)
register_indicator_max_lookback("ATR_Breakout_With", max_lookback_only_timeperiod)
register_indicator_lookback("ATR_Breakout_With", "timeperiod")


def atr_breakout_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    multiplier = params['multiplier']

    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    close_prices = bars_close[period_index]

    # Calculate ATR bands - using np.roll() instead of .shift()
    previous_close = np.roll(close_prices, 1)
    # Set the first element to the same as the first close price to avoid invalid references
    previous_close[0] = close_prices[0]

    upper_band = previous_close + (atr * multiplier)
    lower_band = previous_close - (atr * multiplier)

    # Mean reversion signals (against breakout)
    long_signal = close_prices < lower_band
    short_signal = close_prices > upper_band

    return long_signal, short_signal


indicator_options["ATR_Breakout_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (1.0, 3.0),
}
register_indicator("ATR_Breakout_Against", atr_breakout_against_logic)
register_indicator_max_lookback("ATR_Breakout_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ATR_Breakout_Against", "timeperiod")


# MACD (Moving Average Convergence Divergence)
def macd_ta_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']

    macd, macdsignal, macdhist = talib.MACD(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod
    )

    return macd > macdsignal, macd < macdsignal


indicator_options["MACD_TA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
}
register_indicator("MACD_TA_With", macd_ta_with_logic)
register_indicator_max_lookback("MACD_TA_With", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACD_TA_With", "fastperiod")
register_indicator_lookback("MACD_TA_With", "slowperiod")
register_indicator_lookback("MACD_TA_With", "signalperiod")


def macd_ta_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']

    macd, macdsignal, macdhist = talib.MACD(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod
    )

    return macd < macdsignal, macd > macdsignal


indicator_options["MACD_TA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
}
register_indicator("MACD_TA_Against", macd_ta_against_logic)
register_indicator_max_lookback("MACD_TA_Against", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACD_TA_Against", "fastperiod")
register_indicator_lookback("MACD_TA_Against", "slowperiod")
register_indicator_lookback("MACD_TA_Against", "signalperiod")


# MACD Histogram
def macd_hist_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']

    macd, macdsignal, macdhist = talib.MACD(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod
    )

    return macdhist > 0, macdhist < 0


indicator_options["MACD_Hist_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
}
register_indicator("MACD_Hist_With", macd_hist_with_logic)
register_indicator_max_lookback("MACD_Hist_With", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACD_Hist_With", "fastperiod")
register_indicator_lookback("MACD_Hist_With", "slowperiod")
register_indicator_lookback("MACD_Hist_With", "signalperiod")


def macd_hist_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']

    macd, macdsignal, macdhist = talib.MACD(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod
    )

    return macdhist < 0, macdhist > 0


indicator_options["MACD_Hist_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
}
register_indicator("MACD_Hist_Against", macd_hist_against_logic)
register_indicator_max_lookback("MACD_Hist_Against", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACD_Hist_Against", "fastperiod")
register_indicator_lookback("MACD_Hist_Against", "slowperiod")
register_indicator_lookback("MACD_Hist_Against", "signalperiod")


# Moving Average Crossover - Fix for fastperiod/fast_period issue
def ma_crossover_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']  # Changed from fast_period to fastperiod
    slowperiod = params['slowperiod']  # Changed from slow_period to slowperiod
    ma_type = params['ma_type']

    if ma_type == 0:  # SMA
        fast_ma = talib.SMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.SMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 1:  # EMA
        fast_ma = talib.EMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.EMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 2:  # WMA
        fast_ma = talib.WMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.WMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 3:  # DEMA
        fast_ma = talib.DEMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.DEMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 4:  # TEMA
        fast_ma = talib.TEMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.TEMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 5:  # TRIMA
        fast_ma = talib.TRIMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.TRIMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 6:  # KAMA
        fast_ma = talib.KAMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.KAMA(bars_close[period_index], timeperiod=slowperiod)
    else:  # Default to SMA
        fast_ma = talib.SMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.SMA(bars_close[period_index], timeperiod=slowperiod)

    return fast_ma > slow_ma, fast_ma < slow_ma


indicator_options["MA_Crossover_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),  # Changed from fast_period to fastperiod
    "slowperiod": (min_lookback, max_lookback),  # Changed from slow_period to slowperiod
    "ma_type": (0, 6),  # 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA
}
register_indicator("MA_Crossover_With", ma_crossover_with_logic)
register_indicator_max_lookback("MA_Crossover_With", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("MA_Crossover_With", "fastperiod")  # Changed from fast_period to fastperiod
register_indicator_lookback("MA_Crossover_With", "slowperiod")  # Changed from slow_period to slowperiod


def ma_crossover_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']  # Changed from fast_period to fastperiod
    slowperiod = params['slowperiod']  # Changed from slow_period to slowperiod
    ma_type = params['ma_type']

    if ma_type == 0:  # SMA
        fast_ma = talib.SMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.SMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 1:  # EMA
        fast_ma = talib.EMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.EMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 2:  # WMA
        fast_ma = talib.WMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.WMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 3:  # DEMA
        fast_ma = talib.DEMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.DEMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 4:  # TEMA
        fast_ma = talib.TEMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.TEMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 5:  # TRIMA
        fast_ma = talib.TRIMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.TRIMA(bars_close[period_index], timeperiod=slowperiod)
    elif ma_type == 6:  # KAMA
        fast_ma = talib.KAMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.KAMA(bars_close[period_index], timeperiod=slowperiod)
    else:  # Default to SMA
        fast_ma = talib.SMA(bars_close[period_index], timeperiod=fastperiod)
        slow_ma = talib.SMA(bars_close[period_index], timeperiod=slowperiod)

    return fast_ma < slow_ma, fast_ma > slow_ma


indicator_options["MA_Crossover_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),  # Changed from fast_period to fastperiod
    "slowperiod": (min_lookback, max_lookback),  # Changed from slow_period to slowperiod
    "ma_type": (0, 6),  # 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA
}
register_indicator("MA_Crossover_Against", ma_crossover_against_logic)
register_indicator_max_lookback("MA_Crossover_Against", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("MA_Crossover_Against", "fastperiod")  # Changed from fast_period to fastperiod
register_indicator_lookback("MA_Crossover_Against", "slowperiod")  # Changed from slow_period to slowperiod


# KAMA (Kaufman Adaptive Moving Average)
def kama_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    kama = talib.KAMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > kama, bars_close[period_index] < kama


indicator_options["KAMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("KAMA_With", kama_with_logic)
register_indicator_max_lookback("KAMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("KAMA_With", "timeperiod")

def kama_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    kama = talib.KAMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < kama, bars_close[period_index] > kama


indicator_options["KAMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("KAMA_Against", kama_against_logic)
register_indicator_max_lookback("KAMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("KAMA_Against", "timeperiod")


# HT_TRENDLINE (Hilbert Transform - Instantaneous Trendline)
def ht_trendline_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    ht_trendline = talib.HT_TRENDLINE(bars_close[period_index])
    return bars_close[period_index] > ht_trendline, bars_close[period_index] < ht_trendline


indicator_options["HT_Trendline_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("HT_Trendline_With", ht_trendline_with_logic)
register_indicator_max_lookback("HT_Trendline_With", max_lookback_none)


def ht_trendline_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    ht_trendline = talib.HT_TRENDLINE(bars_close[period_index])
    return bars_close[period_index] < ht_trendline, bars_close[period_index] > ht_trendline


indicator_options["HT_Trendline_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("HT_Trendline_Against", ht_trendline_against_logic)
register_indicator_max_lookback("HT_Trendline_Against", max_lookback_none)


# T3 (Triple Smoothed Moving Average)
def t3_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    vfactor = params['vfactor']
    t3 = talib.T3(bars_close[period_index], timeperiod=timeperiod, vfactor=vfactor)
    return bars_close[period_index] > t3, bars_close[period_index] < t3


indicator_options["T3_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "vfactor": (0.1, 1.0),  # Volume factor for smoothing
}
register_indicator("T3_With", t3_with_logic)
register_indicator_max_lookback("T3_With", max_lookback_only_timeperiod)
register_indicator_lookback("T3_With", "timeperiod")


def t3_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    vfactor = params['vfactor']
    t3 = talib.T3(bars_close[period_index], timeperiod=timeperiod, vfactor=vfactor)
    return bars_close[period_index] < t3, bars_close[period_index] > t3


indicator_options["T3_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "vfactor": (0.1, 1.0),
}
register_indicator("T3_Against", t3_against_logic)
register_indicator_max_lookback("T3_Against", max_lookback_only_timeperiod)
register_indicator_lookback("T3_Against", "timeperiod")


# Bollinger Bands (BBANDS)
def bbands_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    nbdevup = params['nbdevup']
    nbdevdn = params['nbdevdn']
    upperband, middleband, lowerband = talib.BBANDS(
        bars_close[period_index], timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=0
    )
    return bars_close[period_index] > upperband, bars_close[period_index] < lowerband


indicator_options["BBANDS_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "nbdevup": (0.0, 3.0),
    "nbdevdn": (0.0, 3.0),
}
register_indicator("BBANDS_With", bbands_with_logic)
register_indicator_max_lookback("BBANDS_With", max_lookback_only_timeperiod)
register_indicator_lookback("BBANDS_With", "timeperiod")


def bbands_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    nbdevup = params['nbdevup']
    nbdevdn = params['nbdevdn']
    upperband, middleband, lowerband = talib.BBANDS(
        bars_close[period_index], timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=0
    )
    return bars_close[period_index] < lowerband, bars_close[period_index] > upperband


indicator_options["BBANDS_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "nbdevup": (0.0, 3.0),
    "nbdevdn": (0.0, 3.0),
}
register_indicator("BBANDS_Against", bbands_against_logic)
register_indicator_max_lookback("BBANDS_Against", max_lookback_only_timeperiod)
register_indicator_lookback("BBANDS_Against", "timeperiod")


# DEMA (Double Exponential Moving Average)
def dema_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    dema = talib.DEMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > dema, bars_close[period_index] < dema


indicator_options["DEMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("DEMA_With", dema_with_logic)
register_indicator_max_lookback("DEMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("DEMA_With", "timeperiod")


def dema_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    dema = talib.DEMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < dema, bars_close[period_index] > dema


indicator_options["DEMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("DEMA_Against", dema_against_logic)
register_indicator_max_lookback("DEMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("DEMA_Against", "timeperiod")


# MAMA (MESA Adaptive Moving Average)
def mama_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastlimit = params['fastlimit']
    slowlimit = params['slowlimit']
    mama, fama = talib.MAMA(bars_close[period_index], fastlimit=fastlimit, slowlimit=slowlimit)
    return bars_close[period_index] > mama, bars_close[period_index] < mama


indicator_options["MAMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastlimit": (0.01, 0.99),
    "slowlimit": (0.01, 0.99),
}
register_indicator("MAMA_With", mama_with_logic)
register_indicator_max_lookback("MAMA_With", max_lookback_none)


def mama_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastlimit = params['fastlimit']
    slowlimit = params['slowlimit']
    mama, fama = talib.MAMA(bars_close[period_index], fastlimit=fastlimit, slowlimit=slowlimit)
    return bars_close[period_index] < mama, bars_close[period_index] > mama


indicator_options["MAMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastlimit": (0.01, 0.99),
    "slowlimit": (0.01, 0.99),
}
register_indicator("MAMA_Against", mama_against_logic)
register_indicator_max_lookback("MAMA_Against", max_lookback_none)


# SAR (Parabolic SAR)
def sar_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    acceleration = params['acceleration']
    maximum = params['maximum']
    sar = talib.SAR(bars_high[period_index], bars_low[period_index], acceleration=acceleration, maximum=maximum)
    return bars_close[period_index] > sar, bars_close[period_index] < sar


indicator_options["SAR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "acceleration": (0.02, 0.5),
    "maximum": (0.2, 0.5),
}
register_indicator("SAR_With", sar_with_logic)
register_indicator_max_lookback("SAR_With", max_lookback_none)


def sar_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    acceleration = params['acceleration']
    maximum = params['maximum']
    sar = talib.SAR(bars_high[period_index], bars_low[period_index], acceleration=acceleration, maximum=maximum)
    return bars_close[period_index] < sar, bars_close[period_index] > sar


indicator_options["SAR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "acceleration": (0.02, 0.5),
    "maximum": (0.2, 0.5),
}
register_indicator("SAR_Against", sar_against_logic)
register_indicator_max_lookback("SAR_Against", max_lookback_none)


# TEMA (Triple Exponential Moving Average)
def tema_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    tema = talib.TEMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > tema, bars_close[period_index] < tema


indicator_options["TEMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TEMA_With", tema_with_logic)
register_indicator_max_lookback("TEMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("TEMA_With", "timeperiod")

def tema_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    tema = talib.TEMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < tema, bars_close[period_index] > tema


indicator_options["TEMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TEMA_Against", tema_against_logic)
register_indicator_max_lookback("TEMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("TEMA_Against", "timeperiod")


# TRIMA (Triangular Moving Average)
def trima_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    trima = talib.TRIMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > trima, bars_close[period_index] < trima


indicator_options["TRIMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TRIMA_With", trima_with_logic)
register_indicator_max_lookback("TRIMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("TRIMA_With", "timeperiod")


def trima_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    trima = talib.TRIMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < trima, bars_close[period_index] > trima


indicator_options["TRIMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TRIMA_Against", trima_against_logic)
register_indicator_max_lookback("TRIMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("TRIMA_Against", "timeperiod")


# WMA (Weighted Moving Average)
def wma_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    wma = talib.WMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > wma, bars_close[period_index] < wma


indicator_options["WMA_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("WMA_With", wma_with_logic)
register_indicator_max_lookback("WMA_With", max_lookback_only_timeperiod)
register_indicator_lookback("WMA_With", "timeperiod")


def wma_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    wma = talib.WMA(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < wma, bars_close[period_index] > wma


indicator_options["WMA_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("WMA_Against", wma_against_logic)
register_indicator_max_lookback("WMA_Against", max_lookback_only_timeperiod)
register_indicator_lookback("WMA_Against", "timeperiod")


# ADX (Average Directional Movement Index)
def adx_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    adx = talib.ADX(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return adx > params['threshold'], adx < params['threshold']


indicator_options["ADX_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ADX_With", adx_with_logic)
register_indicator_max_lookback("ADX_With", max_lookback_only_timeperiod)
register_indicator_lookback("ADX_With", "timeperiod")


def adx_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    adx = talib.ADX(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return adx < params['threshold'], adx > params['threshold']


indicator_options["ADX_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ADX_Against", adx_against_logic)
register_indicator_max_lookback("ADX_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ADX_Against", "timeperiod")


# ADXR (Average Directional Movement Index Rating)
def adxr_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    adxr = talib.ADXR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return adxr > params['threshold'], adxr < params['threshold']


indicator_options["ADXR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ADXR_With", adxr_with_logic)
register_indicator_max_lookback("ADXR_With", max_lookback_only_timeperiod)
register_indicator_lookback("ADXR_With", "timeperiod")


def adxr_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    adxr = talib.ADXR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return adxr < params['threshold'], adxr > params['threshold']


indicator_options["ADXR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ADXR_Against", adxr_against_logic)
register_indicator_max_lookback("ADXR_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ADXR_Against", "timeperiod")


# APO (Absolute Price Oscillator)
def apo_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    apo = talib.APO(bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, matype=0)
    return apo > 0, apo < 0


indicator_options["APO_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
}
register_indicator("APO_With", apo_with_logic)
register_indicator_max_lookback("APO_With", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("APO_With", "fastperiod")
register_indicator_lookback("APO_With", "slowperiod")


def apo_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    apo = talib.APO(bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, matype=0)
    return apo < 0, apo > 0


indicator_options["APO_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
}
register_indicator("APO_Against", apo_against_logic)
register_indicator_max_lookback("APO_Against", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("APO_Against", "fastperiod")
register_indicator_lookback("APO_Against", "slowperiod")


# AROON
def aroon_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    aroondown, aroonup = talib.AROON(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return aroonup > aroondown, aroonup < aroondown


indicator_options["AROON_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("AROON_With", aroon_with_logic)
register_indicator_max_lookback("AROON_With", max_lookback_only_timeperiod)
register_indicator_lookback("AROON_With", "timeperiod")

def aroon_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    aroondown, aroonup = talib.AROON(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return aroonup < aroondown, aroonup > aroondown


indicator_options["AROON_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("AROON_Against", aroon_against_logic)
register_indicator_max_lookback("AROON_Against", max_lookback_only_timeperiod)
register_indicator_lookback("AROON_Against", "timeperiod")

# AROONOSC (Aroon Oscillator)
def aroonosc_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    aroonosc = talib.AROONOSC(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return aroonosc > 0, aroonosc < 0


indicator_options["AROONOSC_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("AROONOSC_With", aroonosc_with_logic)
register_indicator_max_lookback("AROONOSC_With", max_lookback_only_timeperiod)
register_indicator_lookback("AROONOSC_With", "timeperiod")

def aroonosc_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    aroonosc = talib.AROONOSC(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return aroonosc < 0, aroonosc > 0


indicator_options["AROONOSC_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("AROONOSC_Against", aroonosc_against_logic)
register_indicator_max_lookback("AROONOSC_Against", max_lookback_only_timeperiod)
register_indicator_lookback("AROONOSC_Against", "timeperiod")


# BOP (Balance Of Power)
def bop_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    bop = talib.BOP(bars_open[period_index], bars_high[period_index], bars_low[period_index], bars_close[period_index])
    return bop > 0, bop < 0


indicator_options["BOP_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("BOP_With", bop_with_logic)
register_indicator_max_lookback("BOP_With", max_lookback_none)


def bop_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    bop = talib.BOP(bars_open[period_index], bars_high[period_index], bars_low[period_index], bars_close[period_index])
    return bop < 0, bop > 0


indicator_options["BOP_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("BOP_Against", bop_against_logic)
register_indicator_max_lookback("BOP_Against", max_lookback_none)


# CCI (Commodity Channel Index)
def cci_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    cci = talib.CCI(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return cci > params['threshold'], cci < (-1 * params['threshold'])


indicator_options["CCI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("CCI_With", cci_with_logic)
register_indicator_max_lookback("CCI_With", max_lookback_only_timeperiod)
register_indicator_lookback("CCI_With", "timeperiod")


def cci_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    cci = talib.CCI(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return cci < (-1 * params['threshold']), cci > params['threshold']


indicator_options["CCI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("CCI_Against", cci_against_logic)
register_indicator_max_lookback("CCI_Against", max_lookback_only_timeperiod)
register_indicator_lookback("CCI_Against", "timeperiod")

# CMO (Chande Momentum Oscillator)
def cmo_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    cmo = talib.CMO(bars_close[period_index], timeperiod=timeperiod)
    return cmo > 0, cmo < 0


indicator_options["CMO_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("CMO_With", cmo_with_logic)
register_indicator_max_lookback("CMO_With", max_lookback_only_timeperiod)
register_indicator_lookback("CMO_With", "timeperiod")


def cmo_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    cmo = talib.CMO(bars_close[period_index], timeperiod=timeperiod)
    return cmo < 0, cmo > 0


indicator_options["CMO_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("CMO_Against", cmo_against_logic)
register_indicator_max_lookback("CMO_Against", max_lookback_only_timeperiod)
register_indicator_lookback("CMO_Against", "timeperiod")


# DX (Directional Movement Index)
def dx_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    dx = talib.DX(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return dx > params['threshold'], dx < params['threshold']


indicator_options["DX_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("DX_With", dx_with_logic)
register_indicator_max_lookback("DX_With", max_lookback_only_timeperiod)
register_indicator_lookback("DX_With", "timeperiod")


def dx_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    dx = talib.DX(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)
    return dx < params['threshold'], dx > params['threshold']


indicator_options["DX_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("DX_Against", dx_against_logic)
register_indicator_max_lookback("DX_Against", max_lookback_only_timeperiod)
register_indicator_lookback("DX_Against", "timeperiod")


# MACDEXT (MACD with controllable MA type)
def macdext_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']
    fastmatype = params['fastmatype']
    slowmatype = params['slowmatype']
    signalmatype = params['signalmatype']
    macd, macdsignal, _ = talib.MACDEXT(
        bars_close[period_index],
        fastperiod=fastperiod,
        slowperiod=slowperiod,
        signalperiod=signalperiod,
        fastmatype=fastmatype,
        slowmatype=slowmatype,
        signalmatype=signalmatype
    )
    return macd > macdsignal, macd < macdsignal


indicator_options["MACDEXT_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
    "fastmatype": (0, 8),  # MA Type (SMA, EMA, etc.)
    "slowmatype": (0, 8),
    "signalmatype": (0, 8),
}
register_indicator("MACDEXT_With", macdext_with_logic)
register_indicator_max_lookback("MACDEXT_With", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACDEXT_With", "fastperiod")
register_indicator_lookback("MACDEXT_With", "slowperiod")
register_indicator_lookback("MACDEXT_With", "signalperiod")


def macdext_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    signalperiod = params['signalperiod']
    fastmatype = params['fastmatype']
    slowmatype = params['slowmatype']
    signalmatype = params['signalmatype']
    macd, macdsignal, _ = talib.MACDEXT(
        bars_close[period_index],
        fastperiod=fastperiod,
        slowperiod=slowperiod,
        signalperiod=signalperiod,
        fastmatype=fastmatype,
        slowmatype=slowmatype,
        signalmatype=signalmatype
    )
    return macd < macdsignal, macd > macdsignal


indicator_options["MACDEXT_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "signalperiod": (min_lookback, max_lookback),
    "fastmatype": (0, 8),  # MA Type (SMA, EMA, etc.)
    "slowmatype": (0, 8),
    "signalmatype": (0, 8),
}
register_indicator("MACDEXT_Against", macdext_against_logic)
register_indicator_max_lookback("MACDEXT_Against", max_lookback_fastperiod_slowperiod_signalperiod)
register_indicator_lookback("MACDEXT_Against", "fastperiod")
register_indicator_lookback("MACDEXT_Against", "slowperiod")
register_indicator_lookback("MACDEXT_Against", "signalperiod")

# MINUS_DI (Minus Directional Indicator)
def minus_di_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    minus_di = talib.MINUS_DI(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return minus_di > params['threshold'], minus_di < params['threshold']


indicator_options["MINUS_DI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("MINUS_DI_With", minus_di_with_logic)
register_indicator_max_lookback("MINUS_DI_With", max_lookback_only_timeperiod)
register_indicator_lookback("MINUS_DI_With", "timeperiod")


def minus_di_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    minus_di = talib.MINUS_DI(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return minus_di < params['threshold'], minus_di > params['threshold']


indicator_options["MINUS_DI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("MINUS_DI_Against", minus_di_against_logic)
register_indicator_max_lookback("MINUS_DI_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MINUS_DI_Against", "timeperiod")


# MINUS_DM (Minus Directional Movement)
def minus_dm_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    minus_dm = talib.MINUS_DM(
        bars_high[period_index], bars_low[period_index], timeperiod=timeperiod
    )
    return minus_dm > params['threshold'], minus_dm < params['threshold']


indicator_options["MINUS_DM_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("MINUS_DM_With", minus_dm_with_logic)
register_indicator_max_lookback("MINUS_DM_With", max_lookback_only_timeperiod)
register_indicator_lookback("MINUS_DM_With", "timeperiod")


def minus_dm_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    minus_dm = talib.MINUS_DM(
        bars_high[period_index], bars_low[period_index], timeperiod=timeperiod
    )
    return minus_dm < params['threshold'], minus_dm > params['threshold']


indicator_options["MINUS_DM_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("MINUS_DM_Against", minus_dm_against_logic)
register_indicator_max_lookback("MINUS_DM_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MINUS_DM_Against", "timeperiod")


# MOM (Momentum)
def mom_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    mom = talib.MOM(bars_close[period_index], timeperiod=timeperiod)
    return mom > 0, mom < 0


indicator_options["MOM_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MOM_With", mom_with_logic)
register_indicator_max_lookback("MOM_With", max_lookback_only_timeperiod)
register_indicator_lookback("MOM_With", "timeperiod")


def mom_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    mom = talib.MOM(bars_close[period_index], timeperiod=timeperiod)
    return mom < 0, mom > 0


indicator_options["MOM_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MOM_Against", mom_against_logic)
register_indicator_max_lookback("MOM_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MOM_Against", "timeperiod")


# PLUS_DI (Plus Directional Indicator)
def plus_di_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    plus_di = talib.PLUS_DI(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return plus_di > params['threshold'], plus_di < params['threshold']


indicator_options["PLUS_DI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("PLUS_DI_With", plus_di_with_logic)
register_indicator_max_lookback("PLUS_DI_With", max_lookback_only_timeperiod)
register_indicator_lookback("PLUS_DI_With", "timeperiod")


def plus_di_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    plus_di = talib.PLUS_DI(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return plus_di < params['threshold'], plus_di > params['threshold']


indicator_options["PLUS_DI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("PLUS_DI_Against", plus_di_against_logic)
register_indicator_max_lookback("PLUS_DI_Against", max_lookback_only_timeperiod)
register_indicator_lookback("PLUS_DI_Against", "timeperiod")


# PLUS_DM (Plus Directional Movement)
def plus_dm_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    plus_dm = talib.PLUS_DM(
        bars_high[period_index], bars_low[period_index], timeperiod=timeperiod
    )
    return plus_dm > params['threshold'], plus_dm < params['threshold']


indicator_options["PLUS_DM_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("PLUS_DM_With", plus_dm_with_logic)
register_indicator_max_lookback("PLUS_DM_With", max_lookback_only_timeperiod)
register_indicator_lookback("PLUS_DM_With", "timeperiod")


def plus_dm_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    plus_dm = talib.PLUS_DM(
        bars_high[period_index], bars_low[period_index], timeperiod=timeperiod
    )
    return plus_dm < params['threshold'], plus_dm > params['threshold']


indicator_options["PLUS_DM_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("PLUS_DM_Against", plus_dm_against_logic)
register_indicator_max_lookback("PLUS_DM_Against", max_lookback_only_timeperiod)
register_indicator_lookback("PLUS_DM_Against", "timeperiod")


# PPO (Percentage Price Oscillator)
def ppo_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    matype = params['matype']
    ppo = talib.PPO(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, matype=matype
    )
    return ppo > params['threshold'], ppo < params['threshold']


indicator_options["PPO_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "matype": (0, 8),  # MA Type (SMA, EMA, etc.)
    "threshold": (-5.0, 5.0),
}
register_indicator("PPO_With", ppo_with_logic)
register_indicator_max_lookback("PPO_With", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("PPO_With", "fastperiod")
register_indicator_lookback("PPO_With", "slowperiod")


def ppo_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']
    matype = params['matype']
    ppo = talib.PPO(
        bars_close[period_index], fastperiod=fastperiod, slowperiod=slowperiod, matype=matype
    )
    return ppo < params['threshold'], ppo > params['threshold']


indicator_options["PPO_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
    "matype": (0, 8),
    "threshold": (-5.0, 5.0),
}
register_indicator("PPO_Against", ppo_against_logic)
register_indicator_max_lookback("PPO_Against", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("PPO_Against", "fastperiod")
register_indicator_lookback("PPO_Against", "slowperiod")


# ROC (Rate of Change)
def roc_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    roc = talib.ROC(bars_close[period_index], timeperiod=timeperiod)
    return roc > params['threshold'], roc < params['threshold']


indicator_options["ROC_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (-5.0, 5.0),
}
register_indicator("ROC_With", roc_with_logic)
register_indicator_max_lookback("ROC_With", max_lookback_only_timeperiod)
register_indicator_lookback("ROC_With", "timeperiod")


def roc_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    roc = talib.ROC(bars_close[period_index], timeperiod=timeperiod)
    return roc < params['threshold'], roc > params['threshold']


indicator_options["ROC_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (-5.0, 5.0),
}
register_indicator("ROC_Against", roc_against_logic)
register_indicator_max_lookback("ROC_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ROC_Against", "timeperiod")


# ROCP (Rate of Change Percentage)
def rocp_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocp = talib.ROCP(bars_close[period_index], timeperiod=timeperiod)
    return rocp > params['threshold'], rocp < params['threshold']


indicator_options["ROCP_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (-0.5, 0.5),  # Percentage
}
register_indicator("ROCP_With", rocp_with_logic)
register_indicator_max_lookback("ROCP_With", max_lookback_only_timeperiod)
register_indicator_lookback("ROCP_With", "timeperiod")


def rocp_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocp = talib.ROCP(bars_close[period_index], timeperiod=timeperiod)
    return rocp < params['threshold'], rocp > params['threshold']


indicator_options["ROCP_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (-0.5, 0.5),
}
register_indicator("ROCP_Against", rocp_against_logic)
register_indicator_max_lookback("ROCP_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ROCP_Against", "timeperiod")


# ROCR (Rate of Change Ratio)
def rocr_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocr = talib.ROCR(bars_close[period_index], timeperiod=timeperiod)
    return rocr > params['threshold'], rocr < params['threshold']


indicator_options["ROCR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.0, 2.0),  # Ratio
}
register_indicator("ROCR_With", rocr_with_logic)
register_indicator_max_lookback("ROCR_With", max_lookback_only_timeperiod)
register_indicator_lookback("ROCR_With", "timeperiod")


def rocr_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocr = talib.ROCR(bars_close[period_index], timeperiod=timeperiod)
    return rocr < params['threshold'], rocr > params['threshold']


indicator_options["ROCR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.0, 2.0),
}
register_indicator("ROCR_Against", rocr_against_logic)
register_indicator_max_lookback("ROCR_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ROCR_Against", "timeperiod")


# ROCR100 (Rate of Change Ratio 100 Scale)
def rocr100_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocr100 = talib.ROCR100(bars_close[period_index], timeperiod=timeperiod)
    return rocr100 > params['threshold'], rocr100 < params['threshold']


indicator_options["ROCR100_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 200),
}
register_indicator("ROCR100_With", rocr100_with_logic)
register_indicator_max_lookback("ROCR100_With", max_lookback_only_timeperiod)
register_indicator_lookback("ROCR100_With", "timeperiod")


def rocr100_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    rocr100 = talib.ROCR100(bars_close[period_index], timeperiod=timeperiod)
    return rocr100 < params['threshold'], rocr100 > params['threshold']


indicator_options["ROCR100_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0, 200),
}
register_indicator("ROCR100_Against", rocr100_against_logic)
register_indicator_max_lookback("ROCR100_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ROCR100_Against", "timeperiod")


# STOCH (Stochastic Oscillator)
def stoch_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastk_period = params['fastk_period']
    slowk_period = params['slowk_period']
    slowk_matype = params['slowk_matype']
    slowd_period = params['slowd_period']
    slowd_matype = params['slowd_matype']
    slowk, slowd = talib.STOCH(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        fastk_period=fastk_period, slowk_period=slowk_period, slowk_matype=slowk_matype,
        slowd_period=slowd_period, slowd_matype=slowd_matype
    )
    return slowk > slowd, slowk < slowd


indicator_options["STOCH_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastk_period": (min_lookback, max_lookback),
    "slowk_period": (min_lookback, max_lookback),
    "slowk_matype": (0, 8),
    "slowd_period": (min_lookback, max_lookback),
    "slowd_matype": (0, 8),
}
register_indicator("STOCH_With", stoch_with_logic)
register_indicator_max_lookback("STOCH_With", max_lookback_fastk_period_slowk_period_slowd_period)
register_indicator_lookback("STOCH_With", "fastk_period")
register_indicator_lookback("STOCH_With", "slowk_period")
register_indicator_lookback("STOCH_With", "slowd_period")

def stoch_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastk_period = params['fastk_period']
    slowk_period = params['slowk_period']
    slowk_matype = params['slowk_matype']
    slowd_period = params['slowd_period']
    slowd_matype = params['slowd_matype']
    slowk, slowd = talib.STOCH(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        fastk_period=fastk_period, slowk_period=slowk_period, slowk_matype=slowk_matype,
        slowd_period=slowd_period, slowd_matype=slowd_matype
    )
    return slowk < slowd, slowk > slowd


indicator_options["STOCH_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastk_period": (min_lookback, max_lookback),
    "slowk_period": (min_lookback, max_lookback),
    "slowk_matype": (0, 8),
    "slowd_period": (min_lookback, max_lookback),
    "slowd_matype": (0, 8),
}
register_indicator("STOCH_Against", stoch_against_logic)
register_indicator_max_lookback("STOCH_Against", max_lookback_fastk_period_slowk_period_slowd_period)
register_indicator_lookback("STOCH_Against", "fastk_period")
register_indicator_lookback("STOCH_Against", "slowk_period")
register_indicator_lookback("STOCH_Against", "slowd_period")


# STOCHF (Fast Stochastic Oscillator)
def stochf_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastk_period = params['fastk_period']
    fastd_period = params['fastd_period']
    fastd_matype = params['fastd_matype']
    fastk, fastd = talib.STOCHF(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        fastk_period=fastk_period, fastd_period=fastd_period, fastd_matype=fastd_matype
    )
    return fastk > fastd, fastk < fastd


indicator_options["STOCHF_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastk_period": (min_lookback, max_lookback),
    "fastd_period": (min_lookback, max_lookback),
    "fastd_matype": (0, 8),
}
register_indicator("STOCHF_With", stochf_with_logic)
register_indicator_max_lookback("STOCHF_With", max_lookback_fastk_period_fastd_period)
register_indicator_lookback("STOCHF_With", "fastk_period")
register_indicator_lookback("STOCHF_With", "fastd_period")


def stochf_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastk_period = params['fastk_period']
    fastd_period = params['fastd_period']
    fastd_matype = params['fastd_matype']
    fastk, fastd = talib.STOCHF(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        fastk_period=fastk_period, fastd_period=fastd_period, fastd_matype=fastd_matype
    )
    return fastk < fastd, fastk > fastd


indicator_options["STOCHF_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastk_period": (min_lookback, max_lookback),
    "fastd_period": (min_lookback, max_lookback),
    "fastd_matype": (0, 8),
}
register_indicator("STOCHF_Against", stochf_against_logic)
register_indicator_max_lookback("STOCHF_Against", max_lookback_fastk_period_fastd_period)
register_indicator_lookback("STOCHF_Against", "fastk_period")
register_indicator_lookback("STOCHF_Against", "fastd_period")


# ULTOSC (Ultimate Oscillator)
def ultosc_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod1 = params['timeperiod1']
    timeperiod2 = params['timeperiod2']
    timeperiod3 = params['timeperiod3']
    ultosc = talib.ULTOSC(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        timeperiod1=timeperiod1, timeperiod2=timeperiod2, timeperiod3=timeperiod3
    )
    return ultosc > params['threshold'], ultosc < params['threshold']


indicator_options["ULTOSC_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod1": (min_lookback, max_lookback),
    "timeperiod2": (min_lookback, max_lookback),
    "timeperiod3": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ULTOSC_With", ultosc_with_logic)
register_indicator_max_lookback("ULTOSC_With", max_lookback_timeperiod1_timeperiod2_timeperiod3)
register_indicator_lookback("ULTOSC_With", "timeperiod1")
register_indicator_lookback("ULTOSC_With", "timeperiod2")
register_indicator_lookback("ULTOSC_With", "timeperiod3")

def ultosc_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod1 = params['timeperiod1']
    timeperiod2 = params['timeperiod2']
    timeperiod3 = params['timeperiod3']
    ultosc = talib.ULTOSC(
        bars_high[period_index], bars_low[period_index], bars_close[period_index],
        timeperiod1=timeperiod1, timeperiod2=timeperiod2, timeperiod3=timeperiod3
    )
    return ultosc < params['threshold'], ultosc > params['threshold']


indicator_options["ULTOSC_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod1": (min_lookback, max_lookback),
    "timeperiod2": (min_lookback, max_lookback),
    "timeperiod3": (min_lookback, max_lookback),
    "threshold": (0, 100),
}
register_indicator("ULTOSC_Against", ultosc_against_logic)
register_indicator_max_lookback("ULTOSC_Against", max_lookback_timeperiod1_timeperiod2_timeperiod3)
register_indicator_lookback("ULTOSC_Against", "timeperiod1")
register_indicator_lookback("ULTOSC_Against", "timeperiod2")
register_indicator_lookback("ULTOSC_Against", "timeperiod3")


# WILLR (Williams' %R)
def willr_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    willr = talib.WILLR(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return willr < lower_threshold, willr > upper_threshold


indicator_options["WILLR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (-100, 0),
    "lower_threshold": (-100, 0),
}
register_indicator("WILLR_With", willr_with_logic)
register_indicator_max_lookback("WILLR_With", max_lookback_only_timeperiod)
register_indicator_lookback("WILLR_With", "timeperiod")


def willr_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']
    willr = talib.WILLR(
        bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod
    )
    return willr > upper_threshold, willr < lower_threshold


indicator_options["WILLR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (-100, 0),
    "lower_threshold": (-100, 0),
}
register_indicator("WILLR_Against", willr_against_logic)
register_indicator_max_lookback("WILLR_Against", max_lookback_only_timeperiod)
register_indicator_lookback("WILLR_Against", "timeperiod")


# Hilbert Transform - Dominant Cycle Period
def ht_dcperiod_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    dcperiod = talib.HT_DCPERIOD(bars_close[period_index])
    return dcperiod > params['threshold'], dcperiod < params['threshold']


indicator_options["HT_DCPERIOD_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (0, 100),  # Example thresholds for cycle period
}
register_indicator("HT_DCPERIOD_With", ht_dcperiod_with_logic)
register_indicator_max_lookback("HT_DCPERIOD_With", max_lookback_none)


def ht_dcperiod_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    dcperiod = talib.HT_DCPERIOD(bars_close[period_index])
    return dcperiod < params['threshold'], dcperiod > params['threshold']


indicator_options["HT_DCPERIOD_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (0, 100),
}
register_indicator("HT_DCPERIOD_Against", ht_dcperiod_against_logic)
register_indicator_max_lookback("HT_DCPERIOD_Against", max_lookback_none)


# Hilbert Transform - Phasor Components
def ht_phasor_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    inphase, quadrature = talib.HT_PHASOR(bars_close[period_index])
    return inphase > quadrature, inphase < quadrature


indicator_options["HT_PHASOR_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("HT_PHASOR_With", ht_phasor_with_logic)
register_indicator_max_lookback("HT_PHASOR_With", max_lookback_none)


def ht_phasor_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    inphase, quadrature = talib.HT_PHASOR(bars_close[period_index])
    return inphase < quadrature, inphase > quadrature


indicator_options["HT_PHASOR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("HT_PHASOR_Against", ht_phasor_against_logic)
register_indicator_max_lookback("HT_PHASOR_Against", max_lookback_none)


# Weighted Close Price
def wclprice_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    wclprice = talib.WCLPRICE(bars_high[period_index], bars_low[period_index], bars_close[period_index])
    return bars_close[period_index] > wclprice, bars_close[period_index] < wclprice


indicator_options["WCLPRICE_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("WCLPRICE_With", wclprice_with_logic)
register_indicator_max_lookback("WCLPRICE_With", max_lookback_none)


def wclprice_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    wclprice = talib.WCLPRICE(bars_high[period_index], bars_low[period_index], bars_close[period_index])
    return bars_close[period_index] < wclprice, bars_close[period_index] > wclprice


indicator_options["WCLPRICE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("WCLPRICE_Against", wclprice_against_logic)
register_indicator_max_lookback("WCLPRICE_Against", max_lookback_none)


# Linear Regression - Slope
def linearreg_slope_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']
    slope = talib.LINEARREG_SLOPE(bars_close[period_index], timeperiod=lookback)
    return slope > 0, slope < 0


indicator_options["LinearReg_Slope_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
}
register_indicator("LinearReg_Slope_With", linearreg_slope_with_logic)
register_indicator_max_lookback("LinearReg_Slope_With", max_lookback_only_lookback)
register_indicator_lookback("LinearReg_Slope_With", "lookback")


def linearreg_slope_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']
    slope = talib.LINEARREG_SLOPE(bars_close[period_index], timeperiod=lookback)
    return slope < 0, slope > 0


indicator_options["LinearReg_Slope_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
}
register_indicator("LinearReg_Slope_Against", linearreg_slope_against_logic)
register_indicator_max_lookback("LinearReg_Slope_Against", max_lookback_only_lookback)
register_indicator_lookback("LinearReg_Slope_Against", "lookback")


# VOLUME BASED INDICATORS

# On-Balance Volume (OBV)
def obv_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    obv = talib.OBV(bars_close[period_index], bars_volume[period_index].astype(np.float64))
    return obv > params['threshold'], obv < params['threshold']

indicator_options["OBV_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (-1e6, 1e6),  # Threshold range for OBV
}
register_indicator("OBV_With", obv_with_logic)
register_indicator_max_lookback("OBV_With", max_lookback_none)

def obv_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    obv = talib.OBV(bars_close[period_index], bars_volume[period_index].astype(np.float64))
    return obv < params['threshold'], obv > params['threshold']

indicator_options["OBV_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (-1e6, 1e6),
}
register_indicator("OBV_Against", obv_against_logic)
register_indicator_max_lookback("OBV_Against", max_lookback_none)


# Volume Weighted Average Price (VWAP)
def vwap_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']  # Lookback window provided in params

    # Calculate Typical Price
    typical_price = (bars_high[period_index] +
                     bars_low[period_index] +
                     bars_close[period_index]) / 3

    volume = bars_volume[period_index]
    tp_vol = typical_price * volume

    # Calculate VWAP using rolling window
    with np.errstate(divide='ignore', invalid='ignore'):
        tp_vol_series = pd.Series(tp_vol)
        volume_series = pd.Series(volume)
        tp_vol_rolling = tp_vol_series.rolling(window=lookback, min_periods=1)
        volume_rolling = volume_series.rolling(window=lookback, min_periods=1)
        tp_vol_sum = tp_vol_rolling.sum()
        volume_rolling_sum = volume_rolling.sum()
        vwap = (tp_vol_sum/volume_rolling_sum)

    long_signals = bars_close[period_index] > vwap.values
    short_signals = bars_close[period_index] < vwap.values
    
    del tp_vol, tp_vol_series, volume_series, tp_vol_rolling, volume_rolling, tp_vol_sum, volume_rolling_sum, vwap    
    return long_signals, short_signals

indicator_options["VWAP_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),  # Optimization range for the lookback parameter
}
register_indicator("VWAP_With", vwap_with_logic)
register_indicator_max_lookback("VWAP_With", max_lookback_only_lookback)
register_indicator_lookback("VWAP_With", "lookback")


def vwap_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']  # Lookback window provided in params

    # Calculate Typical Price
    typical_price = (bars_high[period_index] +
                     bars_low[period_index] +
                     bars_close[period_index]) / 3

    volume = bars_volume[period_index]
    tp_vol = typical_price * volume

    # Calculate VWAP using rolling window
    with np.errstate(divide='ignore', invalid='ignore'):
        tp_vol_series = pd.Series(tp_vol)
        volume_series = pd.Series(volume)
        tp_vol_rolling = tp_vol_series.rolling(window=lookback, min_periods=1)
        volume_rolling = volume_series.rolling(window=lookback, min_periods=1)
        tp_vol_sum = tp_vol_rolling.sum()
        volume_rolling_sum = volume_rolling.sum()
        vwap = (tp_vol_sum/volume_rolling_sum)
        
    long_signals = bars_close[period_index] < vwap.values
    short_signals = bars_close[period_index] > vwap.values

    del tp_vol, tp_vol_series, volume_series, tp_vol_rolling, volume_rolling, tp_vol_sum, volume_rolling_sum, vwap    
    return long_signals, short_signals

indicator_options["VWAP_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),  # Optimization range for the lookback parameter
}
register_indicator("VWAP_Against", vwap_against_logic)
register_indicator_max_lookback("VWAP_Against", max_lookback_only_lookback)
register_indicator_lookback("VWAP_Against", "lookback")


# Accumulation/Distribution Line (AD)
def ad_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    ad = talib.AD(bars_high[period_index], bars_low[period_index], bars_close[period_index], bars_volume[period_index].astype(np.float64))
    return ad > params['threshold'], ad < params['threshold']

indicator_options["AD_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (-1e6, 1e6),  # Range for AD line
}
register_indicator("AD_With", ad_with_logic)
register_indicator_max_lookback("AD_With", max_lookback_none)


def ad_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    ad = talib.AD(bars_high[period_index], bars_low[period_index], bars_close[period_index], bars_volume[period_index].astype(np.float64))
    return ad < params['threshold'], ad > params['threshold']

indicator_options["AD_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (-1e6, 1e6),
}
register_indicator("AD_Against", ad_against_logic)
register_indicator_max_lookback("AD_Against", max_lookback_none)

# Ease of Movement (EOM)
def eom_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']
    high = bars_high[period_index]
    low = bars_low[period_index]
    volume = bars_volume[period_index]
    eom = (high - low) / volume
    cumsum_eom = np.cumsum(eom)
    rolling_mean_eom = np.full_like(eom, np.nan, dtype=np.float64)
    for i in range(lookback, len(eom) - 1):
        rolling_mean_eom[i] = (cumsum_eom[i] - cumsum_eom[i - lookback]) / lookback

    long_signals = rolling_mean_eom > params['threshold']
    short_signals = rolling_mean_eom < params['threshold']
    del eom, cumsum_eom, rolling_mean_eom
    return long_signals, short_signals

indicator_options["EOM_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "threshold": (-1.0, 1.0),  # Range for EOM
}
register_indicator("EOM_With", eom_with_logic)
register_indicator_max_lookback("EOM_With", max_lookback_only_lookback)
register_indicator_lookback("EOM_With", "lookback")


def eom_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    lookback = params['lookback']

    high = bars_high[period_index]
    low = bars_low[period_index]
    volume = bars_volume[period_index]
    eom = (high - low) / volume
    cumsum_eom = np.cumsum(eom)
    rolling_mean_eom = np.full_like(eom, np.nan, dtype=np.float64)
    for i in range(lookback, len(eom) - 1):
        rolling_mean_eom[i] = (cumsum_eom[i] - cumsum_eom[i - lookback]) / lookback

    long_signals = rolling_mean_eom < params['threshold']
    short_signals = rolling_mean_eom > params['threshold']

    del eom, cumsum_eom, rolling_mean_eom
    return long_signals, short_signals

indicator_options["EOM_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "lookback": (min_lookback, max_lookback),
    "threshold": (-1.0, 1.0),
}
register_indicator("EOM_Against", eom_against_logic)
register_indicator_max_lookback("EOM_Against", max_lookback_only_lookback)
register_indicator_lookback("EOM_Against", "lookback")

# MIDPOINT (MidPoint over period)
def midpoint_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    midpoint = talib.MIDPOINT(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > midpoint, bars_close[period_index] < midpoint


indicator_options["MIDPOINT_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MIDPOINT_With", midpoint_with_logic)
register_indicator_max_lookback("MIDPOINT_With", max_lookback_only_timeperiod)
register_indicator_lookback("MIDPOINT_With", "timeperiod")


def midpoint_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    midpoint = talib.MIDPOINT(bars_close[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < midpoint, bars_close[period_index] > midpoint


indicator_options["MIDPOINT_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MIDPOINT_Against", midpoint_against_logic)
register_indicator_max_lookback("MIDPOINT_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MIDPOINT_Against", "timeperiod")


# MIDPRICE (Midpoint Price over period)
def midprice_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    midprice = talib.MIDPRICE(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return bars_close[period_index] > midprice, bars_close[period_index] < midprice


indicator_options["MIDPRICE_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MIDPRICE_With", midprice_with_logic)
register_indicator_max_lookback("MIDPRICE_With", max_lookback_only_timeperiod)
register_indicator_lookback("MIDPRICE_With", "timeperiod")


def midprice_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    midprice = talib.MIDPRICE(bars_high[period_index], bars_low[period_index], timeperiod=timeperiod)
    return bars_close[period_index] < midprice, bars_close[period_index] > midprice


indicator_options["MIDPRICE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("MIDPRICE_Against", midprice_against_logic)
register_indicator_max_lookback("MIDPRICE_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MIDPRICE_Against", "timeperiod")


# SAREXT (Parabolic SAR Extended)
def sarext_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    startvalue = params['startvalue']
    offsetonreverse = params['offsetonreverse']
    accelerationinitlong = params['accelerationinitlong']
    accelerationlong = params['accelerationlong']
    accelerationmaxlong = params['accelerationmaxlong']
    accelerationinitshort = params['accelerationinitshort']
    accelerationshort = params['accelerationshort']
    accelerationmaxshort = params['accelerationmaxshort']

    sarext = talib.SAREXT(
        bars_high[period_index],
        bars_low[period_index],
        startvalue=startvalue,
        offsetonreverse=offsetonreverse,
        accelerationinitlong=accelerationinitlong,
        accelerationlong=accelerationlong,
        accelerationmaxlong=accelerationmaxlong,
        accelerationinitshort=accelerationinitshort,
        accelerationshort=accelerationshort,
        accelerationmaxshort=accelerationmaxshort
    )

    return bars_close[period_index] > sarext, bars_close[period_index] < sarext


indicator_options["SAREXT_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "startvalue": (0.0, 0.1),
    "offsetonreverse": (0.0, 0.1),
    "accelerationinitlong": (0.01, 0.3),
    "accelerationlong": (0.01, 0.3),
    "accelerationmaxlong": (0.01, 0.3),
    "accelerationinitshort": (0.01, 0.3),
    "accelerationshort": (0.01, 0.3),
    "accelerationmaxshort": (0.01, 0.3),
}
register_indicator("SAREXT_With", sarext_with_logic)
register_indicator_max_lookback("SAREXT_With", max_lookback_none)


def sarext_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    startvalue = params['startvalue']
    offsetonreverse = params['offsetonreverse']
    accelerationinitlong = params['accelerationinitlong']
    accelerationlong = params['accelerationlong']
    accelerationmaxlong = params['accelerationmaxlong']
    accelerationinitshort = params['accelerationinitshort']
    accelerationshort = params['accelerationshort']
    accelerationmaxshort = params['accelerationmaxshort']

    sarext = talib.SAREXT(
        bars_high[period_index],
        bars_low[period_index],
        startvalue=startvalue,
        offsetonreverse=offsetonreverse,
        accelerationinitlong=accelerationinitlong,
        accelerationlong=accelerationlong,
        accelerationmaxlong=accelerationmaxlong,
        accelerationinitshort=accelerationinitshort,
        accelerationshort=accelerationshort,
        accelerationmaxshort=accelerationmaxshort
    )

    return bars_close[period_index] < sarext, bars_close[period_index] > sarext


indicator_options["SAREXT_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "startvalue": (0.0, 0.1),
    "offsetonreverse": (0.0, 0.1),
    "accelerationinitlong": (0.01, 0.3),
    "accelerationlong": (0.01, 0.3),
    "accelerationmaxlong": (0.01, 0.3),
    "accelerationinitshort": (0.01, 0.3),
    "accelerationshort": (0.01, 0.3),
    "accelerationmaxshort": (0.01, 0.3),
}
register_indicator("SAREXT_Against", sarext_against_logic)
register_indicator_max_lookback("SAREXT_Against", max_lookback_none)


# MOMENTUM INDICATORS

# MFI (Money Flow Index)
def mfi_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']

    mfi = talib.MFI(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        bars_volume[period_index].astype(np.float64),
        timeperiod=timeperiod
    )

    return mfi < lower_threshold, mfi > upper_threshold  # Buy when oversold, sell when overbought


indicator_options["MFI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),  # Overbought threshold
    "lower_threshold": (0, 100),  # Oversold threshold
}
register_indicator("MFI_With", mfi_with_logic)
register_indicator_max_lookback("MFI_With", max_lookback_only_timeperiod)
register_indicator_lookback("MFI_With", "timeperiod")


def mfi_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']

    mfi = talib.MFI(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        bars_volume[period_index].astype(np.float64),
        timeperiod=timeperiod
    )

    return mfi > upper_threshold, mfi < lower_threshold  # Sell when overbought, buy when oversold


indicator_options["MFI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
}
register_indicator("MFI_Against", mfi_against_logic)
register_indicator_max_lookback("MFI_Against", max_lookback_only_timeperiod)
register_indicator_lookback("MFI_Against", "timeperiod")


# STOCHRSI (Stochastic Relative Strength Index)
def stochrsi_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    fastk_period = params['fastk_period']
    fastd_period = params['fastd_period']
    fastd_matype = params['fastd_matype']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']

    fastk, fastd = talib.STOCHRSI(
        bars_close[period_index],
        timeperiod=timeperiod,
        fastk_period=fastk_period,
        fastd_period=fastd_period,
        fastd_matype=fastd_matype
    )

    return fastk < lower_threshold, fastk > upper_threshold  # Buy when oversold, sell when overbought


indicator_options["STOCHRSI_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "fastk_period": (min_lookback, max_lookback),
    "fastd_period": (min_lookback, max_lookback),
    "fastd_matype": (0, 8),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
}
register_indicator("STOCHRSI_With", stochrsi_with_logic)
register_indicator_max_lookback("STOCHRSI_With", max_lookback_timeperiod_fastk_fastd)
register_indicator_lookback("STOCHRSI_With", "timeperiod")
register_indicator_lookback("STOCHRSI_With", "fastk_period")
register_indicator_lookback("STOCHRSI_With", "fastd_period")


def stochrsi_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    fastk_period = params['fastk_period']
    fastd_period = params['fastd_period']
    fastd_matype = params['fastd_matype']
    upper_threshold = params['upper_threshold']
    lower_threshold = params['lower_threshold']

    fastk, fastd = talib.STOCHRSI(
        bars_close[period_index],
        timeperiod=timeperiod,
        fastk_period=fastk_period,
        fastd_period=fastd_period,
        fastd_matype=fastd_matype
    )

    return fastk > upper_threshold, fastk < lower_threshold  # Sell when overbought, buy when oversold


indicator_options["STOCHRSI_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "fastk_period": (min_lookback, max_lookback),
    "fastd_period": (min_lookback, max_lookback),
    "fastd_matype": (0, 8),
    "upper_threshold": (0, 100),
    "lower_threshold": (0, 100),
}
register_indicator("STOCHRSI_Against", stochrsi_against_logic)
register_indicator_max_lookback("STOCHRSI_Against", max_lookback_timeperiod_fastk_fastd)
register_indicator_lookback("STOCHRSI_Against", "timeperiod")
register_indicator_lookback("STOCHRSI_Against", "fastk_period")
register_indicator_lookback("STOCHRSI_Against", "fastd_period")


# TRIX (1-day Rate-Of-Change of a Triple Smooth EMA)
def trix_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']

    trix = talib.TRIX(bars_close[period_index], timeperiod=timeperiod)
    return trix > 0, trix < 0  # Buy when positive, sell when negative


indicator_options["TRIX_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TRIX_With", trix_with_logic)
register_indicator_max_lookback("TRIX_With", max_lookback_only_timeperiod)
register_indicator_lookback("TRIX_With", "timeperiod")


def trix_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']

    trix = talib.TRIX(bars_close[period_index], timeperiod=timeperiod)
    return trix < 0, trix > 0  # Buy when negative, sell when positive


indicator_options["TRIX_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
}
register_indicator("TRIX_Against", trix_against_logic)
register_indicator_max_lookback("TRIX_Against", max_lookback_only_timeperiod)
register_indicator_lookback("TRIX_Against", "timeperiod")


# VOLUME INDICATORS

# ADOSC (Chaikin A/D Oscillator)
def adosc_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']

    adosc = talib.ADOSC(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        bars_volume[period_index].astype(np.float64),
        fastperiod=fastperiod,
        slowperiod=slowperiod
    )

    return adosc > 0, adosc < 0  # Buy when positive, sell when negative


indicator_options["ADOSC_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
}
register_indicator("ADOSC_With", adosc_with_logic)
register_indicator_max_lookback("ADOSC_With", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("ADOSC_With", "fastperiod")
register_indicator_lookback("ADOSC_With", "slowperiod")


def adosc_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    fastperiod = params['fastperiod']
    slowperiod = params['slowperiod']

    adosc = talib.ADOSC(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        bars_volume[period_index].astype(np.float64),
        fastperiod=fastperiod,
        slowperiod=slowperiod
    )

    return adosc < 0, adosc > 0  # Buy when negative, sell when positive


indicator_options["ADOSC_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "fastperiod": (min_lookback, max_lookback),
    "slowperiod": (min_lookback, max_lookback),
}
register_indicator("ADOSC_Against", adosc_against_logic)
register_indicator_max_lookback("ADOSC_Against", max_lookback_fastperiod_slowperiod)
register_indicator_lookback("ADOSC_Against", "fastperiod")
register_indicator_lookback("ADOSC_Against", "slowperiod")

# PRICE TRANSFORM

# AVGPRICE (Average Price)
def avgprice_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    avgprice = talib.AVGPRICE(
        bars_open[period_index],
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return bars_close[period_index] > avgprice, bars_close[period_index] < avgprice


indicator_options["AVGPRICE_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("AVGPRICE_With", avgprice_with_logic)
register_indicator_max_lookback("AVGPRICE_With", max_lookback_none)


def avgprice_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    avgprice = talib.AVGPRICE(
        bars_open[period_index],
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return bars_close[period_index] < avgprice, bars_close[period_index] > avgprice


indicator_options["AVGPRICE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("AVGPRICE_Against", avgprice_against_logic)
register_indicator_max_lookback("AVGPRICE_Against", max_lookback_none)


# MEDPRICE (Median Price)
def medprice_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    medprice = talib.MEDPRICE(bars_high[period_index], bars_low[period_index])
    return bars_close[period_index] > medprice, bars_close[period_index] < medprice


indicator_options["MEDPRICE_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("MEDPRICE_With", medprice_with_logic)
register_indicator_max_lookback("MEDPRICE_With", max_lookback_none)


def medprice_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    medprice = talib.MEDPRICE(bars_high[period_index], bars_low[period_index])
    return bars_close[period_index] < medprice, bars_close[period_index] > medprice


indicator_options["MEDPRICE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("MEDPRICE_Against", medprice_against_logic)
register_indicator_max_lookback("MEDPRICE_Against", max_lookback_none)


# TYPPRICE (Typical Price)
def typprice_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    typprice = talib.TYPPRICE(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return bars_close[period_index] > typprice, bars_close[period_index] < typprice


indicator_options["TYPPRICE_With"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("TYPPRICE_With", typprice_with_logic)
register_indicator_max_lookback("TYPPRICE_With", max_lookback_none)


def typprice_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]

    typprice = talib.TYPPRICE(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return bars_close[period_index] < typprice, bars_close[period_index] > typprice


indicator_options["TYPPRICE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
}
register_indicator("TYPPRICE_Against", typprice_against_logic)
register_indicator_max_lookback("TYPPRICE_Against", max_lookback_none)


# VOLATILITY INDICATORS

# NATR (Normalized Average True Range)
def natr_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    threshold = params['threshold']

    natr = talib.NATR(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        timeperiod=timeperiod
    )

    return natr > threshold, natr < threshold  # Buy when volatility is high, sell when low


indicator_options["NATR_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),  # Normalized ATR threshold (percentage)
}
register_indicator("NATR_With", natr_with_logic)
register_indicator_max_lookback("NATR_With", max_lookback_only_timeperiod)
register_indicator_lookback("NATR_With", "timeperiod")


def natr_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    threshold = params['threshold']

    natr = talib.NATR(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index],
        timeperiod=timeperiod
    )

    return natr < threshold, natr > threshold  # Buy when volatility is low, sell when high


indicator_options["NATR_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "threshold": (0.5, 5.0),
}
register_indicator("NATR_Against", natr_against_logic)
register_indicator_max_lookback("NATR_Against", max_lookback_only_timeperiod)
register_indicator_lookback("NATR_Against", "timeperiod")


# TRANGE (True Range)
def trange_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    threshold = params['threshold']

    trange = talib.TRANGE(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return trange > threshold, trange < threshold  # Buy when range is wide, sell when narrow


indicator_options["TRANGE_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (0.5, 5.0),  # True Range threshold
}
register_indicator("TRANGE_With", trange_with_logic)
register_indicator_max_lookback("TRANGE_With", max_lookback_none)


def trange_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    threshold = params['threshold']

    trange = talib.TRANGE(
        bars_high[period_index],
        bars_low[period_index],
        bars_close[period_index]
    )

    return trange < threshold, trange > threshold  # Buy when range is narrow, sell when wide


indicator_options["TRANGE_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "threshold": (0.5, 5.0),
}
register_indicator("TRANGE_Against", trange_against_logic)
register_indicator_max_lookback("TRANGE_Against", max_lookback_none)

## MORE BESPOKE AND SLOW INDICATORS ## ---------------------------------------------------------

# OPTIMIZED VOLATILITY & TREND INDICATORS

# Keltner Channels - Optimized
def keltner_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    multiplier = params['multiplier']

    # Calculate EMA and ATR using TA-Lib (already optimized)
    ema = talib.EMA(bars_close[period_index], timeperiod=timeperiod)
    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)

    # Vectorized band calculation
    upper_band = ema + (atr * multiplier)
    lower_band = ema - (atr * multiplier)

    # Vectorized signal generation
    long_signal = bars_close[period_index] < lower_band
    short_signal = bars_close[period_index] > upper_band

    return long_signal, short_signal


indicator_options["Keltner_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (0.5, 3.0)
}
register_indicator("Keltner_With", keltner_with_logic)
register_indicator_max_lookback("Keltner_With", max_lookback_only_timeperiod)
register_indicator_lookback("Keltner_With", "timeperiod")


def keltner_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    multiplier = params['multiplier']

    # Reuse the same calculation logic to ensure consistency
    ema = talib.EMA(bars_close[period_index], timeperiod=timeperiod)
    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)

    upper_band = ema + (atr * multiplier)
    lower_band = ema - (atr * multiplier)

    # Just flip the signals for the "against" logic
    long_signal = bars_close[period_index] > upper_band
    short_signal = bars_close[period_index] < lower_band

    return long_signal, short_signal


indicator_options["Keltner_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (0.5, 3.0)
}
register_indicator("Keltner_Against", keltner_against_logic)
register_indicator_max_lookback("Keltner_Against", max_lookback_only_timeperiod)
register_indicator_lookback("Keltner_Against", "timeperiod")


# Supertrend - Optimized
def supertrend_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    multiplier = params['multiplier']

    # Get the lengths
    length = len(bars_close[period_index])

    # Use TA-Lib for ATR
    atr = talib.ATR(bars_high[period_index], bars_low[period_index], bars_close[period_index], timeperiod=timeperiod)

    # Calculate midpoint of each bar
    hl2 = (bars_high[period_index] + bars_low[period_index]) / 2.0

    # Basic bands efficiently
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)

    # Initialize supertrend array
    supertrend = np.zeros_like(bars_close[period_index])
    trend = np.zeros_like(bars_close[period_index])

    # The first timeperiod+1 values will be NaN/undefined
    supertrend[:timeperiod + 1] = 0
    trend[:timeperiod + 1] = 0

    # Only process the defined values (after timeperiod)
    if length > timeperiod + 1:
        # Initialize
        if bars_close[period_index][timeperiod] <= upper_band[timeperiod]:
            supertrend[timeperiod] = upper_band[timeperiod]
            trend[timeperiod] = -1  # Downtrend
        else:
            supertrend[timeperiod] = lower_band[timeperiod]
            trend[timeperiod] = 1  # Uptrend

        # Calculate remaining values using vectorized operations where possible
        for i in range(timeperiod + 1, length):
            # Determine upper band
            if (upper_band[i] < upper_band[i - 1]) or (bars_close[period_index][i - 1] > upper_band[i - 1]):
                upper_band[i] = upper_band[i]
            else:
                upper_band[i] = upper_band[i - 1]

            # Determine lower band
            if (lower_band[i] > lower_band[i - 1]) or (bars_close[period_index][i - 1] < lower_band[i - 1]):
                lower_band[i] = lower_band[i]
            else:
                lower_band[i] = lower_band[i - 1]

            # Update supertrend based on previous trend
            if trend[i - 1] == -1:  # Previous downtrend
                if bars_close[period_index][i] > upper_band[i]:
                    supertrend[i] = lower_band[i]
                    trend[i] = 1
                else:
                    supertrend[i] = upper_band[i]
                    trend[i] = -1
            else:  # Previous uptrend
                if bars_close[period_index][i] < lower_band[i]:
                    supertrend[i] = upper_band[i]
                    trend[i] = -1
                else:
                    supertrend[i] = lower_band[i]
                    trend[i] = 1

    # Generate signals directly from trend
    long_signal = trend > 0
    short_signal = trend < 0

    return long_signal, short_signal


indicator_options["Supertrend_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (1.0, 5.0)
}
register_indicator("Supertrend_With", supertrend_with_logic)
register_indicator_max_lookback("Supertrend_With", max_lookback_only_timeperiod)
register_indicator_lookback("Supertrend_With", "timeperiod")


def supertrend_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    # Reuse the same calculation but invert the signals
    long_signal, short_signal = supertrend_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume,
                                                      period_index, params)
    return short_signal, long_signal  # Just swap the signals


indicator_options["Supertrend_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "multiplier": (1.0, 5.0)
}
register_indicator("Supertrend_Against", supertrend_against_logic)
register_indicator_max_lookback("Supertrend_Against", max_lookback_only_timeperiod)
register_indicator_lookback("Supertrend_Against", "timeperiod")



# Chaikin Volatility - Optimized
def chaikin_volatility_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    bar_type = minutes_type[params['bar_type']]
    timeperiod = params['timeperiod']
    roc_period = params['roc_period']
    threshold = params['threshold']

    # Calculate high-low range (vectorized)
    hl_diff = bars_high[period_index] - bars_low[period_index]

    # Use TA-Lib EMA
    ema_hl_diff = talib.EMA(hl_diff, timeperiod=timeperiod)

    # Use TA-Lib ROC for rate of change calculation
    chaikin_vol = talib.ROC(ema_hl_diff, timeperiod=roc_period)

    # Generate signals (vectorized)
    long_signal = chaikin_vol > threshold
    short_signal = chaikin_vol < -threshold

    return long_signal, short_signal


indicator_options["ChaikinVol_With"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "roc_period": (min_lookback, max_lookback),
    "threshold": (5.0, 30.0)
}
register_indicator("ChaikinVol_With", chaikin_volatility_with_logic)
register_indicator_max_lookback("ChaikinVol_With", max_lookback_only_timeperiod)
register_indicator_lookback("ChaikinVol_With", "timeperiod")
register_indicator_lookback("ChaikinVol_With", "roc_period")


def chaikin_volatility_against_logic(bars_open, bars_high, bars_low, bars_close, bars_volume, period_index, params):
    # Inverse signals
    long_signal, short_signal = chaikin_volatility_with_logic(bars_open, bars_high, bars_low, bars_close, bars_volume,
                                                              period_index, params)
    return short_signal, long_signal


indicator_options["ChaikinVol_Against"] = {
    "bar_type": (1, len(BarMinutes)),
    "timeperiod": (min_lookback, max_lookback),
    "roc_period": (min_lookback, max_lookback),
    "threshold": (5.0, 30.0)
}
register_indicator("ChaikinVol_Against", chaikin_volatility_against_logic)
register_indicator_max_lookback("ChaikinVol_Against", max_lookback_only_timeperiod)
register_indicator_lookback("ChaikinVol_Against", "timeperiod")
register_indicator_lookback("ChaikinVol_Against", "roc_period")
