# feature_calculator.py
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
from scipy.stats import pearsonr
import logging
from config import Z_WINDOW, RSI_PERIOD, ATR_PERIOD, EMA_SHORT, EMA_LONG

logger = logging.getLogger(__name__)

def safe_float(value, default=0.0):
    """Безопасное преобразование в float"""
    try:
        if pd.isna(value) or value is None:
            return default
        return float(value)
    except:
        return default

def calculate_z_score(series, window=Z_WINDOW):
    if len(series) < window or window <= 0:
        return 0.0, 0.0, 0.0
    try:
        window_data = series[-window:]
        mean = window_data.mean()
        std = window_data.std()
        if pd.isna(std) or std == 0:
            return 0.0, mean, std
        z = (series.iloc[-1] - mean) / std
        return safe_float(z, 0.0), safe_float(mean, 0.0), safe_float(std, 0.0)
    except:
        return 0.0, 0.0, 0.0

def calculate_rsi(series, period=RSI_PERIOD):
    """Исправленный расчет RSI"""
    if len(series) < max(2, period):
        return 50.0
    try:
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        # Используем EMA для более точного расчета
        avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        rs = rs.replace([np.inf, -np.inf], np.nan)
        rsi = 100 - (100 / (1 + rs))
        # Возвращаем последнее значение, игнорируя NaN
        last_valid = rsi.dropna()
        if len(last_valid) > 0:
            return safe_float(last_valid.iloc[-1], 50.0)
        return 50.0
    except:
        return 50.0

def calculate_macd(series, fast=12, slow=26, signal=9):
    if len(series) < max(fast, slow, signal) + 2:
        return 0.0, 0.0, 0.0
    try:
        ema_fast = series.ewm(span=fast, min_periods=fast).mean()
        ema_slow = series.ewm(span=slow, min_periods=slow).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal, min_periods=signal).mean()
        histogram = macd - macd_signal
        macd_val = safe_float(macd.iloc[-1] if len(macd) > 0 else 0.0, 0.0)
        signal_val = safe_float(macd_signal.iloc[-1] if len(macd_signal) > 0 else 0.0, 0.0)
        hist_val = safe_float(histogram.iloc[-1] if len(histogram) > 0 else 0.0, 0.0)
        return macd_val, signal_val, hist_val
    except:
        return 0.0, 0.0, 0.0

def calculate_stoch_rsi(series, period=RSI_PERIOD, smooth_k=3, smooth_d=3):
    if len(series) < max(period, smooth_k, smooth_d) + 5:
        return 0.5, 0.5
    try:
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(period, min_periods=1).mean()
        avg_loss = loss.rolling(period, min_periods=1).mean()
        rs = avg_gain / avg_loss.replace(0, 1e-10)
        rsi = 100 - (100 / (1 + rs))
        min_rsi = rsi.rolling(period, min_periods=1).min()
        max_rsi = rsi.rolling(period, min_periods=1).max()
        stoch_rsi = (rsi - min_rsi) / (max_rsi - min_rsi).replace(0, 1e-10) # Исправлено: заменяем 0 на 1e-10 вместо 1
        stoch_rsi = stoch_rsi.fillna(0.5).replace([np.inf, -np.inf], 0.5) # Убрана замена 0 на 1
        stoch_k = stoch_rsi.rolling(smooth_k, min_periods=1).mean()
        stoch_d = stoch_k.rolling(smooth_d, min_periods=1).mean()
        k_val = safe_float(stoch_k.iloc[-1] if len(stoch_k) > 0 else 0.5, 0.5)
        d_val = safe_float(stoch_d.iloc[-1] if len(stoch_d) > 0 else 0.5, 0.5)
        return k_val, d_val
    except:
        return 0.5, 0.5

def calculate_atr_level(df, period=ATR_PERIOD):
    if len(df) < max(period, 2):
        return 0.0
    try:
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(period, min_periods=1).mean()
        atr_pct = (atr / df['close']) * 100
        return safe_float(atr_pct.iloc[-1] if len(atr_pct) > 0 else 0.0, 0.0)
    except:
        return 0.0

def calculate_ema(series, period):
    """Рассчитать EMA"""
    if len(series) < max(period, 2):
        return series.ewm(span=max(period, 1), min_periods=1).mean()
    return series.ewm(span=max(period, 1), min_periods=period).mean()

def check_divergence_improved(df, indicator_series, indicator_name, periods=5):
    """Улучшенная проверка дивергенции между ценой и индикатором"""
    try:
        if len(df) < periods + 10 or len(indicator_series) < periods + 10:
            return False, "Недостаточно данных"
        # Используем последние 50 точек для поиска дивергенций
        lookback = min(50, len(df))
        df_recent = df.tail(lookback)
        indicator_recent = indicator_series.tail(lookback)
        # Находим локальные минимумы и максимумы для цены
        price_minima = find_peaks(-df_recent['low'].values, distance=5)[0]
        price_maxima = find_peaks(df_recent['high'].values, distance=5)[0]
        # Находим локальные минимумы и максимумы для индикатора
        indicator_minima = find_peaks(-indicator_recent.values, distance=5)[0]
        indicator_maxima = find_peaks(indicator_recent.values, distance=5)[0]
        # Проверяем bullish divergence (цена делает ниже минимумы, индикатор - выше)
        if len(price_minima) >= 2 and len(indicator_minima) >= 2:
            # Сравниваем последние два минимума
            if (len(price_minima) >= 2 and len(indicator_minima) >= 2 and
                df_recent['low'].iloc[price_minima[-1]] < df_recent['low'].iloc[price_minima[-2]] and
                indicator_recent.iloc[indicator_minima[-1]] > indicator_recent.iloc[indicator_minima[-2]]):
                return True, f"Bullish {indicator_name} divergence"
        # Проверяем bearish divergence (цена делает выше максимумы, индикатор - ниже)
        if len(price_maxima) >= 2 and len(indicator_maxima) >= 2:
            # Сравниваем последние два максимума
            if (len(price_maxima) >= 2 and len(indicator_maxima) >= 2 and
                df_recent['high'].iloc[price_maxima[-1]] > df_recent['high'].iloc[price_maxima[-2]] and
                indicator_recent.iloc[indicator_maxima[-1]] < indicator_recent.iloc[indicator_maxima[-2]]):
                return True, f"Bearish {indicator_name} divergence"
        return False, "No divergence"
    except Exception as e:
        logger.error(f"Ошибка расчета дивергенции: {e}")
        return False, "Ошибка расчета"

def calculate_correlation(series1, series2, window=30):
    """Расчет корреляции между двумя рядами"""
    try:
        if len(series1) < window or len(series2) < window:
            return 0.0
        # Выравниваем длины рядов
        min_len = min(len(series1), len(series2))
        s1 = series1[-min_len:]
        s2 = series2[-min_len:]
        # Берем последние window значений
        s1_window = s1[-window:]
        s2_window = s2[-window:]
        # Рассчитываем корреляцию Пирсона
        correlation, _ = pearsonr(s1_window, s2_window)
        # Проверка на NaN после pearsonr
        if np.isnan(correlation):
            return 0.0
        return safe_float(correlation, 0.0)
    except:
        return 0.0

def get_volatility_regime(atr_pct):
    if atr_pct > 5.0:
        return 'high'
    elif atr_pct > 2.0:
        return 'medium'
    else:
        return 'low'

def get_market_regime(z_4h, rsi_4h, atr_pct, volume_z_4h, trend_direction):
    if atr_pct < 1.5:
        volatility = 'low'
    elif atr_pct < 3.0:
        volatility = 'medium'
    else:
        volatility = 'high'
    if abs(z_4h) < 1.0 and 40 < rsi_4h < 60:
        regime = 'range'
    elif z_4h < -2.0 and rsi_4h < 30 and trend_direction == 'up':
        regime = 'oversold'
    elif z_4h > 2.0 and rsi_4h > 70 and trend_direction == 'down':
        regime = 'overbought'
    elif z_4h < -1.0 and volume_z_4h > 1.0:
        regime = 'accumulation'
    elif z_4h > 1.0 and volume_z_4h < -1.0:
        regime = 'distribution'
    else:
        regime = 'trending'
    return regime, volatility

# --- Функции для расчета признаков на основе данных ---
def calculate_indicators_for_timeframe(df, timeframe_name):
    """Рассчитывает все индикаторы для одного таймфрейма"""
    if df is None or len(df) < 10:
        return None

    close = df['close']
    volume = df['volume']

    # 1. Z-score
    z, _, _ = calculate_z_score(close, Z_WINDOW)
    # 2. RSI
    rsi_val = calculate_rsi(close, RSI_PERIOD)
    rsi_signal = 'buy' if rsi_val < 30 else 'sell' if rsi_val > 70 else 'hold'
    # 3. MACD
    macd_line, macd_signal_line, macd_hist = calculate_macd(close)
    macd_signal = 'buy' if macd_hist > 0 else 'sell' if macd_hist < 0 else 'hold'
    # 4. Stochastic RSI
    stoch_k, stoch_d = calculate_stoch_rsi(close, RSI_PERIOD)
    stoch_signal = 'buy' if stoch_k < 20 and stoch_k > stoch_d else \
                   'sell' if stoch_k > 80 and stoch_k < stoch_d else 'hold'
    # 5. Volume Z-score
    vol_z, _, _ = calculate_z_score(volume, Z_WINDOW)
    # 6. ATR (волатильность)
    atr_pct = calculate_atr_level(df, ATR_PERIOD)
    volatility_regime = get_volatility_regime(atr_pct)

    return {
        'tf': timeframe_name,
        'price': safe_float(close.iloc[-1], 0.0),
        'z': safe_float(z, 0.0),
        'rsi': safe_float(rsi_val, 50.0),
        'macd_hist': safe_float(macd_hist, 0.0),
        'stoch_k': safe_float(stoch_k, 0.5),
        'vol_z': safe_float(vol_z, 0.0),
        'atr_pct': safe_float(atr_pct, 0.0),
        'volatility': volatility_regime,
        # Для консенсуса
        'rsi_signal': rsi_signal,
        'macd_signal': macd_signal,
        'stoch_signal': stoch_signal,
    }

def get_consensus_signal(values_dict, threshold=2, signal_type='binary'):
    if not values_dict:
        return 'HOLD', 0
    signals = []
    for tf, val in values_dict.items():
        if signal_type == 'binary':
            signals.append('buy' if val > 0 else 'sell' if val < 0 else 'hold')
        elif signal_type == 'ternary':
            if val == 'buy' or val == 'oversold':
                signals.append('buy')
            elif val == 'sell' or val == 'overbought':
                signals.append('sell')
            else:
                signals.append('hold')
        elif signal_type == 'level':
            signals.append(val)
    buy_count = signals.count('buy')
    sell_count = signals.count('sell')
    hold_count = signals.count('hold')
    if buy_count >= threshold:
        return 'BUY', buy_count
    elif sell_count >= threshold:
        return 'SELL', sell_count
    else:
        return 'HOLD', 0

def calculate_consolidated_indicators(indicators_per_tf):
    """Вычисляет консенсусные сигналы и другие агрегированные признаки"""
    if not indicators_per_tf:
        return {}, {}

    consensus = {}
    confidences = {}
    indicators = {
        'z': {}, 'rsi': {}, 'macd': {}, 'stoch_rsi': {}, 'volume_z': {}, 'atr_pct': {}
    }

    for item in indicators_per_tf:
        tf_name = item['tf']
        indicators['z'][tf_name] = item['z']
        indicators['rsi'][tf_name] = item['rsi_signal']
        indicators['macd'][tf_name] = item['macd_hist']
        indicators['stoch_rsi'][tf_name] = item['stoch_signal']
        indicators['volume_z'][tf_name] = item['vol_z']
        indicators['atr_pct'][tf_name] = item['atr_pct']

    # === КОНСЕНСУС ПО ИНДИКАТОРАМ ===
    # Z-score: buy если Z < -1.5 (перепродан), sell если > 1.5
    z_vals = {tf: 1 if v < -1.5 else -1 if v > 1.5 else 0 for tf, v in indicators['z'].items()}
    consensus['z'], confidences['z'] = get_consensus_signal(z_vals, threshold=2, signal_type='binary')
    # RSI
    consensus['rsi'], confidences['rsi'] = get_consensus_signal(indicators['rsi'], threshold=2, signal_type='ternary')
    # MACD (по гистограмме)
    macd_vals = {tf: 1 if v > 0 else -1 if v < 0 else 0 for tf, v in indicators['macd'].items()}
    consensus['macd'], confidences['macd'] = get_consensus_signal(macd_vals, threshold=2, signal_type='binary')
    # Stoch RSI
    consensus['stoch'], confidences['stoch'] = get_consensus_signal(indicators['stoch_rsi'], threshold=2, signal_type='ternary')
    # Volume Z: сила подтверждения
    vol_vals = {tf: 1 if v > 0.5 else -1 if v < -0.5 else 0 for tf, v in indicators['volume_z'].items()}
    consensus['volume'], confidences['volume'] = get_consensus_signal(vol_vals, threshold=2, signal_type='binary')

    return consensus, confidences, indicators

def calculate_trend(dfs, main_tf_order=['4h', '1h', '15m']):
    """Рассчитывает тренд на основе EMA"""
    main_df = None
    for tf_name in main_tf_order:
        if tf_name in dfs and dfs[tf_name] is not None and len(dfs[tf_name]) > 0:
            main_df = dfs[tf_name]
            break
    trend_direction = 'neutral'
    if main_df is not None and len(main_df) >= max(EMA_SHORT, 10):
        try:
            ema_50 = calculate_ema(main_df['close'], EMA_SHORT)
            ema_200 = calculate_ema(main_df['close'], EMA_LONG)
            if len(ema_50) > 0 and len(ema_200) > 0:
                if ema_50.iloc[-1] > ema_200.iloc[-1]:
                    trend_direction = 'up'
                elif ema_50.iloc[-1] < ema_200.iloc[-1]:
                    trend_direction = 'down'
        except Exception as e:
            logger.error(f"Ошибка расчета тренда: {e}")
    return trend_direction, main_df

def calculate_divergences(main_df, rsi_period=RSI_PERIOD):
    """Рассчитывает дивергенции RSI и MACD"""
    rsi_divergence = (False, "")
    macd_divergence = (False, "")
    if main_df is not None and len(main_df) >= 20:
        try:
            # Создаем серию RSI для всего датафрейма
            rsi_series = pd.Series(index=main_df.index, dtype=float)
            for i in range(len(main_df)):
                if i + 1 >= max(2, rsi_period):
                    rsi_series.iloc[i] = calculate_rsi(main_df['close'].iloc[:i+1], rsi_period)
                else:
                    rsi_series.iloc[i] = 50
            rsi_divergence = check_divergence_improved(main_df, rsi_series, "RSI")
            # Создаем серию MACD histogram
            macd_hist_series = pd.Series(index=main_df.index, dtype=float)
            for i in range(len(main_df)):
                if i + 1 >= max(2, 26):  # нужно минимум 26 точек для MACD
                    _, _, hist = calculate_macd(main_df['close'].iloc[:i+1])
                    macd_hist_series.iloc[i] = hist if not pd.isna(hist) else 0
                else:
                    macd_hist_series.iloc[i] = 0
            macd_divergence = check_divergence_improved(main_df, macd_hist_series, "MACD")
        except Exception as e:
            logger.error(f"Ошибка расчета дивергенций: {e}")
    return rsi_divergence, macd_divergence

def calculate_btc_correlation(btc_context, main_df):
    """Рассчитывает корреляцию с BTC"""
    btc_correlation_data = None
    if btc_context and main_df is not None and len(main_df) > 0 and btc_context.get('close_series') is not None:
        try:
            # Выравниваем длины данных
            min_len = min(len(btc_context['close_series']), len(main_df))
            if min_len > 0: # Дополнительная проверка
                btc_close_aligned = btc_context['close_series'].tail(min_len).reset_index(drop=True)
                alt_close_aligned = main_df['close'].tail(min_len).reset_index(drop=True)
                # Расчет корреляции
                btc_correlation = calculate_correlation(btc_close_aligned, alt_close_aligned, 30)
                # Расчет дивергенции с BTC
                btc_divergence = (False, "")
                if len(btc_close_aligned) >= 20:
                    btc_divergence = check_divergence_improved(
                        pd.DataFrame({'close': btc_close_aligned, 'high': btc_close_aligned, 'low': btc_close_aligned}),
                        alt_close_aligned,
                        "BTC"
                    )
                btc_correlation_data = {
                    'correlation': btc_correlation,
                    'divergence': btc_divergence
                }
        except Exception as e:
            logger.error(f"Ошибка анализа корреляции с BTC: {e}")
    return btc_correlation_data
