# signal_analyzer.py
import numpy as np
import pandas as pd
import logging
from config import Z_WINDOW, RSI_PERIOD, ATR_PERIOD, EMA_SHORT, EMA_LONG, BTC_CACHE_TTL
from feature_calculator import (calculate_z_score, calculate_rsi, calculate_macd, calculate_stoch_rsi,
                                calculate_atr_level, calculate_ema, check_divergence_improved,
                                calculate_correlation, get_volatility_regime, get_market_regime,
                                calculate_indicators_for_timeframe, calculate_consolidated_indicators,
                                calculate_trend, calculate_divergences, calculate_btc_correlation,
                                safe_float, get_consensus_signal)
from utils import analyze_orderbook_density, check_liquidity

logger = logging.getLogger(__name__)

# --- Функции анализа BTC ---
def analyze_btc_data(dfs, btc_cache_instance):
    """Анализ BTC/USDT — формирует рыночный контекст с мультифреймовым анализом"""
    try:
        # Проверяем кэш
        cached_data = btc_cache_instance.get()
        if cached_data is not None:
            logger.info("BTC анализ: Использован кэш")
            return cached_data

        logger.info("Анализ BTC/USDT...")

        # Анализируем каждый таймфрейм
        btc_results = []
        for tf_name, df in dfs.items():
            try:
                result = calculate_indicators_for_timeframe(df, tf_name)
                if result:
                    btc_results.append(result)
            except Exception as e:
                logger.warning(f"Ошибка анализа таймфрейма {tf_name} для BTC: {e}")
                continue

        if len(btc_results) == 0:
            logger.warning("Недостаточно данных для анализа BTC")
            return None

        # Консолидация индикаторов
        consensus, _, btc_indicators = calculate_consolidated_indicators(btc_results)

        # === КОНСЕНСУС ПО ИНДИКАТОРАМ BTC ===
        btc_consensus = {}
        logger.debug(f"Индикаторы BTC для консенсуса: {btc_indicators}")
        
        # Z-score: buy если Z < -1.5 (перепродан), sell если > 1.5
        z_vals = {tf: 1 if v < -1.5 else -1 if v > 1.5 else 0 for tf, v in btc_indicators['z'].items() if v is not None}
        btc_consensus['z'], z_count = get_consensus_signal(z_vals, threshold=2, signal_type='binary')
        # RSI
        btc_consensus['rsi'], rsi_count = get_consensus_signal(btc_indicators['rsi'], threshold=2, signal_type='ternary')
        # MACD (по гистограмме)
        macd_vals = {tf: 1 if v > 0 else -1 if v < 0 else 0 for tf, v in btc_indicators['macd'].items() if v is not None}
        btc_consensus['macd'], macd_count = get_consensus_signal(macd_vals, threshold=2, signal_type='binary')
        # Stoch RSI
        btc_consensus['stoch'], stoch_count = get_consensus_signal(btc_indicators['stoch_rsi'], threshold=2, signal_type='ternary')
        # Volume Z: сила подтверждения
        vol_vals = {tf: 1 if v > 0.5 else -1 if v < -0.5 else 0 for tf, v in btc_indicators['volume_z'].items() if v is not None}
        btc_consensus['volume'], vol_count = get_consensus_signal(vol_vals, threshold=2, signal_type='binary')
        
        logger.debug(f"BTC консенсус структура: {btc_consensus}")

        # === АНАЛИЗ ТРЕНДА BTC (EMA 50/200) ===
        btc_trend_direction, main_btc_df = calculate_trend(dfs, ['4h', '1h', '15m'])

        # === ПРОВЕРКА ДИВЕРГЕНЦИЙ BTC ===
        btc_rsi_divergence = (False, "No divergence")
        btc_macd_divergence = (False, "No divergence")
        
        if main_btc_df is not None and len(main_btc_df) > RSI_PERIOD:
            try:
                btc_rsi_divergence, btc_macd_divergence = calculate_divergences(main_btc_df, RSI_PERIOD)
            except Exception as e:
                logger.warning(f"Ошибка анализа дивергенций BTC: {e}")

        # === ФОРМИРОВАНИЕ СИГНАЛА BTC ===
        # Данные с 4h (или другого основного таймфрейма)
        main_result = None
        for result in btc_results:
            if result['tf'] == '4h':
                main_result = result
                break
        # Если 4h нет, берем последний
        if main_result is None and len(btc_results) > 0:
            main_result = btc_results[-1]
        # Если все еще нет данных, пропускаем
        if main_result is None:
            logger.warning("Не удалось получить основные данные для анализа BTC")
            return None

        z_4h = safe_float(main_result.get('z', 0))
        rsi_4h = safe_float(main_result.get('rsi', 50))
        vol_z_4h = safe_float(main_result.get('vol_z', 0))
        price = safe_float(main_result.get('price', 0))
        atr_pct = safe_float(main_result.get('atr_pct', 0))
        macd_hist = safe_float(main_result.get('macd_hist', 0))

        # Режим рынка с учетом тренда
        regime, volatility = get_market_regime(z_4h, rsi_4h, atr_pct, vol_z_4h, btc_trend_direction)

        # Уверенность (на основе силы сигналов)
        confidence_score = abs(z_4h) + (1 if abs(rsi_4h - 50) > 20 else 0) + (0.5 if abs(macd_hist) > 0.001 else 0)
        confidence = "high" if confidence_score > 3 else "medium" if confidence_score > 1.5 else "low"

        # Скор (простая сумма индикаторов)
        score = 0
        if z_4h < -2.0:
            score += 2
        elif z_4h > 2.0:
            score -= 2
        if rsi_4h < 30:
            score += 2
        elif rsi_4h > 70:
            score -= 2
        if macd_hist > 0:
            score += 1
        elif macd_hist < 0:
            score -= 1
        
        # Безопасный доступ к индикатору
        stoch_signal_4h = btc_indicators['stoch_rsi'].get('4h') if 'stoch_rsi' in btc_indicators and '4h' in btc_indicators['stoch_rsi'] else None
        if stoch_signal_4h == 'buy':
            score += 1
        elif stoch_signal_4h == 'sell':
            score -= 1
        if vol_z_4h > 1.0:
            score += 1
        elif vol_z_4h < -1.0:
            score -= 1
        if btc_trend_direction == 'up':
            score += 2
        elif btc_trend_direction == 'down':
            score -= 2
        if btc_rsi_divergence[0] and 'Bullish' in btc_rsi_divergence[1]:
            score += 2
        elif btc_rsi_divergence[0] and 'Bearish' in btc_rsi_divergence[1]:
            score -= 2
        if btc_macd_divergence[0] and 'Bullish' in btc_macd_divergence[1]:
            score += 2
        elif btc_macd_divergence[0] and 'Bearish' in btc_macd_divergence[1]:
            score -= 2

        logger.info(f"BTC анализ завершен. Сигнал: {score}, Уверенность: {confidence}")

        # Безопасный доступ к close_series
        close_series = main_btc_df['close'] if main_btc_df is not None and 'close' in main_btc_df else None
        
        result_data = {
            'symbol': 'BTC/USDT',
            'price': price,
            'z': z_4h,
            'rsi': rsi_4h,
            'macd_hist': macd_hist,
            'stoch_k': safe_float(main_result.get('stoch_k', 0)),
            'vol_z': vol_z_4h,
            'atr_pct': atr_pct,
            'trend': btc_trend_direction,
            'regime': regime,
            'volatility': volatility,
            'rsi_divergence': btc_rsi_divergence,
            'macd_divergence': btc_macd_divergence,
            'confidence': confidence,
            'score': score,
            'close_series': close_series,
            'frames': btc_results,
            'consensus': btc_consensus,
            'timestamp': pd.Timestamp.now()
        }

        # Сохраняем в кэш
        btc_cache_instance.set(result_data)
        return result_data
        
    except Exception as e:
        logger.error(f"Критическая ошибка анализа BTC: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# --- Функции анализа альткоинов ---
def analyze_altcoin_data(alt_symbol, dfs, orderbook, btc_context, exchange_sync):
    """Анализ одного альткоина на основе собранных данных"""
    alt_usdt = f"{alt_symbol}/USDT"
    logger.info(f"Начало анализа {alt_usdt}...")

    try:
        results = []
        # Анализируем каждый таймфрейм
        for tf_name, df in dfs.items():
            try:
                result = calculate_indicators_for_timeframe(df, tf_name)
                if result:
                    results.append(result)
            except Exception as e:
                logger.warning(f"Ошибка анализа таймфрейма {tf_name} для {alt_symbol}: {e}")
                continue

        if len(results) == 0:
            logger.warning(f"Нет результатов анализа индикаторов для {alt_symbol}")
            return None

        logger.debug(f"Индикаторы рассчитаны для {alt_usdt}")

        # Консолидация индикаторов
        consensus, confidences, indicators = calculate_consolidated_indicators(results)

        # === АНАЛИЗ ТРЕНДА (EMA 50/200) ===
        trend_direction, main_df = calculate_trend(dfs, ['4h', '1h', '15m'])
        logger.debug(f"Тренд для {alt_usdt}: {trend_direction}")

        # === ПРОВЕРКА ДИВЕРГЕНЦИЙ ===
        rsi_divergence = (False, "No divergence")
        macd_divergence = (False, "No divergence")
        
        if main_df is not None and len(main_df) > RSI_PERIOD:
            try:
                rsi_divergence, macd_divergence = calculate_divergences(main_df, RSI_PERIOD)
            except Exception as e:
                logger.warning(f"Ошибка анализа дивергенций для {alt_symbol}: {e}")

        # === АНАЛИЗ КОРРЕЛЯЦИИ С BTC ===
        btc_correlation_data = calculate_btc_correlation(btc_context, main_df)
        logger.debug(f"Корреляция с BTC для {alt_usdt}: {btc_correlation_data}")

        # === ПРОВЕРКА ЛИКВИДНОСТИ (УЛУЧШЕННАЯ) ===
        is_liquid = True
        liquidity_info = {'base_info': 'OK', 'orderbook_analysis': None}
        try:
            is_liquid, liquidity_info = check_liquidity(exchange_sync, alt_usdt)
        except Exception as e:
            logger.warning(f"Ошибка проверки ликвидности для {alt_symbol}: {e}")
            
        orderbook_analysis_from_liquidity = liquidity_info.get('orderbook_analysis')
        liquidity_base_info = liquidity_info.get('base_info', 'OK')
        
        if not is_liquid:
            logger.warning(f"  ⚠️  Низкая ликвидность {alt_symbol}: {liquidity_base_info}")

        # Используем orderbook_analysis_from_liquidity
        final_orderbook_analysis = orderbook_analysis_from_liquidity

        # === ГЕНЕРАЦИЯ СИГНАЛА ДЛЯ АЛЬТКОИНА ===
        # Данные с 4h (или другим основным таймфреймом)
        latest = None
        for result in results:
            if result['tf'] == '4h':
                latest = result
                break
        # Если 4h нет, берем последний
        if latest is None and len(results) > 0:
            latest = results[-1]
        # Если все еще нет данных, пропускаем
        if latest is None:
            logger.warning(f"Не удалось получить данные для генерации сигнала {alt_symbol}")
            return None

        z_4h = safe_float(latest.get('z', 0))
        rsi_4h = safe_float(latest.get('rsi', 50))
        vol_z_4h = safe_float(latest.get('vol_z', 0))
        price = safe_float(latest.get('price', 0))
        atr_pct = safe_float(latest.get('atr_pct', 0))
        macd_hist = safe_float(latest.get('macd_hist', 0))
        
        # Режим рынка с учетом тренда
        regime, volatility = get_market_regime(z_4h, rsi_4h, atr_pct, vol_z_4h, trend_direction)
        # Оценка движения к среднему
        mean_reversion_pct = abs(z_4h * atr_pct) if z_4h != 0 else 0
        risk_pct = atr_pct * 1.5  # стоп на 1.5 ATR
        rr = mean_reversion_pct / risk_pct if risk_pct > 0 else 0

        # === ВЗВЕШЕННАЯ СИСТЕМА ОЧКОВ ДЛЯ АЛЬТКОИНА (С УЛУЧШЕНИЯМИ) ===
        score = 0
        reasons = []
        # Z-score (вес 2)
        if z_4h < -2.0:
            score += 2
            reasons.append(f"Z-score перепродан ({z_4h:.2f})")
        elif z_4h > 2.0:
            score -= 2
            reasons.append(f"Z-score перекуплен ({z_4h:.2f})")
        # RSI (вес 2)
        if rsi_4h < 30:
            score += 2
            reasons.append(f"RSI перепродан ({rsi_4h:.1f})")
        elif rsi_4h > 70:
            score -= 2
            reasons.append(f"RSI перекуплен ({rsi_4h:.1f})")
        # MACD (вес 1)
        if macd_hist > 0:
            score += 1
            reasons.append("MACD гистограмма положительная")
        elif macd_hist < 0:
            score -= 1
            reasons.append("MACD гистограмма отрицательная")
        # Stochastic RSI (вес 1)
        stoch_k = safe_float(latest.get('stoch_k', 0))
        if stoch_k < 20:
            score += 1
            reasons.append(f"StochRSI перепродан ({stoch_k:.1f})")
        elif stoch_k > 80:
            score -= 1
            reasons.append(f"StochRSI перекуплен ({stoch_k:.1f})")
        # Объем (вес 1)
        if vol_z_4h > 1.0:
            score += 1
            reasons.append(f"Высокий объем (Z={vol_z_4h:.1f})")
        elif vol_z_4h < -1.0:
            score -= 1
            reasons.append(f"Низкий объем (Z={vol_z_4h:.1f})")
        # Тренд (вес 2)
        if trend_direction == 'up':
            score += 2
            reasons.append("Восходящий тренд")
        elif trend_direction == 'down':
            score -= 2
            reasons.append("Нисходящий тренд")
        
        # Дивергенции (вес 3-4)
        div_weight = 4 if (btc_context and abs(safe_float(btc_context.get('score', 0))) <= 2) else 3
        if rsi_divergence[0] and 'Bullish' in str(rsi_divergence[1]):
            score += div_weight
            reasons.append(str(rsi_divergence[1]))
        elif rsi_divergence[0] and 'Bearish' in str(rsi_divergence[1]):
            score -= div_weight
            reasons.append(str(rsi_divergence[1]))
        if macd_divergence[0] and 'Bullish' in str(macd_divergence[1]):
            score += div_weight
            reasons.append(str(macd_divergence[1]))
        elif macd_divergence[0] and 'Bearish' in str(macd_divergence[1]):
            score -= div_weight
            reasons.append(str(macd_divergence[1]))
            
        # Корреляция с BTC (вес 1)
        if btc_correlation_data and 'correlation' in btc_correlation_data:
            btc_corr = safe_float(btc_correlation_data['correlation'])
            if btc_corr > 0.8:
                score += 1
                reasons.append(f"Высокая корреляция с BTC ({btc_corr:.2f})")
            elif btc_corr < -0.5:
                score -= 1
                reasons.append(f"Отрицательная корреляция с BTC ({btc_corr:.2f})")
        # Дивергенции с BTC (вес 2)
        if btc_correlation_data and 'divergence' in btc_correlation_data and btc_correlation_data['divergence'][0]:
            score += 2
            reasons.append(f"Дивергенция с BTC: {btc_correlation_data['divergence'][1]}")
        
        # Интеграция анализа стакана в оценку силы сигнала
        ob_analysis_to_use = final_orderbook_analysis if final_orderbook_analysis else None
        
        if ob_analysis_to_use and 'density_score' in ob_analysis_to_use:
            density_score = safe_float(ob_analysis_to_use.get('density_score', 0))
            current_price = safe_float(ob_analysis_to_use.get('current_price', price))
            nearest_bid = safe_float(ob_analysis_to_use.get('nearest_bid', 0))
            nearest_ask = safe_float(ob_analysis_to_use.get('nearest_ask', 0))
            price_impact = safe_float(ob_analysis_to_use.get('price_impact', 0))
            spread_pct = safe_float(ob_analysis_to_use.get('spread_pct', 0))
            
            # Проверка плотности рядом с ценой
            if density_score > 0:  
                if nearest_bid and abs(current_price - nearest_bid) / current_price < 0.005:
                    score += 2
                    reasons.append(f"Поддержка на уровне {nearest_bid:.8f}")
                elif nearest_ask and abs(nearest_ask - current_price) / current_price < 0.005:
                    score -= 2
                    reasons.append(f"Сопротивление на уровне {nearest_ask:.8f}")
                elif density_score > 100:
                    score += 1
                    reasons.append("Высокая плотность стакана")
            
        # Модификация уверенности на основе ликвидности
        base_confidence = "low"
        if score >= 6 or score <= -6:
            base_confidence = "high"
        elif score >= 4 or score <= -4:
            base_confidence = "medium"
        
        # Модифицируем уверенность на основе ликвидности и стакана
        final_confidence = base_confidence
        if not is_liquid:
             if final_confidence == "high":
                 final_confidence = "medium"
             elif final_confidence == "medium":
                 final_confidence = "low"
        
        if ob_analysis_to_use:
            price_impact = safe_float(ob_analysis_to_use.get('price_impact', 0))
            spread_pct = safe_float(ob_analysis_to_use.get('spread_pct', 0))
            if price_impact > 1.0 or spread_pct > 0.5:
                if final_confidence == "high":
                    final_confidence = "medium"
                elif final_confidence == "medium":
                    final_confidence = "low"

        # ГЕНЕРАЦИЯ СИГНАЛА НА ОСНОВЕ ОЧКОВ
        signal = "HOLD"
        reason = "Нет подтверждённого сигнала"
        color = "\033[93m"
        confidence = "low"
        entry = None
        stop = None
        target = None
        
        # Модификация R/R на основе ликвидности
        adjusted_rr = rr
        if not is_liquid or (ob_analysis_to_use and (safe_float(ob_analysis_to_use.get('price_impact', 0)) > 1.0 or safe_float(ob_analysis_to_use.get('spread_pct', 0)) > 0.5)):
            adjusted_rr *= 0.8

        # Определяем сигнал на основе очков
        if score >= 6:  # Сильный лонг
            signal = "STRONG LONG"
            reason = f"🔥 Сильный сигнал на покупку (очки: {score}) | " + ", ".join(reasons[-3:])
            color = "\033[1;92m"
            confidence = final_confidence
            target = price * (1 + mean_reversion_pct / 100)
            stop = price * (1 - risk_pct / 100)
            entry = "Лимитный ордер около текущей цены"
        elif score <= -6:  # Сильный шорт
            signal = "STRONG SHORT"
            reason = f"🧨 Сильный сигнал на продажу (очки: {score}) | " + ", ".join(reasons[-3:])
            color = "\033[1;91m"
            confidence = final_confidence
            target = price * (1 - mean_reversion_pct / 100)
            stop = price * (1 + risk_pct / 100)
            entry = "Лимитный ордер на откате"
        elif score >= 4:  # Слабый лонг
            signal = "WEAK LONG"
            reason = f"🟢 Слабый сигнал на покупку (очки: {score}) | " + ", ".join(reasons[-2:])
            color = "\033[96m"
            confidence = final_confidence
            target = price * (1 + mean_reversion_pct / 200)
            stop = price * (1 - risk_pct / 200)
            entry = "Вход по подтверждению"
        elif score <= -4:  # Слабый шорт
            signal = "WEAK SHORT"
            reason = f"🔴 Слабый сигнал на продажу (очки: {score}) | " + ", ".join(reasons[-2:])
            color = "\033[93m"
            confidence = final_confidence
            target = price * (1 - mean_reversion_pct / 200)
            stop = price * (1 + risk_pct / 200)
            entry = "Вход по подтверждению"

        alt_signal_data = {
            'symbol': alt_usdt,
            'signal': signal,
            'reason': reason,
            'color': color,
            'confidence': confidence,
            'regime': regime,
            'volatility': volatility,
            'rr': round(adjusted_rr, 2) if not np.isnan(adjusted_rr) and adjusted_rr is not None else 0,
            'entry': entry,
            'stop': stop,
            'target': target,
            'price': price,
            'trend': trend_direction,
            'score': score,
            'close_series': main_df['close'] if main_df is not None and 'close' in main_df else None,
            'additional_factors': {
                'btc_correlation': btc_correlation_data,
                'orderbook_analysis': ob_analysis_to_use,
                'liquidity_info': liquidity_info
            }
        }
        logger.info(f"Анализ {alt_symbol} завершен. Сигнал: {signal}, Уверенность: {confidence}")
        return alt_signal_data

    except Exception as e:
        logger.error(f"Критическая ошибка анализа {alt_symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


# --- Функции комбинирования сигналов ---
def combine_signals(btc_signal, alt_signal):
    """
    Объединяет сигналы с приоритетом BTC, но с учётом дивергенции и режима Alt Season
    """
    if not btc_signal or not alt_signal:
        return None
    
    try:
        # Динамический вес BTC
        base_weight = 0.7
        btc_regime = btc_signal.get('regime', '')
        btc_trend = btc_signal.get('trend', '')
        if btc_regime == 'range' or btc_trend == 'neutral':
            base_weight = 0.5

        # Анализируем btc_consensus для получения дополнительной уверенности
        btc_consensus = btc_signal.get('consensus', {})
        consensus_strength = 0
        consensus_indicators_count = 0
        
        # Правильная обработка возвращаемых значений из get_consensus_signal
        for indicator, consensus_result in btc_consensus.items():
            if isinstance(consensus_result, tuple) and len(consensus_result) >= 2:
                consensus_signal, count = consensus_result
                consensus_indicators_count += 1
                if consensus_signal == 'BUY':
                    consensus_strength += 1
                elif consensus_signal == 'SELL':
                    consensus_strength -= 1

        # Нормализуем силу консенсуса
        normalized_consensus_strength = consensus_strength / consensus_indicators_count if consensus_indicators_count > 0 else 0
        
        # Комбинируем оригинальную уверенность и уверенность из консенсуса
        btc_confidence_base = btc_signal.get('confidence', 'low')
        consensus_weight = 0.3
        
        combined_btc_confidence_score = 0
        if btc_confidence_base == 'high':
            combined_btc_confidence_score = 1.0
        elif btc_confidence_base == 'medium':
            combined_btc_confidence_score = 0.5
        else:
            combined_btc_confidence_score = 0.25
            
        # Учитываем консенсус
        combined_btc_confidence_score = combined_btc_confidence_score * (1 - consensus_weight) + \
                                     ((normalized_consensus_strength + 1) / 2) * consensus_weight
        
        # Определяем финальную уверенность BTC
        final_btc_confidence = "low"
        if combined_btc_confidence_score > 0.75:
            final_btc_confidence = "high"
        elif combined_btc_confidence_score > 0.4:
            final_btc_confidence = "medium"
        
        # btc_confidence_factor теперь основан на улучшенной уверенности
        btc_confidence_factor = 0.5 if final_btc_confidence == 'low' else 1.0
        btc_weight = base_weight * btc_confidence_factor
        alt_weight = 1 - btc_weight
        
        # Корреляция между ALT и BTC
        corr = 0
        try:
            corr = calculate_correlation(alt_signal.get('close_series'), btc_signal.get('close_series'), 30)
        except Exception as e:
            logger.warning(f"Ошибка расчета корреляции: {e}")
            
        is_divergent = abs(corr) < 0.5  # слабая корреляция = дивергенция
        
        # Режим Alt Season
        if is_divergent:
            btc_weight = 0.3
            alt_weight = 0.7

        # Условие дивергенции
        btc_score = safe_float(btc_signal.get('score', 0))
        alt_score = safe_float(alt_signal.get('score', 0))
        
        # Улучшенная логика дивергенции
        if is_divergent and alt_score > 4 and (abs(btc_score) <= 4 or final_btc_confidence != 'high'): 
            final_score = alt_score * 1.3
            reason = f"Дивергенция с BTC: {alt_signal.get('signal')} (корреляция {corr:.2f})"
        else:
            final_score = (btc_score * btc_weight) + (alt_score * alt_weight)
            reason = f"Контекст BTC ({btc_weight:.2f}, ув. {final_btc_confidence}) + сигнал альткоина ({alt_weight:.2f})" 

        # Проверка R/R
        alt_rr = safe_float(alt_signal.get('rr', 0))
        potential_long_signal = final_score > 5  # УЖЕСТРОЧИЛИ
        potential_short_signal = final_score < -5  # УЖЕСТРОЧИЛИ
        
        # R/R как фильтр и усилитель - БОЛЕЕ КОНСЕРВАТИВНО
        if alt_rr is not None and alt_rr < 0.5:  # ПОВЫСИЛИ ПОРОГ
            final_score = min(final_score, 0) if final_score > 0 else max(final_score, 0)
            reason += " | R/R < 0.5 (подавлен)"
        elif alt_rr is not None and alt_rr > 1.5:  # ПОВЫСИЛИ ПОРОГ
            final_score *= 1.1  # УМЕНЬШИЛИ УСИЛЕНИЕ
            reason += " | R/R > 1.5 (усилен)"
        
        # Генерация финального сигнала - БОЛЕЕ КОНСЕРВАТИВНАЯ
        signal = "HOLD"
        color = "\033[93m"
        confidence = "low"
        
        # УЖЕСТРОЧЕННЫЕ условия генерации сигналов
        if potential_long_signal and alt_rr is not None and alt_rr >= 0.8:  # ПОВЫСИЛИ ПОРОГ
            signal = "STRONG_LONG" if alt_rr >= 1.2 else "WEAK_LONG"  # ПОВЫСИЛИ ПОРОГИ
            color = "\033[1;92m" if signal == "STRONG_LONG" else "\033[96m"
            confidence = "high" if final_score > 7 else "medium"  # УЖЕСТРОЧИЛИ
        elif potential_short_signal and alt_rr is not None and alt_rr >= 0.8:  # ПОВЫСИЛИ ПОРОГ
            signal = "STRONG_SHORT" if alt_rr >= 1.2 else "WEAK_SHORT"  # ПОВЫСИЛИ ПОРОГИ
            color = "\033[1;91m" if signal == "STRONG_SHORT" else "\033[93m"
            confidence = "high" if final_score < -7 else "medium"  # УЖЕСТРОЧИЛИ
        elif abs(final_score) > 3 and alt_rr is not None and alt_rr >= 1.0:  # ПОВЫСИЛИ ПОРОГИ
            # Только сильные слабые сигналы
            if final_score > 0:
                signal = "WEAK_LONG"
                color = "\033[96m"
            else:
                signal = "WEAK_SHORT"
                color = "\033[93m"
            confidence = "medium"
        else:
            signal = "HOLD"
            color = "\033[93m"
            confidence = "low"
            
        return {
            'final_signal': signal,
            'reason': reason,
            'color': color,
            'confidence': confidence,
            'score': final_score,
            'btc': btc_signal,
            'alt': alt_signal,
            'correlation': corr if corr is not None else 0,
            'is_divergent': is_divergent,
            'btc_weight': btc_weight,
            'alt_weight': alt_weight,
            'btc_confidence_used': final_btc_confidence
        }
        
    except Exception as e:
        logger.error(f"Ошибка комбинирования сигналов: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None