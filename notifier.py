# notifier.py
import requests
import logging
from config import TELEGRAM_TOKEN, CHAT_ID, TELEGRAM_ENABLED

logger = logging.getLogger(__name__)

def send_telegram_message(text):
    """Отправка сообщения через Telegram API"""
    if not TELEGRAM_ENABLED:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            'chat_id': CHAT_ID,
            'text': text,
            'parse_mode': 'Markdown'
        }
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            logger.info("✅ Сообщение отправлено в Telegram")
            return True
        else:
            logger.error(f"❌ Ошибка отправки в Telegram: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка Telegram: {e}")
        return False

def send_telegram_alert_enhanced(combined_signal):
    if not TELEGRAM_ENABLED:
        return
    # Отправляем уведомления для сигналов с высокой и средней уверенностью
    if combined_signal['confidence'] in ['high', 'medium']:
        btc = combined_signal['btc']
        alt = combined_signal['alt']
        # Безопасное форматирование значений, которые могут быть None
        btc_z = btc.get('z', 0) if btc else 0
        btc_rsi = btc.get('rsi', 50) if btc else 50
        btc_macd_hist = btc.get('macd_hist', 0) if btc else 0
        btc_trend = btc.get('trend', 'neutral').upper() if btc else 'N/A'
        btc_regime = btc.get('regime', 'N/A') if btc else 'N/A'
        btc_volatility = btc.get('volatility', 'N/A') if btc else 'N/A'
        alt_price = alt.get('price', 0) if alt else 0
        alt_rr = alt.get('rr') # Может быть None
        alt_target = alt.get('target') # Может быть None
        alt_stop = alt.get('stop') # Может быть None
        alt_signal_name = alt.get('signal', 'N/A') if alt else 'N/A'
        alt_trend = alt.get('trend', 'neutral').upper() if alt else 'N/A'
        alt_regime = alt.get('regime', 'N/A') if alt else 'N/A'
        correlation = combined_signal.get('correlation', 0) if combined_signal else 0
        is_divergent = combined_signal.get('is_divergent', False) if combined_signal else False
        reason = combined_signal.get('reason', 'N/A') if combined_signal else 'N/A'
        # Безопасное форматирование для Telegram сообщения
        rr_str_tg = f"{alt_rr:.2f}" if alt_rr is not None else "N/A"
        target_str_tg = f"{alt_target:.8f}" if alt_target is not None else "N/A"
        stop_str_tg = f"{alt_stop:.8f}" if alt_stop is not None else "N/A"
        # Формирование сообщения
        msg = f"""
🔔 `{combined_signal['final_signal'] if combined_signal else 'N/A'}`
*Монета:* `{alt['symbol'] if alt and alt.get('symbol') else 'N/A'}`
*Цена:* `{alt_price:.8f}`
📊 *Контекст BTC:*
  • Тренд: {btc_trend}
  • Режим: {btc_regime}
  • Волатильность: {btc_volatility}
  • Z-score: {btc_z:.2f}
  • RSI: {btc_rsi:.1f}
  • MACD: {btc_macd_hist:.3f}
📈 *Анализ альткоина:*
  • Сигнал: {alt_signal_name}
  • Тренд: {alt_trend}
  • Режим: {alt_regime}
  • R/R: `{rr_str_tg}`
🔗 *Связь с BTC:*
  • Корреляция: `{correlation:.2f}`
  • Дивергенция: {'✅ Да' if is_divergent else '❌ Нет'}
💡 *Причина:* {reason}
🎯 *Цель:* `{target_str_tg}`  
🛑 *Стоп:* `{stop_str_tg}`
        """
        send_telegram_message(msg)
    # --- Изменение 3: Отдельное уведомление для ACCUMULATION ---
    elif combined_signal['alt']['signal'] == 'ACCUMULATION' and TELEGRAM_ENABLED:
        alt = combined_signal['alt']
        alt_price = alt.get('price', 0) if alt else 0
        alt_rr = alt.get('rr') # Может быть None
        rr_str_tg = f"{alt_rr:.2f}" if alt_rr is not None else "N/A"
        msg = f"""
🔍 *Монета в фазе накопления* 
*Монета:* `{alt['symbol'] if alt and alt.get('symbol') else 'N/A'}`
*Цена:* `{alt_price:.8f}`
📈 *Сигнал:* ACCUMULATION
📊 *R/R:* `{rr_str_tg}`
💡 *Возможное накопление крупных игроков. Следите за подтверждением.*
        """
        send_telegram_message(msg)
