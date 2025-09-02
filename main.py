# main.py
import asyncio
import pandas as pd
import time
import ccxt
import logging
import signal
import sys
from datetime import datetime
from config import (MAX_CONCURRENT_REQUESTS, ALT_COINS, BTC_USDT, BTC_TIMEFRAMES, ALT_TIMEFRAMES,
                    Colors, TELEGRAM_ENABLED, TELEGRAM_TOKEN, CHAT_ID, BTC_CACHE_TTL)
from cache import BTCCache
from data_collector import fetch_ohlcv_async, fetch_orderbook_async
from signal_analyzer import analyze_btc_data, analyze_altcoin_data, combine_signals
from notifier import send_telegram_alert_enhanced
from market_data import fetch_top_altcoins

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Глобальная переменная для контроля работы
running = True

def signal_handler(signum, frame):
    """Обработчик сигналов остановки"""
    global running
    logger.info("Получен сигнал остановки...")
    running = False

def run_triad_scan():
    """Синхронный сканер для обратной совместимости - не реализован в новой версии"""
    logger.warning("Синхронный режим не поддерживается в новой версии. Используйте асинхронный режим.")
    return None

async def run_triad_scan_async():
    """Асинхронный сканер всех монет с ограничением по семафору"""
    global running
    if not running:
        return None
        
    # Создаем семафор внутри активного event loop
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    # Инициализируем кэш BTC
    btc_cache = BTCCache(BTC_CACHE_TTL)

    # Инициализируем синхронный exchange для проверки ликвидности
    exchange_sync = ccxt.binance({
        'rateLimit': 1000,
        'enableRateLimit': True,
    })

    logger.info("✨" * 60)
    logger.info("       МУЛЬТИ-ИНДИКАТОРНЫЙ БОТ v6: ИЕРАРХИЧЕСКИЙ АНАЛИЗ")
    logger.info("       BTC/USDT -> ALT/USDT | Z | RSI | MACD | StochRSI | Volume | Volatility | R/R")
    logger.info("✨" * 60)

    try:
        logger.info("Получение списка топ альткоинов по объему...")
        alt_coins_list = await fetch_top_altcoins(limit=15, exclude_stables=True) # Получаем 15 монет
        if not alt_coins_list:
             logger.warning("Список топ альткоинов пуст. Используется список по умолчанию.")
             alt_coins_list = ALT_COINS # fallback на конфиг
    except Exception as e:
        logger.error(f"Ошибка при получении топ альткоинов, используем список по умолчанию: {e}")
        alt_coins_list = ALT_COINS # fallback на конфиг
    logger.info(f"Анализ будет проводиться для: {alt_coins_list}")

    # 1. Сначала собираем данные для BTC/USDT
    logger.info("Сбор данных для BTC/USDT...")
    btc_dfs = {}
    tasks = []
    
    for tf_name, tf in BTC_TIMEFRAMES:
        task = fetch_ohlcv_async(BTC_USDT, tf, max(20 + 50, 200 + 50), sem=semaphore)
        tasks.append((tf_name, task))

    # Выполняем все запросы параллельно с таймаутом
    try:
        task_results = await asyncio.wait_for(
            asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
            timeout=300.0
        )
    except asyncio.TimeoutError:
        logger.error("Таймаут при сборе данных BTC/USDT")
        return None
    except Exception as e:
        logger.error(f"Ошибка при сборе данных BTC/USDT: {e}")
        return None

    # Обрабатываем результаты
    successful_btc_data = 0
    for i, (tf_name, _) in enumerate(tasks):
        if not running:
            return None
            
        result = task_results[i]
        if isinstance(result, Exception):
            logger.error(f"Ошибка получения данных для BTC/USDT {tf_name}: {result}")
            continue
        if result is None or len(result) < 10:
            logger.warning(f"Недостаточно данных для BTC/USDT {tf_name}")
            continue
        btc_dfs[tf_name] = result
        successful_btc_data += 1

    if successful_btc_data == 0:
        logger.error("Недостаточно данных для анализа BTC")
        return None

    # 2. Затем анализируем BTC
    try:
        btc_signal = analyze_btc_data(btc_dfs, btc_cache)
        if not btc_signal:
            logger.error("Не удалось получить сигнал по BTC. Прерывание анализа.")
            return None
    except Exception as e:
        logger.error(f"Ошибка анализа BTC: {e}")
        return None

    logger.info(f"📊 Контекст BTC: {btc_signal.get('trend', 'N/A').upper()} | "
                f"{btc_signal.get('regime', 'N/A')} | {btc_signal.get('volatility', 'N/A')}")
    logger.info(f"  Z-score: {btc_signal.get('z', 0):.2f} | "
                f"RSI: {btc_signal.get('rsi', 50):.1f} | "
                f"MACD: {btc_signal.get('macd_hist', 0):.3f}")
    logger.info(f"  Уверенность: {btc_signal.get('confidence', 'LOW').upper()}")

    # 3. Собираем данные для всех альткоинов
    logger.info("Сбор данных для альткоинов...")
    all_results = []
    alt_tasks = []
    
    for alt_symbol in alt_coins_list:
        if not running:
            return None
        alt_tasks.append(analyze_altcoin_task(alt_symbol, ALT_TIMEFRAMES, semaphore, btc_signal, exchange_sync))

    # Выполняем все задачи анализа альткоинов параллельно
    try:
        alt_results = await asyncio.wait_for(
            asyncio.gather(*alt_tasks, return_exceptions=True),
            timeout=300.0  # 5 минут на все альткоины
        )
    except asyncio.TimeoutError:
        logger.error("Таймаут при анализе альткоинов")
        return None
    except Exception as e:
        logger.error(f"Ошибка при анализе альткоинов: {e}")
        return None

    # Обрабатываем результаты
    successful_analyses = 0
    failed_analyses = 0
    
    for i, result in enumerate(alt_results):
        if not running:
            return None
            
        if isinstance(result, Exception):
            failed_analyses += 1
            logger.error(f"Ошибка анализа {ALT_COINS[i]}: {result}")
            continue
        if result is not None:
            successful_analyses += 1
            all_results.append(result)
            
            # Отправляем уведомление в Telegram
            try:
                send_telegram_alert_enhanced(result)
            except Exception as e:
                logger.warning(f"Ошибка отправки Telegram уведомления для {ALT_COINS[i]}: {e}")
            
            # Выводим результат в консоль
            try:
                alt = result['alt']
                # Безопасное форматирование значений
                rr_val = alt.get('rr')
                target_val = alt.get('target')
                stop_val = alt.get('stop')
                rr_str = f"{rr_val:.2f}" if rr_val is not None else "N/A"
                target_str = f"{target_val:.8f}" if target_val is not None else "N/A"
                stop_str = f"{stop_val:.8f}" if stop_val is not None else "N/A"
                
                logger.info(f"\n🔍 {alt.get('symbol', 'N/A')}")
                logger.info(f"  Сигнал альткоина: {alt.get('signal', 'N/A')} ({alt.get('confidence', 'LOW').upper()})")
                logger.info(f"  Финальный сигнал: {result.get('final_signal', 'N/A')} ({result.get('confidence', 'LOW').upper()})")
                logger.info(f"  Корреляция с BTC: {result.get('correlation', 0):.2f}")
                logger.info(f"  Дивергенция: {'Да' if result.get('is_divergent', False) else 'Нет'}")
                logger.info(f"  Причина: {result.get('reason', 'N/A')}")
                logger.info(f"  R/R: {rr_str} | Цель: {target_str} | Стоп: {stop_str}")
            except Exception as e:
                logger.error(f"Ошибка форматирования результата для {ALT_COINS[i]}: {e}")

    logger.info(f"📈 Статистика: успешно {successful_analyses}, ошибок {failed_analyses}, всего {len(ALT_COINS)}")

    # Сохранение результатов
    if all_results:
        try:
            df_data = []
            for res in all_results:
                try:
                    alt = res.get('alt', {})
                    btc = res.get('btc', {})
                    
                    df_data.append({
                        'alt': alt.get('symbol', 'N/A'),
                        'final_signal': res.get('final_signal', 'N/A'),
                        'alt_signal': alt.get('signal', 'N/A'),
                        'confidence': res.get('confidence', 'low'),
                        'btc_trend': btc.get('trend', 'neutral'),
                        'btc_regime': btc.get('regime', 'N/A'),
                        'correlation': res.get('correlation', 0),
                        'is_divergent': res.get('is_divergent', False),
                        'reason': res.get('reason', 'N/A'),
                        'rr': alt.get('rr'),
                        'target': alt.get('target'),
                        'stop': alt.get('stop'),
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as e:
                    logger.error(f"Ошибка обработки результата для сохранения: {e}")
                    continue
                    
            if df_data:
                df = pd.DataFrame(df_data)
                df.to_csv('signals_hierarchical.csv', index=False)
                logger.info(f"✅ Сохранено: {len(df)} активов")
                return df
            else:
                logger.warning("❌ Нет валидных данных для сохранения")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка сохранения результатов: {e}")
            return None
    else:
        logger.info("❌ Нет данных для сохранения")
        return None

async def analyze_altcoin_task(alt_symbol, timeframes, semaphore, btc_context, exchange_sync):
    """Асинхронная задача для анализа одного альткоина"""
    if not running:
        return None
        
    alt_usdt = f"{alt_symbol}/USDT"
    logger.info(f"Начало анализа {alt_usdt}...")

    # Сбор данных OHLCV
    dfs = {}
    ohlcv_tasks = []
    
    for tf_name, tf in timeframes:
        task = fetch_ohlcv_async(alt_usdt, tf, max(20 + 50, 200 + 50), sem=semaphore)
        ohlcv_tasks.append((tf_name, task))

    try:
        ohlcv_results = await asyncio.wait_for(
            asyncio.gather(*[task for _, task in ohlcv_tasks], return_exceptions=True),
            timeout=300.0
        )
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут при получении данных OHLCV для {alt_symbol}")
        return None
    except asyncio.CancelledError:
        logger.info(f"Задача для {alt_symbol} была отменена")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении данных OHLCV для {alt_symbol}: {e}")
        return None

    # Обработка результатов
    valid_dataframes = 0
    for i, (tf_name, _) in enumerate(ohlcv_tasks):
        result = ohlcv_results[i]
        if isinstance(result, Exception):
            logger.warning(f"Ошибка получения данных для {alt_symbol} {tf_name}: {result}")
            continue
        if result is None or len(result) < 10:
            logger.warning(f"Недостаточно данных для {alt_symbol} {tf_name}")
            continue
        dfs[tf_name] = result
        valid_dataframes += 1

    if valid_dataframes == 0:
        logger.warning(f"Нет валидных данных OHLCV для {alt_symbol}")
        return None

    # Сбор данных стакана
    try:
        orderbook = await fetch_orderbook_async(alt_usdt, limit=50)
        if not orderbook:
            logger.warning(f"Не удалось получить стакан для {alt_symbol}")
            orderbook = {}
    except Exception as e:
        logger.warning(f"Ошибка при получении стакана для {alt_symbol}: {e}")
        orderbook = {}

    # Анализ данных
    try:
        alt_signal_data = analyze_altcoin_data(alt_symbol, dfs, orderbook, btc_context, exchange_sync)
        
        if alt_signal_data:
            combined_signal = combine_signals(btc_context, alt_signal_data)
            return combined_signal
        else:
            logger.info(f"Нет сигнала для {alt_symbol}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка анализа данных для {alt_symbol}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# =============== ЗАПУСК ===============
if __name__ == "__main__":
    # Регистрация обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("🚀 Запуск мульти-индикаторного бота v6 (иерархический анализ)...")
    logger.info(f"📊 Максимальное количество одновременных подключений: {MAX_CONCURRENT_REQUESTS}")
    
    # Проверка Telegram настроек
    if TELEGRAM_ENABLED and TELEGRAM_TOKEN and CHAT_ID:
        token_display = TELEGRAM_TOKEN[:10] + "..." if len(TELEGRAM_TOKEN) > 10 else "N/A"
        logger.info(f"✅ Telegram включен. Токен: {token_display} | Chat ID: {CHAT_ID}")
    else:
        logger.warning("⚠️  Telegram отключен или неправильно настроен")

    logger.info("Режим: Асинхронный (иерархический анализ)")
    
    # Основной цикл
    while running:
        try:
            logger.info(f"\n🔄 Начало анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            df = asyncio.run(run_triad_scan_async())
            
            if df is not None and len(df) > 0:
                logger.info(f"✅ Анализ завершен. Найдено {len(df)} сигналов")
            elif df is not None:
                logger.info("ℹ️  Анализ завершен. Сигналов не найдено")
            else:
                logger.info("ℹ️  Анализ прерван или не завершен")
                
        except KeyboardInterrupt:
            logger.info(f"\n{Colors.SELL}🛑 Бот остановлен пользователем.{Colors.RESET}")
            break
        except Exception as e:
            logger.error(f"🔴 Критическая ошибка в основном цикле: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
        if not running:
            break
            
        try:
            now = datetime.now().strftime("%H:%M:%S")
            logger.info(f"\n⏳ Следующее обновление через 10 минут... ({now})")
            
            # Ожидание перед следующим циклом
            for _ in range(600):  # 10 минут по 1 секунде
                if not running:
                    break
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info(f"\n{Colors.SELL}🛑 Бот остановлен во время ожидания.{Colors.RESET}")
            break
    
    logger.info(f"\n{Colors.SELL}🛑 Бот корректно завершил работу.{Colors.RESET}")