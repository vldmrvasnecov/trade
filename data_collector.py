# data_collector.py
import ccxt.async_support as ccxt_async
import pandas as pd
import asyncio
import logging
from config import MAX_RETRY_ATTEMPTS, RETRY_DELAY, ORDERBOOK_DEPTH

logger = logging.getLogger(__name__)

async def fetch_ohlcv_async(symbol, timeframe, limit=1000, sem=None):
    """Асинхронная функция получения OHLCV данных с ограничением по семафору и повторными попытками"""
    semaphore_to_use = sem if sem is not None else asyncio.Semaphore(100) # Fallback
    async with semaphore_to_use:  # Ограничиваем количество одновременных запросов
        exchange_async = None
        for attempt in range(MAX_RETRY_ATTEMPTS + 1):
            try:
                # Используем ccxt.async_support для асинхронных запросов
                exchange_async = ccxt_async.binance({
                    'rateLimit': 1000,
                    'enableRateLimit': True,
                })
                data = await exchange_async.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                if data is None or len(data) == 0:
                    return None
                df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                # Проверяем, что все значения существуют
                if df[['open', 'high', 'low', 'close', 'volume']].isnull().any().any():
                    return None
                return df
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ Таймаут при получении OHLCV для {symbol} {timeframe} (попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS + 1})")
                if attempt < MAX_RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"❌ Таймаут при получении OHLCV для {symbol} {timeframe} после {MAX_RETRY_ATTEMPTS + 1} попыток")
                    return None
            except Exception as e:
                logger.error(f"❌ {symbol} {timeframe}: {e} (попытка {attempt + 1}/{MAX_RETRY_ATTEMPTS + 1})")
                if attempt < MAX_RETRY_ATTEMPTS:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"❌ Ошибка при получении OHLCV для {symbol} {timeframe} после {MAX_RETRY_ATTEMPTS + 1} попыток")
                    return None
            finally:
                # ВАЖНО: Закрываем соединение в блоке finally
                if exchange_async:
                    await exchange_async.close()
    return None # Достигнут максимальный лимит попыток

async def fetch_orderbook_async(symbol, limit=ORDERBOOK_DEPTH):
    """Асинхронная функция получения стакана"""
    exchange_async = None
    try:
        exchange_async = ccxt_async.binance({
            'rateLimit': 1000,
            'enableRateLimit': True,
        })
        orderbook = await asyncio.wait_for(
            exchange_async.fetch_order_book(symbol, limit=limit),
            timeout=15.0 # Отдельный таймаут для стакана
        )
        return orderbook
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут при получении стакана для {symbol}")
        return None
    except Exception as e:
        if "Invalid symbol" in str(e) or "Market not found" in str(e) or "does not have market symbol" in str(e):
            logger.info(f"ℹ️  Пара {symbol} не найдена на бирже или недоступна.")
            return None # Прекращаем анализ этой монеты
        else:
            logger.warning(f"⚠️ Ошибка получения стакана для {symbol}: {e}")
        return None
    finally:
        if exchange_async:
            await exchange_async.close()

# Синхронная функция из оригинального скрипта, если она нужна
def fetch_ohlcv_sync(exchange, symbol, timeframe, limit=250):
    """Синхронная функция получения OHLCV данных"""
    try:
        data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if data is None or len(data) == 0:
            return None
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        # Проверяем, что все значения существуют
        if df[['open', 'high', 'low', 'close', 'volume']].isnull().any().any():
            return None
        return df
    except Exception as e:
        logger.error(f"❌ {symbol} {timeframe}: {e}")
        return None
