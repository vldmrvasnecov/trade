# market_data.py
import ccxt.async_support as ccxt_async
import asyncio
import logging
from typing import List

logger = logging.getLogger(__name__)

# Список стейблкоинов для исключения (можно расширить)
STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'DAI', 'TUSD', 'USDP', 'USDD', 'FRAX','FDUSD', 'LUSD', 'GUSD',
    'USTC', 'ALUSD', 'MIM', 'USDN', 'FEI', 'HUSD', 'UST', 'RSV', 'VAI', 'TOR',
    'EURT', 'USDX', 'SUSD', 'DOLA', 'USDK', 'XSGD', 'USDP', 'USX', 'USDB', 'USD+',
    'USDT.E', 'USDC.E' # Часто встречающиеся названия на Binance
}

# --- ИЗМЕНЕНО ---
async def fetch_top_altcoins(exchange_id: str = 'binance', quote_currency: str = 'USDT', limit: int = 15, exclude_stables: bool = True) -> List[str]:
    """
    Асинхронно получает список топ N альткоинов по объему торгов за 24ч.

    Args:
        exchange_id: ID биржи (по умолчанию 'binance').
        quote_currency: Валюта котировки (по умолчанию 'USDT').
        limit: Количество топ монет для возврата.
        exclude_stables: Исключать ли стейблкоины.

    Returns:
        Список символов монет (например, ['BTC', 'ETH', 'SOL']).
    """
    exchange = None
    try:
        # Инициализируем асинхронный клиент биржи напрямую
        # Проверяем ID биржи и создаем соответствующий экземпляр
        if exchange_id.lower() == 'binance':
            exchange = ccxt_async.binance({
                'enableRateLimit': True, # Включаем ограничение скорости
                'timeout': 20000,  # Увеличиваем таймаут до 20 секунд (значение в миллисекундах)
                # 'rateLimit': 1000, # Можно указать, если нужно, но enableRateLimit=True обычно достаточно
                'options': {
                     'adjustForTimeDifference': True # Автоматическая коррекция времени
                }
            })
        else:
            # Для других бирж можно добавить аналогичные конструкции
            logger.error(f"Биржа {exchange_id} пока не поддерживается в fetch_top_altcoins")
            return []
        # --- ИЗМЕНЕНО ---

        # --- ДОБАВЛЕНО: Проверка на None ---
        if exchange is None:
             logger.error("Не удалось создать экземпляр exchange")
             return []
        # --- ДОБАВЛЕНО ---

        # Загружаем рынки
        markets = await exchange.load_markets()

        # --- ДОБАВЛЕНО: Проверка результата load_markets ---
        if markets is None:
            logger.error("Не удалось загрузить список рынков: load_markets вернул None")
            return []
        if not isinstance(markets, dict):
             logger.error(f"load_markets вернул неожиданный тип: {type(markets)}")
             return []
        # --- ДОБАВЛЕНО ---

        # Фильтруем рынки по котируемой валюте (например, USDT)
        filtered_markets = {
            symbol: market for symbol, market in markets.items()
            if market.get('quote') == quote_currency and # Используем .get() для безопасности
               market.get('active', False) == True and # Только активные рынки
               market.get('spot', False) == True # Только спотовые рынки
        }

        # Получаем тикеры для отфильтрованных рынков
        # Передаем список символов
        ticker_symbols = list(filtered_markets.keys())
        if not ticker_symbols:
             logger.warning("Не найдено активных спотовых рынков для фильтрации")
             return []
             
        tickers = await exchange.fetch_tickers(ticker_symbols)

        # Сортируем по объему торгов за 24ч (quoteVolume - объем в USDT)
        sorted_tickers = sorted(
            tickers.items(),
            key=lambda x: x[1].get('quoteVolume', 0) if x[1] else 0,
            reverse=True
        )

        top_coins = []
        for symbol, ticker_data in sorted_tickers:
            if len(top_coins) >= limit:
                break

            # Получаем базовую валюту из уже отфильтрованного markets
            base_currency = filtered_markets[symbol].get('base', '')
            if not base_currency:
                 logger.warning(f"Не удалось получить базовую валюту для символа {symbol}")
                 continue

            # Исключаем стейблкоины, если нужно
            if exclude_stables and base_currency.upper() in STABLECOINS:
                logger.debug(f"Исключаем стейблкоин: {base_currency}")
                continue

            # Исключаем BTC, так как он анализируется отдельно как базовый актив
            if base_currency.upper() == 'BTC':
                logger.debug(f"Исключаем BTC: {base_currency}")
                continue

            top_coins.append(base_currency.upper())

        logger.info(f"Топ {len(top_coins)} альткоинов по объему ({quote_currency}, стейблкоины {'исключены' if exclude_stables else 'включены'}): {top_coins}")
        return top_coins

    except Exception as e:
        logger.error(f"Ошибка при получении топ альткоинов: {e}", exc_info=True) # Добавлен exc_info для трассировки
        # В случае ошибки можно вернуть фиксированный список или пустой список
        # return [] # Или ваш ALT_COINS по умолчанию
        raise # Пробрасываем исключение, чтобы main.py мог его обработать
    finally:
        if exchange:
            await exchange.close()