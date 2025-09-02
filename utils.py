# utils.py
import ccxt
import ccxt.async_support as ccxt_async
import numpy as np
import logging
import asyncio
import pandas as pd
from config import MAX_RETRY_ATTEMPTS, RETRY_DELAY, ORDERBOOK_DEPTH

logger = logging.getLogger(__name__)

# --- Функции для оценки ликвидности и стакана ---
def check_liquidity(exchange, symbol):
    """
    Проверка ликвидности: объём, спред, стакан.
    Возвращает кортеж: (is_liquid: bool, info: dict)
    """
    base_info = "OK"
    orderbook_analysis = None
    is_liquid = True
    
    try:
        # 1. Проверка спреда
        ticker = exchange.fetch_ticker(symbol)
        bid = ticker.get('bid')
        ask = ticker.get('ask')
        
        if bid is None or ask is None or bid == 0 or ask == 0:
            base_info = "Нет данных по спреду"
            return False, {'base_info': base_info, 'orderbook_analysis': orderbook_analysis}
            
        spread = (ask - bid) / bid * 100
        
        # 2. Проверка объёма за последние 24 часа
        volume_24h = ticker.get('quoteVolume', 0)
        
        # 3. Проверка глубины стакана
        try:
            orderbook = exchange.fetch_order_book(symbol, limit=ORDERBOOK_DEPTH)
            if orderbook and len(orderbook.get('bids', [])) > 0 and len(orderbook.get('asks', [])) > 0:
                target_btc = 0.01  # ~$1000
                price_impact = estimate_price_impact(orderbook, target_btc, 'buy')
                
                # Анализ плотности стакана
                current_price = ticker.get('last', bid)
                nearest_bid, nearest_ask, density_score = analyze_orderbook_density(
                    orderbook, current_price, ORDERBOOK_DEPTH
                )
                
                orderbook_analysis = {
                    'nearest_bid': nearest_bid,
                    'nearest_ask': nearest_ask,
                    'density_score': density_score,
                    'current_price': current_price,
                    'price_impact': price_impact,
                    'spread_pct': spread
                }
                
        except Exception as e:
            logger.warning(f"Не удалось получить стакан для {symbol}: {e}")

        # Финальная оценка ликвидности
        final_base_info = f"OK (спред: {spread:.2f}%, объём: {volume_24h:.0f} BTC)"
        
        # Критерии низкой ликвидности:
        if spread > 2.0 or volume_24h < 10 or (orderbook_analysis and orderbook_analysis.get('price_impact', 0) > 1.0):
            is_liquid = False
            final_base_info = f"Низкая ликвидность (спред: {spread:.2f}%, объём: {volume_24h:.0f} BTC)"
            
        return is_liquid, {
            'base_info': final_base_info, 
            'orderbook_analysis': orderbook_analysis,
            'spread': spread,
            'volume_24h': volume_24h
        }

    except Exception as e:
        base_info = f"Ошибка проверки: {str(e)}"
        logger.error(base_info)
        return False, {'base_info': base_info, 'orderbook_analysis': orderbook_analysis}

def estimate_price_impact(orderbook, target_amount_btc, side):
    """Оценка price impact для заданной суммы"""
    try:
        orders = orderbook.get('asks', []) if side == 'buy' else orderbook.get('bids', [])
        total_btc = 0
        total_qty = 0
        if not orders:
            return 0
        start_price = orders[0][0] if orders and len(orders) > 0 and len(orders[0]) > 0 else 0
        if start_price == 0:
            return 0
        for order in orders:
            if len(order) < 2:
                continue
            price, amount = order[0], order[1]
            if price is None or amount is None or price <= 0:
                continue
            btc_amount = price * amount
            if total_btc + btc_amount > target_amount_btc:
                # Частичное исполнение последнего ордера
                remaining = target_amount_btc - total_btc
                if price > 0:
                    partial_qty = remaining / price
                    total_qty += partial_qty
                total_btc += remaining
                break
            else:
                total_qty += amount
                total_btc += btc_amount
        if total_btc == 0 or total_qty == 0:
            return 0
        avg_price = total_btc / total_qty if total_qty > 0 else 0
        if start_price > 0:
            impact = abs(avg_price - start_price) / start_price * 100
            return impact
        return 0
    except:
        return 0

def analyze_orderbook_density(orderbook, current_price, depth=ORDERBOOK_DEPTH):
    """Анализ плотности стакана и определение зон высокой плотности"""
    try:
        if not orderbook or 'bids' not in orderbook or 'asks' not in orderbook:
            return None, None, 0
            
        bids = orderbook.get('bids', [])[:depth]
        asks = orderbook.get('asks', [])[:depth]
        
        if not bids or not asks:
            return None, None, 0

        # Рассчитываем объемы в BTC для более точного анализа
        bid_volumes_btc = []
        ask_volumes_btc = []
        
        for price, amount in bids:
            if isinstance(price, (int, float)) and isinstance(amount, (int, float)) and price > 0 and amount > 0:
                bid_volumes_btc.append((price, price * amount))
            
        for price, amount in asks:
            if isinstance(price, (int, float)) and isinstance(amount, (int, float)) and price > 0 and amount > 0:
                ask_volumes_btc.append((price, price * amount))

        # Находим ближайшие уровни
        nearest_bid = None
        nearest_ask = None
        
        # Ближайший bid (покупка) ниже текущей цены
        valid_bids = [p for p, _ in bid_volumes_btc if p < current_price]
        if valid_bids:
            nearest_bid = max(valid_bids)
            
        # Ближайший ask (продажа) выше текущей цены
        valid_asks = [p for p, _ in ask_volumes_btc if p > current_price]
        if valid_asks:
            nearest_ask = min(valid_asks)

        # Рассчитываем плотность (суммарный объем в зоне +/- 0.5% от текущей цены)
        density_zone = current_price * 0.005  # 0.5%
        
        bid_density = sum(volume for price, volume in bid_volumes_btc 
                         if abs(price - current_price) <= density_zone)
        ask_density = sum(volume for price, volume in ask_volumes_btc 
                         if abs(price - current_price) <= density_zone)
        
        total_density_score = bid_density + ask_density
        
        return nearest_bid, nearest_ask, total_density_score
        
    except Exception as e:
        logger.error(f"Ошибка анализа плотности стакана: {e}")
        return None, None, 0

# --- Асинхронные функции для сбора данных ---
async def fetch_ohlcv_async(symbol, timeframe, limit=1000, sem=None):
    """Асинхронная функция получения OHLCV данных с ограничением по семафору и повторными попытками"""
    semaphore_to_use = sem if sem is not None else asyncio.Semaphore(100)
    
    async with semaphore_to_use:
        exchange_async = None
        try:
            exchange_async = ccxt_async.binance({
                'rateLimit': 1000,
                'enableRateLimit': True,
                'options': {
                    'adjustForTimeDifference': True
                }
            })
            
            # Попытки получения данных
            for attempt in range(MAX_RETRY_ATTEMPTS + 1):
                try:
                    data = await asyncio.wait_for(
                        exchange_async.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit),
                        timeout=30.0
                    )
                    
                    if data is None or len(data) == 0:
                        if attempt < MAX_RETRY_ATTEMPTS:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None
                    
                    # Создание DataFrame
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    
                    # Проверка на наличие NaN значений
                    if df[['open', 'high', 'low', 'close', 'volume']].isnull().any().any():
                        logger.warning(f"Данные для {symbol} содержат NaN значения")
                        if attempt < MAX_RETRY_ATTEMPTS:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None
                        
                    # Проверка на минимальное количество данных
                    if len(df) < 10:
                        logger.warning(f"Недостаточно данных для {symbol}: {len(df)} свечей")
                        return None
                        
                    return df
                    
                except asyncio.TimeoutError:
                    logger.warning(f"Таймаут для {symbol} {timeframe} (попытка {attempt + 1})")
                    if attempt < MAX_RETRY_ATTEMPTS:
                        await asyncio.sleep(RETRY_DELAY)
                    else:
                        return None
                        
                except Exception as e:
                    error_msg = str(e).lower()
                    if any(keyword in error_msg for keyword in ['invalid symbol', 'market not found', 'does not exist']):
                        logger.info(f"Пара {symbol} недоступна на бирже")
                        return None
                    else:
                        logger.warning(f"Ошибка для {symbol} {timeframe}: {e} (попытка {attempt + 1})")
                        if attempt < MAX_RETRY_ATTEMPTS:
                            await asyncio.sleep(RETRY_DELAY)
                        else:
                            return None
                            
        except Exception as e:
            logger.error(f"Критическая ошибка при создании exchange для {symbol}: {e}")
            return None
            
        finally:
            if exchange_async:
                try:
                    await exchange_async.close()
                except Exception as e:
                    logger.debug(f"Ошибка закрытия соединения: {e}")
                    
    return None

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
            timeout=15.0
        )
        
        # Валидация данных стакана
        if not orderbook:
            return None
            
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        if not bids or not asks:
            return None
            
        # Проверка формата данных
        if not all(len(bid) >= 2 for bid in bids) or not all(len(ask) >= 2 for ask in asks):
            logger.warning(f"Некорректный формат стакана для {symbol}")
            return None
            
        return orderbook
        
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут при получении стакана для {symbol}")
        return None
    except Exception as e:
        error_msg = str(e).lower()
        if any(keyword in error_msg for keyword in ['invalid symbol', 'market not found', 'does not exist']):
            logger.info(f"Пара {symbol} недоступна на бирже")
            return None
        else:
            logger.warning(f"Ошибка получения стакана для {symbol}: {e}")
            return None
    finally:
        if exchange_async:
            try:
                await exchange_async.close()
            except Exception as e:
                logger.debug(f"Ошибка закрытия соединения стакана: {e}")