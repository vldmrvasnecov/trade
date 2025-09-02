# config.py
import os
from dotenv import load_dotenv
import logging

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============== НАСТРОЙКИ ===============
# Exchange settings
EXCHANGE_CONFIG = {
    'rateLimit': 1000,
    'enableRateLimit': True,
}

# Async settings
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY = 2

# Symbols
ALT_COINS = ['ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE', 'DOT', 'AVAX', 'LINK', 'TRX', 'SUI', 'XLM', 'BCH', 'HBAR', 'LTC', 'TON']
BTC_USDT = "BTC/USDT"

# Timeframes
BTC_TIMEFRAMES = [('15m', '15m'), ('1h', '1h'), ('4h', '4h')]
ALT_TIMEFRAMES = [('15m', '15m'), ('1h', '1h'), ('4h', '4h')]

# Indicator parameters
Z_WINDOW = 20
RSI_PERIOD = 14
ATR_PERIOD = 14
EMA_SHORT = 50
EMA_LONG = 200
ORDERBOOK_DEPTH = 50

# Cache settings
BTC_CACHE_TTL = 120  # seconds

# Telegram settings
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "YOUR_CHAT_ID")
TELEGRAM_ENABLED = TELEGRAM_TOKEN != "YOUR_BOT_TOKEN" and CHAT_ID != "YOUR_CHAT_ID"

# =============== ЦВЕТА ===============
class Colors:
    BUY = '\033[92m'        # Зелёный
    SELL = '\033[91m'       # Красный
    HOLD = '\033[93m'       # Жёлтый
    LONG = '\033[1;92m'     # Жирный зелёный
    SHORT = '\033[1;91m'    # Жирный красный
    WARN = '\033[93m'       # Предупреждение
    OPP = '\033[96m'        # Возможность (бирюзовый)
    NEUTRAL = '\033[97m'    # Нейтральный
    RESET = '\033[0m'       # Сброс

logger.info(f"Telegram enabled: {TELEGRAM_ENABLED}")
