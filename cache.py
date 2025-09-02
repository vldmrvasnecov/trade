# cache.py
from datetime import datetime, timedelta
BTC_CACHE_TTL = 120
class BTCCache:
    def __init__(self, ttl_seconds):
        self.ttl = timedelta(seconds=ttl_seconds)
        self.data = None
        self.timestamp = None

    def get(self):
        if self.data is None or self.timestamp is None:
            return None
        if datetime.now() - self.timestamp > self.ttl:
            self.data = None
            self.timestamp = None
            return None
        return self.data

    def set(self, data):
        self.data = data
        self.timestamp = datetime.now()

# Инициализируем кэш
# btc_cache = BTCCache(BTC_CACHE_TTL) # Лучше создавать в main.py
