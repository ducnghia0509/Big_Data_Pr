import os
import time
import json
import ccxt
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()

# --- Cấu hình ---
SYMBOLS = os.getenv("CRYPTO_SYMBOLS", "BTC/USDT,ETH/USDT").split(",")
EXCHANGE_ID = os.getenv("CRYPTO_EXCHANGE", "binance")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_OHLCV_1M_TOPIC", "crypto_ohlcv_1m")
FETCH_INTERVAL = int(os.getenv("MINUTE_CHART_FETCH_INTERVAL", 60))

# Kết nối Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Kết nối sàn
exchange = getattr(ccxt, EXCHANGE_ID)()

def fetch_and_send():
    for symbol in SYMBOLS:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1m", limit=1)
            for candle in ohlcv:
                data = {
                    "symbol": symbol,
                    "timestamp_ms": candle[0],
                    "@timestamp": datetime.utcfromtimestamp(candle[0] / 1000).isoformat() + "Z",
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5]
                }
                producer.send(TOPIC, value=data)
                print(f"[{symbol}] Sent: {data}")
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")

if __name__ == "__main__":
    while True:
        fetch_and_send()
        time.sleep(FETCH_INTERVAL)
