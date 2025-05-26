import ccxt
import json
from kafka import KafkaProducer
import time
import schedule
from datetime import datetime
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
load_dotenv()

SYMBOLS = [s.strip() for s in os.getenv('CRYPTO_SYMBOLS', 'BTC/USDT,ETH/USDT').split(',')]
EXCHANGE_ID = os.getenv('CRYPTO_EXCHANGE', 'binance')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
# TẠO TOPIC MỚI CHO DỮ LIỆU OHLCV 1 PHÚT
KAFKA_OHLCV_1M_TOPIC = 'crypto_ohlcv_1m'
FETCH_OHLCV_INTERVAL_SECONDS = 60 # Fetch nến 1 phút mỗi 60 giây

producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"OHLCV 1m Producer: Connected to Kafka Broker at {KAFKA_BROKER}.")
except Exception as e:
    print(f"OHLCV 1m Producer: Kafka connection error: {e}")
    exit(1)

exchange = None
try:
    exchange = getattr(ccxt, EXCHANGE_ID)()
    if not exchange.has['fetchOHLCV']:
        print(f"OHLCV 1m Producer: Exchange {EXCHANGE_ID} does not support fetchOHLCV.")
        exit(1)
    # exchange.load_markets() # Good practice
    print(f"OHLCV 1m Producer: Connected to exchange {EXCHANGE_ID}.")
except Exception as e:
    print(f"OHLCV 1m Producer: Exchange connection error: {e}")
    exit(1)

def fetch_and_send_ohlcv_1m():
    print(f"[{datetime.now()}] OHLCV 1m Producer: Fetching 1-minute OHLCV data...")
    for symbol_ccxt in SYMBOLS:
        try:
            ohlcv_list = exchange.fetch_ohlcv(symbol_ccxt, '1m', limit=2) # Lấy 2 nến gần nhất

            if ohlcv_list and len(ohlcv_list) > 0:
                latest_candle = ohlcv_list[-1]
                timestamp_ms, open_p, high_p, low_p, close_p, volume = latest_candle
                
                data = {
                    'timestamp': timestamp_ms, # Milliseconds
                    'symbol': symbol_ccxt,    # e.g., "BTC/USDT"
                    'timeframe': '1m',
                    'open': open_p,
                    'high': high_p,
                    'low': low_p,
                    'close': close_p,
                    'volume': volume,
                    'datetime_str': datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()
                }
                producer.send(KAFKA_OHLCV_1M_TOPIC, value=data)
                print(f" -> Sent 1m OHLCV: {symbol_ccxt} - Time: {data['datetime_str']} Close: {data['close']}")
            else:
                print(f"No 1m OHLCV data returned for {symbol_ccxt}")

        except ccxt.NetworkError as e:
            print(f"OHLCV 1m Producer: Network error for {symbol_ccxt}: {e}")
        except ccxt.ExchangeError as e:
            print(f"OHLCV 1m Producer: Exchange error for {symbol_ccxt}: {e}")
        except Exception as e:
            print(f"OHLCV 1m Producer: Unknown error for {symbol_ccxt}: {e}")
        
        if len(SYMBOLS) > 1:
            time.sleep(max(0.1, exchange.rateLimit / 2000)) # Small delay

    producer.flush()
    print(f"[{datetime.now()}] OHLCV 1m Producer: Cycle finished.")

if __name__ == "__main__":
    print(f"OHLCV 1m Producer: Will fetch data every {FETCH_OHLCV_INTERVAL_SECONDS} seconds for topic '{KAFKA_OHLCV_1M_TOPIC}'. Ctrl+C to stop.")
    
    fetch_and_send_ohlcv_1m() # Run once immediately
    schedule.every(FETCH_OHLCV_INTERVAL_SECONDS).seconds.do(fetch_and_send_ohlcv_1m)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nOHLCV 1m Producer: Stopping...")
    finally:
        if producer:
            producer.close()
        print("OHLCV 1m Producer: Closed and exited.")
