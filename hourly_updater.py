import ccxt
import pandas as pd
import time
import os
import subprocess
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, NotFoundError

load_dotenv()

SYMBOLS_STR = os.getenv('CRYPTO_SYMBOLS', 'BTC/USDT,ETH/USDT')
SYMBOLS = [s.strip() for s in SYMBOLS_STR.split(',')]
TIMEFRAME = os.getenv('CRYPTO_TIMEFRAME', '1h') 
EXCHANGE_ID = os.getenv('CRYPTO_EXCHANGE', 'binance')

LOCAL_SAVE_PATH_HOURLY = './temp_crypto_data_hourly'
HDFS_USER = os.environ.get("USER", "hadoop")
HDFS_HOURLY_UPDATES_PATH = f'/user/{HDFS_USER}/crypto_project/raw_hourly_updates' 

ES_HOST = os.getenv("ELASTICSEARCH_HOST", "192.168.30.128")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ES_HISTORICAL_INDEX = "crypto_historical_data"
HDFS_CMD = "/root/hadoop/bin/hdfs"
es_client = None
try:
    if ES_HOST and ES_PORT:
        es_client = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}])
        es_client.ping()
        print(f"Hourly Updater: Connected to Elasticsearch at {ES_HOST}:{ES_PORT}")
    else:
        print("Hourly Updater Error: ELASTICSEARCH_HOST or ELASTICSEARCH_PORT not set.")
        exit(1)
except Exception as e:
    print(f"Hourly Updater: Elasticsearch connection error: {e}")
    exit(1)

try:
    exchange = getattr(ccxt, EXCHANGE_ID)()
    if not exchange.has['fetchOHLCV']:
        print(f"Hourly Updater Error: Exchange {EXCHANGE_ID} does not support fetchOHLCV.")
        exit(1)
    exchange.load_markets()
    print(f"Hourly Updater: Connected to exchange {EXCHANGE_ID}")
except Exception as e:
    print(f"Hourly Updater: Exchange connection error: {e}")
    exit(1)

def get_last_timestamp_from_es(symbol, timeframe_str):
    if not es_client: return None
    try:
        query_body = {
            "size": 0,
            "query": {"bool": {"must": [{"term": {"symbol.keyword": symbol}}, {"term": {"timeframe.keyword": timeframe_str}}]}},
            "aggs": {"max_ts": {"max": {"field": "timestamp"}}}
        }
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query_body)
        max_ts_value = res['aggregations']['max_ts'].get('value')
        return int(max_ts_value) if max_ts_value is not None else None
    except NotFoundError:
        print(f"Hourly Updater: Index {ES_HISTORICAL_INDEX} not found for {symbol}_{timeframe_str}.")
        return None
    except Exception as e:
        print(f"Hourly Updater: Error querying last timestamp for {symbol}_{timeframe_str} from ES: {e}")
        return None

def fetch_data_for_update(symbol, timeframe_ccxt, since_timestamp_ms, end_timestamp_ms):
    all_ohlcv = []
    limit = 1000
    current_fetch_timestamp_ms = since_timestamp_ms
    timeframe_duration_ms = exchange.parse_timeframe(timeframe_ccxt) * 1000

    print(f"Hourly Updater: Fetching {symbol} from {datetime.fromtimestamp(current_fetch_timestamp_ms / 1000, timezone.utc).isoformat()} up to (exclusive) {datetime.fromtimestamp(end_timestamp_ms / 1000, timezone.utc).isoformat()}")

    while current_fetch_timestamp_ms < end_timestamp_ms:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe_ccxt, since=current_fetch_timestamp_ms, limit=limit)
            if not ohlcv: break
            
            ohlcv = [candle for candle in ohlcv if candle[0] < end_timestamp_ms]
            if not ohlcv: break

            all_ohlcv.extend(ohlcv)
            last_candle_timestamp_ms = ohlcv[-1][0]
            current_fetch_timestamp_ms = last_candle_timestamp_ms + timeframe_duration_ms
            time.sleep(exchange.rateLimit / 1000)
        except Exception as e:
            print(f"Hourly Updater: Error during fetch for {symbol}: {e}")
            break
    print(f"Hourly Updater: Fetched {len(all_ohlcv)} candles for {symbol}.")
    return all_ohlcv

if __name__ == "__main__":
    print(f"\n--- Hourly Historical Updater started at {datetime.now(timezone.utc).isoformat()} ---")
    os.makedirs(LOCAL_SAVE_PATH_HOURLY, exist_ok=True)
    try:
        subprocess.run([HDFS_CMD, 'dfs', '-mkdir', '-p', HDFS_HOURLY_UPDATES_PATH], check=True, capture_output=True)
    except Exception as e:
        print(f"Hourly Updater: Could not ensure HDFS dir {HDFS_HOURLY_UPDATES_PATH} exists: {e}")

    now_utc = datetime.now(timezone.utc)
    crawl_until_exclusive_dt = now_utc.replace(minute=0, second=0, microsecond=0)
    crawl_until_exclusive_ms = int(crawl_until_exclusive_dt.timestamp() * 1000)
    
    print(f"Hourly Updater: Will attempt to crawl data up to (exclusive): {crawl_until_exclusive_dt.isoformat()}")

    if TIMEFRAME != "1h": 
        print("Hourly Updater Warning: This script's 'crawl_until_exclusive_dt' logic is best suited for 1h timeframe. Review if using other timeframes.")

    for symbol_ccxt in SYMBOLS:
        symbol_es = symbol_ccxt.replace('/', '_')
        print(f"\nHourly Updater: Processing {symbol_ccxt}...")
        
        last_known_ts_seconds = get_last_timestamp_from_es(symbol_es, TIMEFRAME)
        if last_known_ts_seconds is None:
            print(f"Hourly Updater: No existing data for {symbol_es}_{TIMEFRAME}. Skipping hourly update. Run full historical load first.")
            continue
        
        timeframe_duration_seconds = exchange.parse_timeframe(TIMEFRAME)
        start_crawl_from_seconds = last_known_ts_seconds + timeframe_duration_seconds
        start_crawl_from_ms = start_crawl_from_seconds * 1000

        if start_crawl_from_ms >= crawl_until_exclusive_ms:
            print(f"Hourly Updater: Data for {symbol_ccxt} is up-to-date until {datetime.fromtimestamp(last_known_ts_seconds, timezone.utc).isoformat()}. Nothing new to crawl before {crawl_until_exclusive_dt.isoformat()}.")
            continue

        ohlcv_data = fetch_data_for_update(symbol_ccxt, TIMEFRAME, start_crawl_from_ms, crawl_until_exclusive_ms)
        if not ohlcv_data:
            print(f"Hourly Updater: No new data fetched for {symbol_ccxt} in the target window.")
            continue

        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime_str'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df = df.sort_values('timestamp')
        
        timestamp_filename_part = crawl_until_exclusive_dt.strftime('%Y%m%d_%H00') 
        csv_filename = f"{symbol_es}_{TIMEFRAME}_update_{timestamp_filename_part}.csv"
        local_filepath = os.path.join(LOCAL_SAVE_PATH_HOURLY, csv_filename)
        hdfs_filepath = f"{HDFS_HOURLY_UPDATES_PATH}/{csv_filename}"

        df.to_csv(local_filepath, index=False, header=True)
        print(f"Hourly Updater: Saved new data for {symbol_ccxt} to temp file: {local_filepath}")

        try:
            subprocess.run([HDFS_CMD, 'dfs', '-put', '-f', local_filepath, hdfs_filepath], check=True, capture_output=True, text=True)
            print(f"Hourly Updater: Uploaded {csv_filename} to HDFS.")
        except subprocess.CalledProcessError as e:
            print(f"Hourly Updater: Error uploading {csv_filename} to HDFS: {e.stderr}")
        except FileNotFoundError:
             print("Hourly Updater Error: 'hdfs' command not found.")
    print(f"\n--- Hourly Historical Updater finished at {datetime.now(timezone.utc).isoformat()} ---")
