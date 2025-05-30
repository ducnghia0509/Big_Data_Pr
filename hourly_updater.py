import ccxt
import pandas as pd
import time
import os
import subprocess
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, NotFoundError

# Load environment variables
load_dotenv()

# --- Configuration from .env ---
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

# --- Initialize Elasticsearch Client ---
es_client = None
try:
    if ES_HOST and ES_PORT:
        es_client = Elasticsearch(
            [{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
            request_timeout=10, max_retries=2, retry_on_timeout=True
        )
        if not es_client.ping():
            raise ConnectionError("Hourly Updater: Failed to connect to Elasticsearch.")
        print(f"Hourly Updater: Connected to Elasticsearch at {ES_HOST}:{ES_PORT}")
    else:
        print("Hourly Updater Error: ELASTICSEARCH_HOST or ELASTICSEARCH_PORT not set.")
        exit(1)
except Exception as e:
    print(f"Hourly Updater: Elasticsearch connection error: {e}")
    exit(1)

# --- Initialize CCXT Exchange ---
exchange = None
try:
    exchange = getattr(ccxt, EXCHANGE_ID)()
    if not exchange.has['fetchOHLCV']:
        print(f"Hourly Updater Error: Exchange {EXCHANGE_ID} does not support fetchOHLCV.")
        exit(1)
    exchange.load_markets() # Good practice
    print(f"Hourly Updater: Connected to exchange {EXCHANGE_ID}")
except Exception as e:
    print(f"Hourly Updater: Exchange connection error: {e}")
    exit(1)

def get_last_timestamp_from_es(symbol_query, timeframe_query):
    """
    Gets the latest 'timestamp' (in seconds, as per ES mapping 'epoch_second')
    for a given symbol and timeframe from the ES_HISTORICAL_INDEX.
    symbol_query: e.g., "BTC_USDT" (format stored in ES historical index)
    timeframe_query: e.g., "1h"
    Returns None if not found or error.
    """
    if not es_client:
        print("Hourly Updater: ES client not initialized in get_last_timestamp_from_es.")
        return None
    try:
        query_body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"symbol.keyword": symbol_query}},
                        {"term": {"timeframe.keyword": timeframe_query}}
                    ]
                }
            },
            "aggs": {
                "max_ts": {
                    "max": {"field": "timestamp"} 
                }
            }
        }
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query_body)
        
        max_ts_agg = res['aggregations']['max_ts']
        max_ts_seconds_str = max_ts_agg.get('value_as_string')

        if max_ts_seconds_str:
            return int(float(max_ts_seconds_str)) 
    
        max_ts_value_ms = max_ts_agg.get('value') 
        if max_ts_value_ms is not None:
            print(f"Hourly Updater WARNING: Using 'value' from max_ts agg for {symbol_query}_{timeframe_query} and converting to seconds.")
            return int(max_ts_value_ms / 1000)

        print(f"Hourly Updater DEBUG: No max_ts value found (neither value_as_string nor value) for {symbol_query}_{timeframe_query}")
        return None
    except NotFoundError:
        print(f"Hourly Updater: Index {ES_HISTORICAL_INDEX} not found for {symbol_query}_{timeframe_query}.")
        return None
    except Exception as e:
        print(f"Hourly Updater: Error querying last timestamp for {symbol_query}_{timeframe_query} from ES: {e}")
        return None

def fetch_data_for_update(symbol_ccxt, timeframe_ccxt, since_timestamp_ms, end_timestamp_ms_exclusive):
    all_ohlcv = []
    limit = 1000 
    current_fetch_timestamp_ms = since_timestamp_ms
    try:
        timeframe_duration_ms = exchange.parse_timeframe(timeframe_ccxt) * 1000
    except Exception as e:
        print(f"Hourly Updater Error: Could not parse timeframe '{timeframe_ccxt}': {e}")
        return []


    print(f"Hourly Updater: Fetching {symbol_ccxt} ({timeframe_ccxt}) from "
          f"{datetime.fromtimestamp(current_fetch_timestamp_ms / 1000, timezone.utc).isoformat()} "
          f"up to (exclusive) {datetime.fromtimestamp(end_timestamp_ms_exclusive / 1000, timezone.utc).isoformat()}")

    while current_fetch_timestamp_ms < end_timestamp_ms_exclusive:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol_ccxt, timeframe_ccxt, since=current_fetch_timestamp_ms, limit=limit)
            if not ohlcv:
                print(f"Hourly Updater: No more data for {symbol_ccxt} after {datetime.fromtimestamp(current_fetch_timestamp_ms / 1000, timezone.utc).isoformat()}")
                break
            
            ohlcv = [candle for candle in ohlcv if candle[0] < end_timestamp_ms_exclusive]
            if not ohlcv:
                break 

            all_ohlcv.extend(ohlcv)
            last_candle_timestamp_ms = ohlcv[-1][0]

            current_fetch_timestamp_ms = last_candle_timestamp_ms + timeframe_duration_ms
            
            # Respect exchange rate limits
            if hasattr(exchange, 'rateLimit'):
                time.sleep(exchange.rateLimit / 1000)
            else:
                time.sleep(0.2) # Default small delay

        except ccxt.NetworkError as e:
            print(f"Hourly Updater: Network error for {symbol_ccxt}: {e}. Retrying in 10s...")
            time.sleep(10)
        except ccxt.ExchangeError as e:
            print(f"Hourly Updater: Exchange error for {symbol_ccxt}: {e}. Stopping for this symbol.")
            break
        except Exception as e:
            print(f"Hourly Updater: Unknown error during fetch for {symbol_ccxt}: {e}")
            import traceback
            traceback.print_exc()
            break
            
    print(f"Hourly Updater: Fetched a total of {len(all_ohlcv)} candles for {symbol_ccxt} for this update run.")
    return all_ohlcv

if __name__ == "__main__":
    script_start_time = datetime.now(timezone.utc)
    print(f"\n--- Hourly Historical Updater started at {script_start_time.isoformat()} ---")
    
    os.makedirs(LOCAL_SAVE_PATH_HOURLY, exist_ok=True)
    try:
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', HDFS_HOURLY_UPDATES_PATH], check=True, capture_output=True)
        print(f"Hourly Updater: Ensured HDFS directory exists: {HDFS_HOURLY_UPDATES_PATH}")
    except Exception as e:
        print(f"Hourly Updater: Warning - Could not create/verify HDFS dir {HDFS_HOURLY_UPDATES_PATH}: {e}")

    crawl_until_exclusive_dt = script_start_time.replace(minute=0, second=0, microsecond=0)
    crawl_until_exclusive_ms = int(crawl_until_exclusive_dt.timestamp() * 1000)
    
    print(f"Hourly Updater: Will attempt to crawl data for candles starting BEFORE {crawl_until_exclusive_dt.isoformat()}")

    for symbol_ccxt_format in SYMBOLS: 
        symbol_es_format = symbol_ccxt_format.replace('/', '_') 
        
        print(f"\nHourly Updater: Processing {symbol_ccxt_format} (ES symbol: {symbol_es_format}, Timeframe: {TIMEFRAME})...")
        
        last_known_ts_seconds = get_last_timestamp_from_es(symbol_es_format, TIMEFRAME)

        if last_known_ts_seconds is None:
            print(f"Hourly Updater: No existing historical data found for {symbol_es_format}_{TIMEFRAME} in Elasticsearch. "
                  f"This script is for updates only. Please run the full historical_crawler.py first.")
            continue 
        
        # Calculate the start time for the next fetch.
        timeframe_duration_seconds = exchange.parse_timeframe(TIMEFRAME) # Duration in seconds
        start_crawl_from_seconds_inclusive = last_known_ts_seconds + timeframe_duration_seconds
        start_crawl_from_ms_inclusive = start_crawl_from_seconds_inclusive * 1000

        last_known_dt_utc = datetime.fromtimestamp(last_known_ts_seconds, timezone.utc)
        next_expected_candle_start_dt_utc = datetime.fromtimestamp(start_crawl_from_seconds_inclusive, timezone.utc)

        print(f"Hourly Updater: Last known candle in ES for {symbol_es_format} started at: {last_known_dt_utc.isoformat()}")
        print(f"Hourly Updater: Calculated next expected candle start (since_timestamp for API): {next_expected_candle_start_dt_utc.isoformat()}")

        if start_crawl_from_ms_inclusive >= crawl_until_exclusive_ms:
            print(f"Hourly Updater: Data for {symbol_ccxt_format} seems up-to-date. "
                  f"Next expected candle ({next_expected_candle_start_dt_utc.isoformat()}) "
                  f"is not before target end time ({crawl_until_exclusive_dt.isoformat()}). Nothing to crawl for now.")
            continue

        ohlcv_data = fetch_data_for_update(symbol_ccxt_format, TIMEFRAME, start_crawl_from_ms_inclusive, crawl_until_exclusive_ms)
        
        if not ohlcv_data:
            print(f"Hourly Updater: No new OHLCV data fetched for {symbol_ccxt_format} in the target window.")
            continue

        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime_str'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df = df.sort_values('timestamp')
        
        batch_run_timestamp_str = script_start_time.strftime('%Y%m%d_%H%M%S')
        csv_filename = f"{symbol_es_format}_{TIMEFRAME}_update_{batch_run_timestamp_str}.csv"
        local_filepath = os.path.join(LOCAL_SAVE_PATH_HOURLY, csv_filename)
        # HDFS path uses the same unique filename
        hdfs_filepath = f"{HDFS_HOURLY_UPDATES_PATH}/{csv_filename}"

        df.to_csv(local_filepath, index=False, header=True)
        print(f"Hourly Updater: Saved new data for {symbol_ccxt_format} to temp file: {local_filepath}")

        try:
            subprocess.run(['hdfs', 'dfs', '-put', '-f', local_filepath, hdfs_filepath], check=True, capture_output=True, text=True)
            print(f"Hourly Updater: Uploaded {csv_filename} to HDFS at {hdfs_filepath}")
        except subprocess.CalledProcessError as e:
            print(f"Hourly Updater: Error uploading {csv_filename} to HDFS: {e.stderr}")
        except FileNotFoundError:
             print("Hourly Updater Error: 'hdfs' command not found. Ensure Hadoop bin is in PATH.")

    print(f"\n--- Hourly Historical Updater finished at {datetime.now(timezone.utc).isoformat()} ---")
