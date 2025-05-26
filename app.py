from flask import Flask, render_template, jsonify, request
from elasticsearch import Elasticsearch, NotFoundError
from datetime import datetime, timezone, timedelta
import os
import math
import tempfile
import pandas as pd
from torch.optim import AdamW
from torch.optim.lr_scheduler import OneCycleLR
from transformers import Trainer, TrainingArguments, set_seed
from tsfm_public import TimeSeriesPreprocessor, get_model, get_datasets
from tsfm_public.toolkit.visualization import plot_predictions

# TSFM Configuration
SEED = 42
set_seed(SEED)
TTM_MODEL_PATH = "ibm-granite/granite-timeseries-ttm-r2"
CONTEXT_LENGTH = 512
PREDICTION_LENGTH = 96

app = Flask(__name__)

# --- Configuration ---
# ES_HOST = os.getenv("ELASTICSEARCH_HOST", "192.168.30.128")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ES_HISTORICAL_INDEX = "crypto_historical_data"
# ES_OHLCV_LATEST_INDEX = "crypto_ohlcv_1m_latest"    # For latest 1m OHLCV candle
ES_OHLCV_LATEST_INDEX = "crypto-ohlcv"
ES_OHLCV_STATS_INDEX = "crypto_ohlcv_1m_stats"     # For windowed stats from 1m OHLCV
ES_CHART_1M_INDEX_FROM_SPARK = "crypto_ohlcv_1m_chartdata" # For raw 1m OHLCV for chart
ES_CHART_1M_INDEX_PATTERN_FOR_QUERY = "crypto_ohlcv_1m_chartdata-*"
# --- Initialize Elasticsearch Client ---
es_client = None
try:
    if ES_HOST and ES_PORT:
        es_client = Elasticsearch(
            [{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
            request_timeout=30, max_retries=2, retry_on_timeout=True
        )
        if not es_client.ping():
            raise ConnectionError("Failed to connect to Elasticsearch")
        print(f"Successfully connected to Elasticsearch at {ES_HOST}:{ES_PORT}")
    else:
        print("Error: ELASTICSEARCH_HOST or ELASTICSEARCH_PORT not set or invalid.")
        exit(1)
except Exception as e:
    print(f"Elasticsearch connection error: {e}")
    es_client = None
    exit(1)

# --- Helper Functions ---
def get_available_symbols_from_index(index_name_or_pattern, symbol_field="symbol.keyword"):
    if not es_client: return []
    try:
        index_exists = False
        if "*" in index_name_or_pattern:
            if es_client.indices.exists(index=index_name_or_pattern.split("*")[0] + "*"):
                index_exists = True
        elif es_client.indices.exists(index=index_name_or_pattern):
            index_exists = True
        
        if not index_exists:
            print(f"No indices found matching/for {index_name_or_pattern}")
            return []

        query = {"size": 0, "aggs": {"distinct_symbols": {"terms": {"field": symbol_field, "size": 500}}}}
        res = es_client.search(index=index_name_or_pattern, body=query, ignore_unavailable=True)
        if 'aggregations' in res and 'distinct_symbols' in res['aggregations']:
            return sorted([bucket['key'] for bucket in res['aggregations']['distinct_symbols']['buckets'] if bucket['key']])
        return []
    except Exception as e:
        print(f"Error fetching symbols from {index_name_or_pattern}: {e}")
        return []

def get_available_symbols_timeframes_historical():
    if not es_client or not es_client.indices.exists(index=ES_HISTORICAL_INDEX):
        print(f"Index {ES_HISTORICAL_INDEX} does not exist or ES client not available.")
        return []
    try:
        query = {
            "size": 0,
            "aggs": {
                "symbols_timeframes": {
                    "multi_terms": {"terms": [{"field": "symbol.keyword"}, {"field": "timeframe.keyword"}], "size": 1000}
                }
            }
        }
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query)
        symbols_list = []
        if 'aggregations' in res and 'symbols_timeframes' in res['aggregations']:
            for bucket in res['aggregations']['symbols_timeframes']['buckets']:
                if len(bucket['key']) == 2 and bucket['key'][0] and bucket['key'][1]:
                    symbols_list.append(f"{bucket['key'][0]}_{bucket['key'][1]}")
        return sorted(list(set(symbols_list)))
    except Exception as e:
        print(f"Error fetching available symbols_timeframes from {ES_HISTORICAL_INDEX}: {e}")
        return []

# --- Real-time Dashboard Routes ---
@app.route('/')
def realtime_page():
    available_symbols_rt = get_available_symbols_from_index(ES_OHLCV_LATEST_INDEX)
    initial_sym_rt = available_symbols_rt[0] if available_symbols_rt else None
    return render_template('realtime_dashboard.html',
                           available_symbols=available_symbols_rt,
                           initial_symbol=initial_sym_rt)

@app.route('/api/realtime_stats/<encoded_symbol_str>')
def api_realtime_stats(encoded_symbol_str):
    symbol_str = encoded_symbol_str.replace('-', '/')
    latest_data, stats_data = {}, {}
    try:
        doc = es_client.get(index=ES_OHLCV_LATEST_INDEX, id=symbol_str)
        latest_data = doc.get('_source', {})
    except NotFoundError:
        print(f"No latest OHLCV 1m data for {symbol_str} in {ES_OHLCV_LATEST_INDEX}")
    except Exception as e:
        print(f"Error fetching latest OHLCV 1m for {symbol_str}: {e}")

    try:
        query_stats = {"size": 1, "query": {"term": {"symbol.keyword": symbol_str}}, "sort": [{"window_end": "desc"}]}
        res_stats = es_client.search(index=ES_OHLCV_STATS_INDEX, body=query_stats)
        if res_stats['hits']['hits']: stats_data = res_stats['hits']['hits'][0]['_source']
    except NotFoundError:
        print(f"No windowed OHLCV 1m stats for {symbol_str} in {ES_OHLCV_STATS_INDEX}")
    except Exception as e:
        print(f"Error fetching windowed OHLCV 1m stats for {symbol_str}: {e}")
    return jsonify({"latest": latest_data, "stats": stats_data})

@app.route('/api/chart_data_1m/<encoded_symbol_str>')
def api_chart_data_1m(encoded_symbol_str):
    symbol_str = encoded_symbol_str.replace('-', '/')
    now_utc = datetime.now(timezone.utc)
    time_35_minutes_ago_ms = int((now_utc - timedelta(minutes=35)).timestamp() * 1000)

    query_body = {
        "size": 200, 
        "query": {
            "bool": {
                "must": [
                    {"term": {"symbol": symbol_str}},
                    {
                        "range": {
                            "@timestamp": { 
                                "gte": time_35_minutes_ago_ms,
                                "lte": int(now_utc.timestamp() * 1000) 
                            }
                        }
                    }
                ]
            }
        },
        "sort": [{"@timestamp": "asc"}] # Lấy theo thứ tự thời gian tăng dần 
    }
    try:
        res = es_client.search(index=ES_CHART_1M_INDEX_PATTERN_FOR_QUERY, body=query_body, ignore_unavailable=True)
        hits = res['hits']['hits']
        chart_points = []
        for hit in hits:
            source = hit['_source']
            ts_ms = source.get('timestamp_ms')
            if ts_ms is None and '@timestamp' in source:
                try:
                    ts_ms = int(datetime.fromisoformat(source['@timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
                except ValueError: continue

            if ts_ms is not None and 'close' in source:
                chart_points.append([ts_ms, source.get('close')])
        return jsonify(chart_points) # Trả về tất cả điểm trong 35 phút qua

    except NotFoundError:
        print(f"No chart data indices found matching {ES_CHART_1M_INDEX_PATTERN_FOR_QUERY} for {symbol_str}")
        return jsonify([])
    except Exception as e:
        print(f"Error fetching 1m chart data for {symbol_str}: {e}")
        return jsonify({"error": str(e)}), 500

# --- Historical Data Routes ---
@app.route('/historical')
def historical_page():
    available_hist = get_available_symbols_timeframes_historical()
    initial_hist = available_hist[0] if available_hist else None
    return render_template('historical_data.html',
                           available_symbols_timeframes=available_hist,
                           initial_symbol_timeframe=initial_hist)

@app.route('/api/historical_data/<symbol_timeframe_str>')
def api_historical_data(symbol_timeframe_str):
    time_range_str = request.args.get('range', 'all')
    parts = symbol_timeframe_str.split('_')
    if len(parts) < 2: return jsonify({"error": "Invalid symbol_timeframe format."}), 400
    
    timeframe = parts[-1]
    symbol = "_".join(parts[:-1])

    now_utc = datetime.now(timezone.utc)
    start_time_seconds = None
    if time_range_str == '1m': start_time_seconds = int((now_utc - timedelta(days=30)).timestamp())
    elif time_range_str == '3m': start_time_seconds = int((now_utc - timedelta(days=90)).timestamp())
    elif time_range_str == '6m': start_time_seconds = int((now_utc - timedelta(days=180)).timestamp())
    elif time_range_str == '1y': start_time_seconds = int((now_utc - timedelta(days=365)).timestamp())

    must_clauses = [{"term": {"symbol.keyword": symbol}}, {"term": {"timeframe.keyword": timeframe}}]
    if start_time_seconds:
        must_clauses.append({"range": {"timestamp": {"gte": start_time_seconds}}})

    query_body = {"size": 10000, "query": {"bool": {"must": must_clauses}}, "sort": [{"timestamp": "asc"}]}
    try:
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query_body)
        hits = res['hits']['hits']
        labels, close_prices, sma7_prices, sma30_prices = [], [], [], []
        for h in hits:
            source = h['_source']
            ts_seconds = source.get('timestamp')
            if isinstance(ts_seconds, (int, float)):
                try:
                    dt = datetime.fromtimestamp(ts_seconds, timezone.utc)
                    labels.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    close_prices.append(source.get('close'))
                    sma7_prices.append(source.get('sma_7'))
                    sma30_prices.append(source.get('sma_30'))
                except Exception as e:
                    print(f"Error converting hist timestamp {ts_seconds} for {symbol}_{timeframe}: {e}")
        datasets = [
            {'label': f'{symbol} Close ({timeframe})', 'data': close_prices, 'borderColor': 'rgb(75, 192, 192)', 'tension': 0.1, 'fill': False},
            {'label': f'{symbol} SMA 7 ({timeframe})', 'data': sma7_prices, 'borderColor': 'rgb(255, 159, 64)', 'tension': 0.1, 'fill': False, 'hidden': True},
            {'label': f'{symbol} SMA 30 ({timeframe})', 'data': sma30_prices, 'borderColor': 'rgb(153, 102, 255)', 'tension': 0.1, 'fill': False, 'hidden': True}
        ]
        return jsonify({'labels': labels, 'datasets': datasets})
    except NotFoundError:
        return jsonify({"error": f"Index {ES_HISTORICAL_INDEX} not found."}), 404
    except Exception as e:
        print(f"Error in api_historical_data for {symbol_timeframe_str}: {e}")
        return jsonify({"error": str(e)}), 500
    
def fetch_historical_data_for_tsfm(symbol, timeframe, num_records=10000):
    try:
        query_body = {
            "size": num_records,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"symbol.keyword": symbol}},
                        {"term": {"timeframe.keyword": timeframe}}
                    ]
                }
            },
            "sort": [{"timestamp": "asc"}]
        }
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query_body)
        hits = res['hits']['hits']
        data = []
        for hit in hits:
            source = hit['_source']
            data.append({
                'date': datetime.fromtimestamp(source['timestamp'], timezone.utc),
                'close': source['close']
            })
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        print(f"Error fetching historical data for {symbol}_{timeframe}: {e}")
        return pd.DataFrame()
def preprocess_data_for_tsfm(df, timestamp_column='date', target_column='close'):
    column_specifiers = {
        "timestamp_column": timestamp_column,
        "id_columns": [],
        "target_columns": [target_column],
        "control_columns": []
    }
    tsp = TimeSeriesPreprocessor(
        **column_specifiers,
        context_length=CONTEXT_LENGTH,
        prediction_length=PREDICTION_LENGTH,
        scaling=True,
        encode_categorical=False,
        scaler_type="standard"
    )
    # Split data (adjust indices based on your data size)
    split_config = {
        "train": [0, int(len(df) * 0.6)],
        "valid": [int(len(df) * 0.6), int(len(df) * 0.8)],
        "test": [int(len(df) * 0.8), len(df)]
    }
    train_dataset, valid_dataset, test_dataset = get_datasets(tsp, df, split_config)
    return tsp, train_dataset, valid_dataset, test_dataset

def zeroshot_forecast(symbol, timeframe, batch_size=64):
    df = fetch_historical_data_for_tsfm(symbol, timeframe)
    if df.empty:
        return {"error": f"No data found for {symbol}_{timeframe}"}, None

    tsp, _, _, test_dataset = preprocess_data_for_tsfm(df)
    zeroshot_model = get_model(
        TTM_MODEL_PATH,
        context_length=CONTEXT_LENGTH,
        prediction_length=PREDICTION_LENGTH,
        freq_prefix_tuning=False,
        freq=None,
        prefer_l1_loss=False,
        prefer_longer_context=True
    )
    temp_dir = tempfile.mkdtemp()
    zeroshot_trainer = Trainer(
        model=zeroshot_model,
        args=TrainingArguments(
            output_dir=temp_dir,
            per_device_eval_batch_size=batch_size,
            seed=SEED,
            report_to="none"
        )
    )
    predictions_dict = zeroshot_trainer.predict(test_dataset)
    predictions_np = predictions_dict.predictions[0]  # Shape: (num_samples, prediction_length, num_targets)
    # Convert predictions to a list for JSON response
    forecast = predictions_np[:, :, 0].tolist()  # Assuming 'close' is the only target
    # Generate future timestamps
    last_timestamp = df['date'].iloc[-1]
    timeframe_duration = {'1h': timedelta(hours=1), '1d': timedelta(days=1), '15m': timedelta(minutes=15)}[timeframe]
    future_timestamps = [(last_timestamp + (i + 1) * timeframe_duration).strftime('%Y-%m-%d %H:%M:%S') for i in range(PREDICTION_LENGTH)]
    return {"labels": future_timestamps, "predictions": forecast}, tsp

@app.route('/api/forecast/<symbol_timeframe_str>')
def api_forecast(symbol_timeframe_str):
    parts = symbol_timeframe_str.split('_')
    if len(parts) < 2:
        return jsonify({"error": "Invalid symbol_timeframe format."}), 400
    timeframe = parts[-1]
    symbol = "_".join(parts[:-1])
    result, _ = zeroshot_forecast(symbol, timeframe)
    if "error" in result:
        return jsonify(result), 400
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
