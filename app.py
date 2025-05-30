from flask import Flask, render_template, jsonify, request
from elasticsearch import Elasticsearch, NotFoundError
from datetime import datetime, timezone, timedelta
import os
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import MinMaxScaler
from sklearn.exceptions import NotFittedError
import traceback

app = Flask(__name__)

# --- Configuration (Giữ nguyên) ---
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "192.168.30.128")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ES_HISTORICAL_INDEX = "crypto_historical_data"
ES_OHLCV_LATEST_INDEX = "crypto_ohlcv_1m_latest"
ES_OHLCV_STATS_INDEX = "crypto_ohlcv_1m_stats"
ES_CHART_1M_INDEX_FROM_SPARK = "crypto_ohlcv_1m_chartdata"
ES_CHART_1M_INDEX_PATTERN_FOR_QUERY = "crypto_ohlcv_1m_chartdata-*"
XGBOOST_MODEL_PATH = "./trained_models"
PREDICTION_STEPS_XGBOOST = 24

# --- Initialize Elasticsearch Client (Giữ nguyên) ---
es_client = None
try:
    if ES_HOST and ES_PORT:
        es_client = Elasticsearch(
            [{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
            request_timeout=10, max_retries=2, retry_on_timeout=True
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


# --- Helper Functions (Giữ nguyên) ---
def get_available_symbols_from_index(index_name_or_pattern, symbol_field="symbol.keyword"):
    # ... (Giữ nguyên code) ...
    if not es_client: return []
    try:
        index_exists = False
        if "*" in index_name_or_pattern:
            if es_client.indices.exists(index=index_name_or_pattern.split("*")[0] + "*"): index_exists = True
        elif es_client.indices.exists(index=index_name_or_pattern): index_exists = True
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
    # ... (Giữ nguyên code) ...
    if not es_client or not es_client.indices.exists(index=ES_HISTORICAL_INDEX):
        print(f"Index {ES_HISTORICAL_INDEX} does not exist for historical symbols.")
        return []
    try:
        query = {"size": 0, "aggs": {"symbols_timeframes": {"multi_terms": {"terms": [{"field": "symbol"}, {"field": "timeframe"}], "size": 1000}}}}
        res = es_client.search(index=ES_HISTORICAL_INDEX, body=query)
        symbols_list = []
        if res.get('aggregations') and res['aggregations'].get('symbols_timeframes') and isinstance(res['aggregations']['symbols_timeframes'].get('buckets'), list):
            for bucket in res['aggregations']['symbols_timeframes']['buckets']:
                if isinstance(bucket.get('key'), list) and len(bucket['key']) == 2 and bucket['key'][0] and bucket['key'][1]:
                    symbols_list.append(f"{bucket['key'][0]}_{bucket['key'][1]}")
        if not symbols_list: print(f"WARNING: No symbol_timeframe buckets found in {ES_HISTORICAL_INDEX}.")
        return sorted(list(set(symbols_list)))
    except Exception as e:
        print(f"Error fetching symbols_timeframes from {ES_HISTORICAL_INDEX}: {e}")
        traceback.print_exc(); return []

# --- Real-time Dashboard Routes (Giữ nguyên) ---
@app.route('/')
def realtime_page(): 
    available_symbols_rt = get_available_symbols_from_index(ES_OHLCV_LATEST_INDEX)
    initial_sym_rt = available_symbols_rt[0] if available_symbols_rt else None
    return render_template('realtime_dashboard.html', available_symbols=available_symbols_rt, initial_symbol=initial_sym_rt)

@app.route('/api/realtime_stats/<encoded_symbol_str>')
def api_realtime_stats(encoded_symbol_str): 
    symbol_str = encoded_symbol_str.replace('-', '/')
    latest_data, stats_data = {}, {}
    try:
        doc = es_client.get(index=ES_OHLCV_LATEST_INDEX, id=symbol_str)
        latest_data = doc.get('_source', {})
    except NotFoundError: print(f"No latest OHLCV 1m data for {symbol_str} in {ES_OHLCV_LATEST_INDEX}")
    except Exception as e: print(f"Error fetching latest OHLCV 1m for {symbol_str}: {e}")
    try:
        query_stats = {"size": 1, "query": {"term": {"symbol.keyword": symbol_str}}, "sort": [{"window_end": "desc"}]}
        res_stats = es_client.search(index=ES_OHLCV_STATS_INDEX, body=query_stats)
        if res_stats['hits']['hits']: stats_data = res_stats['hits']['hits'][0]['_source']
    except NotFoundError: print(f"No windowed OHLCV 1m stats for {symbol_str} in {ES_OHLCV_STATS_INDEX}")
    except Exception as e: print(f"Error fetching windowed OHLCV 1m stats for {symbol_str}: {e}")
    return jsonify({"latest": latest_data, "stats": stats_data})

@app.route('/api/chart_data_1m/<encoded_symbol_str>')
def api_chart_data_1m(encoded_symbol_str): 
    symbol_str = encoded_symbol_str.replace('-', '/')
    now_utc = datetime.now(timezone.utc)
    time_35_minutes_ago_ms = int((now_utc - timedelta(minutes=35)).timestamp() * 1000)
    now_ms = int(now_utc.timestamp() * 1000)
    query_body = {
        "size": 200, 
        "query": {"bool": {"must": [{"term": {"symbol.keyword": symbol_str}}, {"range": {"@timestamp": {"gte": time_35_minutes_ago_ms, "lte": now_ms}}}]}},
        "sort": [{"@timestamp": "asc"}]
    }
    try:
        res = es_client.search(index=ES_CHART_1M_INDEX_PATTERN_FOR_QUERY, body=query_body, ignore_unavailable=True)
        hits = res['hits']['hits']
        chart_points = []
        for hit in hits:
            source = hit['_source']
            ts_ms = source.get('timestamp_ms')
            if ts_ms is None and '@timestamp' in source:
                try: ts_ms = int(datetime.fromisoformat(source['@timestamp'].replace('Z', '+00:00')).timestamp() * 1000)
                except ValueError: continue
            if ts_ms is not None and 'close' in source: chart_points.append([ts_ms, source.get('close')])
        return jsonify(chart_points)
    except NotFoundError: return jsonify([])
    except Exception as e:
        print(f"Error fetching 1m chart data for {symbol_str}: {e}")
        return jsonify({"error": str(e)}), 500

# --- Historical Data Routes ---
@app.route('/historical')
def historical_page():
    if not es_client:
        return "Elasticsearch client not available.", 503
    
    available_hist = get_available_symbols_timeframes_historical()
    
    initial_hist_symbol = "ETH_USDT_1h" 
    if initial_hist_symbol not in available_hist:
        initial_hist_symbol = available_hist[0] if available_hist else None
        
    return render_template('historical_data.html',
                           available_symbols_timeframes=available_hist,
                           initial_symbol_timeframe=initial_hist_symbol)

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
    must_clauses = [{"term": {"symbol": symbol}}, {"term": {"timeframe": timeframe}}]
    if start_time_seconds: must_clauses.append({"range": {"timestamp": {"gte": start_time_seconds}}})
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
                except Exception as e: print(f"Error converting hist ts {ts_seconds} for {symbol}_{timeframe}: {e}")
        datasets = [
            {'label': f'{symbol} Close ({timeframe})', 'data': close_prices, 'borderColor': 'rgb(75, 192, 192)', 'tension': 0.1, 'fill': False},
            {'label': f'{symbol} SMA 7 ({timeframe})', 'data': sma7_prices, 'borderColor': 'rgb(255, 159, 64)', 'tension': 0.1, 'fill': False, 'hidden': True},
            {'label': f'{symbol} SMA 30 ({timeframe})', 'data': sma30_prices, 'borderColor': 'rgb(153, 102, 255)', 'tension': 0.1, 'fill': False, 'hidden': True}
        ]
        return jsonify({'labels': labels, 'datasets': datasets})
    except NotFoundError: return jsonify({"error": f"Index {ES_HISTORICAL_INDEX} not found."}), 404
    except Exception as e:
        print(f"Error in api_historical_data for {symbol_timeframe_str}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/predict_xgboost/<symbol_timeframe_str>')
def api_predict_xgboost(symbol_timeframe_str):
    parts = symbol_timeframe_str.split('_')
    if len(parts) < 2: return jsonify({"error": "Invalid format. Expected SYMBOL_TIMEFRAME (e.g., BTC_USDT_1h)"}), 400
    timeframe_req = parts[-1] 
    symbol_req = "_".join(parts[:-1]) 
    if timeframe_req != "1h": 
        return jsonify({"error": f"XGBoost prediction only for 1h timeframe. Requested: {timeframe_req}"}), 400
    model_specific_window_size = 0
    if "BTC_USDT" in symbol_req: model_specific_window_size = 5
    elif "ETH_USDT" in symbol_req: model_specific_window_size = 24
    else: return jsonify({"error": f"No specific XGBoost window size configured for symbol {symbol_req}."}), 400
    model_filename = os.path.join(XGBOOST_MODEL_PATH, f"{symbol_req}_xgboost_model.pkl")
    scaler_filename = os.path.join(XGBOOST_MODEL_PATH, f"{symbol_req}_scaler.pkl")
    if not os.path.exists(model_filename): return jsonify({"error": f"Model for {symbol_req} not found."}), 404
    if not os.path.exists(scaler_filename): return jsonify({"error": f"Scaler for {symbol_req} not found."}), 404
    try:
        model = joblib.load(model_filename)
        scaler = joblib.load(scaler_filename)
        if not (hasattr(scaler, 'min_') and hasattr(scaler, 'scale_')):
             return jsonify({"error": f"Scaler {scaler_filename} is loaded but seems not fitted."}), 500
        if hasattr(model, 'n_features_in_') and model.n_features_in_ != model_specific_window_size:
            return jsonify({"error": f"Model feature mismatch. Expects {model.n_features_in_}, config {model_specific_window_size}."}), 500
    except Exception as e: return jsonify({"error": f"Failed to load model or scaler: {str(e)}"}), 500
    query_body_hist = {
        "size": model_specific_window_size, 
        "query": {"bool": {"must": [{"term": {"symbol": symbol_req}}, {"term": {"timeframe": timeframe_req}}]}},
        "sort": [{"timestamp": "desc"}]}
    try:
        res_hist = es_client.search(index=ES_HISTORICAL_INDEX, body=query_body_hist)
        hits = res_hist['hits']['hits']
        if len(hits) < model_specific_window_size:
            return jsonify({"error": f"Not enough recent historical data for {symbol_req} (found {len(hits)}, need {model_specific_window_size})."}), 404
        current_prices_for_input = np.array([hit['_source']['close'] for hit in reversed(hits)])
        last_known_timestamp_seconds = hits[0]['_source']['timestamp']
        last_known_timestamp_dt = datetime.fromtimestamp(last_known_timestamp_seconds, timezone.utc)
        predictions_24h = []
        current_window_scaled = scaler.transform(current_prices_for_input.reshape(-1, 1)).flatten()
        for i in range(PREDICTION_STEPS_XGBOOST):
            input_for_model = current_window_scaled.reshape(1, -1)
            predicted_price_scaled = model.predict(input_for_model)[0]
            predicted_price_actual = scaler.inverse_transform(np.array([[predicted_price_scaled]]))[0,0]
            next_timestamp_dt = last_known_timestamp_dt + timedelta(hours=(i + 1))
            predictions_24h.append({"timestamp": int(next_timestamp_dt.timestamp() * 1000), "predicted_price": float(predicted_price_actual)})
            current_window_scaled = np.append(current_window_scaled[1:], predicted_price_scaled)
        return jsonify(predictions_24h)
    except Exception as e:
        print(f"Error in XGBoost prediction API for {symbol_req}_{timeframe_req}: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Internal error during XGBoost prediction: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
