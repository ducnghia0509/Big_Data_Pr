from flask import Flask, render_template, jsonify, request
from elasticsearch import Elasticsearch, NotFoundError
from datetime import datetime, timezone, timedelta
import os

app = Flask(__name__)

# --- Configuration ---
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "192.168.30.128")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ES_HISTORICAL_INDEX = "crypto_historical_data"
ES_OHLCV_LATEST_INDEX = "crypto_ohlcv_1m_latest"    # For latest 1m OHLCV candle
ES_OHLCV_STATS_INDEX = "crypto_ohlcv_1m_stats"     # For windowed stats from 1m OHLCV
ES_CHART_1M_INDEX_FROM_SPARK = "crypto_ohlcv_1m_chartdata" # For raw 1m OHLCV for chart
ES_CHART_1M_INDEX_PATTERN_FOR_QUERY = "crypto_ohlcv_1m_chartdata-*"
# --- Initialize Elasticsearch Client ---
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
