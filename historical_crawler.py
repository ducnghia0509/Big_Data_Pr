# -*- coding: utf-8 -*-
import ccxt
import pandas as pd
import time
import os
import subprocess # Để chạy lệnh hdfs
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Cấu hình từ .env file ---
SYMBOLS_STR = os.getenv('CRYPTO_SYMBOLS', 'BTC/USDT,ETH/USDT')
SYMBOLS = [s.strip() for s in SYMBOLS_STR.split(',')]
TIMEFRAME = os.getenv('CRYPTO_TIMEFRAME', '1h')
EXCHANGE_ID = os.getenv('CRYPTO_EXCHANGE', 'binance') 
START_DATE_STR = os.getenv('CRYPTO_START_DATE')
END_DATE_STR = os.getenv('CRYPTO_END_DATE') 

LOCAL_SAVE_PATH = './temp_crypto_data'
HDFS_USER = os.environ.get("USER", "hadoop") 
HDFS_TARGET_PATH = f'/user/{HDFS_USER}/crypto_project/raw_historical_data'

# --- Validate configurations ---
if not START_DATE_STR:
    print("Lỗi: CRYPTO_START_DATE phải được thiết lập trong file .env")
    exit(1)

# --- Kết nối sàn ---
try:
    exchange = getattr(ccxt, EXCHANGE_ID)()
    if exchange.has['fetchOHLCV']:
        exchange.load_markets()
        print(f"Kết nối thành công sàn {EXCHANGE_ID}")
    else:
        print(f"Sàn {EXCHANGE_ID} không hỗ trợ fetchOHLCV.")
        exit(1)
except Exception as e:
    print(f"Lỗi kết nối sàn {EXCHANGE_ID}: {e}")
    exit(1)

# --- Hàm tải dữ liệu ---
def fetch_historical_data(symbol, timeframe, since_timestamp, end_timestamp=None):
    all_ohlcv = []
    limit = 1000
    print(f"Bắt đầu tải {symbol} từ {datetime.fromtimestamp(since_timestamp / 1000, timezone.utc)}...")
    if end_timestamp:
        print(f"Sẽ dừng tải trước {datetime.fromtimestamp(end_timestamp / 1000, timezone.utc)}")

    current_fetch_timestamp = since_timestamp
    timeframe_duration_ms = exchange.parse_timeframe(timeframe) * 1000

    while True:
        if end_timestamp and current_fetch_timestamp >= end_timestamp:
            print(f"Đã đạt hoặc vượt qua END_DATE ({datetime.fromtimestamp(end_timestamp / 1000, timezone.utc)}) cho {symbol}. Dừng tải.")
            break
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=current_fetch_timestamp, limit=limit)

            if not ohlcv: # len(ohlcv) == 0
                print(f"Không còn dữ liệu {symbol} sau timestamp {datetime.fromtimestamp(current_fetch_timestamp / 1000, timezone.utc)}")
                break

            if end_timestamp:
                ohlcv = [candle for candle in ohlcv if candle[0] < end_timestamp]
                if not ohlcv:
                    print(f"Tất cả nến trong batch này cho {symbol} đều bắt đầu sau hoặc bằng END_DATE. Dừng.")
                    break
            
            all_ohlcv.extend(ohlcv)
            last_candle_timestamp = ohlcv[-1][0]
            print(f"Đã tải {len(all_ohlcv)} nến {symbol}, timestamp cuối trong batch: {datetime.fromtimestamp(last_candle_timestamp / 1000, timezone.utc)}")

            current_fetch_timestamp = last_candle_timestamp + timeframe_duration_ms

            if end_timestamp and current_fetch_timestamp >= end_timestamp:
                 print(f"Timestamp tiếp theo ({datetime.fromtimestamp(current_fetch_timestamp / 1000, timezone.utc)}) sẽ vượt END_DATE. Dừng tải {symbol}.")
                 break

            time.sleep(exchange.rateLimit / 1000) 

        except ccxt.NetworkError as e:
            print(f"Lỗi mạng khi tải {symbol}: {e}, thử lại sau 5 giây...")
            time.sleep(5)
        except ccxt.ExchangeError as e:
            print(f"Lỗi sàn khi tải {symbol}: {e}")
            break
        except Exception as e:
            print(f"Lỗi không xác định khi tải {symbol}: {e}")
            import traceback
            traceback.print_exc()
            break
    
    print(f"Hoàn tất tải {len(all_ohlcv)} nến cho {symbol}.")
    return all_ohlcv

# --- Xử lý và Lưu trữ ---
if __name__ == "__main__":
    os.makedirs(LOCAL_SAVE_PATH, exist_ok=True)
    # Đảm bảo thư mục HDFS tồn tại
    try:
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', HDFS_TARGET_PATH], check=True, capture_output=True)
        print(f"Thư mục HDFS {HDFS_TARGET_PATH} đã được đảm bảo tồn tại.")
    except subprocess.CalledProcessError as e:
        print(f"Lỗi khi tạo thư mục HDFS {HDFS_TARGET_PATH}: {e.stderr.decode()}")
    except FileNotFoundError:
        print("Lỗi: Lệnh 'hdfs' không tìm thấy. Đảm bảo Hadoop đã được cài đặt và PATH được cấu hình.")
        exit(1)


    start_timestamp_ms = exchange.parse8601(START_DATE_STR)
    end_timestamp_ms = None
    if END_DATE_STR:
        end_timestamp_ms = exchange.parse8601(END_DATE_STR)
        if end_timestamp_ms <= start_timestamp_ms:
            print(f"Lỗi: CRYPTO_END_DATE ({END_DATE_STR}) phải sau CRYPTO_START_DATE ({START_DATE_STR}).")
            exit(1)

    for symbol in SYMBOLS:
        symbol_filename_base = symbol.replace('/', '_') # e.g., BTC_USDT
        csv_filename = f"{symbol_filename_base}_{TIMEFRAME}.csv"
        local_filepath = os.path.join(LOCAL_SAVE_PATH, csv_filename)
        hdfs_filepath = f"{HDFS_TARGET_PATH}/{csv_filename}"

        print(f"\n--- Bắt đầu xử lý cho {symbol} ---")
        ohlcv_data = fetch_historical_data(symbol, TIMEFRAME, start_timestamp_ms, end_timestamp_ms)

        if not ohlcv_data:
            print(f"Không có dữ liệu cho {symbol} trong khoảng thời gian đã cho. Bỏ qua.")
            continue

        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Thêm cột datetime_str để batch processor có thể đọc được nếu schema yêu cầu
        df['datetime_str'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df = df.sort_values('timestamp')

        print(f"Đang lưu {symbol} vào file tạm: {local_filepath}")
        df.to_csv(local_filepath, index=False, header=True) # Luôn ghi header

        print(f"Đang upload {local_filepath} lên HDFS: {hdfs_filepath}")
        try:
            rm_result = subprocess.run(['hdfs', 'dfs', '-rm', '-f', hdfs_filepath], capture_output=True, text=True)

            put_result = subprocess.run(['hdfs', 'dfs', '-put', local_filepath, hdfs_filepath], check=True, capture_output=True, text=True)
            print(f"Upload thành công {symbol} lên HDFS.")
        except subprocess.CalledProcessError as e:
            print(f"Lỗi khi upload {symbol} ({local_filepath}) lên HDFS ({hdfs_filepath}):")
            print("Lệnh:", " ".join(e.cmd))
            print("Return code:", e.returncode)
            print("Error:", e.stderr)
        except FileNotFoundError:
             print("Lỗi: Lệnh 'hdfs' không tìm thấy.")

    print("\n--- Hoàn tất quá trình tải dữ liệu lịch sử ---")
