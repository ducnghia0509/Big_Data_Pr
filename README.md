# Final Project: Hệ Thống Phân Tích Dữ Liệu Tiền Điện Tử Lambda

Hướng dẫn cài đặt và chạy dự án phân tích dữ liệu tiền điện tử sử dụng kiến trúc Lambda.

## 1. Yêu Cầu Cài Đặt

Trước khi bắt đầu, đảm bảo các thành phần sau đã được cài đặt và cấu hình:

*   **Java Development Kit (JDK):** Phiên bản 8 hoặc 11.
    *   Kiểm tra: `java -version`
*   **Python:** Phiên bản 3.8 trở lên, cùng với `pip`.
    *   Kiểm tra: `python3 --version`, `pip3 --version`
*   **Apache Hadoop (HDFS):** Đã cài đặt và HDFS đang chạy.
    *   Kiểm tra: `hdfs dfs -ls /` (sau khi `start-dfs.sh`)
*   **Apache Spark:** Đã cài đặt. Biến môi trường `SPARK_HOME` nên được thiết lập.
    *   Kiểm tra: `spark-shell --version`
*   **Apache Kafka & ZooKeeper:** Đã cài đặt và đang chạy.
    *   Kiểm tra: `jps` (xem có `QuorumPeerMain` và `Kafka`)
*   **Elasticsearch:** Đã cài đặt và đang chạy.
    *   Kiểm tra: `curl http://<địa_chỉ_ES>:9200`
*   **Thư viện Python:**
    ```bash
    # Tạo và kích hoạt môi trường ảo (khuyến nghị)
    # python3 -m venv venv
    # source venv/bin/activate

    # Cài đặt các thư viện
    pip install -r requirements.txt
    ```
    (File `requirements.txt` cần được tạo từ môi trường phát triển của bạn bằng `pip freeze > requirements.txt`)

## 2. Hướng Dẫn Cấu Hình

1.  **Sao chép file cấu hình mẫu:**
    ```bash
    cp .env.example .env
    ```
2.  **Chỉnh sửa file `.env`:** Mở file `.env` và cập nhật các giá trị sau cho phù hợp với môi trường của bạn:
    *   `CRYPTO_SYMBOLS`: Ví dụ: `"BTC/USDT,ETH/USDT"`
    *   `CRYPTO_TIMEFRAME`: Ví dụ: `"1h"` (cho historical và hourly update)
    *   `CRYPTO_START_DATE`: Cho lần crawl lịch sử đầu tiên (ISO 8601), ví dụ: `"2023-01-01T00:00:00Z"`
    *   `CRYPTO_END_DATE`: Cho lần crawl lịch sử đầu tiên (ISO 8601), ví dụ: `"2024-01-01T00:00:00Z"`
    *   `CRYPTO_EXCHANGE`: Ví dụ: `"binance"`
    *   `KAFKA_BROKER`: Ví dụ: `"localhost:9092"`
    *   `KAFKA_OHLCV_1M_TOPIC`: Ví dụ: `"crypto_ohlcv_1m"`
    *   `ELASTICSEARCH_HOST`: Địa chỉ IP của Elasticsearch server.
    *   `ELASTICSEARCH_PORT`: Port của Elasticsearch (thường là `9200`).
    *   `MINUTE_CHART_FETCH_INTERVAL`: Tần suất (giây) `ohlcv_1m_producer.py` lấy dữ liệu.

3.  **Tạo thư mục HDFS:**
    ```bash
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/raw_historical_data
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/raw_hourly_updates
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/checkpoint # Thư mục cha cho checkpoints
    # Tạo các thư mục con cho checkpoint của từng stream trong stream_processor.py nếu cần
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/checkpoint/stream_ohlcv_1m_processor_latest_ohlcv
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/checkpoint/stream_ohlcv_1m_processor_ohlcv_stats
    hdfs dfs -mkdir -p /user/$(whoami)/crypto_project/checkpoint/stream_ohlcv_1m_processor_raw_ohlcv_chart
    ```

4.  **Tạo Topic Kafka:**
    (Thay `localhost:9092` và tên topic nếu cần, dựa trên file `.env`)
    ```bash
    kafka-topics.sh --create \
      --topic crypto_ohlcv_1m \
      --bootstrap-server localhost:9092 \
      --partitions 1 \
      --replication-factor 1
    ```
    *(Nếu bạn vẫn dùng luồng ticker riêng, tạo thêm topic `crypto_prices` tương tự)*

5.  **Elasticsearch Index Templates & ILM (Khuyến nghị):**
    Để quản lý dữ liệu chart 1 phút (`crypto_ohlcv_1m_chartdata-*`) hiệu quả, nên tạo Index Template và ILM Policy trong Elasticsearch để tự động áp dụng mapping và xóa dữ liệu cũ. Tham khảo tài liệu Elasticsearch để thực hiện.
    *   Ví dụ tạo ILM Policy (xóa sau 2 giờ):
        ```json
        # Chạy trong Kibana Dev Tools hoặc qua API
        PUT _ilm/policy/ohlcv_1m_chart_retention_policy
        {
          "policy": { "phases": { "hot": { "min_age": "0ms", "actions": {} },
              "delete": { "min_age": "2h", "actions": { "delete": {} } } } }
        }
        ```
    *   Ví dụ tạo Index Template:
        ```json
        PUT _index_template/ohlcv_1m_chart_template
        {
          "index_patterns": ["crypto_ohlcv_1m_chartdata-*"],
          "template": {
            "settings": { "number_of_shards": 1, "index.lifecycle.name": "ohlcv_1m_chart_retention_policy" },
            "mappings": { "properties": {
                "symbol": {"type": "keyword"}, "@timestamp": {"type": "date"},
                "timestamp_ms": {"type": "long"}, "open": {"type": "double"},
                "high": {"type": "double"}, "low": {"type": "double"},
                "close": {"type": "double"}, "volume": {"type": "double"}
            }}
          }
        }
        ```

## 3. Hướng Dẫn Chạy Hệ Thống

Khởi động các dịch vụ nền tảng trước: HDFS, ZooKeeper, Kafka, Elasticsearch.
Kích hoạt môi trường ảo Python: `source venv/bin/activate`

### A. Luồng Batch Layer (Dữ liệu Lịch Sử)

1.  **Crawl dữ liệu lịch sử ban đầu (chạy một lần):**
    ```bash
    python historical_crawler.py
    ```
    *(Dữ liệu được lưu vào HDFS `raw_historical_data/`)*

2.  **Xử lý batch ban đầu (chạy một lần sau bước A.1):**
    *(Tùy chọn: Xóa index ES cũ: `curl -X DELETE "http://<ES_HOST>:9200/crypto_historical_data"`)*
    ```bash
    spark-submit \
      --master local[*] \
      --jars ./elasticsearch-spark-30_2.12-8.14.0.jar \
      ./batch_processor.py
    ```

### B. Luồng Speed Layer (Dữ liệu Real-time OHLCV 1 phút)

1.  **Chạy Kafka Producer cho OHLCV 1 phút:**
    ```bash
    python ohlcv_1m_producer.py
    ```
    *(Script này chạy liên tục, gửi dữ liệu vào topic `crypto_ohlcv_1m`)*

2.  **Chạy Spark Streaming Processor:**
    *(Tùy chọn: Xóa checkpoint HDFS cũ nếu muốn reset state: `hdfs dfs -rm -r /user/$(whoami)/crypto_project/checkpoint/stream_ohlcv_1m_processor*`)*
    ```bash
    spark-submit \
      --master local[2] \ # Hoặc local[N] tùy tài nguyên VM
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4 \
      stream_processor.py
    ```
    *(Script này chạy liên tục, xử lý dữ liệu từ Kafka và ghi vào các index ES real-time)*

### C. Chạy Ứng Dụng Web (Flask)

Sau khi các luồng dữ liệu đã chạy và có dữ liệu trong Elasticsearch:
```bash
python app.py
```
Truy cập dashboard tại http://<YOUR_VM_IP>:5000.
### D. Tự Động Hóa Cập Nhật Dữ Liệu Lịch Sử Hàng Giờ (Cron)

Đảm bảo file run_batch_processor.sh có quyền thực thi:
    chmod +x run_batch_processor.sh

Thêm vào crontab (chạy crontab -e):
(Thay /path/to/project/ bằng đường dẫn tuyệt đối đến thư mục dự án của bạn, ví dụ /root/crypto_project/)

 
# Chạy hourly_updater.py mỗi giờ, vào phút thứ 5
5 * * * * /path/to/project/venv/bin/python /path/to/project/hourly_updater.py >> /path/to/project/logs/hourly_updater.log 2>&1

# Chạy batch_processor.py mỗi giờ, vào phút thứ 10 (hoặc 15, 20)
10 * * * * /bin/bash /path/to/project/run_batch_processor.sh >> /path/to/project/logs/batch_processor_cron.log 2>&1

Tạo thư mục logs nếu chưa có: mkdir -p /path/to/project/logs
