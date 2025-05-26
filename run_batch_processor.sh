#!/bin/bash

# Thiết lập các biến môi trường cần thiết cho Spark nếu cron không tự nhận
# export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 # Ví dụ
# export SPARK_HOME=/opt/spark # Ví dụ
# export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_HOME=~/hadoop
# Đường dẫn đến thư mục dự án
PROJECT_DIR="/root/crypto_project"
# Đường dẫn đến Spark virtual environment (nếu có python dependencies cho Spark job)
# PYSPARK_PYTHON="$PROJECT_DIR/venv/bin/python"

echo "Starting batch_processor.py via cron at $(date)"

# Di chuyển đến thư mục dự án để các đường dẫn tương đối (như JAR) hoạt động
cd "$PROJECT_DIR"

# export PYSPARK_PYTHON="$PROJECT_DIR/venv/bin/python"

"$PROJECT_DIR/venv/bin/spark-submit" \
  --master local[2] \
  --jars ./elasticsearch-spark-30_2.12-8.14.0.jar \
  ./batch_processor.py
echo "Finished batch_processor.py via cron at $(date)"
