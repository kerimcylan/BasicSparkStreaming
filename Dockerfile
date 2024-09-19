FROM bitnami/spark:latest

RUN pip install --no-cache-dir pyspark confluent-kafka cassandra-driver six

COPY ./app /app

WORKDIR /app

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0", "/app/streaming.py"]


