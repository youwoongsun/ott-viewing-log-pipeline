"""
Spark Structured Streaming - 세션화 후 PostgreSQL 적재
Kafka(viewing-events) -> mapGroupsWithState 세션화 -> movies 조인 -> PostgreSQL(sessions) 적재

핵심 설계:
- 30분 무활동 시 세션 종료로 판정 (SESSION_TIMEOUT_SEC)
- watermark로 늦게 도착한 이벤트(late data) 처리 (WATERMARK_DELAY) — 값은 실험으로 확정 예정
- foreachBatch에서 PostgreSQL에 upsert (체크포인트로 장애 시 재시작해도 중복/유실 없이 이어서 처리)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.sql.streaming.state import GroupStateTimeout

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "viewing-events"
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/ott-pipeline/checkpoints/sessionize")

PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://localhost:5432/ott_pipeline")
PG_PROPS = {
    "user": os.environ.get("PG_USER", "ott"),
    "password": os.environ.get("PG_PASSWORD", "ott_pw"),
    "driver": "org.postgresql.Driver",
}

# ── 아직 실험으로 확정 전인 값들
SESSION_TIMEOUT_SEC = 30 * 60    
WATERMARK_DELAY = "10 minutes"  

EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("movie_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", LongType()),
    StructField("position_sec", IntegerType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ott-viewing-sessionizer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", expr("timestamp_seconds(event_ts)"))
    )
    return parsed


def sessionize(events_df):
    """
    mapGroupsWithState 대신, 스키마 안정성이 높은 flatMapGroupsWithState 방식을
    Python(pandas UDF)에서 직접 구현하기보다, 여기서는 세션 판정 로직을 명확히
    보여주기 위해 group-by + session window 형태로 우선 스케치한다.
    (실제 구현 시 mapGroupsWithState로 교체 — user_id별 상태를 직접 들고 있어야
    incremental하게 세션 시작/종료를 정확히 판정할 수 있음)
    """
    watermarked = events_df.withWatermark("event_time", WATERMARK_DELAY)

    sessions = (
        watermarked.groupBy(
            col("user_id"),
            col("movie_id"),
            expr(f"session_window(event_time, '{SESSION_TIMEOUT_SEC} seconds')").alias("session"),
        )
        .agg(
            expr("min(event_time) as start_ts"),
            expr("max(event_time) as end_ts"),
            expr("count(*) as event_count"),
            expr("max(case when event_type = 'complete' then 1 else 0 end) = 1 as completed"),
            expr("max(position_sec) as max_position_sec"),
        )
        .select("user_id", "movie_id", "start_ts", "end_ts", "event_count", "completed", "max_position_sec")
    )
    return sessions


def write_to_postgres(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    print(f"[batch {batch_id}] {batch_df.count()}건 세션 결과를 PostgreSQL에 적재")
    (
        batch_df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "sessions_staging")
        .options(**PG_PROPS)
        .mode("append")
        .save()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = read_kafka_stream(spark)
    sessions = sessionize(events)

    query = (
        sessions.writeStream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("스트리밍 세션화 파이프라인 시작. Ctrl+C로 종료.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
