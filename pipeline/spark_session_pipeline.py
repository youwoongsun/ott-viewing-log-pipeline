"""
Spark Structured Streaming - 세션화 후 PostgreSQL 적재
Kafka(viewing-events) -> session_window 세션화 -> PostgreSQL(sessions) 적재

핵심 설계:
- 30분 무활동 시 세션 종료로 판정 (SESSION_TIMEOUT_SEC)
- watermark로 늦게 도착한 이벤트(late data) 처리 (WATERMARK_DELAY)
- session_id는 (user_id, movie_id, session 시작시각)을 xxhash64로 해시한 BIGINT.
  같은 이벤트가 재전송되거나(중복 전송 실험) 체크포인트 재시작으로 같은 배치가
  다시 처리되더라도, 같은 세션이면 항상 같은 session_id가 나오므로
  foreachBatch에서 ON CONFLICT (session_id) DO UPDATE로 upsert하면
  중복 없이 최신 상태로만 덮어써진다 (개수가 부풀지 않음).
- foreachBatch에서 psycopg2로 직접 upsert (JDBC 드라이버 다운로드 불필요)
"""

import os

import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, xxhash64
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "viewing-events"
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/ott-pipeline/checkpoints/sessionize")

PG_DSN = os.environ.get(
    "OTT_PG_DSN",
    "postgresql://ott:ott_pw@localhost:5432/ott_pipeline",
)

SESSION_TIMEOUT_SEC = 30 * 60
WATERMARK_DELAY = "10 minutes"
COMPLETION_THRESHOLD = 0.92  # generate_events_v2.py의 completed_fully 기준과 동일하게 맞춤

# kafka_producer.py는 CSV 헤더를 그대로 JSON으로 실어 보낸다.
# 실제 이벤트 생성기(generate_events_v2.py) v2 스키마 기준 (16컬럼) 중
# 세션화에 필요한 필드만 정의한다. 정의 안 한 필드는 from_json이 무시한다.
EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("movie_id", IntegerType()),
    StructField("genre", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),  # ISO8601 문자열, 예: 2018-01-01T00:00:00+00:00
    StructField("position_sec", DoubleType()),
    StructField("duration_sec", DoubleType()),
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
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        # event_timestamp가 파싱 안 되는 값(장애 실험 (A) 잘못된 입력)이면 null이 되고,
        # 아래 필터에서 걸러진다 (파이프라인이 죽지 않고 그 행만 버림).
        .withColumn("event_time", col("event_timestamp").cast("timestamp"))
        .filter(col("event_time").isNotNull() & col("user_id").isNotNull() & col("movie_id").isNotNull())
    )
    return parsed


def sessionize(events_df):
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
            expr("max(position_sec) as max_position_sec"),
            expr("max(duration_sec) as duration_sec"),
            expr("first(genre, true) as genre"),
        )
        .withColumn(
            "completion_rate",
            expr("CASE WHEN duration_sec > 0 THEN least(max_position_sec / duration_sec, 1.0) ELSE 0.0 END"),
        )
        .withColumn("completed", col("completion_rate") >= COMPLETION_THRESHOLD)
        # (user_id, movie_id, session 시작시각)이 같으면 항상 같은 session_id가 나오도록
        # 결정론적 해시로 BIGINT PK를 만든다. 원본 CSV의 session_id(예: "u1-m2-s1")는
        # 문자열이라 sessions 테이블의 BIGINT PK와 타입이 안 맞아 그대로는 못 쓴다.
        .withColumn("session_id", xxhash64(col("user_id"), col("movie_id"), col("session.start")))
        .select(
            "session_id", "user_id", "movie_id", "start_ts", "end_ts",
            "event_count", "completed", "completion_rate", "genre",
        )
    )
    return sessions


UPSERT_SQL = """
    INSERT INTO sessions
        (session_id, user_id, movie_id, start_ts, end_ts, event_count, completed, completion_rate, genre_list)
    VALUES (%(session_id)s, %(user_id)s, %(movie_id)s, %(start_ts)s, %(end_ts)s,
            %(event_count)s, %(completed)s, %(completion_rate)s, %(genre_list)s)
    ON CONFLICT (session_id) DO UPDATE SET
        start_ts        = EXCLUDED.start_ts,
        end_ts          = EXCLUDED.end_ts,
        event_count     = EXCLUDED.event_count,
        completed       = EXCLUDED.completed,
        completion_rate = EXCLUDED.completion_rate,
        genre_list      = EXCLUDED.genre_list
"""

# session_window는 새 이벤트가 두 세션 사이 빈틈을 메우면 그 둘을 하나의 더 큰 윈도우로
# "병합"한다. 이때 session.start(윈도우 시작 시각)가 바뀌므로, 거기서 파생된 session_id도
# 통째로 바뀐다. 그대로 두면 병합 전에 만들어졌던 옛 session_id 행이 테이블에 고아로
# 남아서, 이벤트가 실제로는 새 세션에 흡수됐는데도 sessions 테이블엔 "안 합쳐진 예전 조각"
# 이 그대로 남아있는 상태가 된다 (event_count 합계가 실제 전송량보다 작게 나오는 원인).
# 그래서 새 세션 행을 upsert하기 직전에, 같은 (user_id, movie_id)에서 시간 구간이 겹치는
# 다른 session_id의 행을 먼저 지운다 — 그게 이번 윈도우에 흡수된 옛 조각이라는 뜻이므로.
DELETE_MERGED_SQL = """
    DELETE FROM sessions
    WHERE user_id = %(user_id)s AND movie_id = %(movie_id)s
      AND session_id <> %(session_id)s
      AND start_ts <= %(end_ts)s
      AND end_ts >= %(start_ts)s
"""


def write_to_postgres(batch_df, batch_id: int):
    rows = batch_df.collect()
    if not rows:
        print(f"[batch {batch_id}] 처리할 세션 없음")
        return

    # Postgres가 죽어있으면(장애 실험 D) 여기서 psycopg2.OperationalError가 그대로
    # 위로 던져져서 query.awaitTermination()이 예외로 종료된다 (의도된 동작).
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                for r in rows:
                    params = {
                        "session_id": r["session_id"],
                        "user_id": r["user_id"],
                        "movie_id": r["movie_id"],
                        "start_ts": r["start_ts"],
                        "end_ts": r["end_ts"],
                        "event_count": r["event_count"],
                        "completed": bool(r["completed"]),
                        "completion_rate": float(r["completion_rate"]),
                        "genre_list": [r["genre"]] if r["genre"] else None,
                    }
                    cur.execute(DELETE_MERGED_SQL, params)
                    cur.execute(UPSERT_SQL, params)
        print(f"[batch {batch_id}] {len(rows)}건 세션 upsert 완료 (병합된 옛 세션 정리 포함)")
    finally:
        conn.close()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = read_kafka_stream(spark)
    sessions = sessionize(events)

    query = (
        sessions.writeStream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        # session_window는 Spark 사양상 "update" 모드를 지원하지 않는다
        # (AnalysisException: Update output mode not supported for session window).
        # "append"만 지원되며, 이는 watermark를 넘어 완전히 닫힌 세션만 한 번 내보낸다는
        # 뜻이라 — 세션이 나중에 병합되며 결과가 여러 번 바뀔 걱정 자체가 없다.
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("스트리밍 세션화 파이프라인 시작. Ctrl+C로 종료.")
    query.awaitTermination()


if __name__ == "__main__":
    main()

