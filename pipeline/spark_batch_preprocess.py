"""
spark_batch_preprocess.py — Kafka로 보낸 것과 같은 구조의 데이터를 Spark 배치로 전처리/저장
=====================================================================================
과제 요구사항:
  - Kafka에 보낸 것과 같은 구조의 데이터를 Spark로 처리 (배치 처리 OK)
  - 프로젝트에 필요한 전처리 수행 후 처리 전후 건수 확인
  - 결과를 파일/DB에 저장, 최종 컬럼/저장 형식 정리

입력: kafka_consumer.py가 저장한 JSONL 파일 (Kafka에서 실제로 받은 것과 동일 스키마)
      또는 producer가 보내기 전 원본 CSV(-jsonl 없이 바로 검증하고 싶을 때)

전처리 내용:
  1. 필수 필드(user_id, movie_id, event_type, event_timestamp) 결측 행 제거
  2. event_timestamp를 실제 timestamp 타입으로 캐스팅 (문자열 -> TimestampType)
  3. position_sec이 음수이거나 duration_sec을 초과하는 비정상 값 제거
  4. 세션(session_id) 단위로 집계: 시작/종료 시각, 이벤트 개수, 완주 여부, 최대 도달 위치

실행 예 (로컬 모드, 클러스터 불필요):
  python spark_batch_preprocess.py --input consumed_events.jsonl --out-dir ./output
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    DoubleType, TimestampType,
)

SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("movie_title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("position_sec", DoubleType(), True),
    StructField("segment_index", IntegerType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("session_seq", IntegerType(), True),
    StructField("total_sessions", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("tag_value", StringType(), True),
    StructField("value", DoubleType(), True),
])


def main():
    ap = argparse.ArgumentParser(description="Kafka 이벤트를 Spark 배치로 전처리/집계/저장")
    ap.add_argument("--input", required=True, help="JSONL 파일 경로 (kafka_consumer.py 산출물과 동일 스키마)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--format", default="parquet", choices=["parquet", "csv"])
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("ott-week4-batch-preprocess")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("1. 원본 이벤트 로드")
    print("=" * 60)
    raw = spark.read.schema(SCHEMA).json(args.input)
    raw_count = raw.count()
    print(f"  처리 전 건수: {raw_count:,}행")

    print("\n" + "=" * 60)
    print("2. 전처리")
    print("=" * 60)

    # (1) 필수 필드 결측 제거
    step1 = raw.dropna(subset=["user_id", "movie_id", "event_type", "event_timestamp"])
    print(f"  필수 필드 결측 제거 후: {step1.count():,}행")

    # (2) 타임스탬프 캐스팅
    step2 = step1.withColumn("event_ts", F.to_timestamp("event_timestamp"))
    step2 = step2.dropna(subset=["event_ts"])
    print(f"  타임스탬프 파싱 실패 제거 후: {step2.count():,}행")

    # (3) 비정상 position_sec 제거 (음수 또는 duration_sec 초과)
    step3 = step2.filter(
        F.col("position_sec").isNull()
        | ((F.col("position_sec") >= 0) & (F.col("position_sec") <= F.col("duration_sec")))
    )
    clean_count = step3.count()
    print(f"  비정상 재생위치 제거 후 (최종): {clean_count:,}행")

    print("\n" + "=" * 60)
    print("3. 세션 단위 집계 (프로젝트에 필요한 전처리)")
    print("=" * 60)
    sessions = (
        step3.groupBy("session_id", "user_id", "movie_id")
        .agg(
            F.first("movie_title").alias("movie_title"),
            F.first("genre").alias("genre"),
            F.first("device").alias("device"),   # 세션당 대표 디바이스 (그룹핑 키가 아니라 첫 값)
            F.min("event_ts").alias("start_ts"),
            F.max("event_ts").alias("end_ts"),
            F.count("*").alias("event_count"),
            F.max(F.when(F.col("event_type") == "session_end", 1).otherwise(0)).alias("completed"),
            F.max("position_sec").alias("max_position_sec"),
            F.first("duration_sec").alias("duration_sec"),
        )
        .withColumn("completion_rate", F.round(F.col("max_position_sec") / F.col("duration_sec"), 3))
        .withColumn("completed", F.col("completed") == 1)
    )
    session_count = sessions.count()
    print(f"  집계된 세션 수: {session_count:,}개")

    print("\n" + "=" * 60)
    print("4. 결과 저장")
    print("=" * 60)
    final_cols = [
        "session_id", "user_id", "movie_id", "movie_title", "genre", "device",
        "start_ts", "end_ts", "event_count", "completed", "max_position_sec",
        "duration_sec", "completion_rate",
    ]
    result = sessions.select(*final_cols).orderBy("start_ts")

    out_path = out_dir / f"sessions_batch.{args.format}"
    if args.format == "parquet":
        result.write.mode("overwrite").parquet(str(out_path))
    else:
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_path))

    print(f"  저장 완료: {out_path} ({args.format})")
    print(f"\n[요약]")
    print(f"  처리 전 이벤트: {raw_count:,}행")
    print(f"  처리 후 이벤트: {clean_count:,}행 (제거: {raw_count - clean_count:,}행)")
    print(f"  집계된 세션: {session_count:,}개")
    print(f"  최종 컬럼: {final_cols}")

    result.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
