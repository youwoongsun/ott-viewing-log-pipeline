"""
Kafka Producer - 이벤트 생성기 결과를 viewing-events 토픽으로 스트리밍
generate_events.py(또는 대용량 버전)가 만든 CSV를 읽어서, 실제 서비스처럼 한 건씩 순차적으로 들어오는 형태로 Kafka에 보냄

파티셔닝: user_id 기준 -> 같은 유저의 이벤트 순서를 보장 (세션화 정합성에 필수)

사용 예:
  # 실시간처럼 천천히 (부하 없이 로직 확인용)
  python kafka_producer.py --csv viewing_events_sample_534k.csv --rate 500

  # 최대 속도로 밀어넣기 (장애/부하 실험용)
  python kafka_producer.py --csv viewing_events_42M.csv --rate 0

  # 장애 실험: 브로커 재시작 중에도 몇 건 보냈는지 확인하려면 --report-every로 진행상황 로깅
"""

import argparse
import csv
import gzip
import io
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "viewing-events"


def open_csv(path: Path):
    if path.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return open(path, "r", encoding="utf-8", newline="")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        retries=10,
        retry_backoff_ms=500,
        acks="all",                
        linger_ms=20,
        max_in_flight_requests_per_connection=5,
    )


def stream_csv(csv_path: Path, producer: KafkaProducer, rate_per_sec: int, report_every: int):
    sent = 0
    failed = 0
    start = time.time()
    interval = (1.0 / rate_per_sec) if rate_per_sec > 0 else 0.0

    def coerce(value: str):
        if value == "" or value is None:
            return None
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            return value

    def on_send_error(exc):
        nonlocal failed
        failed += 1
        print(f"  [전송 실패] {exc}", file=sys.stderr)

    with open_csv(csv_path) as f:
        reader = csv.DictReader(f)
        key_field = "user_id" if "user_id" in reader.fieldnames else reader.fieldnames[0]

        for row in reader:
            payload = {k: coerce(v) for k, v in row.items()}
            key = payload.get(key_field)
            producer.send(TOPIC, key=key, value=payload).add_errback(on_send_error)
            sent += 1

            if interval:
                time.sleep(interval)

            if sent % report_every == 0:
                elapsed = time.time() - start
                print(f"  진행: {sent:,}건 전송 | {elapsed:.1f}초 경과 | 실패 {failed}건")

    producer.flush()
    elapsed = time.time() - start
    print(f"\n전송 완료: 총 {sent:,}건 (실패 {failed}건) | {elapsed:.1f}초 소요")
    return sent, failed


def main():
    parser = argparse.ArgumentParser(description="이벤트 CSV를 Kafka viewing-events 토픽으로 스트리밍")
    parser.add_argument("--csv", required=True, help="이벤트 생성기가 만든 CSV 경로")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=200, help="초당 전송 건수 (0이면 제한 없이 최대 속도)")
    parser.add_argument("--report-every", type=int, default=5000)
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"파일을 찾을 수 없습니다: {csv_path}")

    print(f"Kafka producer 시작 (bootstrap={args.bootstrap_servers}, rate={args.rate}/s)")
    producer = build_producer(args.bootstrap_servers)
    try:
        stream_csv(csv_path, producer, args.rate, args.report_every)
    except KafkaError as e:
        print(f"Kafka 오류: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
