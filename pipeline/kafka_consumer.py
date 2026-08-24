"""
Kafka Consumer - viewing-events 토픽을 소비해서 건수를 확인하고 파일로 저장
================================================================================
kafka_producer.py가 보낸 메시지를 받아서:
  1. 받은 건수를 카운트 (Producer가 보낸 건수와 비교하기 위함)
  2. 그대로 JSONL 파일로 저장 (Spark가 "Kafka에서 받은 것과 같은 구조"로 처리할 수 있도록)

사용 예:
  # 2000건 받으면 자동 종료
  python kafka_consumer.py --max-messages 2000 --out consumed_events.jsonl

  # 개수 제한 없이 계속 대기 (Ctrl+C로 종료)
  python kafka_consumer.py --out consumed_events.jsonl
"""

import argparse
import json
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer

TOPIC = "viewing-events"


def main():
    ap = argparse.ArgumentParser(description="viewing-events 토픽 소비 + 건수 확인 + 파일 저장")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--group-id", default="week4-consumer")
    ap.add_argument("--out", required=True, help="받은 메시지를 저장할 JSONL 파일 경로")
    ap.add_argument("--max-messages", type=int, default=0, help="0이면 무제한 (Ctrl+C로 종료)")
    ap.add_argument("--timeout-ms", type=int, default=30000, help="이 시간 동안 새 메시지가 없으면 종료")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=args.timeout_ms,
    )

    print(f"Consumer 시작 (topic={TOPIC}, group={args.group_id})")
    received = 0
    start = time.time()
    out_path = Path(args.out)

    with open(out_path, "w", encoding="utf-8") as f:
        for msg in consumer:
            f.write(json.dumps(msg.value, ensure_ascii=False) + "\n")
            received += 1
            if received % 200 == 0:
                print(f"  진행: {received:,}건 수신")
            if args.max_messages and received >= args.max_messages:
                break

    elapsed = time.time() - start
    print(f"\n수신 완료: 총 {received:,}건 | {elapsed:.1f}초 소요 | 저장 위치: {out_path}")
    consumer.close()


if __name__ == "__main__":
    main()
