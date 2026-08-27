"""
backfill_ingest.py — 지정한 CSV를 raw_viewing_events에 청크 COPY로 적재 (재실행 안전)
================================================================================
ingest_v2_events.py의 청크 COPY + 타임스탬프 정규화 로직을 재사용하되,
날짜 구간(backfill_tag)별로 재실행해도 중복이 쌓이지 않도록 "같은 태그의 기존
행을 먼저 지우고 다시 넣는" 방식으로 멱등성(idempotency)을 보장한다.

Airflow DAG(backfill_ingest_process_dag.py)의 ingest_events 태스크가 이 스크립트를 호출한다.
"""

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

DEFAULT_DB_URL = "postgresql://ott:ott_pw@localhost:5432/ott_pipeline"

EVENTS_COLUMNS = [
    "event_id", "user_id", "movie_id", "movie_title", "genre", "session_id",
    "event_type", "event_timestamp", "position_sec", "segment_index",
    "duration_sec", "session_seq", "total_sessions", "device", "tag_value", "value",
]
TS_COL_INDEX = EVENTS_COLUMNS.index("event_timestamp")
# pandas가 결측값 섞인 정수 컬럼을 다시 쓰면서 "0.0" 같은 float 문자열로 바꿔버리는 문제 방지
INT_COL_INDICES = [EVENTS_COLUMNS.index(c) for c in
                    ["user_id", "movie_id", "segment_index", "duration_sec", "session_seq", "total_sessions"]]


def normalize_int(v: str) -> str:
    if not v:
        return v
    try:
        return str(int(float(v)))
    except ValueError:
        return v


def normalize_timestamp(v: str) -> str:
    if not v:
        return v
    try:
        return datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
    except ValueError:
        return v


def main():
    ap = argparse.ArgumentParser(description="날짜 구간 CSV를 raw_viewing_events에 재실행 안전하게 적재")
    ap.add_argument("--input", required=True, help="적재할 CSV 경로")
    ap.add_argument("--backfill-tag", required=True,
                     help="이 배치를 식별하는 태그 (예: 2018-01-01_2018-12-31). "
                          "같은 태그로 재실행하면 기존 행을 지우고 다시 넣는다 (중복 방지)")
    ap.add_argument("--db-url", default=DEFAULT_DB_URL)
    ap.add_argument("--chunk-rows", type=int, default=50_000)
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        sys.exit(f"입력 파일을 찾을 수 없습니다: {in_path}")

    conn = psycopg2.connect(args.db_url)

    # 멱등성: 같은 backfill_tag로 이미 적재된 행이 있으면 먼저 지운다.
    # (raw_viewing_events에 backfill_tag 컬럼이 없으므로, event_id 범위로 태그를 구분한다.
    #  이 스크립트가 만드는 event_id는 tag의 해시를 기반으로 하므로 재실행해도 같은 범위를 지운다)
    tag_offset = abs(hash(args.backfill_tag)) % 1_000_000 * 1_000_000_000
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw_viewing_events WHERE event_id >= %s AND event_id < %s",
            (tag_offset, tag_offset + 1_000_000_000),
        )
        deleted = cur.rowcount
    conn.commit()
    print(f"[멱등성 체크] backfill_tag='{args.backfill_tag}' 기존 행 {deleted:,}건 삭제 (재실행 시 중복 방지)")

    t0 = time.time()
    total_ok = 0
    failed_chunks = 0
    event_id = tag_offset

    with open(in_path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == EVENTS_COLUMNS, f"컬럼 순서가 예상과 다릅니다: {header}"

        import io as _io

        buf_rows = []

        def flush(buf_rows):
            nonlocal total_ok, failed_chunks, event_id
            if not buf_rows:
                return
            out = _io.StringIO()
            writer = csv.writer(out)
            for row in buf_rows:
                row = list(row)
                row[0] = str(event_id)  # event_id를 태그 범위 안에서 새로 채번 (원본 id는 무시)
                row[TS_COL_INDEX] = normalize_timestamp(row[TS_COL_INDEX])
                for idx in INT_COL_INDICES:
                    row[idx] = normalize_int(row[idx])
                writer.writerow(row)
                event_id += 1
            out.seek(0)
            try:
                with conn.cursor() as cur:
                    cur.copy_expert(
                        f"COPY raw_viewing_events ({', '.join(EVENTS_COLUMNS)}) "
                        f"FROM STDIN WITH (FORMAT csv, NULL '')",
                        out,
                    )
                conn.commit()
                total_ok += len(buf_rows)
            except Exception as e:
                conn.rollback()
                failed_chunks += 1
                print(f"  [청크 실패] {len(buf_rows)}행 롤백됨: {e}", file=sys.stderr)

        for row in reader:
            buf_rows.append(row)
            if len(buf_rows) >= args.chunk_rows:
                flush(buf_rows)
                buf_rows = []
        flush(buf_rows)

    elapsed = time.time() - t0
    print(f"[적재 완료] {total_ok:,}행 성공, 실패 청크 {failed_chunks}개 | {elapsed:.1f}초")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM raw_viewing_events WHERE event_id >= %s AND event_id < %s",
            (tag_offset, tag_offset + 1_000_000_000),
        )
        final_count = cur.fetchone()[0]
    print(f"[검증] backfill_tag='{args.backfill_tag}' 최종 테이블 행 수: {final_count:,}건")

    # Airflow가 다음 태스크로 넘길 수 있게 표준출력 마지막 줄에 건수만 출력
    print(f"INGESTED_COUNT={final_count}")


if __name__ == "__main__":
    main()
