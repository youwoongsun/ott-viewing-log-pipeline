"""
ingest_v2_events.py — v2 이벤트 데이터셋을 PostgreSQL에 대량 적재 (방어적 버전)
====================================================================================
[이력] 최초 버전은 CSV를 그대로 COPY에 흘려보냈는데, 생성기 버그로 event_timestamp
컬럼에 유닉스 epoch 숫자("964979315.268555")와 ISO8601 문자열("2017-05-03T22:...")
포맷이 섞여 있어서 COPY가 통째로 실패/롤백되는 문제가 있었다. 생성기 소스(emit 함수)의
근본 원인은 고쳤지만, 이미 다른 방식으로 만들어진 데이터가 들어올 가능성에 대비해
적재 스크립트 자체도 두 가지 방어 장치를 추가했다:

  1. event_timestamp 정규화: 숫자로 보이면 ISO로 변환, 이미 날짜 형식이면 그대로 통과
  2. 청크 COPY: 파일 전체를 하나의 트랜잭션으로 묶지 않고 N행(기본 100,000)마다
     끊어서 커밋 → 특정 구간에 문제가 있어도 그 청크만 실패하고 나머지는 살아남는다

왜 COPY(그것도 청크 단위)를 쓰는가:
  psycopg2로 한 행씩 INSERT하면 3,700만 건 기준 몇 시간이 걸릴 수 있다. COPY는
  대량 적재 전용 경로라 같은 데이터를 몇 분 내로 적재한다. 다만 COPY는 기본적으로
  파일 하나 = 트랜잭션 하나라서, 행 하나만 형식이 틀려도 전체가 롤백된다. 그래서
  청크 단위로 잘라 커밋하면 "빠름"과 "일부 실패해도 전체는 안전함"을 동시에 얻는다.

실행 전: docker compose up -d 로 Postgres가 떠 있어야 하고,
         sql/init_schema.sql + sql/schema_v2_events.sql이 적용돼 있어야 함

사용 예:
  python ingest_v2_events.py --data-dir /path/to/viewing_events_v2
  python ingest_v2_events.py --data-dir ... --chunk-rows 50000   # 청크 크기 조정
"""

import argparse
import csv
import gzip
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

DEFAULT_DB_URL = "postgresql://ott:ott_pw@localhost:5432/ott_pipeline"
DEFAULT_CHUNK_ROWS = 100_000

EVENTS_COLUMNS = [
    "event_id", "user_id", "movie_id", "movie_title", "genre", "session_id",
    "event_type", "event_timestamp", "position_sec", "segment_index",
    "duration_sec", "session_seq", "total_sessions", "device", "tag_value", "value",
]
HEATMAP_COLUMNS = ["movie_id", "movie_title", "genre", "segment_index", "watch_count", "is_peak_segment"]
ENGAGEMENT_COLUMNS = [
    "user_id", "movie_id", "movie_title", "genre", "total_sessions",
    "avg_completion", "last_completion", "user_rating", "completed_fully", "is_rewatch",
]

TS_COL_INDEX = EVENTS_COLUMNS.index("event_timestamp")


def open_maybe_gzip(path: Path):
    if path.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return open(path, "r", encoding="utf-8", newline="")


def normalize_timestamp(v: str) -> str:
    """
    epoch 숫자('964979315.268555')든 ISO 문자열('2017-05-03T22:18:56+00:00')이든
    최종적으로 ISO 8601 문자열로 통일한다. 이렇게 정규화해두면 생성기가 어떤 방식으로
    만든 데이터가 들어와도 COPY 단계에서 형식 에러가 나지 않는다.
    """
    if not v:
        return v
    try:
        return datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
    except ValueError:
        return v  # 이미 ISO 형식이거나 다른 정상 포맷 -> 그대로 둠


def copy_events_chunked(conn, csv_path: Path, chunk_rows: int):
    """이벤트 CSV를 청크 단위로 정규화 + COPY. (성공행수, 실패청크수) 반환."""
    total_ok = 0
    failed_chunks = 0
    t0 = time.time()

    with open_maybe_gzip(csv_path) as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == EVENTS_COLUMNS, f"컬럼 순서가 예상과 다릅니다: {header}"

        buf_rows = []

        def flush(buf_rows):
            nonlocal total_ok, failed_chunks
            if not buf_rows:
                return
            out = io.StringIO()
            writer = csv.writer(out)
            for row in buf_rows:
                row = list(row)
                row[TS_COL_INDEX] = normalize_timestamp(row[TS_COL_INDEX])
                writer.writerow(row)
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
                print(f"    [청크 실패] {len(buf_rows)}행 롤백됨: {e}", file=sys.stderr)

        for row in reader:
            buf_rows.append(row)
            if len(buf_rows) >= chunk_rows:
                flush(buf_rows)
                buf_rows = []
        flush(buf_rows)

    elapsed = time.time() - t0
    print(f"  {csv_path.name}: {total_ok:,}행 적재 성공, 실패 청크 {failed_chunks}개 | {elapsed:.1f}초")
    return total_ok, failed_chunks


def copy_csv_simple(conn, csv_path: Path, table: str, columns: list, truncate_first: bool = False):
    """heatmap/summary처럼 이미 정규화된 소규모 파일용 (기존 방식 그대로)."""
    t0 = time.time()
    with conn.cursor() as cur:
        if truncate_first:
            cur.execute(f"TRUNCATE {table}")
        with open_maybe_gzip(csv_path) as f:
            col_list = ", ".join(columns)
            cur.copy_expert(
                f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')",
                f,
            )
    conn.commit()
    elapsed = time.time() - t0
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        total = cur.fetchone()[0]
    print(f"  {csv_path.name} -> {table}: {elapsed:.1f}초 | 테이블 누적 {total:,}행")


def main():
    ap = argparse.ArgumentParser(description="v2 이벤트 데이터셋을 PostgreSQL에 청크 COPY로 대량 적재")
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--db-url", default=DEFAULT_DB_URL)
    ap.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS,
                     help="COPY 한 번에 묶을 행 수 (기본 100,000). 실패 시 이 단위로만 롤백됨")
    ap.add_argument("--skip-events", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)

    print("=" * 60)
    print(f"1. raw_viewing_events 적재 (청크={args.chunk_rows:,}행 단위)")
    print("=" * 60)
    if not args.skip_events:
        part_files = sorted(data_dir.glob("viewing_events_part*.csv*"))
        if not part_files:
            sys.exit(f"viewing_events_part*.csv(.gz) 파일을 찾을 수 없습니다: {data_dir}")

        with psycopg2.connect(args.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE raw_viewing_events")
            conn.commit()

        grand_total, grand_failed = 0, 0
        for fp in part_files:
            with psycopg2.connect(args.db_url) as conn:
                ok, failed = copy_events_chunked(conn, fp, args.chunk_rows)
                grand_total += ok
                grand_failed += failed
        print(f"\n  전체 이벤트 적재: {grand_total:,}행 성공 / 실패 청크 {grand_failed}개")
        if grand_failed:
            print("  경고: 일부 청크가 실패했습니다. 위 로그의 에러 메시지를 확인하세요.", file=sys.stderr)
    else:
        print("  --skip-events 지정됨, 건너뜀")

    print("\n" + "=" * 60)
    print("2. movie_segment_heatmap 적재")
    print("=" * 60)
    heatmap_fp = next(data_dir.glob("movie_segment_heatmap.csv*"), None)
    if heatmap_fp is None:
        sys.exit("movie_segment_heatmap.csv(.gz)를 찾을 수 없습니다")
    with psycopg2.connect(args.db_url) as conn:
        copy_csv_simple(conn, heatmap_fp, "movie_segment_heatmap", HEATMAP_COLUMNS, truncate_first=True)

    print("\n" + "=" * 60)
    print("3. user_movie_engagement_summary 적재")
    print("=" * 60)
    eng_fp = next(data_dir.glob("user_movie_engagement_summary.csv*"), None)
    if eng_fp is None:
        sys.exit("user_movie_engagement_summary.csv(.gz)를 찾을 수 없습니다")
    with psycopg2.connect(args.db_url) as conn:
        copy_csv_simple(conn, eng_fp, "user_movie_engagement_summary", ENGAGEMENT_COLUMNS, truncate_first=True)

    print("\n완료.")


if __name__ == "__main__":
    main()
