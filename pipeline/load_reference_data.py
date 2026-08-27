"""
참조 데이터 적재 스크립트 (load_reference_data.py)
======================================================
movies.csv / links.csv / tags.csv / ratings.csv를 PostgreSQL에 적재한다.
Kafka/Spark 파이프라인이 세션화 결과를 만들 때 조인할 "영화 자체 정보"를
미리 채워두는 1회성 배치 작업이다.

실행 전: docker compose up -d 로 Postgres가 떠 있어야 함
실행:    python load_reference_data.py --data-dir /path/to/movielens/csvs
"""

import argparse
import csv
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql+psycopg2://ott:ott_pw@localhost:5432/ott_pipeline"
DEFAULT_CHUNK_ROWS = 500_000  # ratings/tags COPY 적재 시 한 번에 묶을 행 수


def load_movies(engine, data_dir: Path):
    df = pd.read_csv(data_dir / "movies.csv")
    df = df.rename(columns={"movieId": "movie_id"})
    df["genre_list"] = df["genres"].apply(
        lambda g: [] if g == "(no genres listed)" else g.split("|")
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE movies CASCADE"))
        # genre_list(배열)는 to_sql이 못 다루므로 executemany로 직접 삽입
        rows = df[["movie_id", "title", "genres", "genre_list"]].to_dict("records")
        conn.execute(
            text(
                """
                INSERT INTO movies (movie_id, title, genres, genre_list)
                VALUES (:movie_id, :title, :genres, :genre_list)
                ON CONFLICT (movie_id) DO NOTHING
                """
            ),
            rows,
        )
    print(f"  movies: {len(df):,}행 적재 완료")


def load_links(engine, data_dir: Path):
    df = pd.read_csv(data_dir / "links.csv", dtype=str)
    df = df.rename(columns={"movieId": "movie_id", "imdbId": "imdb_id", "tmdbId": "tmdb_id"})
    df = df.where(pd.notna(df), None)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE movie_links"))
        conn.execute(
            text(
                """
                INSERT INTO movie_links (movie_id, imdb_id, tmdb_id)
                VALUES (:movie_id, :imdb_id, :tmdb_id)
                ON CONFLICT (movie_id) DO NOTHING
                """
            ),
            df.to_dict("records"),
        )
    print(f"  movie_links: {len(df):,}행 적재 완료")


def _copy_timestamped_csv(engine, csv_path: Path, table: str, ts_col_in: str,
                           id_cols_in: list, out_cols: list, chunk_rows: int):
    """
    ratings.csv / tags.csv처럼 (id_cols..., value_col, epoch timestamp) 구조인
    대용량 CSV를 청크 단위로 읽어 unix epoch를 ISO timestamp로 변환하면서 COPY로 적재.

    executemany 방식(row-by-row 파라미터 바인딩)은 ml-25m 규모(ratings 2,500만 행)에서
    사실상 멈추거나 메모리를 다 먹는다. COPY는 같은 데이터를 몇 분 내로 적재하는
    대량 적재 전용 경로라, 여기서도 ingest_v2_events.py와 동일한 패턴을 쓴다.

    id_cols_in: CSV 원본 컬럼명 기준 앞부분 컬럼들 (예: ["userId", "movieId", "tag"])
    ts_col_in : CSV 원본의 epoch timestamp 컬럼명 ("timestamp")
    out_cols  : COPY 대상 테이블 컬럼 순서 (마지막이 timestamp 컬럼)
    """
    t0 = time.time()
    total = 0
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table}")
        raw_conn.commit()

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            in_idx = [header.index(c) for c in id_cols_in]
            ts_idx = header.index(ts_col_in)

            buf_rows = []

            def flush(buf_rows):
                nonlocal total
                if not buf_rows:
                    return
                out = io.StringIO()
                writer = csv.writer(out)
                for row in buf_rows:
                    values = [row[i] for i in in_idx]
                    ts_iso = datetime.fromtimestamp(float(row[ts_idx]), tz=timezone.utc).isoformat()
                    writer.writerow(values + [ts_iso])
                out.seek(0)
                with raw_conn.cursor() as cur:
                    cur.copy_expert(
                        f"COPY {table} ({', '.join(out_cols)}) FROM STDIN WITH (FORMAT csv, NULL '')",
                        out,
                    )
                raw_conn.commit()
                total += len(buf_rows)

            for row in reader:
                buf_rows.append(row)
                if len(buf_rows) >= chunk_rows:
                    flush(buf_rows)
                    buf_rows = []
                    print(f"    ...{total:,}행 적재 중 ({time.time()-t0:.0f}초 경과)")
            flush(buf_rows)
    finally:
        raw_conn.close()

    print(f"  {table}: {total:,}행 COPY 적재 완료 ({time.time()-t0:.1f}초)")


def load_tags(engine, data_dir: Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
    _copy_timestamped_csv(
        engine, data_dir / "tags.csv", table="tags",
        ts_col_in="timestamp",
        id_cols_in=["userId", "movieId", "tag"],
        out_cols=["user_id", "movie_id", "tag", "tagged_at"],
        chunk_rows=chunk_rows,
    )


def load_ratings(engine, data_dir: Path, chunk_rows: int = DEFAULT_CHUNK_ROWS):
    _copy_timestamped_csv(
        engine, data_dir / "ratings.csv", table="ratings_raw",
        ts_col_in="timestamp",
        id_cols_in=["userId", "movieId", "rating"],
        out_cols=["user_id", "movie_id", "rating", "rated_at"],
        chunk_rows=chunk_rows,
    )


def main():
    parser = argparse.ArgumentParser(description="MovieLens 참조 데이터를 PostgreSQL에 적재")
    parser.add_argument("--data-dir", required=True, help="movies.csv 등이 있는 디렉터리")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS,
                         help="ratings/tags COPY 청크 크기 (기본 500,000행). ml-25m처럼 큰 파일일수록 "
                              "이 값을 키우면 커밋 횟수는 줄지만 청크 하나 실패 시 롤백되는 범위가 커진다")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not (data_dir / "movies.csv").exists():
        sys.exit(f"movies.csv를 찾을 수 없습니다: {data_dir}")

    engine = create_engine(args.db_url)
    print("참조 데이터 적재 시작...")
    load_movies(engine, data_dir)
    load_links(engine, data_dir)
    load_tags(engine, data_dir, chunk_rows=args.chunk_rows)
    load_ratings(engine, data_dir, chunk_rows=args.chunk_rows)
    print("완료.")


if __name__ == "__main__":
    main()
