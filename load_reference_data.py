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
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DB_URL = "postgresql+psycopg2://ott:ott_pw@localhost:5432/ott_pipeline"


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


def load_tags(engine, data_dir: Path):
    df = pd.read_csv(data_dir / "tags.csv")
    df = df.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    df["tagged_at"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE tags"))
        conn.execute(
            text(
                """
                INSERT INTO tags (user_id, movie_id, tag, tagged_at)
                VALUES (:user_id, :movie_id, :tag, :tagged_at)
                """
            ),
            df[["user_id", "movie_id", "tag", "tagged_at"]].to_dict("records"),
        )
    print(f"  tags: {len(df):,}행 적재 완료 (비정형 필드)")


def load_ratings(engine, data_dir: Path):
    df = pd.read_csv(data_dir / "ratings.csv")
    df = df.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    df["rated_at"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE ratings_raw"))
        conn.execute(
            text(
                """
                INSERT INTO ratings_raw (user_id, movie_id, rating, rated_at)
                VALUES (:user_id, :movie_id, :rating, :rated_at)
                """
            ),
            df[["user_id", "movie_id", "rating", "rated_at"]].to_dict("records"),
        )
    print(f"  ratings_raw: {len(df):,}행 적재 완료")


def main():
    parser = argparse.ArgumentParser(description="MovieLens 참조 데이터를 PostgreSQL에 적재")
    parser.add_argument("--data-dir", required=True, help="movies.csv 등이 있는 디렉터리")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not (data_dir / "movies.csv").exists():
        sys.exit(f"movies.csv를 찾을 수 없습니다: {data_dir}")

    engine = create_engine(args.db_url)
    print("참조 데이터 적재 시작...")
    load_movies(engine, data_dir)
    load_links(engine, data_dir)
    load_tags(engine, data_dir)
    load_ratings(engine, data_dir)
    print("완료.")


if __name__ == "__main__":
    main()
