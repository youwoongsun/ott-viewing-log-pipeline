-- OTT 시청 로그 파이프라인 - PostgreSQL 스키마
-- docker-compose가 최초 기동 시 자동 실행 (docker-entrypoint-initdb.d)

-- ─────────────────────────────────────────────
-- 1. 참조 데이터 (MovieLens 원본 적재용)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS movies (
    movie_id    INTEGER PRIMARY KEY,
    title       TEXT NOT NULL,
    genres      TEXT,                 -- 파이프(|) 구분 원본 그대로 저장
    genre_list  TEXT[]                -- genres를 배열로 파싱해서 조회 편의성 확보
);

CREATE TABLE IF NOT EXISTS movie_links (
    movie_id    INTEGER PRIMARY KEY REFERENCES movies(movie_id),
    imdb_id     TEXT,
    tmdb_id     TEXT
);

CREATE TABLE IF NOT EXISTS movie_meta_tmdb (
    -- TMDB API로 보강한 영화 부가정보 (포스터, 러닝타임 등)
    movie_id        INTEGER PRIMARY KEY REFERENCES movies(movie_id),
    tmdb_id         TEXT,
    runtime_min     INTEGER,
    release_date    DATE,
    poster_path     TEXT,
    overview        TEXT,
    popularity      NUMERIC,
    fetched_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tags (
    -- 비정형 필드: 사용자가 영화에 자유롭게 붙인 키워드
    user_id     INTEGER NOT NULL,
    movie_id    INTEGER NOT NULL REFERENCES movies(movie_id),
    tag         TEXT NOT NULL,
    tagged_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tags_movie ON tags(movie_id);

CREATE TABLE IF NOT EXISTS ratings_raw (
    -- 이벤트 생성기의 기준점이 된 원본 평점 데이터 (참고/검증용으로 함께 보관)
    user_id     INTEGER NOT NULL,
    movie_id    INTEGER NOT NULL REFERENCES movies(movie_id),
    rating      NUMERIC(2,1) NOT NULL,
    rated_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ratings_user ON ratings_raw(user_id);

-- ─────────────────────────────────────────────
-- 2. Spark가 세션화한 결과가 최종적으로 쌓이는 테이블
--    (Spark Structured Streaming의 foreachBatch에서 이 테이블에 upsert)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    session_id      BIGINT PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    movie_id        INTEGER NOT NULL,
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ NOT NULL,
    event_count     INTEGER NOT NULL,
    completed       BOOLEAN NOT NULL,
    completion_rate NUMERIC(4,3),        -- position_sec(마지막) / runtime_sec 추정치
    genre_list      TEXT[],              -- movies 테이블과 조인해서 채움
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_movie ON sessions(movie_id);
CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(start_ts);

-- ─────────────────────────────────────────────
-- 3. Airflow 배치가 매일 재계산하는 집계 테이블
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_genre_trend (
    trend_date      DATE NOT NULL,
    genre           TEXT NOT NULL,
    session_count   INTEGER NOT NULL,
    completion_rate NUMERIC(4,3),
    PRIMARY KEY (trend_date, genre)
);

CREATE TABLE IF NOT EXISTS daily_movie_ranking (
    trend_date      DATE NOT NULL,
    movie_id        INTEGER NOT NULL,
    rank            INTEGER NOT NULL,
    session_count   INTEGER NOT NULL,
    PRIMARY KEY (trend_date, movie_id)
);

-- ─────────────────────────────────────────────
-- 4. 장애 실험 결과를 기록해두는 테이블 (README의 "시나리오→문제→대응→결과" 표를 DB화)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS failure_experiments (
    experiment_id   SERIAL PRIMARY KEY,
    scenario        TEXT NOT NULL,        -- 예: 'kafka_broker_restart'
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    events_sent     BIGINT,
    events_received BIGINT,
    duplicate_count BIGINT,
    lost_count      BIGINT,
    max_lag_seconds INTEGER,
    notes           TEXT
);
