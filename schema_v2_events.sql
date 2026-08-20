-- v2 이벤트 데이터셋 전용 테이블
-- 기존 init_schema.sql(movies, links, tags, ratings_raw, sessions 등)에 추가로 실행

-- ─────────────────────────────────────────────
-- 1. 원본 이벤트 스트림 (viewing_events_part*.csv 그대로 적재)
--    -- 용도: (a) Kafka producer로 흘려보내기 전 "소스" 데이터
--    --       (b) Spark가 계산한 세션 결과를 검증할 정답지(ground truth)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_viewing_events (
    event_id         BIGINT PRIMARY KEY,
    user_id          INTEGER NOT NULL,
    movie_id         INTEGER NOT NULL,
    movie_title      TEXT,
    genre            TEXT,
    session_id       TEXT NOT NULL,
    event_type       TEXT NOT NULL,      -- session_start/segment_watch/pause/seek_forward/
                                          -- seek_backward/session_end/drop_off/rating_given/tag_added
    event_timestamp  TIMESTAMPTZ NOT NULL,
    position_sec     NUMERIC(10,1),
    segment_index    INTEGER,
    duration_sec     INTEGER,            -- 영화 러닝타임(초), 모든 행에 동일하게 반복 기록됨
    session_seq      INTEGER,
    total_sessions   INTEGER,
    device           TEXT,               -- mobile/smart_tv/web/tablet
    tag_value        TEXT,               -- tag_added 이벤트일 때만 값 있음
    value            NUMERIC(4,2)        -- rating_given 이벤트일 때만 값 있음 (평점)
);
CREATE INDEX IF NOT EXISTS idx_rve_user_movie ON raw_viewing_events(user_id, movie_id);
CREATE INDEX IF NOT EXISTS idx_rve_session ON raw_viewing_events(session_id);
CREATE INDEX IF NOT EXISTS idx_rve_event_type ON raw_viewing_events(event_type);
CREATE INDEX IF NOT EXISTS idx_rve_ts ON raw_viewing_events(event_timestamp);

-- ─────────────────────────────────────────────
-- 2. 영화×구간별 시청 heatmap (movie_segment_heatmap.csv)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS movie_segment_heatmap (
    movie_id         INTEGER NOT NULL,
    movie_title      TEXT,
    genre            TEXT,
    segment_index    INTEGER NOT NULL,
    watch_count      INTEGER NOT NULL,
    is_peak_segment  BOOLEAN NOT NULL,
    PRIMARY KEY (movie_id, segment_index)
);
CREATE INDEX IF NOT EXISTS idx_heatmap_peak ON movie_segment_heatmap(movie_id) WHERE is_peak_segment;

-- ─────────────────────────────────────────────
-- 3. 유저×영화 참여도 요약 (user_movie_engagement_summary.csv)
--    -- 부트스트랩 없이 실제 610명 원본 기준이라 유저-영화 조합이 유일함
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_movie_engagement_summary (
    user_id          INTEGER NOT NULL,
    movie_id         INTEGER NOT NULL,
    movie_title      TEXT,
    genre            TEXT,
    total_sessions   INTEGER,
    avg_completion   NUMERIC(5,4),
    last_completion  NUMERIC(5,4),
    user_rating      NUMERIC(2,1),
    completed_fully  BOOLEAN,
    is_rewatch       BOOLEAN,
    PRIMARY KEY (user_id, movie_id)
);
