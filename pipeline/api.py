"""
api.py — sessions 테이블 조회용 최소 FastAPI 서비스
====================================================
파이프라인(Kafka -> Spark -> PostgreSQL)이 저장한 결과(sessions 테이블)를
실제로 "사용하는" 화면 대신, 가장 단순한 형태로 조회할 수 있는 API를 제공한다.
BI 대시보드나 추천 모델 연동은 이번 범위에 포함하지 않고, "저장된 결과를
외부에서 조회할 수 있다"는 것 자체를 검증하는 목적이다.

실행:
  uvicorn pipeline.api:app --reload --port 8000

확인:
  http://127.0.0.1:8000/docs  (Swagger UI에서 바로 테스트 가능)
"""

import os
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "ott_pipeline")
PG_USER = os.environ.get("PG_USER", "ott")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "ott_pw")

app = FastAPI(
    title="OTT Viewing Sessions API",
    description="Kafka→Spark→PostgreSQL 파이프라인이 저장한 세션 집계 결과 조회용 API",
    version="0.1.0",
)


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


@app.get("/health")
def health():
    """DB 연결 자체가 살아있는지 확인 (파이프라인 저장소가 응답하는지 체크)"""
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


@app.get("/stats")
def stats():
    """전체 세션 통계 — 파이프라인이 지금까지 얼마나 처리했는지 한눈에 확인"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    count(*) AS total_sessions,
                    sum(event_count) AS total_events,
                    round(avg(completion_rate)::numeric, 3) AS avg_completion_rate,
                    sum(CASE WHEN completed THEN 1 ELSE 0 END) AS completed_sessions
                FROM sessions
                """
            )
            return cur.fetchone()
    finally:
        conn.close()


@app.get("/sessions")
def list_sessions(
    genre: Optional[str] = Query(None, description="특정 장르만 필터링"),
    completed: Optional[bool] = Query(None, description="완주 여부 필터"),
    limit: int = Query(20, ge=1, le=200),
):
    """세션 목록 조회 (최신순). 실제 저장 결과를 그대로 클라이언트에 내려주는 엔드포인트."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            conditions = []
            params = []
            if genre:
                conditions.append("%s = ANY(genre_list)")
                params.append(genre)
            if completed is not None:
                conditions.append("completed = %s")
                params.append(completed)
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            params.append(limit)
            cur.execute(
                f"""
                SELECT session_id, user_id, movie_id, start_ts, end_ts,
                       event_count, completed, completion_rate, genre_list, ingested_at
                FROM sessions
                {where_clause}
                ORDER BY ingested_at DESC
                LIMIT %s
                """,
                params,
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/sessions/{session_id}")
def get_session(session_id: int):
    """단건 조회"""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sessions WHERE session_id = %s", (session_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="해당 session_id 없음")
            return row
    finally:
        conn.close()
