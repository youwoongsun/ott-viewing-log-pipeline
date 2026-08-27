"""
daily_batch_dag.py — 일 배치로 장르별 트렌드/영화 랭킹 재계산
==================================================================
README 3-4절 설계를 그대로 구현:
  - 일 배치로 전일 랭킹/리텐션 지표 재계산
  - retry/backoff: 지수 백오프, 최대 재시도 횟수 설정
  - 데이터 품질 체크: 집계 건수 이상 감지, null 체크 등

파이프라인에서 이 DAG의 위치:
  Kafka -> Spark(세션화) -> PostgreSQL(sessions 테이블)  ← 여기까지는 실시간
  PostgreSQL(sessions) -> [이 Airflow DAG, 매일 1회 배치] -> daily_genre_trend / daily_movie_ranking

DAG 구조 (4단계, 순서대로 의존):
  1. check_source_data   : sessions 테이블에 대상 날짜 데이터가 있는지, 필수 필드 null이 없는지 확인
  2. compute_genre_trend : 장르별 세션 수 / 완주율 집계 -> daily_genre_trend
  3. compute_movie_ranking : 영화별 세션 수로 랭킹 산정 -> daily_movie_ranking
  4. validate_results    : 방금 쓴 결과가 정상 범위인지 사후 검증 (건수 0이면 실패 처리)

실행 방법 (Airflow 설치 후):
  export AIRFLOW_HOME=~/airflow
  airflow db init
  cp daily_batch_dag.py $AIRFLOW_HOME/dags/
  airflow tasks test daily_batch_dag check_source_data 2024-01-01
  airflow tasks test daily_batch_dag compute_genre_trend 2024-01-01
  airflow tasks test daily_batch_dag compute_movie_ranking 2024-01-01
  airflow tasks test daily_batch_dag validate_results 2024-01-01

  # 또는 스케줄러/웹서버까지 띄워서 매일 자동 실행:
  airflow scheduler &
  airflow webserver &
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import psycopg2

# 로컬(pip install airflow)로 돌릴 땐 localhost, docker-compose로 돌릴 땐
# 서비스명(postgres)으로 접속해야 한다 -- docker-compose.yml이 OTT_PG_DSN 환경변수로 주입해줌
PG_DSN = os.environ.get("OTT_PG_DSN", "postgresql://ott:ott_pw@localhost:5432/ott_pipeline")

default_args = {
    "owner": "ott-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2분 -> 4분 -> 8분 순으로 지수 백오프
    "max_retry_delay": timedelta(minutes=30),
}


def _conn():
    return psycopg2.connect(PG_DSN)


def check_source_data(**context):
    """1. 데이터 품질 체크: 대상 날짜에 세션 데이터가 있는지, 필수 필드 null이 없는지 확인"""
    target_date = context["ds"]  # YYYY-MM-DD
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE movie_id IS NULL OR user_id IS NULL) "
            "FROM sessions WHERE start_ts::date = %s",
            (target_date,),
        )
        total, null_count = cur.fetchone()

    print(f"[품질체크] {target_date} 세션 {total}건, 필수필드 null {null_count}건")
    if total == 0:
        raise AirflowFailException(f"{target_date}에 해당하는 세션 데이터가 없습니다.")
    if null_count > 0:
        raise AirflowFailException(f"{target_date} 세션에 필수 필드 null이 {null_count}건 있습니다.")

    context["ti"].xcom_push(key="session_count", value=total)
    return total


def compute_genre_trend(**context):
    """2. 장르별 세션 수·완주율 집계 -> daily_genre_trend"""
    target_date = context["ds"]
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM daily_genre_trend WHERE trend_date = %s", (target_date,))
        cur.execute(
            """
            INSERT INTO daily_genre_trend (trend_date, genre, session_count, completion_rate)
            SELECT
                %s::date AS trend_date,
                unnest(m.genre_list) AS genre,
                count(*) AS session_count,
                round(avg(s.completion_rate)::numeric, 3) AS completion_rate
            FROM sessions s
            JOIN movies m ON m.movie_id = s.movie_id
            WHERE s.start_ts::date = %s
            GROUP BY unnest(m.genre_list)
            """,
            (target_date, target_date),
        )
        rows = cur.rowcount
        conn.commit()
    print(f"[genre_trend] {target_date}: {rows}개 장르 집계 완료")
    context["ti"].xcom_push(key="genre_rows", value=rows)
    return rows


def compute_movie_ranking(**context):
    """3. 영화별 세션 수로 랭킹 산정 -> daily_movie_ranking"""
    target_date = context["ds"]
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM daily_movie_ranking WHERE trend_date = %s", (target_date,))
        cur.execute(
            """
            INSERT INTO daily_movie_ranking (trend_date, movie_id, rank, session_count)
            SELECT
                %s::date AS trend_date,
                movie_id,
                rank() OVER (ORDER BY session_count DESC) AS rank,
                session_count
            FROM (
                SELECT movie_id, count(*) AS session_count
                FROM sessions
                WHERE start_ts::date = %s
                GROUP BY movie_id
            ) t
            """,
            (target_date, target_date),
        )
        rows = cur.rowcount
        conn.commit()
    print(f"[movie_ranking] {target_date}: {rows}편 랭킹 산정 완료")
    context["ti"].xcom_push(key="ranking_rows", value=rows)
    return rows


def validate_results(**context):
    """4. 사후 검증: 방금 쓴 결과가 비정상적으로 적거나 0건이면 실패 처리"""
    target_date = context["ds"]
    ti = context["ti"]
    session_count = ti.xcom_pull(task_ids="check_source_data", key="session_count")
    genre_rows = ti.xcom_pull(task_ids="compute_genre_trend", key="genre_rows")
    ranking_rows = ti.xcom_pull(task_ids="compute_movie_ranking", key="ranking_rows")

    print(f"[검증] 세션 {session_count}건 -> 장르 {genre_rows}행, 랭킹 {ranking_rows}행")

    if genre_rows == 0:
        raise AirflowFailException("장르 집계 결과가 0행입니다. 세션-영화 조인이 잘못됐을 수 있습니다.")
    if ranking_rows == 0:
        raise AirflowFailException("랭킹 결과가 0행입니다.")
    if ranking_rows > session_count:
        raise AirflowFailException(
            f"랭킹 행 수({ranking_rows})가 세션 수({session_count})보다 많습니다 — 집계 로직 오류 가능성."
        )

    print(f"[검증 통과] {target_date} 배치 정상 완료")


with DAG(
    dag_id="daily_batch_dag",
    default_args=default_args,
    description="OTT 시청 세션 -> 일 배치 장르 트렌드/영화 랭킹 재계산",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ott-pipeline", "batch"],
) as dag:

    t1 = PythonOperator(task_id="check_source_data", python_callable=check_source_data)
    t2 = PythonOperator(task_id="compute_genre_trend", python_callable=compute_genre_trend)
    t3 = PythonOperator(task_id="compute_movie_ranking", python_callable=compute_movie_ranking)
    t4 = PythonOperator(task_id="validate_results", python_callable=validate_results)

    t1 >> [t2, t3] >> t4
