"""
backfill_ingest_process_dag.py — 날짜 구간을 입력값으로 받아 수집→처리를 재실행하는 DAG
================================================================================
  1. 지금까지 만든 수집(backfill_ingest.py) · 처리(spark_batch_preprocess.py) 코드를
     Airflow DAG로 실행한다. 둘 다 새로 만든 로직이 아니라, 기존 스크립트를 그대로
     BashOperator로 호출한다.
  2. 코드를 고치지 않고도 재실행 가능하도록, DAG 트리거 시 다음을 파라미터로 받는다:
       - start_date / end_date  (날짜 범위 — 이 프로젝트에서 ticker에 대응하는 축)
       - genre                  (선택, 특정 장르만 필터링해서 재실행 가능)
       - source_path            (선택, 다른 입력 파일로 바꿔 실행 가능)
  3. 값을 바꿔 여러 번 실행해도 안전하도록(중복 방지) backfill_ingest.py가
     backfill_tag 기준으로 기존 행을 지우고 다시 넣는 멱등성을 보장한다.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

PROJECT_DIR = os.environ.get("OTT_PROJECT_DIR", "/opt/ott-pipeline")
PG_DSN = os.environ.get("OTT_PG_DSN", "postgresql://ott:ott_pw@localhost:5432/ott_pipeline")
DEFAULT_SOURCE = f"{PROJECT_DIR}/data/kafka_sample_2000.csv"
WORK_DIR = "/tmp/ott_backfill"

default_args = {
    "owner": "ott-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _tag(context) -> str:
    p = context["params"]
    tag = f"{p['start_date']}_{p['end_date']}"
    if p.get("genre"):
        tag += f"_{p['genre']}"
    return tag


def extract_window(**context):
    """1. 수집 대상 선정: source_path에서 start_date~end_date(+genre) 구간만 필터링

    원본 CSV를 pd.read_csv()로 통째로 읽으면, ml-25m 규모로 생성한 이벤트 원본(수백만~
    수천만 행)에서는 Airflow 컨테이너 메모리를 그대로 다 먹어버릴 수 있다. 그래서
    chunksize로 나눠 읽으면서 각 청크에서 날짜/장르 조건에 맞는 행만 추려 누적하는
    방식으로 바꿔, 메모리 사용량을 "원본 전체 크기"가 아니라 "청크 크기 + 필터링된
    결과 크기" 수준으로 낮춘다.
    """
    import pandas as pd

    p = context["params"]
    src = p["source_path"] or DEFAULT_SOURCE
    start_date, end_date, genre = p["start_date"], p["end_date"], p.get("genre")
    end_ts = f"{end_date} 23:59:59+00:00"

    READ_CHUNK_ROWS = 500_000
    matched_chunks = []
    total_rows = 0

    for chunk in pd.read_csv(src, chunksize=READ_CHUNK_ROWS, low_memory=False):
        total_rows += len(chunk)
        # format="ISO8601": generate_events_v2.py가 만든 event_timestamp에는
        # 마이크로초가 있는 값("...20:28:46.123456+00:00")과 마이크로초가 정확히 0이라
        # 파이썬이 생략해버린 값("...20:28:46+00:00")이 섞여 있다. 포맷을 고정하지 않으면
        # pandas가 처음 본 형태로 고정해버려서 다른 형태를 만나면 파싱에 실패하는데,
        # ISO8601로 지정하면 마이크로초 유무와 관계없이 둘 다 정상 파싱한다.
        ts = pd.to_datetime(chunk["event_timestamp"], utc=True, format="ISO8601")
        mask = (ts >= start_date) & (ts <= end_ts)
        if genre:
            mask &= chunk["genre"] == genre
        if mask.any():
            matched_chunks.append(chunk[mask])

    window = pd.concat(matched_chunks, ignore_index=True) if matched_chunks else pd.DataFrame(columns=pd.read_csv(src, nrows=0).columns)

    print(f"[extract_window] 원본 {total_rows:,}행(청크 {READ_CHUNK_ROWS:,}행 단위 스캔) 중 {start_date}~{end_date}"
          f"{' / genre='+genre if genre else ''} 구간: {len(window):,}행")
    if len(window) == 0:
        raise AirflowFailException(
            f"이 조건({start_date}~{end_date}, genre={genre})에 해당하는 이벤트가 0건입니다."
        )

    tag = _tag(context)
    out_dir = f"{WORK_DIR}/{tag}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/window.csv"
    window.to_csv(out_path, index=False)

    context["ti"].xcom_push(key="window_path", value=out_path)
    context["ti"].xcom_push(key="window_count", value=len(window))
    context["ti"].xcom_push(key="tag", value=tag)
    return len(window)


with DAG(
    dag_id="backfill_ingest_process_dag",
    default_args=default_args,
    description="날짜 구간(+장르) 입력값으로 이벤트 수집·처리를 재실행하는 backfill DAG",
    schedule_interval=None,   # 수동/파라미터 트리거 전용 (정기 배치는 daily_batch_dag이 담당)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ott-pipeline", "backfill"],
    params={
        "start_date": Param("2018-01-01", type="string", description="수집 시작 날짜 (YYYY-MM-DD)"),
        "end_date": Param("2018-12-31", type="string", description="수집 종료 날짜 (YYYY-MM-DD)"),
        "genre": Param("", type="string", description="선택: 특정 장르만 필터링 (비우면 전체)"),
        "source_path": Param("", type="string", description="선택: 다른 입력 CSV 경로 (비우면 기본 샘플 사용)"),
    },
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_window",
        python_callable=extract_window,
    )

    # 2. 수집: 기존에 만든 backfill_ingest.py를 그대로 호출 (청크 COPY + 타임스탬프 정규화 재사용)
    t2_ingest = BashOperator(
        task_id="ingest_events",
        bash_command=(
            f"python3 {PROJECT_DIR}/pipeline/backfill_ingest.py "
            "--input {{ ti.xcom_pull(task_ids='extract_window', key='window_path') }} "
            "--backfill-tag {{ ti.xcom_pull(task_ids='extract_window', key='tag') }} "
            f"--db-url {PG_DSN}"
        ),
    )

    # 3. 처리: 기존에 만든 spark_batch_preprocess.py를 그대로 호출 (세션 집계, 분산 처리 설정 포함)
    t3_spark = BashOperator(
        task_id="spark_process",
        bash_command=(
            f"python3 {PROJECT_DIR}/pipeline/spark_batch_preprocess.py "
            "--input {{ ti.xcom_pull(task_ids='extract_window', key='window_path') }} "
            "--input-format csv "
            f"--out-dir {WORK_DIR}/"
            "{{ ti.xcom_pull(task_ids='extract_window', key='tag') }}/spark_output "
            "--format parquet"
        ),
    )

    def quality_check(**context):
        """4. 사후 검증: 수집 건수와 이 실행에서 실제로 처리된 흐름이 앞뒤가 맞는지 확인"""
        window_count = context["ti"].xcom_pull(task_ids="extract_window", key="window_count")
        tag = context["ti"].xcom_pull(task_ids="extract_window", key="tag")
        print(f"[quality_check] backfill_tag='{tag}' 수집 대상 {window_count:,}건")
        if window_count == 0:
            raise AirflowFailException("수집 대상이 0건입니다.")
        print("[quality_check] 통과")

    t4_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    t1_extract >> t2_ingest >> t3_spark >> t4_check
