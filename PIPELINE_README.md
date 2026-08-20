# 데이터 적재 파이프라인 실행 가이드

## 구성 요소

| 파일 | 역할 |
|---|---|
| `docker-compose.yml` | Kafka(KRaft 단일 브로커) + PostgreSQL 로컬 환경 |
| `sql/init_schema.sql` | 기본 스키마 (movies, tags, sessions, 장애 실험 기록 테이블 등) |
| `sql/schema_v2_events.sql` | **v2 이벤트 데이터셋 전용 스키마** (raw_viewing_events, movie_segment_heatmap, user_movie_engagement_summary) |
| `load_reference_data.py` | movies/links/tags/ratings CSV를 PostgreSQL에 적재 |
| `ingest_v2_events.py` | **v2 이벤트 데이터셋(3,700만 건)을 COPY로 대량 적재** |
| `kafka_producer.py` | 이벤트 CSV를 Kafka `viewing-events` 토픽으로 스트리밍 (구버전/v2 스키마 자동 인식) |
| `spark_session_pipeline.py` | Kafka 이벤트를 소비해서 세션화하고 PostgreSQL에 적재하는 Spark 잡 |
| `requirements.txt` | 파이썬 의존성 |

## 이번 주 할 일: 데이터 적재 (Ingestion)

### 1. 인프라 기동 + 스키마 적용

```bash
docker compose up -d
pip install -r requirements.txt

# 스키마 두 개 다 적용 (기본 + v2)
psql postgresql://ott:ott_pw@localhost:5432/ott_pipeline -f sql/init_schema.sql
psql postgresql://ott:ott_pw@localhost:5432/ott_pipeline -f sql/schema_v2_events.sql
```

### 2. 참조 데이터 적재 (movies, links, tags, ratings)

```bash
python load_reference_data.py --data-dir /path/to/movielens/csvs
```

### 3. v2 이벤트 데이터셋 대량 적재 (핵심)

```bash
python ingest_v2_events.py --data-dir /path/to/viewing_events_v2
```

**왜 COPY를 쓰는가**: 3,700만 건을 `INSERT`로 한 행씩 넣으면 몇 시간이 걸릴 수 있습니다. PostgreSQL의 `COPY`는 대량 적재 전용 경로라 같은 데이터를 몇 분 내로 적재합니다. gzip 파일도 압축을 풀면서 바로 스트리밍하므로 디스크에 압축 해제본을 따로 만들 필요가 없습니다.

**적재되는 3개 테이블**:
- `raw_viewing_events` — 원본 이벤트 스트림 (3,700만 건). Kafka로 흘려보내기 전 소스이자, Spark가 계산한 세션 결과를 검증할 정답지 역할
- `movie_segment_heatmap` — 영화×구간별 시청 횟수 + 인기 구간 플래그
- `user_movie_engagement_summary` — 유저×영화 참여도 요약 (부트스트랩 없는 원본 기준이라 유일 키 보장)

**참고**: 로컬 디스크 용량을 넉넉히 확보하세요. 인덱스까지 포함하면 3,700만 건 적재 시 수 GB가 필요합니다.

### 4. (선택) 실시간 파이프라인 시연용 — Kafka로 스트리밍

이벤트 스트림을 Postgres에 직접 COPY하는 대신, "실시간으로 들어오는 것처럼" Kafka에 흘려보내고 싶다면:

```bash
# 별도 터미널에서 Spark 세션화 파이프라인 실행
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  spark_session_pipeline.py

# 이벤트를 실시간처럼 천천히 흘려보내기
python kafka_producer.py --csv /path/to/viewing_events_v2/viewing_events_part0000.csv.gz --rate 500

# 부하 테스트용 (최대 속도)
python kafka_producer.py --csv /path/to/viewing_events_v2/viewing_events_part0001.csv.gz --rate 0
```

`kafka_producer.py`는 CSV 헤더를 그대로 읽어 JSON으로 변환하므로, 구버전 6컬럼 스키마와 v2 16컬럼 스키마 둘 다 그대로 동작합니다.

## 장애 실험 시 이렇게 활용

- **Kafka 브로커 재시작**: `docker stop ott-kafka` → 일정 시간 후 `docker start ott-kafka`, producer 로그의 "전송 완료" 건수와 Postgres `sessions` 테이블의 `event_count` 합을 비교
- **Spark 강제 종료**: `spark_session_pipeline.py` 실행 중인 프로세스를 `kill -9`로 종료 후 재실행, `sessions` 테이블에 중복/누락이 있는지 SQL로 검증
- **부하 초과**: `kafka_producer.py --rate 0`으로 최대 속도 전송 + Kafka Consumer Lag 모니터링 (`kafka-consumer-groups.sh --describe`)

## 참고

- `raw_viewing_events` 테이블 자체가 세션의 "정답"을 담고 있으므로(session_start/session_end가 명시적), Spark가 만든 `sessions` 테이블 결과와 대조하면 세션화 로직 정확도를 정량적으로 검증할 수 있습니다.
- `spark_session_pipeline.py`의 세션화 로직은 스케치 수준이며, 실제 구현 시 `mapGroupsWithState`로 교체해 사용자별 상태를 더 정교하게 관리할 계획입니다 (watermark 값도 실험 후 확정).

