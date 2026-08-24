# 4주차: Kafka 이벤트 스트리밍 & Spark 배치 전처리

## 실제 구현 vs 이후 계획 (먼저 명확히 구분)

| 항목 | 상태 |
|---|---|
| 데이터·메시지 명세 확정 | ✅ 구현 완료 |
| **ml-25m 원본 42만 건 → 이벤트 2,848만 건 대용량 생성** (부트스트랩 없음, 중복 0건 검증) | ✅ 구현 완료 |
| 이번 주 제출용 Kafka 샘플(1,910건) 추출 | ✅ 구현 완료 |
| Kafka Producer/Consumer 코드 작성 | ✅ 구현 완료 (코드 검증됨) |
| **Kafka 실제 실행 (Producer→Consumer 건수 확인)** |  **로컬 Docker 환경에서 실행 필요** (아래 "실행 방법" 참고) |
| Spark 배치 전처리 + 세션 집계 + Parquet 저장 | ✅ **실제로 실행하고 검증 완료** (아래 실행 로그 참고) |
| Kafka→Spark 실시간 스트리밍 연결 (`spark_session_pipeline.py`) | ❌ 다음 계획 (현재는 배치 모드로만 검증) |
| PostgreSQL 최종 적재 | ❌ 다음 계획 (현재는 Parquet 파일로 저장) |
| 2,848만 건 전체를 Kafka/Spark로 실제 스트리밍·부하 테스트 | ❌ 다음 계획 (인프라 준비되는 대로 진행) |

## 데이터 규모

- **대용량 생성**: ml-25m 원본 2,500만 건을 한 번 고정 셔플한 뒤, 겹치지 않는 6개 구간(각 7만 건, 총 42만 건)에서 이벤트를 만들어 **총 2,848만 건**을 생성했습니다. 부트스트랩(복원추출)이 아니라 서로 다른 실제 원본 행만 사용했고, 유저-영화 조합 중복이 0건임을 직접 검증했습니다.
- **이번 주 제출**: 위 대용량 데이터셋 중 한 파트에서 완전한 세션 단위로 무작위 선택해 **1,910건**을 뽑아 Kafka/Spark 검증에 사용했습니다.

자세한 수치는 `DATA_MESSAGE_SPEC.md` 참고.

## 1. 데이터·메시지 명세

`DATA_MESSAGE_SPEC.md` 참고. 필드명/타입/의미, 실제 JSON 예시, Kafka Topic 이름(`viewing-events`)이 정리돼 있습니다.

## 2. Kafka 실행 방법

로컬에 Docker가 있는 환경에서 실행합니다 (이 저장소의 `docker-compose.yml` 사용).

```bash
# 1. Kafka 기동
docker compose up -d

# 2. Consumer를 먼저 띄워서 대기 (별도 터미널)
python kafka_consumer.py --max-messages 1910 --out consumed_events.jsonl

# 3. Producer로 전송
python kafka_producer.py --csv data/kafka_sample_2000.csv --rate 500
```

**건수 확인 방법**: Producer 종료 시 출력되는 `전송 완료: 총 N건`과, Consumer 종료 시 출력되는 `수신 완료: 총 N건`을 비교합니다. 정상이라면 둘 다 1,910건으로 일치해야 합니다.

> 이 저장소를 검토하는 이 세션(샌드박스)에는 Docker와 Kafka 바이너리 다운로드 경로가 막혀 있어 실제 브로커를 띄울 수 없었습니다. Producer/Consumer 코드 자체는 문법 검증 및 로직 검토를 마쳤고, 동일 구조의 데이터를 JSONL로 만들어 아래 Spark 단계에서 실제로 처리·검증했습니다.

## 3. Spark 배치 전처리 실행 방법 (실제로 실행 및 검증 완료)

```bash
python spark_batch_preprocess.py --input consumed_events.jsonl --out-dir output --format parquet
```

Kafka 없이도, Consumer가 저장한 JSONL(또는 이번에 검증용으로 만든 동일 스키마 JSONL)을 그대로 입력받아 로컬 모드로 실행됩니다 (별도 클러스터 불필요).

### 실행 결과 (실제 실행 로그)

```
처리 전 건수: 1,910행

필수 필드 결측 제거 후: 1,910행
타임스탬프 파싱 실패 제거 후: 1,910행
비정상 재생위치 제거 후 (최종): 1,897행

집계된 세션 수: 47개

[요약]
처리 전 이벤트: 1,910행
처리 후 이벤트: 1,897행 (제거: 13행)
집계된 세션: 47개
```

전체 로그는 `spark_run_log_clean.txt` 참고.

### 전처리 내용

1. 필수 필드(`user_id`, `movie_id`, `event_type`, `event_timestamp`) 결측 행 제거
2. `event_timestamp` 문자열 → 실제 timestamp 타입 캐스팅, 파싱 실패 행 제거
3. `position_sec`이 음수이거나 `duration_sec`을 초과하는 비정상 값 제거 → **13건 제거됨**
4. `session_id` 단위로 집계 (세션 시작/종료 시각, 이벤트 개수, 완주 여부, 최대 도달 위치, 완주율)

### 최종 저장 위치·형식·컬럼

| 항목 | 내용 |
|---|---|
| 저장 위치 | `output/sessions_batch.parquet/` |
| 형식 | Parquet (Snappy 압축) |
| 행 수 | 47행 (세션 단위 집계 결과) |

최종 컬럼:

| 컬럼 | 타입 | 의미 |
|---|---|---|
| `session_id` | string | 세션 ID |
| `user_id` | int | 유저 ID |
| `movie_id` | int | 영화 ID |
| `movie_title` | string | 영화 제목 |
| `genre` | string | 장르 |
| `device` | string | 시청 디바이스 (세션 대표값) |
| `start_ts` | timestamp | 세션 시작 시각 |
| `end_ts` | timestamp | 세션 종료 시각 |
| `event_count` | long | 세션 내 이벤트 개수 |
| `completed` | boolean | 정상 종료(session_end) 여부 |
| `max_position_sec` | double | 최대 도달 재생 위치(초) |
| `duration_sec` | int | 영화 러닝타임(초) |
| `completion_rate` | double | 완주율 (max_position_sec / duration_sec) |


