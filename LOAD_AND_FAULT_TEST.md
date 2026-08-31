# 부하·장애 테스트 종합 가이드 (실시간 Kafka→Spark→PostgreSQL 파이프라인)

이 프로젝트는 HTTP/WebSocket API가 아니라 **Kafka 스트리밍 파이프라인**이므로,
k6/Artillery 같은 HTTP 부하 도구 대신 **"이벤트 재생 + 초당 전송량/전체 건수 늘리기"**
방식으로 부하를 준다 (가이드 예시의 "Kafka·스트리밍 파이프라인" 항목에 해당).
DB 장애 재현은 컨테이너를 직접 내렸다 올리는 방식으로 충분해서 Toxiproxy는 쓰지 않는다.

전체 흐름: **0. 기준선 기록 → 1. 대용량 실행 → 2. 장애 재현 4종 → 3. 복구·정합성 검증 → 4. README 반영**

---

## 0단계 — 기준선(baseline) 기록

지금까지 쓰던 소규모 데이터로 "정상 실행"의 기준값을 먼저 남긴다.

```powershell
cd "C:\Users\User\Desktop\ott-pipeline-clean"
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "TRUNCATE sessions;"
```

**터미널 1** (스트리밍, 계속 켜둠):
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pipeline\spark_session_pipeline.py
```

**터미널 2**:
```powershell
py pipeline\kafka_producer.py --csv data\kafka_sample_2000.csv --rate 500
```
`전송 완료: 총 N건 (실패 0건) | T초 소요` 로그를 그대로 기록해둔다.

1~2분 뒤:
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "SELECT count(*) AS sessions, sum(event_count) AS total_events FROM sessions;"
```

**기록할 표 (README에 그대로 옮길 것):**

| 항목 | 값 |
|---|---|
| 입력 파일 | `kafka_sample_2000.csv` |
| 전송 이벤트 수 | (producer 로그의 N) |
| 전송 소요시간 | (producer 로그의 T초) |
| 저장된 세션 수 | (SELECT count(*) 결과) |
| 저장된 이벤트 합계 | (SELECT sum(event_count) 결과) |
| 오류 | 없음 |

캡처: 터미널 1(스트리밍 batch 로그) + 터미널 2(전송 완료 로그) + psql 결과.

---

## 1단계 — 대용량 실행 (부트스트랩으로 볼륨 확대)

```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "TRUNCATE sessions;"
py pipeline\bootstrap_sample.py --input data\kafka_sample_2000.csv --output data\kafka_sample_bootstrap_100k.csv --target-rows 100000
```

**터미널 2**:
```powershell
py pipeline\kafka_producer.py --csv data\kafka_sample_bootstrap_100k.csv --rate 1000
```
(속도도 500→1000/s로 올려서 "초당 전송량"과 "전체 건수" 둘 다 늘림)

완료 후:
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "SELECT count(*) AS sessions, sum(event_count) AS total_events FROM sessions;"
```

**기록할 표:**

| 항목 | 값 |
|---|---|
| 입력 파일 | `kafka_sample_bootstrap_100k.csv` |
| 전송 이벤트 수 / 속도 | N건 / 1000건·s |
| 전송 소요시간 | T초 |
| 저장된 세션 수 | ... |
| 저장된 이벤트 합계 | ... |
| 처리 지연(터미널1 마지막 batch 시각 - 전송 완료 시각) | ... |
| 오류 | 있었다면 내용, 없으면 "없음" |

캡처 1장(터미널1+2 로그) + psql 결과 1장.

---

## 2단계 — 장애 4종 재현

### (A) 잘못된 입력(invalid input)

```powershell
py -c "
import pandas as pd
df = pd.read_csv('data/kafka_sample_2000.csv', encoding='utf-8', nrows=20).copy()
df.loc[0, 'user_id'] = 'NOT_A_NUMBER'         # 타입 깨진 값
df.loc[1, 'event_timestamp'] = 'garbage-date'  # 파싱 불가 타임스탬프
df.loc[2, 'movie_id'] = ''                     # 필수값 누락
df.to_csv('data/bad_input_sample.csv', index=False, encoding='utf-8')
print('생성 완료: data/bad_input_sample.csv')
"
py pipeline\kafka_producer.py --csv data\bad_input_sample.csv --rate 50
```
**관찰할 것**: 터미널 1(스트리밍)이 예외로 죽는지, 아니면 `to_timestamp`가 null을 반환해서
watermark에 안 걸리고 조용히 빠지는지. 죽지 않고 나머지 정상 이벤트는 처리되면 "부분 장애 허용"으로 기록.

### (B) Kafka 연결 장애 (브로커 다운)

```powershell
py pipeline\kafka_producer.py --csv data\kafka_sample_bootstrap_100k.csv --rate 500
```
전송 중간에 새 터미널에서:
```powershell
docker stop ott-kafka
timeout /t 15
docker start ott-kafka
```
**관찰할 것**: producer가 재시도하는지/멈추는지, 스트리밍이 `failOnDataLoss=false` 덕분에 살아있는지, 브로커 복구 후 이어서 처리되는지.

### (C) 처리 작업 강제 중단 (Spark 크래시)

터미널 1에서 전송 중간에 `Ctrl+C`로 스트리밍 강제 종료. (뒤에 3단계에서 재시작하며 복구 검증까지 같이 함.)

### (D) DB 적재 실패 (Postgres 다운)

스트리밍(터미널 1)과 producer(터미널 2)를 켠 채로:
```powershell
docker stop ott-postgres
```
**관찰할 것**: 다음 마이크로배치에서 `psycopg2.connect` 실패로 터미널 1이 예외를 뱉으며 죽는지 확인 (죽는 게 정상 — try/except로 감싸지 않았기 때문에 의도된 동작). 이 예외 로그를 캡처.

그 다음:
```powershell
docker start ott-postgres
timeout /t 10
```

### (E) 동일 이벤트 중복 전송

```powershell
py pipeline\kafka_producer.py --csv data\kafka_sample_bootstrap_100k.csv --rate 1000
```
(같은 파일을 한 번 더 그대로 전송 — 완전히 똑같은 `event_id`들이 다시 들어감)

각 장애(A~E)마다 기록:
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "INSERT INTO failure_experiments (scenario, started_at, notes) VALUES ('invalid_input', now(), '진행 중') RETURNING experiment_id;"
```
(scenario 값을 각각 `invalid_input`, `kafka_broker_down`, `spark_task_killed`, `db_write_failure`, `duplicate_event_replay`로 바꿔서 5번 실행)

---

## 3단계 — 복구 + 정합성 검증

(C), (D)로 죽은 스트리밍을 재시작:
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pipeline\spark_session_pipeline.py
```
체크포인트 경로가 그대로라 마지막 커밋된 오프셋부터 이어서 처리된다.

**핵심 검증 쿼리:**
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "SELECT count(*) AS sessions, sum(event_count) AS total_events FROM sessions;"
```

- **유실 확인**: `total_events`가 1단계(100k 정상 실행) 때와 비슷한 수준인지. 너무 적으면 (D) DB 다운 구간에서 재처리 안 된 데이터가 있다는 뜻 — 이 경우 원인은 체크포인트 커밋 시점 문제이니 로그로 원인 짚어서 기록.
- **중복 확인**: (E)에서 같은 CSV를 두 번 보냈는데도 `total_events`가 정확히 2배가 아니라 **1단계와 거의 같은 수준**이면, 코드에 추가한 `dropDuplicates(["event_id"])`가 제대로 작동해서 중복이 걸러진 것 — 이게 핵심 증명 포인트.
  ```powershell
  docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "SELECT session_id, count(*) FROM sessions GROUP BY session_id HAVING count(*) > 1;"
  ```
  (0행 나오는 게 정상 — PK라 애초에 DB 레벨에서 중복 저장 자체가 불가능하고, 위 total_events 비교가 진짜 dedup 여부를 보여주는 지표)

각 experiment 행 업데이트:
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "UPDATE failure_experiments SET ended_at = now(), events_sent = 100000, events_received = <검증 쿼리 결과>, duplicate_count = 0, lost_count = 0, notes = '체크포인트 재시작 후 정상 복구, dropDuplicates로 중복 방지 확인' WHERE experiment_id = <id>;"
```

최종 전체 조회:
```powershell
docker exec -it ott-postgres psql -U ott -d ott_pipeline -c "SELECT * FROM failure_experiments ORDER BY experiment_id;"
```
이 결과를 캡처 — README에 넣을 마지막 증거.

---

## 4단계 — README에 추가할 섹션

`README.md` 맨 아래에 그대로 추가 (캡처 파일명은 실제 저장한 이름으로):

```markdown
## 7주차 — 부하·장애 테스트

Kafka→Spark Structured Streaming→PostgreSQL 실시간 파이프라인에 대해
기준선 대비 대용량 실행, 실제 발생 가능한 장애 5종 재현, 복구 후 정합성 검증을 진행.

### 실행 결과 비교

| 항목 | 기준선 (2천행) | 대용량 (10만행) |
|---|---|---|
| 전송 이벤트 수 | N | N |
| 전송 소요시간 | T초 | T초 |
| 저장된 세션 수 | N | N |
| 저장된 이벤트 합계 | N | N |
| 오류 | 없음 | 없음/있음 |

### 장애 재현 및 복구 결과

| 시나리오 | 발생시킨 방법 | 관찰된 동작 | 복구 방법 | 복구 후 상태 |
|---|---|---|---|---|
| 잘못된 입력 | 타입 오류/누락 필드 섞은 CSV 전송 | (기록) | - | (기록) |
| Kafka 브로커 다운 | `docker stop/start ott-kafka` | (기록) | 자동 재연결 | (기록) |
| Spark 처리 강제 중단 | 전송 중 Ctrl+C | 스트리밍 프로세스 종료 | 체크포인트 기반 재시작 | 유실 없음 |
| DB 적재 실패 | `docker stop/start ott-postgres` | psycopg2 연결 예외로 배치 실패 | Postgres 복구 후 재시작 | 유실 없음 |
| 동일 이벤트 중복 전송 | 같은 CSV 재전송 | `dropDuplicates(event_id)`로 중복 제거 확인 | - | 중복 없음 |

전체 실험 기록: `sql/init_schema.sql`의 `failure_experiments` 테이블 참고.

![기준선 실행](airflow_screenshots/baseline_run.png)
![대용량 실행](airflow_screenshots/scale_run.png)
![장애 실험 결과](airflow_screenshots/failure_experiments_summary.png)
```

---

## 체크리스트

- [ ] 0단계 기준선 캡처 2장 (터미널 로그 + psql 결과)
- [ ] 1단계 대용량 실행 캡처 2장
- [ ] 2단계 장애 5종(A~E) 각각 관찰 로그 캡처
- [ ] 3단계 복구 후 검증 쿼리 결과 캡처
- [ ] `failure_experiments` 최종 조회 캡처
- [ ] `spark_session_pipeline.py` (dropDuplicates 추가된 최신본)으로 교체 커밋
- [ ] `bootstrap_sample.py`, 이 가이드 파일 커밋
- [ ] README.md 7주차 섹션 추가 후 커밋·푸시
- [ ] `git status`로 대용량 CSV(`kafka_sample_bootstrap_100k.csv`, `bad_input_sample.csv`)와 `.env`가 커밋 목록에 없는지 확인 (`.gitignore`에 `data/*bootstrap*`, `data/bad_input_sample.csv` 추가 권장)
