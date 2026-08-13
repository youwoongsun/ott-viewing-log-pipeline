# OTT 시청 로그 실시간 세션화 & 장애 대응 파이프라인

## 1. 프로젝트 목표

OTT(넷플릭스, 왓챠 등) 서비스의 시청 이벤트를 실시간으로 수집하고, 유저 단위 시청 세션으로 묶어서 소비 트렌드(장르별 인기도, 완주율 등)를 집계한다. 여기서 그치지 않고 파이프라인을 의도적으로 고장 내보면서, 장애 상황에서도 데이터 유실 없이 복구되는 구조인지를 직접 검증하는 것까지를 목표로 한다.

**왜 이 문제를 골랐는지**: 대부분의 데이터 파이프라인 프로젝트는 "정상 동작"까지만 만들고 끝나는데, 실제 현업에서는 "장애가 났을 때 어떻게 되는가"가 더 중요한 경우가 많다고 생각하기 때문이다. 이번 프로젝트는 정상 케이스뿐 아니라 장애 케이스까지 직접 재현하고 대응 과정을 기록하려고 한다.

## 2. 사용할 데이터셋과 출처

### 2-1. MovieLens

| 파일 | 용도 |
|---|---|
| `ratings.csv` | userId, movieId, rating, timestamp — 이벤트 생성기의 시드 데이터 |
| `movies.csv` | movieId, title, genres — 기본 메타데이터 |
| `links.csv` | movieId ↔ imdbId ↔ tmdbId 매핑 — TMDB API 조회에 사용 |

두 가지 버전을 단계별로 사용한다.

| 버전 | 규모 | 사용 시점 | 용도 |
|---|---|---|---|
| ml-latest-small | 평점 약 10만 건, 영화 약 9,700편 | 개발 단계 | 빠른 개발/디버깅 사이클 |
| ml-25m | 평점 약 2,500만 건, 영화 약 62,000편 | 로드 테스트/장애 대응 | 지속적인 부하를 만들어내는 소스 데이터 |

출처: https://grouplens.org/datasets/movielens/

### 2-2. TMDB API

영화 메타데이터(장르, 개봉일, 러닝타임, 포스터 등)를 보강하기 위해서 사용한다. `links.csv`의 tmdbId로 조회한다.

- 문서: https://developer.themoviedb.org/docs/getting-started
- 개인/비상업(Developer) 등급으로 신청 후 무료 사용
- API 키는 `.env` 파일로 관리하고 `.gitignore`로 제외 (레포에 노출되지 않음)

### 2-3. 자체 이벤트 생성기

MovieLens는 "유저가 영화에 평점을 남겼다"는 정적 스냅샷이지, "재생했다가 스킵했다" 같은 세분화된 시청 행동 로그가 아니다. 따라서 평점 타임스탬프를 기준점으로 삼아서, 그 전후로 아래와 같은 이벤트를 생성한다.

- `play`, `pause`, `seek`, `complete`, `drop` 이벤트를 순서대로 생성
- 시청 지속시간: 로그정규분포 (대부분 짧게 이탈, 소수는 끝까지 시청하는 패턴 반영)
- 콘텐츠 인기 쏠림: 파레토 분포 (소수 인기작에 시청 이벤트가 몰리는 실제 서비스 패턴 반영)

**선정 이유**: 완전히 무작위로 합성한 가짜 데이터보다, 실제 사용자 행동 통계(MovieLens)를 기반으로 시드를 삼는 편이 더 현실적인 로그를 만든다. 다만 세밀한 스트리밍 이벤트는 원본에 없으므로 이 부분만 직접 생성기로 채워 넣는다.

## 3. 수집 → 처리 → 저장 흐름

```
[MovieLens CSV] ──(1회 로드)──▶ [PostgreSQL: 영화 메타 테이블 (+ TMDB 보강)]
                                            
[이벤트 생성기 (Python)]                    
  MovieLens 타임스탬프를 시드로             
  play/pause/seek/complete 이벤트 생성      
       │                                    
       ▼                                    
  [Kafka Topic: viewing-events]             
  (JSON, user_id 기준 파티셔닝)             
       │                                    
       ▼                                    
  [Spark Structured Streaming]              
  - mapGroupsWithState 기반 세션화          
    (30분 무활동 시 세션 종료, watermark로  
     late data 처리)                        
       │                                    
       ▼                                    
  [PostgreSQL: sessions 테이블] ◀── JOIN ── [영화 메타 테이블]
       │
       ▼
  [Airflow DAG] — 일 배치로 랭킹/지표 재계산, retry/backoff, 품질 체크
       │
       ▼
  [장애 주입 실험]
  1) Kafka 브로커 재시작 중 Producer/Consumer 동작 확인
  2) Spark 스트리밍 잡 강제 종료 → 체크포인트 기반 재시작 시
     중복/유실 여부 검증
  3) Consumer 처리 속도 < Producer 발행 속도일 때
     consumer lag 관찰 및 대응
  → 결과를 "시나리오 → 발생 문제 → 대응 → 결과" 표로 README에 기록
```

### 3-1. Kafka 설계

- 토픽: `viewing-events` 단일 토픽
- 파티셔닝: `user_id` 기준 → 같은 유저의 이벤트 순서 보장 (세션화 정합성을 위해 필수)
- 파티션 수: 초기값으로 시작, 로드 테스트 결과 보고 조정
- 리텐션: 로컬 개발 기준 짧게 설정 (예: 1일), 이후 필요시 조정

### 3-2. Spark 처리 설계

- Kafka Consumer로 이벤트를 읽어 파싱
- `mapGroupsWithState`로 user_id별 상태를 유지하며 세션 판정
- 세션 종료 조건: 마지막 이벤트 이후 30분 무활동
- watermark: late data(네트워크 지연 등으로 늦게 도착하는 이벤트) 처리를 위해 설정, 구체적인 지연 허용 범위는 실험 후 확정
- 세션 요약 시 영화 메타데이터와 조인하여 장르 정보 포함

### 3-3. 저장 설계

- PostgreSQL 단일 사용
- 이유: 원본 이벤트 전체가 아니라 Spark가 집계한 세션 단위 결과만 저장하므로, 대용량 분산 저장(Parquet 등)보다는 관계형 구조(세션-영화-장르 조인)에 적합한 PostgreSQL이 더 알맞다고 판단
- 예상 스키마 (확정 아님): `sessions(session_id, user_id, movie_id, start_ts, end_ts, event_count, completion_rate)`

### 3-4. Airflow 설계

- 일 배치로 전일 랭킹/리텐션 지표 재계산
- retry/backoff: 지수 백오프, 최대 재시도 횟수 설정
- 데이터 품질 체크: 집계 건수 이상 감지, null 체크 등

## 4. 사용해보고 싶은 기술 후보 (확정 아님)

| 컴포넌트 | 후보 | 선정 이유 |
|---|---|---|
| 데이터 수집 | Kafka (local) | Producer/Consumer 디커플링, 장애 시 디스크 기반 재처리 가능 |
| 가공/집계 | Spark Structured Streaming | 세션화(mapGroupsWithState), 윈도우 집계·조인이 복잡해 Pandas보다 적합 |
| 워크플로우 관리 | Airflow (local) | 일 배치 재계산 + retry/backoff 실습, cron 대비 의존성 관리와 모니터링이 용이 |
| 저장소 | PostgreSQL | 집계 결과가 관계형 구조(세션-영화-장르)라 JOIN이 잦음 |
| API 서빙 (선택) | FastAPI | 세션 기반 집계 결과 조회 엔드포인트 |
| 시각화 (선택) | Streamlit | 장르별/시간대별 트렌드 대시보드 |
| 컨테이너화 | Docker Compose | 전체 스택을 로컬에서 재현 가능하게 구성 |

**아직 결정하지 않은 세팅**

- 세션 타임아웃 기준(30분)과 watermark 지연 허용 범위 → 실험 후 확정
- Kafka 파티션 수 → 로드 테스트 결과를 보고 조정
- 완주율(completion rate) 정의 → 러닝타임 대비 시청 비율로 잠정 설정, 세부 기준은 스키마 설계 시 확정
- 이상탐지, CDC 기반 메타데이터 동기화, 추천 모델 연동 → 시간 여유 시 후속 확장으로 검토


## 5. 데이터 준비 방법

```
data/
└── raw/
    ├── ml-latest-small/     # 레포에 포함 (개발용, 용량 작음)
    │   ├── movies.csv
    │   ├── links.csv
    │   └── ratings.csv
    └── ml-25m/               # 레포에 미포함 (용량 문제, 로드 테스트용)
```

`ml-25m`은 용량 문제로 레포에 포함하지 않았습니다. 아래 링크에서 직접 다운받아 `data/raw/ml-25m/` 경로에 압축한 뒤 사용할 예정이다.
https://grouplens.org/datasets/movielens/25m/


## 7. 향후 확장 계획

현재는 단일 플랫폼(가상의 OTT 하나)을 가정한 이벤트 스키마로 설계되어 있다.
실제 서비스 환경에서는 넷플릭스, 왓챠, 티빙처럼 플랫폼마다 이벤트 필드명,
타임스탬프 단위(초 vs 밀리초), 세션 정의(무활동 기준 시간 등)가 다르다.

- **정규화 계층(Normalization Layer)**: 플랫폼별 원본 스키마를 표준 스키마로
  변환하는 어댑터. Kafka Producer 이전 단계 또는 Spark 초입에 위치시키는 것을
  고려 중이다.
- **스키마 레지스트리**: 플랫폼이 늘어날수록 스키마 버전 관리가 필요해지므로,
  Avro + Schema Registry 도입을 검토할 수 있다.
- **장애 복구 범위 확장**: 정규화 계층 자체의 장애(예: 알 수 없는 플랫폼
  스키마가 들어왔을 때)까지 포함해 장애 시나리오를 확장한다.

이번 프로젝트 범위에서는 단일 플랫폼 기준으로 세션화와 장애 대응을 먼저
완성도 있게 구현하고, 위 확장은 시간 여유가 있을 경우 다음 단계로 진행할 예정이다.
