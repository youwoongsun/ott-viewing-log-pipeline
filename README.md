# OTT 시청 로그 실시간 세션화 & 장애 대응 파이프라인

## 1. 프로젝트 목표

OTT 시청 이벤트를 실시간으로 수집·세션화하여 소비 트렌드를 집계하고, 파이프라인이 장애 상황에서도 데이터 유실 없이 복구되는지 검증한다.

## 2. 사용할 데이터 셋과 출처

| 데이터 | 내용 | 출처 |
|---|---|---|
| MovieLens (ml-latest-small(개발용) / ml-25m(로드 테스트용)) | 사용자-영화 평점 및 타임스탬프. 시청 이벤트 생성의 시드 데이터로 사용 | https://grouplens.org/datasets/movielens/ |
| TMDB API | 영화 메타데이터 보강 (장르, 개봉일, 러닝타임) | https://developer.themoviedb.org/docs/getting-started |
| 자체 이벤트 생성기 | MovieLens 타임스탬프를 시드로 play/pause/seek/complete 등 세분화 이벤트를 확률분포 기반으로 합성 | 직접 구현 (Python, Faker + numpy) |

**선정 이유**: MovieLens는 실제 사용자 행동의 통계적 패턴(인기 콘텐츠 편중, 세션성)을 갖고 있어 완전 합성 데이터보다 현실적이기 때문이다. 다만 세밀한 스트리밍 이벤트(재생/스킵/완주)는 없으므로, 평점 이벤트를 시드로 삼아 이벤트 생성기로 확장한다. 개발 단계는 ml-latest-small(경량)로, 로드 테스트 단계는 ml-25m(대용량)으로 나눠서 사용한다.

## 3. 수집 → 처리 → 저장 흐름 스케치

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
  [장애 주입 실험] Kafka 브로커 재시작 / Spark 강제 종료 후
  체크포인트 기반 복구 검증 → 결과를 README에 문서화


## 4. 사용해보고 싶은 기술 후보 (확정 아님)

| 컴포넌트 | 후보 | 선정 이유 |
|---|---|---|
| 데이터 수집 | Kafka (local) | Producer/Consumer 디커플링, 장애 시 디스크 기반 재처리 가능 |
| 가공/집계 | Spark Structured Streaming | 세션화(mapGroupsWithState), 윈도우 집계·조인이 복잡해 Pandas보다 적합 |
| 워크플로우 관리 | Airflow (local) | 일 배치 재계산 + retry/backoff 실습 |
| 저장소 | PostgreSQL | 집계 결과가 관계형 구조(세션-영화-장르)라 JOIN이 잦음 |
| API 서빙 (선택) | FastAPI | 세션 기반 집계 결과 조회 엔드포인트 |
| 시각화 (선택) | Streamlit | 장르별/시간대별 트렌드 대시보드 |

**아직 결정하지 않은 세팅**
- 세션 타임아웃 기준(30분)과 watermark 지연 허용 범위는 2회차에서 실험 후 확정
- Kafka 파티션 수는 6회차 로드 테스트 결과를 보고 조정
- 이상탐지, CDC 기반 메타데이터 동기화, 추천 모델 연동은 시간 여유 시 후속 확장으로 검토
