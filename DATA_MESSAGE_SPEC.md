# 데이터 · 메시지 명세

## 1. Kafka Topic

| 항목 | 값 |
|---|---|
| Topic 이름 | `viewing-events` |
| 파티션 키 | `user_id` (같은 유저의 이벤트 순서를 같은 파티션 안에서 보장 → 세션화 정합성에 필수) |
| 메시지 포맷 | JSON (UTF-8) |

## 2. 필드 명세

원본 데이터: **MovieLens `ml-25m`** 전체(25,000,095건, 162,541명, 62,423편)에서 **비복원·중복 없이 42만 건**을 뽑아 기준점으로 삼아 생성한 시청 이벤트. 부트스트랩(복원추출)이 아니라 전체를 한 번 고정 셔플한 뒤 겹치지 않는 6개 구간으로 나눠 뽑은 실제 원본 행입니다.

| 필드명 | 타입 | 의미 |
|---|---|---|
| `event_id` | integer | 이벤트 고유 ID |
| `user_id` | integer | 유저 ID (Kafka 파티션 키) |
| `movie_id` | integer | 영화 ID |
| `movie_title` | string | 영화 제목 |
| `genre` | string | 대표 장르 1개 |
| `session_id` | string | 세션 ID (`u{user_id}-m{movie_id}-s{순번}` 형식) |
| `event_type` | string | 이벤트 종류 (아래 표 참고) |
| `event_timestamp` | string (ISO 8601) | 이벤트 발생 시각, UTC |
| `position_sec` | float \| null | 영화 재생 위치(초). session_start는 항상 0.0, rating_given/tag_added는 null |
| `segment_index` | integer \| null | 3분 단위 구간 번호 (segment_watch/pause/seek 계열에만 존재) |
| `duration_sec` | integer | 영화 전체 러닝타임(초), 같은 세션의 모든 행에 동일하게 반복 기록 |
| `session_seq` | integer | 이 유저-영화 조합에서 몇 번째 시청 세션인지 (1부터 시작) |
| `total_sessions` | integer | 이 유저-영화 조합의 총 세션 수 |
| `device` | string | `mobile` \| `smart_tv` \| `web` \| `tablet` |
| `tag_value` | string \| null | `tag_added` 이벤트일 때만 값 존재 |
| `value` | float \| null | `rating_given` 이벤트일 때만 값 존재 (평점, 0.5~5.0) |

## 3. event_type 9종

| 값 | 의미 |
|---|---|
| `session_start` | 시청 세션 시작 |
| `segment_watch` | 3분 단위 구간을 진행하며 남기는 시청 기록 |
| `pause` | 일시정지 |
| `seek_forward` | 앞으로 건너뛰기 |
| `seek_backward` | 뒤로 되감기 |
| `session_end` | 세션 정상 종료 (완주 또는 의도적 중단) |
| `drop_off` | 중간 이탈 |
| `rating_given` | 평점 등록 |
| `tag_added` | 태그 등록 |

## 4. Kafka로 보낼 JSON 예시 (실제 샘플 데이터에서 추출)

```json
{
  "event_id": 1863976,
  "user_id": 3236,
  "movie_id": 750,
  "movie_title": "Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1964)",
  "genre": "Comedy",
  "session_id": "u3236-m750-s1",
  "event_type": "session_start",
  "event_timestamp": "1996-06-06T13:25:59.809467+00:00",
  "position_sec": 0.0,
  "segment_index": null,
  "duration_sec": 7409,
  "session_seq": 1,
  "total_sessions": 1,
  "device": "mobile",
  "tag_value": null,
  "value": null
}
```

## 5. 데이터 규모

### 전체 생성 데이터셋 (대용량, GitHub 미포함)

| 항목 | 값 |
|---|---|
| 원본 소스 | MovieLens **ml-25m** `ratings.csv` 전체 25,000,095건 |
| 샘플링 방식 | 전체를 고정 시드로 1회 셔플 후, 겹치지 않는 6개 구간(각 7만 건)으로 분할 사용 — **부트스트랩 아님, 중복 없음** |
| 사용한 원본 평점 수 | 420,000건 (25,000,095건 중) |
| 생성된 전체 이벤트 수 | **28,483,808건** |
| 영화 커버리지 | 17,175편 |
| 유저-영화 조합 중복 | 0건 (검증 완료) |
| 압축 후 용량 | 약 526MB (6개 파트, gzip) |

### 데이터 샘플 (GitHub 포함)

| 항목 | 값 |
|---|---|
| 방식 | 위 대용량 데이터셋 중 한 파트에서 완전한 세션 단위로 무작위 선택 |
| Kafka 샘플 | **1,910건** |
| 샘플 내 다양성 | 유저 47명, 영화 45편 |

## 데이터 규모 관련 결정 사항

이번 주 실제 제출물(Kafka 전송, GitHub 업로드)은 과제 요구사항(권장 1,000건, 대용량 원본 제외)에 맞춰 작게 유지했습니다. 다만 그 기반이 되는 이벤트 데이터셋 자체는 ml-25m 원본 2,500만 건 중 42만 건(부트스트랩 없이 서로 다른 행)을 사용해 **2,848만 건 규모로 대용량 생성**했습니다. 
