"""
이벤트 생성기 v2
==================
기존(업로드된) 데이터셋의 강점을 유지하면서 개선한 버전:
- 세션 경계 명시 (session_start / session_end / drop_off)
- 영화를 고정 길이 구간(segment)으로 나눠 segment_watch 이벤트로 진행 상황 기록
- seek_forward뿐 아니라 seek_backward(되감기)도 반영 -> 인기 구간(heatmap)이
  "그냥 오래 머문 곳"이 아니라 "일부러 되돌아가서 다시 본 곳"까지 포함
- 디바이스(mobile/smart_tv/web/tablet) 페르소나별로 다르게 배정
- tags.csv를 실제 이벤트 스트림에 tag_added로 삽입 (비정형 필드의 실제 활용)
- ratings.csv 전체(9,724편)를 커버 (업로드본은 1,763편만 커버했음)

출력 3종 (업로드본과 동일한 스키마 유지):
  viewing_events.csv               - 이벤트 스트림 원본
  movie_segment_heatmap.csv        - 영화×구간별 시청 횟수 + 인기 구간 플래그
  user_movie_engagement_summary.csv - 유저×영화 단위 요약 (세션수/완주율/평점 등)
"""

import argparse
import csv
import hashlib
import sys
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

INPUT_DIR = Path("/mnt/user-data/uploads")  # main()에서 args.input_dir로 재설정됨

PERSONAS = {
    # completion_bias: 완주 확률 보정 / seek_rate,backseek_rate,pause_rate: 세션당 평균 횟수
    # rewatch_mult: 재시청 확률 배율 / devices: 디바이스 선호 분포
    "binge":         dict(weight=0.20, completion_bias=+0.15, seek_rate=1.0, backseek_rate=0.6, pause_rate=1.0,
                           rewatch_mult=1.4, devices={"smart_tv": 0.55, "mobile": 0.15, "web": 0.15, "tablet": 0.15}),
    "casual":        dict(weight=0.50, completion_bias=-0.10, seek_rate=1.6, backseek_rate=0.3, pause_rate=2.2,
                           rewatch_mult=0.7, devices={"mobile": 0.50, "smart_tv": 0.20, "web": 0.15, "tablet": 0.15}),
    "completionist": dict(weight=0.15, completion_bias=+0.30, seek_rate=0.4, backseek_rate=0.2, pause_rate=0.7,
                           rewatch_mult=1.1, devices={"smart_tv": 0.40, "web": 0.30, "mobile": 0.15, "tablet": 0.15}),
    "explorer":      dict(weight=0.15, completion_bias=+0.00, seek_rate=2.4, backseek_rate=1.1, pause_rate=1.3,
                           rewatch_mult=1.0, devices={"mobile": 0.30, "tablet": 0.30, "web": 0.25, "smart_tv": 0.15}),
}

SEGMENT_LEN_SEC = 180          # 구간 길이 3분
MEAN_RUNTIME_MIN, RUNTIME_STD_MIN = 105, 25


def h(key: str) -> float:
    return (int(hashlib.md5(key.encode()).hexdigest(), 16) % 1_000_000) / 1_000_000


def assign_persona(user_id: int) -> str:
    r = h(f"persona-{user_id}")
    cum = 0.0
    for name, cfg in PERSONAS.items():
        cum += cfg["weight"]
        if r < cum:
            return name
    return "casual"


def pick_device(persona: str, salt: str) -> str:
    dist = PERSONAS[persona]["devices"]
    r = h(salt)
    cum = 0.0
    for dev, p in dist.items():
        cum += p
        if r < cum:
            return dev
    return "mobile"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-index", type=int, default=0)
    ap.add_argument("--chunk-size", type=int, default=0, help="0이면 원본 그대로(부트스트랩 없음)")
    ap.add_argument("--seed", type=int, default=100)
    ap.add_argument("--out-dir", default="/home/claude/event_gen_v2/out")
    ap.add_argument("--jitter-years", type=float, default=2.5)
    ap.add_argument("--input-dir", default="/mnt/user-data/uploads",
                     help="ratings.csv/movies.csv/tags.csv가 있는 디렉터리")
    ap.add_argument("--limit-ratings", type=int, default=0,
                     help="0이면 전체, 아니면 원본 앞부분 N건만 사용 (부트스트랩 아님, 원본 그대로 슬라이스)")
    ap.add_argument("--sample-ratings", type=int, default=0,
                     help="0이면 미사용. N을 주면 원본 전체 범위에서 비복원 무작위 샘플 N건 사용"
                          " (--limit-ratings와 달리 파일 앞부분에 쏠리지 않고 전체 유저/영화 범위를 대표함)")
    ap.add_argument("--shuffle-seed", type=int, default=999,
                     help="여러 청크를 나눠 돌릴 때, 청크끼리 겹치지 않게 하는 전체 셔플 시드 (모든 청크 호출에서 동일하게 유지)")
    args = ap.parse_args()

    global INPUT_DIR
    INPUT_DIR = Path(args.input_dir)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed + args.chunk_index)

    print("=" * 60)
    print(f"1. 원본 로드 (청크 {args.chunk_index}, chunk_size={args.chunk_size})")
    print("=" * 60)
    ratings = pd.read_csv(INPUT_DIR / "ratings.csv", encoding="utf-8")
    movies = pd.read_csv(INPUT_DIR / "movies.csv", encoding="utf-8")
    tags = pd.read_csv(INPUT_DIR / "tags.csv", encoding="utf-8")
    _original_ratings_count = len(ratings)

    if args.limit_ratings > 0:
        ratings = ratings.iloc[:args.limit_ratings].copy()
        print(f"  --limit-ratings 지정됨: 원본 앞부분 {len(ratings):,}건만 사용 (부트스트랩 아님)")
    elif args.sample_ratings > 0:
        # 복원추출(bootstrap) 아님. 25M건 전체를 한 번 고정 시드로 셔플한 뒤,
        # chunk_index 순서대로 겹치지 않는 블록을 잘라 쓴다. 그래서 여러 청크를
        # 나눠 돌려도 같은 행이 두 번 뽑히지 않고, 합치면 원본 전체를 고르게 대표한다.
        shuffled = ratings.sample(frac=1.0, random_state=args.shuffle_seed).reset_index(drop=True)
        start = args.chunk_index * args.sample_ratings
        end = start + args.sample_ratings
        ratings = shuffled.iloc[start:end].reset_index(drop=True)
        print(f"  --sample-ratings 지정됨 (청크 {args.chunk_index}): 원본 전체 {_original_ratings_count:,}건을 "
              f"고정 셔플 후 [{start:,}:{end:,}] 구간 {len(ratings):,}건 사용 (청크 간 중복 없음)")

    if args.chunk_size > 0:
        idx = rng.integers(0, len(ratings), size=args.chunk_size)
        base = ratings.iloc[idx].reset_index(drop=True).copy()
        jitter = rng.uniform(-args.jitter_years, args.jitter_years, size=args.chunk_size) * 365.25 * 86400
        base["timestamp"] = (base["timestamp"] + jitter).astype(np.int64)
    else:
        base = ratings.copy()
    print(f"  대상 평점: {len(base):,}건 / 영화 커버리지: {base.movieId.nunique():,}편")

    # ── 참조 데이터 준비 ──────────────────────────────────
    movies_idx = movies.set_index("movieId")
    tag_lookup = defaultdict(list)
    for row in tags.itertuples(index=False):
        tag_lookup[(row.userId, row.movieId)].append(row.tag)

    user_persona = {uid: assign_persona(uid) for uid in ratings.userId.unique()}

    movie_meta = {}
    for mid in movies.movieId.unique():
        runtime = max(35 * 60, int(rng.normal(MEAN_RUNTIME_MIN, RUNTIME_STD_MIN) * 60))
        n_seg = max(3, runtime // SEGMENT_LEN_SEC)
        peak_seg = int((0.55 + h(f"peak-{mid}") * 0.35) * n_seg)
        movie_meta[mid] = dict(runtime=runtime, n_seg=n_seg, peak_seg=peak_seg)

    pop_rank = ratings.movieId.value_counts().rank(ascending=False)
    pop_weight = ((pop_rank ** -0.6) / (pop_rank ** -0.6).max()).to_dict()

    def completion_prob(rating, persona):
        base_p = 1 / (1 + np.exp(-(rating - 3.0) * 1.7))
        return float(np.clip(base_p + PERSONAS[persona]["completion_bias"], 0.02, 0.98))

    # ── 세션 이벤트 생성 ──────────────────────────────────
    def gen_session(uid, mid, title, genre, persona, start_ts, rating, seq, total, is_first):
        meta = movie_meta[mid]
        n_seg, peak_seg, runtime = meta["n_seg"], meta["peak_seg"], meta["runtime"]
        cfg = PERSONAS[persona]
        device = pick_device(persona, f"dev-{uid}-{mid}-{seq}")
        sid = f"u{uid}-m{mid}-s{seq}"

        completes = rng.random() < completion_prob(rating, persona) * (1.05 if not is_first else 1.0)
        if completes:
            target_frac = rng.uniform(0.94, 1.0)
        else:
            # 평점이 높을수록 중간에 그만두더라도 더 오래 보고 그만두는 경향 반영
            rating_floor = 0.15 + 0.35 * (rating / 5.0)
            target_frac = float(np.clip(rng.beta(2.0, 2.2) * (1 - rating_floor) + rating_floor, 0.04, 0.9))
        target_pos = target_frac * runtime

        rows = []
        t = start_ts
        pos = 0.0
        last_seg = -1

        def emit(evt, position=None, segment=None, tag_value=None, value=None):
            nonlocal t
            ts_str = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()
            rows.append((uid, mid, title, genre, sid, evt, ts_str,
                         None if position is None else round(position, 1),
                         segment, runtime, seq, total, device, tag_value, value))

        emit("session_start", 0.0, None)

        guard = 0
        while pos < target_pos and guard < n_seg * 3:
            guard += 1
            r = rng.random()
            if r < cfg["pause_rate"] / (cfg["pause_rate"] + cfg["seek_rate"] + cfg["backseek_rate"] + 3):
                t += rng.lognormal(4.3, 0.9)  # 일시정지(잠깐 딴짓) 후 재개
                emit("pause", pos, int(pos // SEGMENT_LEN_SEC))
            elif pos > peak_seg * SEGMENT_LEN_SEC * 0.6 and rng.random() < cfg["backseek_rate"] / 10:
                # 인기 구간 근처를 되감아 다시 봄
                back = rng.uniform(1, 3) * SEGMENT_LEN_SEC
                pos = max(0.0, peak_seg * SEGMENT_LEN_SEC + rng.normal(0, SEGMENT_LEN_SEC * 0.4) - back / 3)
                t += rng.lognormal(3.0, 0.7)
                emit("seek_backward", pos, int(pos // SEGMENT_LEN_SEC))
            elif rng.random() < cfg["seek_rate"] / 10:
                pos += rng.uniform(1, 4) * SEGMENT_LEN_SEC
                t += rng.lognormal(3.0, 0.7)
                emit("seek_forward", pos, int(pos // SEGMENT_LEN_SEC))
            else:
                pos += SEGMENT_LEN_SEC * rng.uniform(0.9, 1.1)
                t += rng.lognormal(4.9, 0.35)  # 실제 시청 3분 안팎 진행
                seg = int(pos // SEGMENT_LEN_SEC)
                if seg != last_seg:
                    emit("segment_watch", pos, seg)
                    last_seg = seg

        final_pos = min(pos, runtime)
        if completes:
            t += rng.lognormal(3.5, 0.6)
            emit("session_end", final_pos, int(final_pos // SEGMENT_LEN_SEC))
        else:
            # 마지막으로 '확실히 시청'한 지점은 seek로 도달한 지점보다 약간 낮을 수 있음
            drop_pos = final_pos * rng.uniform(0.82, 1.0)
            t += rng.lognormal(3.2, 0.8)
            emit("drop_off", drop_pos, int(drop_pos // SEGMENT_LEN_SEC))

        completion_frac = final_pos / runtime
        return rows, completion_frac, t, completes

    print("\n" + "=" * 60)
    print("2. 이벤트 생성")
    print("=" * 60)
    start = time.time()

    events_path = out_dir / f"viewing_events_part{args.chunk_index:04d}.csv"
    EVENT_COLS = ["user_id", "movie_id", "movie_title", "genre", "session_id", "event_type",
                  "event_timestamp", "position_sec", "segment_index", "duration_sec",
                  "session_seq", "total_sessions", "device", "tag_value", "value"]

    engagement_rows = []
    heatmap_counter = defaultdict(int)  # (movie_id, segment_index) -> watch_count

    n_total = len(base)
    report_every = max(20_000, n_total // 10)
    event_id = args.chunk_index * 10_000_000_000  # 청크 간 충돌 방지

    with open(events_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id"] + EVENT_COLS)

        for i, row in enumerate(base.itertuples(index=False)):
            uid, mid, rating, ts = row.userId, row.movieId, row.rating, row.timestamp
            if mid not in movies_idx.index:
                continue
            title = movies_idx.loc[mid, "title"]
            genres_raw = movies_idx.loc[mid, "genres"]
            genre = "Unknown" if genres_raw == "(no genres listed)" else genres_raw.split("|")[0]
            persona = user_persona[uid]
            popw = pop_weight.get(mid, 0.3)

            # 재시청 횟수 결정 (평점이 높고 인기 있을수록, 페르소나 배율 반영)
            lam = (0.45 + 1.3 * (rating / 5.0) ** 2) * PERSONAS[persona]["rewatch_mult"] * (0.7 + 0.3 * popw)
            n_extra = min(int(rng.poisson(max(lam, 0))), 8)
            total_sessions = 1 + n_extra

            base_dt = datetime.fromtimestamp(ts - rng.uniform(0.3, 1.0) * movie_meta[mid]["runtime"], tz=timezone.utc)
            session_start_times = [base_dt]
            for _ in range(n_extra):
                gap_days = rng.uniform(2, 260)
                session_start_times.append(session_start_times[-1] + timedelta(days=gap_days))
            session_start_times.sort()

            first_session_end_t = None
            for seq, sdt in enumerate(session_start_times, start=1):
                rows, comp_frac, end_t, completed = gen_session(
                    uid, mid, title, genre, persona, sdt.timestamp(), rating, seq, total_sessions, seq == 1
                )
                for r_ in rows:
                    writer.writerow([event_id] + list(r_))
                    if r_[5] == "segment_watch" and r_[8] is not None:
                        heatmap_counter[(mid, r_[8])] += 1
                    event_id += 1
                if seq == 1:
                    first_session_end_t = end_t
                engagement_rows.append((uid, mid, title, genre, seq, total_sessions, comp_frac, completed))

            # tag_added: 이 유저가 이 영화에 실제로 태그를 남겼다면 이벤트로 삽입
            if (uid, mid) in tag_lookup:
                tag_t = base_dt.timestamp() + rng.uniform(60, 600)
                for tag_text in tag_lookup[(uid, mid)]:
                    writer.writerow([event_id, uid, mid, title, genre, f"u{uid}-m{mid}-s1", "tag_added",
                                      datetime.fromtimestamp(tag_t, tz=timezone.utc).isoformat(),
                                      None, None, movie_meta[mid]["runtime"], 1, total_sessions,
                                      pick_device(persona, f"dev-{uid}-{mid}-tag"), tag_text, None])
                    event_id += 1
                    tag_t += rng.uniform(1, 5)

            # rating_given: 첫 세션 종료 후 얼마 뒤에 평점 등록
            if first_session_end_t is not None:
                rating_t = first_session_end_t + rng.uniform(600, 10800)
                writer.writerow([event_id, uid, mid, title, genre, f"u{uid}-m{mid}-s1", "rating_given",
                                  datetime.fromtimestamp(rating_t, tz=timezone.utc).isoformat(),
                                  None, None, movie_meta[mid]["runtime"], 1, total_sessions,
                                  pick_device(persona, f"dev-{uid}-{mid}-rate"), None, float(rating)])
                event_id += 1

            if (i + 1) % report_every == 0:
                elapsed = time.time() - start
                print(f"  진행: {i+1:,}/{n_total:,} ({(i+1)/n_total*100:.1f}%) | {elapsed:.0f}초 경과")

    elapsed = time.time() - start
    print(f"\n  이벤트 생성 완료: {elapsed:.1f}초 소요 | 총 이벤트 {event_id - args.chunk_index * 10_000_000_000:,}건")

    # ── heatmap 저장 ──────────────────────────────────
    print("\n3. movie_segment_heatmap 집계")
    heat_rows = defaultdict(list)
    for (mid, seg), cnt in heatmap_counter.items():
        heat_rows[mid].append((seg, cnt))

    heatmap_path = out_dir / f"movie_segment_heatmap_part{args.chunk_index:04d}.csv"
    with open(heatmap_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_id", "movie_title", "genre", "segment_index", "watch_count", "is_peak_segment"])
        for mid, seglist in heat_rows.items():
            title = movies_idx.loc[mid, "title"]
            genres_raw = movies_idx.loc[mid, "genres"]
            genre = "Unknown" if genres_raw == "(no genres listed)" else genres_raw.split("|")[0]
            peak_seg_idx = max(seglist, key=lambda x: x[1])[0]
            for seg, cnt in sorted(seglist):
                writer.writerow([mid, title, genre, seg, cnt, seg == peak_seg_idx])
    print(f"  저장: {heatmap_path}")

    # ── engagement summary 저장 ──────────────────────────────────
    print("\n4. user_movie_engagement_summary 집계")
    eng_df = pd.DataFrame(engagement_rows, columns=[
        "user_id", "movie_id", "movie_title", "genre", "session_seq", "total_sessions", "completion_frac", "completed"
    ])
    summary = eng_df.groupby(["user_id", "movie_id", "movie_title", "genre"]).agg(
        total_sessions=("total_sessions", "max"),
        avg_completion=("completion_frac", "mean"),
        last_completion=("completion_frac", "last"),
    ).reset_index()
    ratings_lookup = ratings.set_index(["userId", "movieId"]).rating
    summary["user_rating"] = summary.apply(lambda r: ratings_lookup.get((r.user_id, r.movie_id), np.nan), axis=1)
    summary["completed_fully"] = summary["last_completion"] >= 0.92
    summary["is_rewatch"] = summary["total_sessions"] > 1

    summary_path = out_dir / f"user_movie_engagement_summary_part{args.chunk_index:04d}.csv"
    summary.to_csv(summary_path, index=False)
    print(f"  저장: {summary_path} ({len(summary):,}행)")

    print("\n완료.")


if __name__ == "__main__":
    main()
