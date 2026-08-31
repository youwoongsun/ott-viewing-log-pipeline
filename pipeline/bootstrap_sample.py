"""
부하 테스트용 데이터 증폭 스크립트
=========================================================================
data/kafka_sample_2000.csv 처럼 작은 샘플을 --target-rows 만큼 늘려서
kafka_producer.py로 흘려보낼 대용량(예: 10만 건) 테스트 파일을 만든다.

단순히 원본을 그대로 여러 번 이어붙이면 (user_id, movie_id, 시간대)가
완전히 겹쳐서 매 cycle이 "같은 세션 중복 전송"이 돼버린다. 그러면
2단계(대용량 실행)에서 만들어지는 세션 수가 늘지 않고 사실상
장애 실험 (E)(중복 전송)를 미리 섞어버리는 셈이 된다. 그래서 cycle마다
event_timestamp를 큼직하게(기본 400일) 밀어서, cycle끼리는 서로 다른
세션으로 판정되게 만든다. event_id/session_id도 cycle 번호를 붙여
구분 가능하게 한다.

사용 예:
  python pipeline/bootstrap_sample.py --input data/kafka_sample_2000.csv \
      --output data/kafka_sample_bootstrap_100k.csv --target-rows 100000
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd


def main():
    ap = argparse.ArgumentParser(description="샘플 CSV를 target-rows까지 반복 증폭")
    ap.add_argument("--input", required=True, help="원본 샘플 CSV 경로")
    ap.add_argument("--output", required=True, help="증폭된 CSV를 저장할 경로")
    ap.add_argument("--target-rows", type=int, required=True, help="최종 목표 행 수")
    ap.add_argument("--cycle-shift-days", type=int, default=400,
                     help="cycle마다 event_timestamp를 얼마나 밀지 (기본 400일). "
                          "세션 타임아웃(30분)보다 훨씬 크면 cycle끼리 세션이 안 섞인다")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        sys.exit(f"입력 파일을 찾을 수 없습니다: {in_path}")

    base = pd.read_csv(in_path, encoding="utf-8")
    if "event_timestamp" not in base.columns:
        sys.exit("event_timestamp 컬럼이 없는 CSV입니다. v2 이벤트 생성기 출력만 지원합니다.")

    base_ts = pd.to_datetime(base["event_timestamp"], utc=True)
    n_base = len(base)
    n_cycles = -(-args.target_rows // n_base)  # ceil division

    print(f"원본 {n_base:,}행 -> 목표 {args.target_rows:,}행 (cycle {n_cycles}회 필요)")

    chunks = []
    remaining = args.target_rows
    for cycle in range(n_cycles):
        take = min(n_base, remaining)
        if take <= 0:
            break
        chunk = base.iloc[:take].copy()
        shift = timedelta(days=args.cycle_shift_days * cycle)
        chunk["event_timestamp"] = (base_ts.iloc[:take] + shift).apply(
            lambda t: t.isoformat()
        )
        # cycle 0은 원본 그대로 (기존 세션 캡처 화면과 비교하기 쉽게),
        # cycle 1 이상만 id에 -c{n} 접미사를 붙여 구분한다.
        if cycle > 0:
            if "event_id" in chunk.columns:
                chunk["event_id"] = chunk["event_id"].astype(str) + f"c{cycle}"
            if "session_id" in chunk.columns:
                chunk["session_id"] = chunk["session_id"].astype(str) + f"-c{cycle}"
        chunks.append(chunk)
        remaining -= take

    result = pd.concat(chunks, ignore_index=True)
    result.to_csv(args.output, index=False, encoding="utf-8")
    print(f"완료: {len(result):,}행 저장 -> {args.output}")


if __name__ == "__main__":
    main()
