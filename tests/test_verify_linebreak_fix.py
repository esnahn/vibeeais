# -*- coding: utf-8 -*-
"""
주택인허가 동별개요 Parquet 데이터의 줄바꿈(Linebreak) 포맷 오류 해결 검증 테스트

이 테스트는 과거 동명 필드 내부에 원치 않는 줄바꿈 문자(\n)가 포함되어 있어
레코드가 여러 줄로 쪼개지는 파싱 오류를 일으켰던 특정 주택 레코드(PK: 1017100005635)가
최신 Parquet DB에 정상적으로 유실 없이 통합 적재되었는지 검증합니다.

오류 현상 (오류 해결 전):
- PK "1017100005635" 레코드는 '동명' 필드 내부에 줄바꿈 문자(\n)가 포함되어 있었습니다.
- 이로 인해 원본 텍스트 파일 파싱 시 하나의 레코드가 아래처럼 2개의 라인으로 쪼개졌습니다:
  > Line 1: 1017100005635|1017100007809|서울특별시 강서구 마곡동 블록|SH아파트(10-1BL)|11500|10500|2|0000|0000|마곡도시개발사업지구|10-1단지||1|부속건축물|관리사무소(2F),
  > Line 2: \t\t\t\t|02000|공동주택|0|0|0|0|0|0|0|0|0|0|21|철근콘크리트구조|10|(철근)콘크리트|503.26|1267.08|0|341.37|0|1|5.2|0|0|5.1|3.0|0|0|15|0|20220813
- 결과적으로 Polars 등에서 데이터 적재 시:
  * 첫 번째 라인(Line 1)은 '동명' 필드 도중에서 잘려 뒤쪽 컬럼들이 모두 null로 채워지고 (연면적, 생성일자 등 유실),
  * 두 번째 라인(Line 2)은 비정상적인 PK(탭/공백)로 시작하는 잘못된 레코드로 쪼개져 데이터 오염이 발생했습니다.

오류 해결 방식 (scripts/linebreak_replace.awk 참고):
- 컬럼 구분자(pipe '|') 개수가 부족하여 레코드가 잘린 경우, 줄바꿈 문자(\n)를
  공백(" ")으로 대체하여 다음 라인과 이어 붙임으로써 정상적인 1개의 레코드로 복원하였습니다.
  * 수정 후: '동명' 필드의 줄바꿈(\n)이 공백(" ")으로 치환되어 전체 데이터가 하나의 라인에 정상적으로 로드됩니다.
"""

import sys
from decimal import Decimal
from pathlib import Path

import polars as pl


def find_latest_db_dir(base_dir: Path) -> Path:
    """data/parquet 하위의 가장 최신 YYYYMM 데이터베이스 디렉토리를 탐색합니다."""
    data_dir = base_dir / "data" / "parquet"
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    subdirs = [
        d
        for d in data_dir.iterdir()
        if d.is_dir() and d.name.isdigit() and len(d.name) == 6
    ]
    if not subdirs:
        raise FileNotFoundError(f"No YYYYMM directories found in {data_dir}")
    subdirs.sort(key=lambda x: int(x.name))
    return subdirs[-1]


def test_verify_linebreak_fix():
    """
    주택인허가 동별개요 Parquet 파일에서 줄바꿈(\n) 오류가 공백(" ")으로 대체되어
    정상적으로 통합 적재된 특정 레코드의 필드 정합성을 검증합니다.
    """
    base_dir = Path(__file__).resolve().parent.parent
    db_dir = find_latest_db_dir(base_dir)
    print(f"Latest database directory: {db_dir.name}")

    parquet_path = db_dir / "주택인허가_동별개요.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"File not found: {parquet_path}")

    # Polars를 사용하여 Parquet 데이터 로드
    df = pl.read_parquet(parquet_path)
    pk_value = "1017100005635"
    result = df.filter(pl.col("관리_동별_개요_PK") == pk_value)

    # 1. 대상 레코드 존재 여부 확인 (줄바꿈 오류가 해결되어 1개의 레코드로 정상 적재됨)
    assert result.height > 0, (
        f"Record with PK {pk_value} not found in {parquet_path.name}!"
    )

    # 2. 데이터 정상성 검증용 기대값 스냅샷
    expected = {
        "관리_동별_개요_PK": "1017100005635",
        "관리_주택대장_PK": "1017100007809",
        "건물_명": "SH아파트(10-1BL)",
        # 줄바꿈(\n) 문자가 공백(" ")으로 치환되어 정상 복원된 동명 텍스트 값
        "동명": "관리사무소(2F), 주민공동시설(2F), 작은도서관, 보육시설, 경로당, 커뮤니티시설, 게스트룸 \t\t\t\t",
        "연면적(㎡)": Decimal("1267.080000000"),
        "생성_일자": "20220813",
    }

    actual = result.to_dicts()[0]

    # 3. 스냅샷 필드 값 1:1 대조 및 유실 여부 검증
    for key, expected_val in expected.items():
        assert actual.get(key) == expected_val, (
            f"Mismatch for key '{key}': {actual.get(key)} != {expected_val}"
        )

    print(
        f"Verification successful! Record with PK {pk_value} matches expected snapshot."
    )


if __name__ == "__main__":
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    test_verify_linebreak_fix()
