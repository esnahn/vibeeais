# 세움터 건축물대장 데이터 분석

국토교통부 건축물대장 원시 데이터(`.zip`)를 Parquet으로 변환하는 파이프라인입니다.

## 디렉토리 구조

```
data/
  original/   # 원본 zip 파일 (git 추적 제외)
  schema/     # 컬럼 정의 txt 파일
  parquet/    # 변환된 parquet 파일 (git 추적 제외)
  dataset_catalog.json  # zip ↔ schema 매핑
tests/        # 테스트 스크립트
```

## 신규 데이터 갱신 절차

### 1. 원본 데이터 교체

`data/original/` 안의 `.zip` 파일을 새 버전으로 교체합니다.

> 파일명 형식: `국토교통부_건축물대장_{데이터셋명}+(YYYY년+MM월).zip`

### 2. 카탈로그 재생성

zip 파일과 schema를 매핑하는 `dataset_catalog.json`을 갱신합니다.

> **주의:** `build_catalog.py` 내부의 연월 패턴(`2025년+12월`)을 새 데이터의 연월로 수정하세요.

```bash
python build_catalog.py
```

### 3. 기존 Parquet 삭제 (선택)

재변환이 필요한 데이터셋의 parquet 파일만 삭제하면, 해당 항목만 재처리됩니다.

```bash
# 전체 삭제 후 재변환
Remove-Item data\parquet\*.parquet
```

### 4. Parquet 변환 실행

```bash
python convert_to_parquet.py
```

이미 parquet이 존재하는 데이터셋은 자동으로 건너뜁니다.

---

## 주요 스크립트

| 파일 | 역할 |
|---|---|
| `build_catalog.py` | zip ↔ schema 매핑 카탈로그 생성 |
| `convert_to_parquet.py` | zip → parquet 일괄 변환 |
| `show_parquet.py` | 변환된 전체 parquet 파일들의 스키마 및 샘플 데이터 연속 조회 |
| `analyze_structure.py` | parquet 스키마/샘플 확인 |
| `tests/test_cleanup.py` | Polars 임시 파일 정리 테스트 |
| `tests/delete_tmp.py` | 남은 임시 폴더 수동 삭제 |

## 참고

- 변환 중 `data/tmp*/` 임시 폴더가 생성되었다가 자동 삭제됩니다.
- 스크립트 강제 종료 시 `tests/delete_tmp.py`로 잔여 임시 폴더를 제거할 수 있습니다.
