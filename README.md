# 세움터 건축물대장 데이터 분석

국토교통부 건축물대장 원시 데이터(`.zip`)를 Parquet으로 변환하는 파이프라인입니다.

## 디렉토리 및 파일 구조

```
data/
  original/               # 원본 zip 파일 (git 추적 제외)
  parquet/                # 변환된 parquet 파일 (git 추적 제외)
  schema/                 # 컬럼 정의 txt 파일
  dataset_catalog.json    # zip ↔ schema 매핑
notebooks/                # 분석용 노트북
results/                  # 분석 결과 저장
scripts/                  # 데이터 처리 스크립트
tests/                    # 테스트 및 클린업 스크립트
auri.py                   # 표 및 그래프 스타일 설정 스크립트 (import auri 로 사용)
requirements.txt          # 패키지 목록
```

## 초기 설정

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Jupytext 사용 가이드 (Notebook 동기화)

이 프로젝트는 Jupytext를 사용하여 Jupyter Notebook 파일(`.ipynb`)과 파이썬 스크립트(`.py`)를 연동하여 사용합니다. (`pyproject.toml`에 `formats = "ipynb,py:percent"`로 설정되어 있습니다.)

```powershell
# notebooks 폴더 내의 노트북과 파이썬 스크립트를 동기화하고 포맷팅(isort, ruff)을 적용합니다.
.\scripts\sync_notebooks.ps1
```

- **버전 관리 권장사항**: Git에는 충돌 해결이 어려운 `.ipynb` 파일 대신 변환된 `.py` 스크립트를 커밋하고 이를 기준으로 작업하는 것을 권장합니다.
- **팁**: VSCode 등의 에디터에서 Jupytext 익스텐션을 설치하여 사용하면 파일 열기 및 저장 시 자동 동기화가 가능합니다. (다만, 동기화 후 린트 및 포맷팅이 적용되므로, 커밋 전에는 수동으로 동기화하는 것을 권장합니다.)

## 신규 데이터 갱신 절차

### 1. 원본 데이터 교체

`data/original/` 안의 `.zip` 파일을 새 버전으로 교체합니다.

> 파일명 형식: `국토교통부_건축물대장_{데이터셋명}+(YYYY년+MM월).zip`

### 2. 카탈로그 재생성

`build_catalog.py`을 실행하여 zip 파일과 schema를 매핑하는 `dataset_catalog.json`을 갱신합니다.

> **주의:** `build_catalog.py` 내부의 연월 변수(`YEAR`, `MONTH`)를 새 데이터의 연월로 수정하세요.

```powershell
python scripts/build_catalog.py
```

### 3. 기존 Parquet 삭제 (선택)

재변환이 필요한 데이터셋의 parquet 파일만 삭제하면, 해당 항목만 재처리됩니다.

```powershell
# 전체 삭제 후 재변환
Remove-Item data\parquet\*.parquet
```

### 4. Parquet 변환 실행

```powershell
python scripts/convert_to_parquet.py
```

이미 parquet이 존재하는 데이터셋은 자동으로 건너뜁니다.

---

## 주요 스크립트

| 파일 | 역할 |
|---|---|
| `scripts/build_catalog.py` | zip ↔ schema 매핑 카탈로그 생성 (`convert_to_parquet.py` 실행 전 실행 필수)|
| `scripts/convert_to_parquet.py` | zip → parquet 일괄 변환 |
| `scripts/show_parquet.py` | 변환된 전체 parquet 파일들의 스키마 및 샘플 데이터 연속 조회 |
| `scripts/analyze_structure.py` | parquet 스키마/샘플 확인 |
| `tests/test_cleanup.py` | Polars 임시 파일 정리 테스트 (경로 자동 인식) |
| `tests/delete_tmp.py` | 남은 임시 폴더 수동 삭제 |

## 참고

- 변환 중 `data/tmp*/` 임시 폴더가 생성되었다가 자동 삭제됩니다.
- 스크립트 강제 종료 시 `tests/delete_tmp.py`로 잔여 임시 폴더를 제거할 수 있습니다.
