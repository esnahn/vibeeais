# 세움터 건축물대장 데이터 분석

국토교통부 건축물대장 원시 데이터(`.zip`)를 Parquet으로 변환하는 파이프라인입니다.

## 디렉토리 및 파일 구조

```
data/
  original/               # 원본 zip 파일 (git 추적 제외)
    202502/               #   └ 연월 하위 폴더 (예: 2025년 02월)
    202512/               #   └ 연월 하위 폴더 (예: 2025년 12월)
  parquet/                # 변환된 parquet 파일 (git 추적 제외)
    202502/               #   └ 연월 하위 폴더
    202512/               #   └ 연월 하위 폴더
  schema/                 # 컬럼 정의 txt 파일
    202502/               #   └ 연월 하위 폴더
    202512/               #   └ 연월 하위 폴더
  region/                 # 행정구역 코드 CSV (시도·시군구·법정동)
  replacement_cost/       # 재조달원가 관련 CSV (용도·구조·원가·내구연한)
    2024/                 #   └ 기준연도 하위 폴더
  dataset_catalog.json    # zip ↔ schema 매핑
notebooks/                # 분석용 노트북 (.ipynb + .py)
results/                  # 분석 결과 저장
  stat_construction_duration/  # 공사기간 통계
  stat_econvalue/              # 경제적 가치(재조달원가) 통계
  stat_permit_completion/      # 인허가·사용승인 통계
scripts/                  # 데이터 처리·변환·유틸 스크립트
tests/                    # 테스트 및 클린업 스크립트
auri.py                   # 표 및 그래프 스타일 설정 스크립트 (import auri 로 사용)
auri.mplstyle             # matplotlib 스타일 시트 (auri.py 에서 참조)
.pre-commit-config.yaml   # pre-commit 훅 설정
pyproject.toml            # Jupytext·Ruff 설정
requirements.txt          # pip 패키지 목록
freeze.txt                # pip freeze 스냅샷
```

## 초기 설정

```powershell
scoop install python gawk
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

### 1. 원본 데이터 및 스키마 수집 (자동화 스크립트)

데이터 포털(hub.go.kr)에서 스키마와 원본 `.zip` 파일을 자동으로 다운로드합니다. 각 스크립트 파일 상단의 `YEAR`와 `MONTH` 변수를 타겟 연월(예: "2025", "12")로 수정한 후 아래 순서대로 실행하세요.

**1.1. 스키마 수집**: 대상 연월의 스키마(컬럼 정보) 텍스트 파일을 `data/schema/YYYYMM/`에 저장합니다.
   ```powershell
   python scripts/scrape_schemas.py
   ```
**1.2. 다운로드 목록 생성**: 다운로드 대상 항목을 파악하여 `data/original/originals_list_YYYYMM.json`에 목록을 저장합니다. 필요 시 JSON 파일을 열어 다운로드하지 않을 항목을 편집할 수 있습니다.
   ```powershell
   python scripts/originals_list_collect.py
   ```
**1.3. 원본 파일 다운로드**: 생성된 목록을 바탕으로 `data/original/YYYYMM/` 폴더에 `.zip` 파일들을 일괄 다운로드합니다.
   - **실패 항목 재시도**: 다운로드 중 실패한 항목이 발생하면 `originals_list_YYYYMM_failed.json` 파일에 별도로 기록됩니다. 스크립트 내부의 `RETRY_FAILED = True` 로 변경하고 다시 실행하면 실패한 항목만 재다운로드를 시도합니다. 성공 시 실패 파일은 자동 삭제됩니다.
   ```powershell
   python scripts/originals_download.py
   ```

> **참고 (수동 다운로드 시)**: 파일명 형식은 `국토교통부_{카테고리}_{데이터셋명}+(YYYY년+MM월).zip` 이어야 합니다.

### 2. 카탈로그 (재)생성

`build_catalog.py`을 실행하여 zip 파일과 schema를 매핑하는 `dataset_catalog.json`을 갱신합니다.
(`data/original/` 하위의 모든 연월(YYYYMM) 폴더를 자동 탐색하여 전체 카탈로그를 빌드합니다.)

```powershell
python scripts/build_catalog.py
```

### 3. 기존 Parquet 삭제 (선택)

재변환이 필요한 데이터셋의 parquet 파일만 삭제하면, 해당 항목만 재처리됩니다.

```powershell
# 특정 연월 전체 삭제 후 재변환
Remove-Item data\parquet\202512\*.parquet
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
| `scripts/scrape_schemas.py` | 대상 연월의 스키마 수집 |
| `scripts/originals_list_collect.py` | 다운로드 목록 수집 |
| `scripts/originals_download.py` | 원본 zip 파일 다운로드 |
| `scripts/build_catalog.py` | zip ↔ schema 매핑 카탈로그 생성 |
| `scripts/convert_to_parquet.py` | zip → parquet 일괄 변환 |
| `scripts/show_parquet.py` | 변환된 전체 parquet 파일들의 스키마 및 샘플 데이터 연속 조회 |
| `scripts/analyze_structure.py` | parquet 스키마/샘플 확인 |
| `scripts/build_bjdong.py` | 법정동 코드 CSV 빌드 (`data/region/`) |
| `scripts/build_kcad_sgg.py` | 한국행정구역분류 시군구 코드 CSV 빌드 (`data/region/`) |
| `scripts/check_encoding.py` | zip 원본 파일 인코딩 진단 |
| `scripts/compare_excel.py` | 두 Excel 파일 비교 (describe 통계 기반) |
| `scripts/linebreak_check.awk` | 파이프 구분 텍스트의 줄바꿈 오류 탐지 (gawk) |
| `scripts/linebreak_replace.awk` | 줄바꿈 오류 복원 (gawk) |
| `scripts/sync_notebooks.ps1` | Jupytext 동기화 + isort/ruff 포맷팅 |
| `scripts/run_notebook.ps1` | .py 노트북 실행 → HTML 출력 (ipynb 자동 삭제) |
| `tests/test_cleanup.py` | Polars 임시 파일 정리 테스트 (경로 자동 인식) |
| `tests/delete_tmp.py` | 남은 임시 폴더 수동 삭제 |
| `tests/test_kcad_assumptions.py` | 행정구역분류 코드 가정 검증 |
| `tests/test_verify_linebreak_fix.py` | 줄바꿈 오류 복원 결과 검증 |

## 참고

- 변환 중 `data/tmp*/` 임시 폴더가 생성되었다가 자동 삭제됩니다.
- 스크립트 강제 종료 시 `tests/delete_tmp.py`로 잔여 임시 폴더를 제거할 수 있습니다.
