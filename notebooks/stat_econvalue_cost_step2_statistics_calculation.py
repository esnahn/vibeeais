# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd

pd.options.display.unicode.east_asian_width = True

# %% [markdown]
# # Step 2: 재조달원가 산출
#
# Step 1에서 생성한 조인 Parquet에 재조달원가 단가/내구연한 참조 데이터를 결합하여
# 층별 재조달원가 및 잔존재조달원가를 산출하고 Parquet으로 저장합니다.
#
# 시도명, 층_용도_대분류_코드/명 파생 컬럼을 추가하여 print tables (Step 3)에서
# 기존 결과표와 동일한 출력을 낼 수 있도록 합니다.

# %%
# date for data source (should be YYYY/12 for normal year, but for 2024 set as 2025/02)
YEAR = "2025"
MONTH = "02"
upper = "20241231"  # 통계 산출을 위한 기준일

try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"
ref_dir = base_dir / "data" / "replacement_cost" / upper[:4]  # year from upper
region_dir = base_dir / "data" / "region"
results_dir = base_dir / "results" / "stat_econvalue" / f"{YEAR}{int(MONTH):02d}"

# print(f"Base Directory: {base_dir}")
print(f"Data Directory: {data_dir.relative_to(base_dir)}")
print(f"Reference Directory: {ref_dir.relative_to(base_dir)}")
print(f"Region Directory: {region_dir.relative_to(base_dir)}")
print(f"Results Directory: {results_dir.relative_to(base_dir)}")

# %% [markdown]
# ### 데이터 로드

# %%
con = duckdb.connect()

# Step 1 조인 결과
path_data = results_dir / "기본개요_표제부_층별개요_조인.parquet"
con.execute(f"CREATE VIEW joined_db AS SELECT * FROM '{path_data}'")

count_joined = con.sql("SELECT COUNT(*) FROM joined_db").fetchone()[0]
print(f"Step 1 조인 데이터 건수: {count_joined:,}")

# %%
# 재조달원가 참조 데이터
df_재조달구조내구연한 = pd.read_csv(
    ref_dir / "재조달구조내구연한.csv",
    encoding="cp949",
    converters={
        # Decimal instead of float: preserve all digits from text
        "내구연한(중앙값)": lambda x: Decimal(x)
    },  # NOTE: "If converters are specified, they will be applied INSTEAD of dtype conversion."
)
df_재조달구조코드연계 = pd.read_csv(
    ref_dir / "재조달구조코드연계.csv", encoding="cp949", dtype=str
)
df_재조달용도구조원가내구연한 = pd.read_csv(
    ref_dir / "재조달용도구조원가내구연한.csv",
    encoding="cp949",
    converters={
        # Decimal instead of float: preserve all digits from text
        "재조달원가 단가(원/㎡; 중앙값)": lambda x: Decimal(x),
        "내구연한(중앙값)": lambda x: Decimal(x),
    },
)
df_재조달용도원가 = pd.read_csv(
    ref_dir / "재조달용도원가.csv",
    encoding="cp949",
    converters={
        # Decimal instead of float: preserve all digits from text
        "재조달원가 단가(원/㎡; 중앙값)": lambda x: Decimal(x)
    },
)
df_재조달용도코드연계 = pd.read_csv(
    ref_dir / "재조달용도코드연계.csv", encoding="cp949", dtype=str
)

print("참조 데이터 로드 완료")

# %% [markdown]
# ### 기초 파생변수 생성 및 유효 레코드 필터링
#
# 1. **`준공년도` / `경과년수`**: `표제부_사용승인_일`(YYYYMMDD)에서 연도를 추출하여 산출;
#    형식이 유효하지 않은 경우 각각 `0` / `100`으로 대체합니다.
# 2. **층 용도 코드 필터**: `층_주_용도_코드`가 5자리이고 `'40000'` 미만인 층만 선별합니다.
# 3. **시군구 코드 유효성 검증**: `code_sgg.csv`와 INNER JOIN하여 유효하지 않은 시군구 코드를 제거합니다.

# %%
# 표제부의 사용승인일을 기준으로 준공년도 및 경과년수를 추출하고
# 층별개요 중 주용도코드가 유효한(길이가 5이고 40000 미만인) 층 정보만 선별합니다.

con.execute(f"""
CREATE TEMP VIEW base_age_usage AS
SELECT
    *,
    CASE
        WHEN regexp_matches(표제부_사용승인_일, '^[0-9]{{8}}$')
        THEN CAST(LEFT(표제부_사용승인_일, 4) AS INTEGER)
        ELSE 0
    END AS 준공년도,
    CASE
        WHEN regexp_matches(표제부_사용승인_일, '^[0-9]{{8}}$')
        THEN {upper[:4]} - CAST(LEFT(표제부_사용승인_일, 4) AS INTEGER)
        ELSE 100
    END AS 경과년수
FROM joined_db
WHERE LENGTH(층_주_용도_코드) = 5
    AND 층_주_용도_코드 < '40000'
""")

count_base = con.sql("SELECT COUNT(*) FROM base_age_usage").fetchone()[0]
print(f"층 용도 코드 필터링 후 건수: {count_base:,}")


# 시군구 코드 로드 및 조인 (유효하지 않은 시군구 코드 2,510건 필터링 용도)
df_sgg = pd.read_csv(region_dir / "code_sgg.csv", dtype=str)

con.execute("""
CREATE TEMP VIEW base_sgg_joined AS
SELECT
    r.*,
    s.시군구명
FROM base_age_usage r
JOIN df_sgg s
    ON r.시군구_코드 = s.시군구코드
""")

count_filtered = con.sql("SELECT COUNT(*) FROM base_sgg_joined").fetchone()[0]
print(f"유효 지역 필터링 후 건수: {count_filtered:,}")

# %% [markdown]
# ### 재조달원가 코드 연계 및 단가/내구연한 조인

# %%
# 층_주_용도_코드 → 재조달용도코드, 층_구조_코드 → 재조달구조코드 매핑
con.execute("""
CREATE TEMP VIEW result_ref AS
SELECT
    r.*,
    u."재조달용도코드",
    u."재조달용도명",
    s."재조달구조코드",
    s."재조달구조명"
FROM base_sgg_joined r
LEFT JOIN df_재조달용도코드연계 u
    ON r."층_주_용도_코드" = u."층_주_용도_코드"
LEFT JOIN df_재조달구조코드연계 s
    ON r."층_구조_코드" = s."구조_코드"
""")

# 용도×구조 단가 → 용도 단가 → 구조 내구연한 순 fallback으로 재조달_단가/내구연한 결정
con.execute("""
CREATE TEMP VIEW result_calc_base AS
SELECT
    r.*,
    COALESCE(
        b."재조달원가 단가(원/㎡; 중앙값)",
        u."재조달원가 단가(원/㎡; 중앙값)"
    ) AS "재조달_단가",
    COALESCE(
        b."내구연한(중앙값)",
        s."내구연한(중앙값)",
        45.0
    ) AS "내구연한"
FROM result_ref r
LEFT JOIN df_재조달용도구조원가내구연한 b
    ON r."재조달용도코드" = b."재조달용도코드" AND r."재조달구조코드" = b."재조달구조코드"
LEFT JOIN df_재조달용도원가 u
    ON r."재조달용도코드" = u."재조달용도코드"
LEFT JOIN df_재조달구조내구연한 s
    ON r."재조달구조코드" = s."재조달구조코드"
""")

# %% [markdown]
# ### 잔존비율 및 재조달원가 산출

# %%
# 잔존비율(최소 0.1) → 잔존연한 → 재조달원가 → 잔존재조달원가 일괄 산출
con.execute("""
CREATE TEMP VIEW econvalue_result AS
WITH cte_ratio AS (
    SELECT
        *,
        GREATEST((내구연한 - 경과년수) / NULLIF(내구연한, 0), 0.1) AS 잔존비율_최소값보정
    FROM result_calc_base
),
cte_cost AS (
    SELECT
        *,
        내구연한 * 잔존비율_최소값보정 AS 잔존연한,
        CAST(COALESCE(CAST(층_면적 AS DOUBLE) * CAST(재조달_단가 AS DOUBLE), 0) AS INT64) AS 재조달원가
    FROM cte_ratio
)
SELECT
    *,
    -- 원본 코드와 동일: 이미 INT64로 변환된 재조달원가 × 잔존비율
    CAST(COALESCE(재조달원가 * 잔존비율_최소값보정, 0) AS INT64) AS 잔존재조달원가
FROM cte_cost
""")

# %% [markdown]
# ### Sanity Check

# %%
stats = con.sql("""
    SELECT
        COUNT(DISTINCT 관리_건축물대장_PK) AS 총_동수,
        COUNT(*) AS 총_레코드_수,
        SUM(층_면적) AS 총_층_면적,
        SUM(재조달원가) AS 총_재조달원가,
        SUM(잔존재조달원가) AS 총_잔존재조달원가
    FROM econvalue_result
""").fetchdf()

print("재조달원가 산출 결과 (Sanity Check):")
print(
    stats.to_string(
        index=False,
        formatters={
            "총_동수": lambda x: f"{x:,.0f}",
            "총_레코드_수": lambda x: f"{x:,.0f}",
            "총_층_면적": lambda x: f"{x:,.0f}",
            "총_재조달원가": lambda x: f"{x:,.0f}",
            "총_잔존재조달원가": lambda x: f"{x:,.0f}",
        },
    )
)
# 기존 결과 (2024년 말 기준, 사용승인_일 > '19000101'):
# 총_동수:         7,290,246
# 총_레코드_수:   19,560,687
# 총_층_면적:  4,356,518,178
# 총_재조달원가: 6,466,205,512,623,296
# 총_잔존재조달원가: 3,893,514,963,436,596
#
# 수정 후 결과 (2024년 말 기준, 사용승인_일 >= '19000101'):
# 총_동수:         7,291,347
# 총_레코드_수:   19,562,876
# 총_층_면적:  4,356,587,118
# 총_재조달원가: 6,466,289,925,694,434
# 총_잔존재조달원가: 3,893,523,348,409,434

# %% [markdown]
# ### Parquet 파일 저장

# %%
path_to = results_dir / "econvalue_재조달원가.parquet"
print(f"\nSaving to {path_to.name} ...")
con.execute(
    f"COPY (SELECT * FROM econvalue_result) TO '{path_to.as_posix()}' (FORMAT PARQUET)"
)
print("Save complete.")
