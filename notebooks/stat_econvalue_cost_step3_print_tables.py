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
from pathlib import Path

import duckdb
import pandas as pd

pd.options.display.unicode.east_asian_width = True

# %% [markdown]
# # Step 3: 재조달원가 통계표 생성 (Print Tables)
#
# Step 2에서 생성한 재조달원가 Parquet 데이터를 사용하여 각종 집계 통계표를 생성합니다.

# %%
YEAR = "2025"
MONTH = "02"

try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"
region_dir = base_dir / "data" / "region"
results_dir = base_dir / "results" / "stat_econvalue" / f"{YEAR}{int(MONTH):02d}"

print(f"Data Directory: {data_dir.relative_to(base_dir)}")
print(f"Region Directory: {region_dir.relative_to(base_dir)}")
print(f"Results Directory: {results_dir.relative_to(base_dir)}")

# %%
con = duckdb.connect()

# Step 2 결과 로드
path_db = results_dir / "econvalue_재조달원가.parquet"
con.execute(f"CREATE VIEW db AS SELECT * FROM read_parquet('{path_db.as_posix()}')")

count_db = con.sql("SELECT COUNT(*) FROM db").fetchone()[0]
print(f"Step 2 데이터 건수: {count_db:,}")

# %% [markdown]
# ### 표제부 기반 용도 코드 매핑 생성
# 동_주_용도_코드별_합계.csv (표제부 기준) 대체

# %%
# 표제부 용도 코드–코드명 매핑 생성
path_표제부 = data_dir / "건축물대장_표제부.parquet"
query_usage_code = f"""
CREATE TEMP VIEW usage_code AS
SELECT
    주_용도_코드,
    FIRST(주_용도_코드_명) AS 주_용도_코드_명
FROM read_parquet('{path_표제부.as_posix()}')
WHERE 주_용도_코드 IS NOT NULL
GROUP BY 주_용도_코드
"""
con.execute(query_usage_code)
print("용도 코드 매핑 (usage_code) 생성 완료")

# %%
# 시도 코드 로드
df_sido_code = pd.read_csv(region_dir / "code_sido.csv", dtype=str)
con.execute("CREATE TEMP VIEW sido_code AS SELECT * FROM df_sido_code")
print("시도 코드 로드 완료")

# %% [markdown]
# ### 기초 파생변수 (시도_코드, 층_용도_대분류_코드) 생성
# 기존 코드와 동일하게 처리합니다.

# %%
# result1: 시도코드, 층_용도_대분류코드_임시 생성
con.execute("""
CREATE TEMP VIEW result1 AS
SELECT
    *,
    CASE
        WHEN regexp_matches(시군구_코드, '^[0-9]{5}$')
        THEN LEFT(시군구_코드, 2)
        ELSE NULL
    END AS 시도_코드,
    CASE
        WHEN LENGTH(층_주_용도_코드) = 5  -- includes Zxxxx codes in result2
        THEN LEFT(층_주_용도_코드, 2) || '000'
        ELSE NULL
    END AS 층_용도_대분류_코드_임시
FROM db
""")

# %% [markdown]
# ### Z코드 재매핑 및 명칭 조인
# 구 법령 코드(Z코드)를 현행 코드로 매핑 후 명칭(시도_명, 층_용도_대분류_명)을 조인합니다.
#
# - Z3000 근린생활시설은 03000 제1종근린생활시설으로,
# - Z5000 문화및집회시설은 05000 문화및집회시설으로,
# - Z6000 판매및영업시설은 07000 판매시설으로,
# - Z8000 교육연구및복지시설은 10000 교육연구시설으로,
# - Z9000 공공용시설은 14000 업무시설로 집계함.

# %%
# result2: Z코드 재매핑 및 명칭 조인
con.execute("""
CREATE TEMP VIEW result2 AS
WITH cte_remapped AS (
    SELECT
        *,
        CASE
            WHEN 층_용도_대분류_코드_임시 = 'Z3000' THEN '03000'  -- 제1종근린생활시설
            WHEN 층_용도_대분류_코드_임시 = 'Z5000' THEN '05000'  -- 문화및집회시설
            WHEN 층_용도_대분류_코드_임시 = 'Z6000' THEN '07000'  -- 판매시설
            WHEN 층_용도_대분류_코드_임시 = 'Z8000' THEN '10000'  -- 교육연구시설
            WHEN 층_용도_대분류_코드_임시 = 'Z9000' THEN '14000'  -- 업무시설
            ELSE 층_용도_대분류_코드_임시
        END AS 층_용도_대분류_코드
    FROM result1
)
SELECT
    r.*,
    sc.시도명 AS 시도_명,
    uc.주_용도_코드_명 AS 층_용도_대분류_명
FROM cte_remapped r
LEFT JOIN usage_code AS uc
    ON r.층_용도_대분류_코드 = uc.주_용도_코드
LEFT JOIN sido_code AS sc
    ON r.시도_코드 = sc.시도코드
""")

# %%
# 정상 연계 확인 (시도)
query_sido_count = """
SELECT
    시도_코드,
    시도_명,
    COUNT(*) AS value_count
FROM result2
GROUP BY 시도_코드, 시도_명
ORDER BY 시도_코드
"""
print("=== 시도별 건수 ===")
print(con.sql(query_sido_count))

# %%
# 정상 연계 확인 (용도)
query_usage_count = """
SELECT
    층_용도_대분류_코드,
    층_용도_대분류_명,
    COUNT(*) AS value_count
FROM result2
GROUP BY 층_용도_대분류_코드, 층_용도_대분류_명
ORDER BY 층_용도_대분류_코드
"""
print("=== 용도별 건수 ===")
print(con.sql(query_usage_count))

# %% [markdown]
# ### 최종 점검
# - 동
# - 층
# - 연면적
# - 재조달원가
# - 잔존재조달원가

# %%
stats = con.sql("""
    SELECT
        COUNT(DISTINCT 관리_건축물대장_PK) AS 총_동수,
        COUNT(*) AS 총_레코드_수,
        SUM(층_면적) AS 총_층_면적,
        SUM(재조달원가) AS 총_재조달원가,
        SUM(잔존재조달원가) AS 총_잔존재조달원가
    FROM result2
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
# ### 최종 테이블 출력 (CSV 저장)
# 기존 스크립트와 동일한 형태로 CSV를 저장합니다.

# %%
# 시도별 동수, 연면적
query_sido_scale = """
SELECT
    시도_코드,
    시도_명,
    COUNT(DISTINCT 관리_건축물대장_PK) AS 동수,
    SUM(층_면적) AS 연면적
FROM result2
GROUP BY 시도_코드, 시도_명
ORDER BY 시도_코드
"""
df_sido_scale = con.sql(query_sido_scale).df()
df_sido_scale.to_csv(
    results_dir / "동수_연면적_시도별.csv",
    float_format="%.14g",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %%
# 용도별 동수, 연면적
query_usage_scale = """
SELECT
    층_용도_대분류_코드,
    층_용도_대분류_명,
    COUNT(DISTINCT 관리_건축물대장_PK) AS 동수,
    SUM(층_면적) AS 연면적
FROM result2
GROUP BY 층_용도_대분류_코드, 층_용도_대분류_명
ORDER BY 층_용도_대분류_코드
"""
df_usage_scale = con.sql(query_usage_scale).df()
df_usage_scale.to_csv(
    results_dir / "동수_연면적_용도별.csv",
    float_format="%.14g",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %% [markdown]
# ### 잔존연한 산출

# %%
# 시도별 잔존연한
query_sido_life = """
SELECT *
FROM (
    SELECT
        시도_코드,
        시도_명,
        AVG(경과년수) AS 평균_경과년수,
        AVG(내구연한) AS 평균_내구연한,
        AVG(잔존연한) AS 평균_잔존연한,
        AVG(잔존비율_최소값보정) AS 평균_잔존비율
    FROM result2
    GROUP BY 시도_코드, 시도_명

    UNION ALL

    -- 전국 합계
    SELECT
        '00' AS 시도_코드,
        '전국' AS 시도_명,
        AVG(경과년수),
        AVG(내구연한),
        AVG(잔존연한),
        AVG(잔존비율_최소값보정)
    FROM result2
)
ORDER BY
    시도_코드
"""
df_sido_life = con.sql(query_sido_life).df()
df_sido_life.to_csv(
    results_dir / "잔존연한_시도별.csv",
    float_format="%.14g",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %%
# 용도별 잔존연한
query_usage_life = """
SELECT
    층_용도_대분류_코드,
    층_용도_대분류_명,
    AVG(경과년수) AS 평균_경과년수,
    AVG(내구연한) AS 평균_내구연한,
    AVG(잔존연한) AS 평균_잔존연한,
    AVG(잔존비율_최소값보정) AS 평균_잔존비율
FROM result2
GROUP BY 층_용도_대분류_코드, 층_용도_대분류_명
ORDER BY 층_용도_대분류_코드
"""
df_usage_life = con.sql(query_usage_life).df()
df_usage_life.to_csv(
    results_dir / "잔존연한_용도별.csv",
    float_format="%.14g",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %% [markdown]
# ### 재조달원가

# %%
# 시도별 재조달원가
query_sido_cost = """
SELECT *
FROM (
    SELECT
        시도_코드,
        시도_명,
        SUM(재조달원가) AS 재조달원가,
        SUM(잔존재조달원가) AS 잔존재조달원가
    FROM result2
    GROUP BY 시도_코드, 시도_명

    UNION ALL

    -- 전국 합계
    SELECT
        '00' AS 시도_코드,
        '전국' AS 시도_명,
        SUM(재조달원가) AS 재조달원가,
        SUM(잔존재조달원가) AS 잔존재조달원가
    FROM result2
)
ORDER BY
    시도_코드
"""
df_sido_cost = con.sql(query_sido_cost).df()
df_sido_cost.to_csv(
    results_dir / "재조달원가_시도별.csv",
    float_format="%.1f",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %%
# 용도별 재조달원가
query_usage_cost = """
SELECT
    층_용도_대분류_코드,
    층_용도_대분류_명,
    SUM(재조달원가) AS 재조달원가,
    SUM(잔존재조달원가) AS 잔존재조달원가
FROM result2
GROUP BY 층_용도_대분류_코드, 층_용도_대분류_명
ORDER BY 층_용도_대분류_코드
"""
df_usage_cost = con.sql(query_usage_cost).df()
df_usage_cost.to_csv(
    results_dir / "재조달원가_용도별.csv",
    float_format="%.1f",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)

# %%
# 연도별 재조달원가
# 준공년도	데이터_건수	층_면적_합계	재조달원가_합계	잔존재조달원가_합계

query_year_cost = """
SELECT
    준공년도,
    COUNT(*) AS 데이터_건수,
    SUM(층_면적) AS 층_면적_합계,
    SUM(재조달원가) AS 재조달원가_합계,
    SUM(잔존재조달원가) AS 잔존재조달원가_합계
FROM result2
GROUP BY ALL
ORDER BY 준공년도
"""
df_year_cost = con.sql(query_year_cost).df()
df_year_cost.to_csv(
    results_dir / "재조달원가_연도별.csv",
    float_format="%.1f",  # 부동소수점 오류 방지 위하여 유효숫자 설정
    index=False,
    encoding="utf-8-sig",
)
