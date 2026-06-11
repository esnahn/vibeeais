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


def print_df(df):
    df_print = df.copy()
    numeric_cols = df_print.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        df_print[col] = df_print[col].apply(
            lambda x: (
                f"{x:,.0f}"
                if pd.notnull(x) and float(x).is_integer()
                else (f"{x:,.2f}" if pd.notnull(x) else x)
            )
        )
    print(df_print.to_string(index=False, na_rep=pd.NA))


# %% [markdown]
# # Step 0: 재조달원가 데이터 탐색적 분석
# 원천 Parquet 데이터(기본개요, 표제부, 층별개요)의 기본 현황을 확인합니다.

# %%
YEAR = "2025"
MONTH = "02"
upper = "20241231"

try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    # Jupyter 환경 등 __file__이 없는 경우를 위한 Fallback
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"

# print(f"Base Directory: {base_dir}")
# print(f"Data Directory: {data_dir}")

# %%
# Parquet 파일 로드
print("Loading parquet files...")
con = duckdb.connect()

기본개요_path = data_dir / "건축물대장_기본개요.parquet"
표제부_path = data_dir / "건축물대장_표제부.parquet"
층별개요_path = data_dir / "건축물대장_층별개요.parquet"

# %% [markdown]
# ### 테이블별 레코드 수

# %%
# 테이블별 레코드 수 확인
tables = {"기본개요": 기본개요_path, "표제부": 표제부_path, "층별개요": 층별개요_path}

table_counts = []
for name, path in tables.items():
    count = con.sql(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    table_counts.append({"Table": name, "Record Count": count})

record_counts_df = pd.DataFrame(table_counts)
print("테이블별 레코드 수:")
print_df(record_counts_df)

# %% [markdown]
# ### 컬럼별 결측치 및 Non-null 카운트 현황

# %%
for name, path in tables.items():
    print(f"\n[{name}] 컬럼 요약 (Non-Null 비중 상위 20개):")

    cols = con.sql(f"DESCRIBE SELECT * FROM '{path}'").fetchdf()
    total_count = con.sql(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]

    count_queries = [f'COUNT("{c}") AS "{c}"' for c in cols["column_name"]]
    query = f"SELECT {', '.join(count_queries)} FROM '{path}'"
    counts = con.sql(query).fetchone()

    summary_data = []
    for col_name, col_type, non_null_count in zip(
        cols["column_name"], cols["column_type"], counts
    ):
        summary_data.append(
            {
                "column_name": col_name,
                "column_type": col_type,
                "non_null_count": non_null_count,
                "non_null_percentage": (non_null_count / total_count * 100)
                if total_count > 0
                else 0,
            }
        )

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.sort_values(by="non_null_percentage", ascending=False).head(
        20
    )
    print_df(summary_df)

# %% [markdown]
# ### 사용승인일 검증

# %%
# 사용승인일 검증 (표제부)
date_validation = con.sql(f"""
    SELECT
        COUNT(*) AS 전체_건수,
        SUM(CASE WHEN "사용승인_일" IS NULL THEN 1 ELSE 0 END) AS 결측치_건수,
        SUM(CASE WHEN "사용승인_일" IS NOT NULL AND LENGTH("사용승인_일") != 8 THEN 1 ELSE 0 END) AS 형식오류_건수,
        SUM(CASE WHEN LENGTH("사용승인_일") = 8 AND "사용승인_일" > '{upper}' THEN 1 ELSE 0 END) AS 미래일_건수,
        SUM(CASE WHEN LENGTH("사용승인_일") = 8 AND "사용승인_일" < '19000101' THEN 1 ELSE 0 END) AS 과거_이상치_건수,
        SUM(CASE WHEN "사용승인_일" >= '19000101' AND "사용승인_일" <= '{upper}' AND LENGTH("사용승인_일") = 8 THEN 1 ELSE 0 END) AS 정상_건수
    FROM '{표제부_path}'
""").fetchdf()

print("사용승인일 검증 결과:")
print_df(date_validation)

# %% [markdown]
# ### 층별개요 면적 검증

# %%
# 면적 제외 여부 분포
area_exclusion = con.sql(f"""
    SELECT
        "면적_제외_여부",
        COUNT(*) AS 건수
    FROM '{층별개요_path}'
    GROUP BY "면적_제외_여부"
    ORDER BY 건수 DESC
""").fetchdf()

print("면적_제외_여부 분포:")
print_df(area_exclusion)

# %%
# 면적 이상치 검증
area_validation = con.sql(f"""
    SELECT
        COUNT(*) AS 층별개요_총_건수,
        SUM(CASE WHEN "면적(㎡)" < 0 THEN 1 ELSE 0 END) AS 면적_음수_건수,
        SUM(CASE WHEN "면적(㎡)" = 0 THEN 1 ELSE 0 END) AS 면적_0_건수,
        SUM(CASE WHEN "면적(㎡)" > 500000 THEN 1 ELSE 0 END) AS "면적_50만㎡_초과_건수"
    FROM '{층별개요_path}'
""").fetchdf()

print("면적 값 검증:")
print_df(area_validation)

# %%
# 50만㎡ 초과 면적을 이상치로 판단한 근거 확인 (극단치 상위 10개 출력)
extreme_areas = con.sql(f"""
    SELECT "관리_건축물대장_PK", "건물_명", "면적(㎡)"
    FROM '{층별개요_path}'
    WHERE "면적(㎡)" > 500000
    ORDER BY "면적(㎡)" DESC
    LIMIT 10
""").fetchdf()

print("50만㎡ 초과 면적 극단치 (상위 10건):")
print_df(extreme_areas)

# %% [markdown]
# ### 코드 분포 (표제부 구조/용도/지붕)

# %%
# 구조 코드 분포 (상위 10개)
structure_code_dist = con.sql(f"""
    SELECT "구조_코드", "구조_코드_명", COUNT(*) AS 건수
    FROM '{표제부_path}'
    GROUP BY "구조_코드", "구조_코드_명"
    ORDER BY 건수 DESC
    LIMIT 10
""").fetchdf()
print("구조 코드 분포 (상위 10):")
print_df(structure_code_dist)

# %%
# 용도 코드 분포 (상위 10개)
usage_code_dist = con.sql(f"""
    SELECT "주_용도_코드", "주_용도_코드_명", COUNT(*) AS 건수
    FROM '{표제부_path}'
    GROUP BY "주_용도_코드", "주_용도_코드_명"
    ORDER BY 건수 DESC
    LIMIT 10
""").fetchdf()
print("주 용도 코드 분포 (상위 10):")
print_df(usage_code_dist)

# %%
# 지붕 코드 분포 (상위 10개)
roof_code_dist = con.sql(f"""
    SELECT "지붕_코드", "지붕_코드_명", COUNT(*) AS 건수
    FROM '{표제부_path}'
    GROUP BY "지붕_코드", "지붕_코드_명"
    ORDER BY 건수 DESC
    LIMIT 10
""").fetchdf()
print("지붕 코드 분포 (상위 10):")
print_df(roof_code_dist)
