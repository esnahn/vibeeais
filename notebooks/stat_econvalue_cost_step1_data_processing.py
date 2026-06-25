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
# # Step 1: 재조달원가 데이터 조인 및 필터링
# 원천 Parquet 데이터(기본개요, 표제부, 층별개요)를 조인하고,
# 사용승인일 및 층별개요 면적 기준에 맞게 필터링하여 조인된 Parquet 파일을 생성합니다.

# %%
# date for data source (should be YYYY/12 for normal year, but for 2024 set as 2025/02)
YEAR = "2025"
MONTH = "02"
upper = "20241231"  # 통계 산출을 위한 기준일

try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    # Jupyter 환경 등 __file__이 없는 경우를 위한 Fallback
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"
results_dir = base_dir / "results" / "stat_econvalue" / f"{YEAR}{int(MONTH):02d}"

# Create results directory if it doesn't exist
results_dir.mkdir(parents=True, exist_ok=True)

# print(f"Base Directory: {base_dir}")
print(f"Data Directory: {data_dir.relative_to(base_dir)}")
print(f"Results Directory: {results_dir.relative_to(base_dir)}")

# %%
# DuckDB 연결 및 원천 테이블 뷰 생성
con = duckdb.connect()

기본개요_path = data_dir / "건축물대장_기본개요.parquet"
표제부_path = data_dir / "건축물대장_표제부.parquet"
층별개요_path = data_dir / "건축물대장_층별개요.parquet"

# Parquet 파일을 View로 등록하여 SQL에서 바로 사용
con.execute(f"CREATE VIEW 기본개요 AS SELECT * FROM '{기본개요_path}'")
con.execute(f"CREATE VIEW 표제부 AS SELECT * FROM '{표제부_path}'")
con.execute(f"CREATE VIEW 층별개요 AS SELECT * FROM '{층별개요_path}'")

# %% [markdown]
# ### 기본개요 ⨝ 표제부 (조인 및 필터링)
# - 기본개요와 표제부를 INNER JOIN
# - 사용승인일: 1900년 이후 ~ 기준일(upper) 이전, 그리고 길이가 8자리인 정상 데이터만 사용
# - 표제부에서는 특정 컬럼만 추출

# %%
query_join_basic_title = f"""
SELECT
    b.*,
    t."대지_면적(㎡)" AS "표제부_대지_면적",
    t."건축_면적(㎡)" AS "표제부_건축_면적",
    t."건폐_율(%)" AS "표제부_건폐_율",
    t."연면적(㎡)" AS "표제부_연면적",
    t."용적_률_산정_연면적(㎡)" AS "표제부_용적_률_산정_연면적",
    t."용적_률(%)" AS "표제부_용적_률",
    t."구조_코드" AS "표제부_구조_코드",
    t."구조_코드_명" AS "표제부_구조_코드_명",
    t."기타_구조" AS "표제부_기타_구조",
    t."주_용도_코드" AS "표제부_주_용도_코드",
    t."주_용도_코드_명" AS "표제부_주_용도_코드_명",
    t."기타_용도" AS "표제부_기타_용도",
    t."지붕_코드" AS "표제부_지붕_코드",
    t."지붕_코드_명" AS "표제부_지붕_코드_명",
    t."기타_지붕" AS "표제부_기타_지붕",
    t."사용승인_일" AS "표제부_사용승인_일"
FROM 기본개요 b
INNER JOIN 표제부 t ON b."관리_건축물대장_PK" = t."관리_건축물대장_PK"
WHERE
    t."사용승인_일" >= '19000101' AND  -- 주의! 2025년 quacklytics 코드와 다름: 사용승인일이 19000101과 같은 경우를 포함해 선별됨
    t."사용승인_일" <= '{upper}' AND
    LENGTH(t."사용승인_일") = 8
"""
기본개요_표제부 = con.sql(query_join_basic_title)

# 확인
count_basic_title = con.sql("SELECT COUNT(*) FROM 기본개요_표제부").fetchone()[0]
print(f"기본개요 ⨝ 표제부 조인 및 필터링 후 건수: {count_basic_title:,}")

# %% [markdown]
# ### 층별개요 필터링 및 컬럼 Prefix 부여
# - 면적_제외_여부 NULL 또는 != 1 (면적 제외 미해당 및 미기재 포함)
# - 0 < 면적 <= 500,000

# %%
query_filter_floor = """
SELECT
    "관리_건축물대장_PK",
    "동_명" AS "층_건물_동_명",
    "구조_코드" AS "층_구조_코드",
    "구조_코드_명" AS "층_구조_코드_명",
    "기타_구조" AS "층_기타_구조",
    "주_용도_코드" AS "층_주_용도_코드",
    "주_용도_코드_명" AS "층_주_용도_코드_명",
    "기타_용도" AS "층_기타_용도",
    "면적(㎡)" AS "층_면적",
    "주_부속_구분_코드" AS "층_주_부속_구분_코드",
    "주_부속_구분_코드_명" AS "층_주_부속_구분_코드_명",
    "면적_제외_여부" AS "층_면적_제외_여부",
    "층_구분_코드" AS "층_구분_코드",
    "층_구분_코드_명" AS "층_구분_코드_명",
    "층_번호" AS "층_번호",
    "층_번호_명" AS "층_번호_명"
FROM 층별개요
WHERE
    ("면적_제외_여부" IS NULL OR "면적_제외_여부" != '1') AND
    "면적(㎡)" > 0 AND
    "면적(㎡)" <= 500000
"""
층별개요_필터링 = con.sql(query_filter_floor)

# 확인
count_floor = con.sql("SELECT COUNT(*) FROM 층별개요_필터링").fetchone()[0]
print(f"층별개요 필터링 후 건수: {count_floor:,}")

# %% [markdown]
# ### 최종 조인: 기본개요_표제부 ⨝ 층별개요

# %%
query_final_join = """
SELECT
    b.*,
    f."층_건물_동_명",
    f."층_구조_코드",
    f."층_구조_코드_명",
    f."층_기타_구조",
    f."층_주_용도_코드",
    f."층_주_용도_코드_명",
    f."층_기타_용도",
    f."층_면적",
    f."층_주_부속_구분_코드",
    f."층_주_부속_구분_코드_명",
    f."층_면적_제외_여부",
    f."층_구분_코드",
    f."층_구분_코드_명",
    f."층_번호",
    f."층_번호_명"
FROM 기본개요_표제부 b
INNER JOIN 층별개요_필터링 f ON b."관리_건축물대장_PK" = f."관리_건축물대장_PK"
"""
기본개요_표제부_층별개요_조인 = con.sql(query_final_join)

# Sanity Check
final_stats = con.sql("""
    SELECT
        COUNT(*) AS 총_레코드_수,
        SUM("층_면적") AS 총_층_면적
    FROM 기본개요_표제부_층별개요_조인
""").fetchdf()

print("최종 조인 결과 확인:")
print(
    final_stats.to_string(
        index=False,
        formatters={
            "총_레코드_수": lambda x: f"{x:,.0f}",  # was 19,614,969
            "총_층_면적": lambda x: f"{x:,.0f}",  # was 4,372,971,283
        },
    )
)

# %% [markdown]
# ### Parquet 파일로 저장

# %%
output_parquet_file = results_dir / "기본개요_표제부_층별개요_조인.parquet"
print(f"Saving to {output_parquet_file.name} ...")

con.sql(
    f"COPY 기본개요_표제부_층별개요_조인 TO '{output_parquet_file}' (FORMAT PARQUET)"
)
print("Save complete.")


# %%
# 층_구조_코드 고유값 목록 저장
df_층_구조_코드 = con.sql("""
    SELECT DISTINCT 층_구조_코드, 층_구조_코드_명
    FROM 기본개요_표제부_층별개요_조인
    ORDER BY 층_구조_코드
""").df()
output_구조_코드 = results_dir / "층_구조_코드.csv"
df_층_구조_코드.to_csv(output_구조_코드, index=False, encoding="utf-8-sig")
print(f"층_구조_코드 저장 완료: {len(df_층_구조_코드)}건 → {output_구조_코드.name}")

# %%
# 층_주_용도_코드 고유값 목록 저장
df_층_용도_코드 = con.sql("""
    SELECT DISTINCT 층_주_용도_코드, 층_주_용도_코드_명
    FROM 기본개요_표제부_층별개요_조인
    ORDER BY 층_주_용도_코드
""").df()
output_용도_코드 = results_dir / "층_주_용도_코드.csv"
df_층_용도_코드.to_csv(output_용도_코드, index=False, encoding="utf-8-sig")
print(f"층_주_용도_코드 저장 완료: {len(df_층_용도_코드)}건 → {output_용도_코드.name}")
