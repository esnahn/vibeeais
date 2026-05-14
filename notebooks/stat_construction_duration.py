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

# %% [markdown]
# # 건축 소요기간 통계 (2025년 12월 데이터 기준)
#
# 10년간 (2015년 ~ 2024년) 신축 건물을 대상으로 허가부터 착공, 착공부터 준공까지의 소요 기간을 분석

# %%
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import pandas as pd

# %%
# Parquet 파일 경로 설정
try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    # Jupyter 환경 등 __file__이 없는 경우를 위한 Fallback
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet"
path_건축 = data_dir / "건축인허가_기본개요.parquet"
path_주택 = data_dir / "주택인허가_기본개요.parquet"

plt.style.use(base_dir / "auri.mplstyle")

pd.options.display.unicode.east_asian_width = True

# DuckDB 인메모리 연결
con = duckdb.connect()

# %%
# 날짜 범위 설정
lower = "20150101"
upper = "20241231"


# %% [markdown]
# ## 1. 데이터 Sanity Check
# 각 Parquet 파일의 전체 레코드 수 및 주요 날짜 컬럼의 레코드 수를 확인합니다.


# %%
def count_records(con, parquet_path, date_columns, lower=lower, upper=upper):
    results = []
    for col in date_columns:
        query = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet('{parquet_path.as_posix()}')
        WHERE TRIM("{col}") >= '{lower}' AND TRIM("{col}") <= '{upper}'
        """
        res = con.sql(query).fetchone()[0]
        results.append({"컬럼명": col, "조건_이상_이하_건수": res})
    return pd.DataFrame(results)


print(f"=== 건축인허가 데이터 건수({lower} 이상 {upper} 이하) ===")
건축_date_cols = [
    "건축_허가_일",
    "착공_예정_일",
    "착공_연기_일",
    "실제_착공_일",
    "사용승인_일",
]

print(
    count_records(con, path_건축, 건축_date_cols, lower, upper).to_string(
        formatters={"조건_이상_이하_건수": "{:,}".format}
    )
)

print(f"\n=== 주택인허가 데이터 건수({lower} 이상 {upper} 이하) ===")
주택_date_cols = [
    "승인_일",
    "착공_예정_일",
    "착공_일",
    "사용_검사_예정_일",
    "사용_검사_일",
]
print(
    count_records(con, path_주택, 주택_date_cols, lower, upper).to_string(
        formatters={"조건_이상_이하_건수": "{:,}".format}
    )
)

# %% [markdown]
# 실제 착공일(건축: 실제 착공 일, 주택: 착공 일) 건수가 착공예정일보다 적음.
#
# 준공일(건축: 사용승인 일, 주택: 사용 검사 일)이 있는 경우, 착공을 했다고 보고, 실제 착공일(건축: 실제 착공 일, 주택: 착공 일) → 착공 연기일(건축) → 착공 예정일(건축, 주택) 순으로 착공일 추정

# %% [markdown]
# ## 2. 건축 소요기간 통계
#
# ### 데이터 전처리 조건:
# - 집계대상: 기본개요
# - 기간(착공일, 준공일): **최근 10년 (2015 ~ 2024년)**
# - 건축행위: **신축** (건축: 건축_구분_코드 0100, 주택: 용도_코드 부여)
#   - 주택의 경우 아파트 단지의 최초 신축만 구분할 수 없음. 행위개요 테이블에서도 나타나지 않음. 주소도 블록 주소에서 도로명 주소로 바뀌기 때문에 주소 기준 구분도 어려움. 기본개요 용도_코드 부여 여부로 판단함. (용도_코드가 부여된 = 최초 신축 시, 연면적 사용 가능)
# - 연면적: **30,000,000㎡ 미만** (명백한 오류 데이터 제외)
# - 착공일 추정: 준공일(사용승인일/사용검사일)이 있는 경우 실제 착공일(건축: 실제 착공 일, 주택: 착공 일) → 착공 연기일(건축) → 착공 예정일(건축, 주택) 순으로 착공일 추정
# <!-- - 연면적 극단값: 작년 추출 코드에는 3천만으로 되어있는데 주석에는 3백만으로 되어있음. 실제 적용된 코드를 따름. 향후 연구에선 변경될 수 있음 -->

# %%
# 건축인허가 신축 데이터 필터링 및 소요기간 계산
sql_건축 = f"""
WITH base AS (
    SELECT
        CONCAT('건축', "관리_허가대장_PK") AS pk,
        "시군구_코드",
        "주_용도_코드",
        "주_용도_코드_명",
        "건축_구분_코드",
        "건축_구분_코드_명",
        CAST(NULLIF("연면적(㎡)", 0) AS DOUBLE) AS 연면적,
        "건축_허가_일" AS 허가일,
        "착공_예정_일",
        "착공_연기_일",
        "실제_착공_일",
        "사용승인_일" AS 준공일,
        CASE
            WHEN "사용승인_일" IS NOT NULL AND TRIM("사용승인_일") != ''
                THEN COALESCE(NULLIF(TRIM("실제_착공_일"), ''), NULLIF(TRIM("착공_연기_일"), ''), NULLIF(TRIM("착공_예정_일"), ''))
            ELSE TRIM("실제_착공_일")
        END AS 착공일
    FROM read_parquet('{path_건축.as_posix()}')
    WHERE "건축_구분_코드" IN ('0100')  -- 신축만
      AND CAST(NULLIF("연면적(㎡)", 0) AS DOUBLE) < 30000000  -- 30_000_000 ㎡ 이상 극단치 제외
)
SELECT
    pk,
    허가일,
    착공일,
    준공일,
    LEFT(TRIM(착공일), 4) AS 착공_년,
    LEFT(TRIM(준공일), 4) AS 준공_년,
    LEFT(TRIM(시군구_코드), 2) AS 시도_코드,
    주_용도_코드,
    주_용도_코드_명,
    연면적,
    date_diff('day', try_strptime(허가일, '%Y%m%d'), try_strptime(착공일, '%Y%m%d')) AS 허가착공_기간,
    date_diff('day', try_strptime(착공일, '%Y%m%d'), try_strptime(준공일, '%Y%m%d')) AS 착공준공_기간,
    date_diff('day', try_strptime(허가일, '%Y%m%d'), try_strptime(준공일, '%Y%m%d')) AS 허가준공_기간
FROM base
"""

rel_건축_duration = con.sql(sql_건축)
print(f"건축인허가 전체 데이터 건수: {len(rel_건축_duration.df()):,}")


def filter_date_range(
    rel: duckdb.DuckDBPyRelation, col: str, lower: str, upper: str
) -> duckdb.DuckDBPyRelation:
    """
    Filters a DuckDB relation by a date range and length requirement.
    Equivalent to:
    WHERE {col} >= '{lower}' AND {col} <= '{upper}' AND LENGTH(TRIM({col})) = 8
    """
    condition = f"{col} >= '{lower}' AND {col} <= '{upper}' AND LENGTH(TRIM({col})) = 8"
    return rel.filter(condition)


print(
    f"건축인허가 착공일 기준 대상 건수: {len(filter_date_range(rel_건축_duration, '착공일', lower, upper).df()):,}"
)
print(
    f"건축인허가 준공일 기준 대상 건수: {len(filter_date_range(rel_건축_duration, '준공일', lower, upper).df()):,}"
)

# %%
# 주택인허가 신축 데이터 필터링 및 소요기간 계산
sql_주택 = f"""
WITH base AS (
    SELECT
        CONCAT('주택', "관리_주택대장_PK") AS pk,
        "시군구_코드",
        "용도_코드" AS 주_용도_코드,
        "용도_코드_명" AS 주_용도_코드_명,
        '0100' AS 건축_구분_코드,
        '신축' AS 건축_구분_코드_명,
        CAST(NULLIF("연면적(㎡)", 0) AS DOUBLE) AS 연면적,
        "승인_일" AS 허가일,
        "착공_예정_일",
        "착공_일" AS 원_기재_착공_일,
        "사용_검사_일" AS 준공일,
        CASE
            WHEN "사용_검사_일" IS NOT NULL AND TRIM("사용_검사_일") != ''
                THEN COALESCE(NULLIF(TRIM("착공_일"), ''), NULLIF(TRIM("착공_예정_일"), ''))
            ELSE TRIM("착공_일")
        END AS 착공일
    FROM read_parquet('{path_주택.as_posix()}')
    WHERE "용도_코드" IS NOT NULL  -- 주택인허가에서의 신축 추정
      AND CAST(NULLIF("연면적(㎡)", 0) AS DOUBLE) < 30000000  -- 30_000_000 ㎡ 이상 극단치 제외
)
SELECT
    pk,
    허가일,
    착공일,
    준공일,
    LEFT(TRIM(착공일), 4) AS 착공_년,
    LEFT(TRIM(준공일), 4) AS 준공_년,
    LEFT(TRIM(시군구_코드), 2) AS 시도_코드,
    주_용도_코드,
    주_용도_코드_명,
    연면적,
    date_diff('day', try_strptime(허가일, '%Y%m%d'), try_strptime(착공일, '%Y%m%d')) AS 허가착공_기간,
    date_diff('day', try_strptime(착공일, '%Y%m%d'), try_strptime(준공일, '%Y%m%d')) AS 착공준공_기간,
    date_diff('day', try_strptime(허가일, '%Y%m%d'), try_strptime(준공일, '%Y%m%d')) AS 허가준공_기간
FROM base
"""

rel_주택_duration = con.sql(sql_주택)
print(f"주택인허가 분석 대상 건수: {len(rel_주택_duration):,}")

print(
    f"주택인허가 착공일 기준 대상 건수: {len(filter_date_range(rel_주택_duration, '착공일', lower, upper).df()):,}"
)
print(
    f"주택인허가 준공일 기준 대상 건수: {len(filter_date_range(rel_주택_duration, '준공일', lower, upper).df()):,}"
)

# %%
# 통합 데이터 생성
df_total = pd.concat(
    [rel_건축_duration.df(), rel_주택_duration.df()], ignore_index=True
)

# %%
# 기간 오류 데이터 확인

# 오류 데이터(마이너스 기간 등) 필터링
valid_all_mask = (
    (df_total["허가착공_기간"] >= 0)
    & (df_total["착공준공_기간"] >= 0)
    & (df_total["허가준공_기간"] >= 0)
)
df_valid_all = df_total[valid_all_mask].copy()

print(f"유효한 소요기간 산출 건수: {len(df_valid_all):,} (총 {len(df_total):,} 건 중)")

# 음수 데이터 (하나라도)
invalid_mask = (
    (df_total["허가착공_기간"] < 0)
    | (df_total["착공준공_기간"] < 0)
    | (df_total["허가준공_기간"] < 0)
)
df_invalid = df_total[invalid_mask].copy()

# 누락 데이터 (하나라도)
missing_mask = (
    df_total["허가착공_기간"].isna()
    | df_total["착공준공_기간"].isna()
    | df_total["허가준공_기간"].isna()
)
df_missing = df_total[missing_mask].copy()

print(f"오류 데이터 건수: {len(df_invalid):,} (총 {len(df_total):,} 건 중)")
print(f"누락 데이터 건수: {len(df_missing):,} (총 {len(df_total):,} 건 중)")

df_invalid_and_missing = df_total[invalid_mask & missing_mask].copy()

print(
    f"오류이면서 동시에 누락인 데이터 건수: {len(df_invalid_and_missing):,} (총 {len(df_total):,} 건 중)"
)

print(
    f"""Check if {len(df_total) ==
len(df_valid_all) + len(df_invalid) + len(df_missing) - len(df_invalid_and_missing)
=}"""
)

# 오류 데이터 일부 표시
print("\n오류 데이터:")
print(df_invalid.head())

# 누락 데이터 일부 표시
print("\n누락 데이터:")
print(df_missing.head())

# %% [markdown]
# ### 최근 10년 연도별 중위 소요기간 (단위: 일)

# %%
# 착공일 기준 최근 10년 데이터
df_착공10년 = df_total[
    (df_total["착공_년"] >= lower[:4]) & (df_total["착공_년"] <= upper[:4])
]
print(f"착공일 기준 최근 10년 데이터 건수: {len(df_착공10년):,}")

# 준공일 기준 최근 10년 데이터
df_준공10년 = df_total[
    (df_total["준공_년"] >= lower[:4]) & (df_total["준공_년"] <= upper[:4])
]
print(f"준공일 기준 최근 10년 데이터 건수: {len(df_준공10년):,}")

# %%
# 착공일/준공일 기준 최근 10년 연도별 중위 소요기간
# 허가착공 기간
print("허가착공 기간: 착공일 기준")
stat_by_year_허가착공 = (
    df_착공10년[df_착공10년["허가착공_기간"] >= 0]
    .groupby("착공_년")[["허가착공_기간"]]
    .median()
    .astype("Int64")  # cast median output(float64) to nullable int type
)

# 착공준공 기간
print("착공준공 기간: 준공일 기준")
stat_by_year_착공준공 = (
    df_준공10년[df_준공10년["착공준공_기간"] >= 0]
    .groupby("준공_년")[["착공준공_기간"]]
    .median()
    .astype("Int64")  # cast median output(float64) to nullable int type
)

# 허가준공 기간
print("허가준공 기간: 준공일 기준")
stat_by_year_허가준공 = (
    df_준공10년[df_준공10년["허가준공_기간"] >= 0]
    .groupby("준공_년")[["허가준공_기간"]]
    .median()
    .astype("Int64")  # cast median output(float64) to nullable int type
)

stat_by_year_허가착공.index.name = "연도"
stat_by_year_착공준공.index.name = "연도"
stat_by_year_허가준공.index.name = "연도"

stat_by_year = stat_by_year_허가착공.join(stat_by_year_착공준공, how="outer").join(
    stat_by_year_허가준공, how="outer"
)

print(stat_by_year)
stat_by_year.dtypes

# %%
# 허가준공 기간 분포 대푯값

# 허가준공 histogram using plt, with mean, median, mode, min, max

# data
df_dist = df_준공10년["허가준공_기간"].dropna()

# Calculate statistics
mean_val = df_dist.mean()
median_val = df_dist.median()
mode_val = df_dist.mode().iloc[0]
min_val = df_dist.min()
max_val = df_dist.max()

# print values
print(f"Mean: {mean_val}")
print(f"Median: {median_val}")
print(f"Mode: {mode_val}")
print(f"Min: {min_val}")
print(f"Max: {max_val}")

# %%
# 허가준공 301234인 데이터 확인
df_301234 = df_total[df_total["허가준공_기간"] == 301234].copy()

print(df_301234.head())
print(f"허가준공 301234인 데이터 건수: {len(df_301234):,}")

# %%
# Create histogram with vertical lines

# create new Figure
plt.figure()
# log scale y axis
plt.yscale("log")
# create histogram
plt.hist(
    df_dist,
    bins=100,
    alpha=0.7,
    color="skyblue",
    edgecolor="black",
)

# Add vertical lines for mean, median, and mode
# get colors from tab10 colormap for vertical lines
colors = plt.get_cmap("tab10").colors

plt.axvline(
    mean_val,
    color=colors[0],
    linestyle="dashed",
    linewidth=2,
    label=f"Mean: {mean_val:.2f}",
)
plt.axvline(
    median_val,
    color=colors[1],
    linestyle="dashed",
    linewidth=2,
    label=f"Median: {median_val:.2f}",
)
plt.axvline(
    mode_val,
    color=colors[2],
    linestyle="dashed",
    linewidth=2,
    label=f"Mode: {mode_val:.2f}",
)
plt.axvline(
    min_val,
    color=colors[3],
    linestyle="dashed",
    linewidth=1,
    label=f"Min: {min_val:.2f}",
)
plt.axvline(
    max_val,
    color=colors[4],
    linestyle="dashed",
    linewidth=1,
    label=f"Max: {max_val:.2f}",
)

# Add labels and title
plt.xlabel("허가준공 기간 (일)")
plt.ylabel("건수")
plt.title("허가준공 기간 분포와 주요 통계량")
plt.legend(loc="upper right")  # make it at top right
plt.grid(True, alpha=0.3)

# save
dir_to = base_dir / "results" / "stat_construction_duration"
dir_to.mkdir(parents=True, exist_ok=True)

plt.savefig(dir_to / "허가준공_기간_분포와_주요_통계량.png")

# Show the plot
plt.show()


# %%
# Create zoomed-in histogram

# create new Figure
plt.figure()
# zoom in x range to show difference between mean, median, and mode
plt.xlim(-200, 1000)
# log scale y axis
plt.yscale("log")
# create histogram
plt.hist(
    df_dist,
    bins=100,
    range=(-200, 1000),
    alpha=0.7,
    color="skyblue",
    edgecolor="black",
)

# Add vertical lines for mean, median, and mode
# get colors from tab10 colormap for vertical lines
colors = plt.get_cmap("tab10").colors

plt.axvline(
    mean_val,
    color=colors[0],
    linestyle="dashed",
    linewidth=2,
    label=f"Mean: {mean_val:.2f}",
)
plt.axvline(
    median_val,
    color=colors[1],
    linestyle="dashed",
    linewidth=2,
    label=f"Median: {median_val:.2f}",
)
plt.axvline(
    mode_val,
    color=colors[2],
    linestyle="dashed",
    linewidth=2,
    label=f"Mode: {mode_val:.2f}",
)

# Add labels and title
plt.xlabel("허가준공 기간 (일)")
plt.ylabel("건수")
plt.title("허가준공 기간 분포와 주요 통계량(일부 확대)")
plt.legend(loc="upper right")  # make it at top right
plt.grid(True, alpha=0.3)

# save
dir_to = base_dir / "results" / "stat_construction_duration"
dir_to.mkdir(parents=True, exist_ok=True)

plt.savefig(dir_to / "허가준공_기간_분포와_주요_통계량_zoom.png")

# Show the plot
plt.show()
