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
# # 건축 소요기간 통계 (설정된 기준 연월 기준) - V2
#
# 설정된 YEAR 및 MONTH 변수에 해당하는 데이터를 대상으로, 10년간 신축 건물의 허가부터 착공, 착공부터 준공까지의 소요 기간을 분석합니다.

# %%
from pathlib import Path

import duckdb
import matplotlib
import pandas as pd

# Jupyter Notebook/IPython 환경(HTML 내보내기용 실행 포함)인지 확인하여 CLI 실행 시에만 Headless 백엔드(Agg) 설정
try:
    import builtins

    if hasattr(builtins, "get_ipython"):
        ipy = getattr(builtins, "get_ipython")()
        is_jupyter = ipy is not None and ipy.__class__.__name__ == "ZMQInteractiveShell"
    else:
        is_jupyter = False
except Exception:
    is_jupyter = False

if not is_jupyter:
    matplotlib.use(
        "Agg"
    )  # CLI 실행 시 창이 열려 실행이 차단되는 것을 방지하기 위해 Headless 백엔드 사용

import matplotlib.pyplot as plt  # noqa: E402

# %%
# ── 분석 대상 데이터 연월 및 집계 기간 설정 (매번 여기만 수정) ─────────────────
YEAR = "2025"
MONTH = "02"

# 집계 대상 최근 10년 날짜 범위
lower = "20150101"
upper = "20241231"
# ──────────────────────────────────────────────────────────────────────────

# Parquet 파일 경로 설정
try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    # Jupyter 환경 등 __file__이 없는 경우를 위한 Fallback
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"
path_건축 = data_dir / "건축인허가_기본개요.parquet"
path_주택 = data_dir / "주택인허가_기본개요.parquet"

plt.style.use(base_dir / "auri.mplstyle")

pd.options.display.unicode.east_asian_width = True

# DuckDB 인메모리 연결
con = duckdb.connect()


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
# - 기간(착공일, 준공일): **설정된 범위 (lower ~ upper)**
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
        CAST("연면적(㎡)" AS DOUBLE) AS 연면적,  -- 기존 노트북(hub_인허가_yearly data_process duration2.py) 재현을 위해 연면적 0인 경우를 포함하여 캐스팅
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
      AND CAST("연면적(㎡)" AS DOUBLE) < 30000000  -- 기존 노트북 재현을 위해 연면적 0인 경우를 포함하여 필터링 (기준연월 202502, 분석기간 20150101~20241231 기준 mean은 430.25일)
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
    LEFT(TRIM(주_용도_코드), 2) AS 용도_대분류_코드,  -- 용도별 집계를 위해 추출
    건축_구분_코드,                                    -- 준공 10년 통합 통계 필터링/그룹화를 위해 추출
    건축_구분_코드_명,                                 -- 준공 10년 통합 통계 필터링/그룹화를 위해 추출
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
        CAST("연면적(㎡)" AS DOUBLE) AS 연면적,  -- 기존 노트북(hub_인허가_yearly data_process duration2.py) 재현을 위해 연면적 0인 경우를 포함하여 캐스팅
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
      AND CAST("연면적(㎡)" AS DOUBLE) < 30000000  -- 기존 노트북 재현을 위해 연면적 0인 경우를 포함하여 필터링 (기준연월 202502, 분석기간 20150101~20241231 기준 mean은 430.25일)
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
    LEFT(TRIM(주_용도_코드), 2) AS 용도_대분류_코드,  -- 용도별 집계를 위해 추출
    건축_구분_코드,                                    -- 준공 10년 통합 통계 필터링/그룹화를 위해 추출
    건축_구분_코드_명,                                 -- 준공 10년 통합 통계 필터링/그룹화를 위해 추출
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

# save directory configuration (Year-Month unified directory under results)
dir_to = base_dir / "results" / "stat_construction_duration" / f"{YEAR}{int(MONTH):02d}"
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

plt.savefig(dir_to / "허가준공_기간_분포와_주요_통계량_zoom.png")

# Show the plot
plt.show()

# %% [markdown]
# ## 3. 추가 통계 집계 및 내보내기 (V2)

# %%
print("\n=== 추가 통계 집계 및 내보내기 ===")

# 1. 전국 소요기간
# df_착공10년, df_준공10년은 이미 lower, upper에 맞춰 필터링되어 있음
착공_result = df_착공10년.groupby("착공_년")[["허가착공_기간"]].median().astype(int)
착공_result = 착공_result.sort_index()
착공_result.index.name = "연도"

준공_result = (
    df_준공10년.groupby("준공_년")[["착공준공_기간", "허가준공_기간"]]
    .median()
    .astype(int)
)
준공_result = 준공_result.sort_index()
준공_result.index.name = "연도"

통합_result = pd.merge(
    착공_result,
    준공_result,
    left_index=True,
    right_index=True,
    how="outer",
)
path_전국 = dir_to / "소요기간_전국.csv"
통합_result.to_csv(path_전국, encoding="utf-8-sig")
print(f"Saved: {path_전국.name}")

# 2. 시도별 소요기간
착공_result_시도 = (
    df_착공10년.groupby(["시도_코드", "착공_년"])[["허가착공_기간"]]
    .median()
    .sort_index()
    .rename_axis(index={"착공_년": "연도"})
    .astype("Int64")
)
준공_result_시도 = (
    df_준공10년.groupby(["시도_코드", "준공_년"])[["착공준공_기간", "허가준공_기간"]]
    .median()
    .sort_index()
    .rename_axis(index={"준공_년": "연도"})
    .astype("Int64")
)
통합_result_시도 = pd.merge(
    착공_result_시도,
    준공_result_시도,
    left_index=True,
    right_index=True,
    how="outer",
)
path_시도 = dir_to / "소요기간_시도별.csv"
통합_result_시도.to_csv(path_시도, encoding="utf-8-sig")
print(f"Saved: {path_시도.name}")


# 3. 규모별 소요기간
def categorize_area(area):
    if pd.isna(area):
        return None
    elif area < 100:
        return "1백㎡ 미만"
    elif area < 300:
        return "1백㎡~3백㎡"
    elif area < 1000:
        return "3백㎡~1천㎡"
    elif area < 3000:
        return "1천~3천㎡"
    elif area < 10000:
        return "3천~1만㎡"
    else:
        return "1만㎡ 이상"


size_order = [
    "1백㎡ 미만",
    "1백㎡~3백㎡",
    "3백㎡~1천㎡",
    "1천~3천㎡",
    "3천~1만㎡",
    "1만㎡ 이상",
]

df_total_규모 = df_total.copy()
df_total_규모["규모_구분"] = df_total_규모["연면적"].apply(categorize_area)

df_착공_10년_규모 = df_total_규모[
    (df_total_규모["착공_년"] >= lower[:4])
    & (df_total_규모["착공_년"] <= upper[:4])
    & (df_total_규모["규모_구분"].notna())
]
착공_result_규모 = (
    df_착공_10년_규모.groupby(["규모_구분", "착공_년"])[["허가착공_기간"]]
    .median()
    .rename_axis(index={"착공_년": "연도"})
    .astype("Int64")
)

df_준공_10년_규모 = df_total_규모[
    (df_total_규모["준공_년"] >= lower[:4])
    & (df_total_규모["준공_년"] <= upper[:4])
    & (df_total_규모["규모_구분"].notna())
]
준공_result_규모 = (
    df_준공_10년_규모.groupby(["규모_구분", "준공_년"])[
        ["착공준공_기간", "허가준공_기간"]
    ]
    .median()
    .rename_axis(index={"준공_년": "연도"})
    .astype("Int64")
)

통합_result_규모 = pd.merge(
    착공_result_규모,
    준공_result_규모,
    left_index=True,
    right_index=True,
    how="outer",
)

# 기존 노트북(old)과 계산 결과값(수치)은 100% 동일하게 유지하되,
# 가독성을 위해 기존의 알파벳/가나다순 정렬 대신 논리적인 면적 규모 순서(소형 -> 대형)로 행의 순서를 재정렬하여 출력합니다.
통합_result_규모 = 통합_result_규모.reindex(
    [
        (size, str(year))
        for size in size_order
        for year in range(int(lower[:4]), int(upper[:4]) + 1)
        if (size, str(year)) in 통합_result_규모.index
    ]
)

path_규모 = dir_to / "소요기간_규모별.csv"
통합_result_규모.to_csv(path_규모, encoding="utf-8-sig")
print(f"Saved: {path_규모.name}")

# 4. 용도별 소요기간
df_착공_10년_용도 = df_착공10년[
    (df_착공10년["용도_대분류_코드"].notna())
    & (df_착공10년["용도_대분류_코드"] <= "99")
]
착공_result_용도 = (
    df_착공_10년_용도.groupby(["용도_대분류_코드", "착공_년"])[["허가착공_기간"]]
    .median()
    .sort_index()
    .rename_axis(index={"착공_년": "연도"})
    .astype("Int64")
)

df_준공_10년_용도 = df_준공10년[
    (df_준공10년["용도_대분류_코드"].notna())
    & (df_준공10년["용도_대분류_코드"] <= "99")
]
준공_result_용도 = (
    df_준공_10년_용도.groupby(["용도_대분류_코드", "준공_년"])[
        ["착공준공_기간", "허가준공_기간"]
    ]
    .median()
    .sort_index()
    .rename_axis(index={"준공_년": "연도"})
    .astype("Int64")
)

통합_result_용도 = pd.merge(
    착공_result_용도,
    준공_result_용도,
    left_index=True,
    right_index=True,
    how="outer",
)
path_용도 = dir_to / "소요기간_용도별.csv"
통합_result_용도.to_csv(path_용도, encoding="utf-8-sig")
print(f"Saved: {path_용도.name}")
