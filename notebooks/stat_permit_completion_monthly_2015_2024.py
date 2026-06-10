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
# # 인허가 준공 통계 (2015~2024)
#
# '건축물 생산량 지수' 산출 기초값으로 사용되는 건축허가착공준공통계 중,
# 최근 준공통계의 누락/비어있는 데이터를 대체하기 위해 2025년 2월 기준의 건축/주택인허가 개방데이터를 활용하여
# 2015~2024년(최근 10개년)의 월별 준공(사용승인 및 사용검사) 연면적 실적을 집계합니다.

# %%
from pathlib import Path

import duckdb
import pandas as pd

# %%
# ── 분석 대상 데이터 연월 및 집계 기간 설정 ──────────────────────────────────
YEAR = "2025"
MONTH = "02"

lower = "20150101"
upper = "20241231"
# ──────────────────────────────────────────────────────────────────────────

# %%
# Parquet 파일 경로 설정
try:
    base_dir = Path(__file__).resolve().parent.parent
except NameError:
    # Jupyter 환경 등 __file__이 없는 경우를 위한 Fallback
    base_dir = Path().resolve()
    if base_dir.name == "notebooks":
        base_dir = base_dir.parent

data_dir = base_dir / "data" / "parquet" / f"{YEAR}{int(MONTH):02d}"

path_건축_기본개요 = data_dir / "건축인허가_기본개요.parquet"
path_건축_동별개요 = data_dir / "건축인허가_동별개요.parquet"
path_건축_층별개요 = data_dir / "건축인허가_층별개요.parquet"

path_주택_기본개요 = data_dir / "주택인허가_기본개요.parquet"
path_주택_동별개요 = data_dir / "주택인허가_동별개요.parquet"
path_주택_층별개요 = data_dir / "주택인허가_층별개요.parquet"

# DuckDB 연결
con = duckdb.connect()

# Parquet 파일을 직접 쿼리하기 위해 Relation으로 로드
건축_기본개요 = con.read_parquet(path_건축_기본개요.as_posix())
건축_동별개요 = con.read_parquet(path_건축_동별개요.as_posix())
건축_층별개요 = con.read_parquet(path_건축_층별개요.as_posix())

주택_기본개요 = con.read_parquet(path_주택_기본개요.as_posix())
주택_동별개요 = con.read_parquet(path_주택_동별개요.as_posix())
주택_층별개요 = con.read_parquet(path_주택_층별개요.as_posix())


# %%
# ──────────── 2) 컬럼 정의 & 건수 집계 ────────────
# 2015년부터 2024년까지 YYYYMMDD 형식의 날짜 범위에 해당하는 각 컬럼의 건수 집계
def count_between_dates(
    connection, table_name, column_names, lower="20150101", upper="20241231"
):
    results = []
    for col in column_names:
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {table_name}
        WHERE "{col}" >= '{lower}' AND "{col}" <= '{upper}'
        """
        cnt = connection.execute(sql).fetchone()[0]  # 집계 결과가 (cnt,) 형태로 돌아옴
        results.append({"컬럼명": col, "레코드 수": cnt})
    return pd.DataFrame(results)


columns_건축_일자 = [
    "건축_허가_일",
    "착공_예정_일",
    "착공_연기_일",
    "실제_착공_일",
    "사용승인_일",
]
columns_주택_일자 = [
    "승인_일",
    "착공_예정_일",
    "착공_일",
    "사용_검사_예정_일",
    "사용_검사_일",
]

df_건축_결과 = count_between_dates(
    con, "건축_기본개요", columns_건축_일자, lower, upper
)
df_주택_결과 = count_between_dates(
    con, "주택_기본개요", columns_주택_일자, lower, upper
)

print("── 건축DB 기본개요: 각 컬럼별 ‘20150101’ 이상, ‘20241231’ 이하 건수 ──")
print(df_건축_결과)

print("\n── 주택DB 기본개요: 각 컬럼별 ‘20150101’ 이상, ‘20241231’ 이하 건수 ──")
print(df_주택_결과)
print()

# %% [markdown]
# ## 1. 건축인허가 데이터 전처리 및 Relation 체이닝

# %%
# 건축 기본일부 처리 (추정 착공일 산출)
# [추정 착공일 산출 로직]
# 준공일(사용승인_일)이 존재하는 경우(NULL 또는 빈 문자열이 아닌 경우)
# 건물이 준공되었으므로 착공도 반드시 이루어졌다고 봅니다.
# 따라서 실제_착공_일 → 착공_연기_일 → 착공_예정_일 순으로 우선순위를 부여하여 NULL이 아닌 첫 번째 값을 추정 착공일로 선택합니다.
# 준공일이 없는 경우에는 실제_착공_일(기본값)을 그대로 사용합니다.
sql_건축_기본일부 = """
SELECT
    "관리_허가대장_PK",
    "시군구_코드",
    "건축_구분_코드",
    "건축_구분_코드_명",
    "연면적(㎡)",
    "건축_허가_일",
    "착공_예정_일",
    "착공_연기_일",
    "실제_착공_일",
    "사용승인_일",
    CASE
        WHEN "사용승인_일" IS NOT NULL AND "사용승인_일" != ''
            THEN COALESCE(NULLIF("실제_착공_일", ''), NULLIF("착공_연기_일", ''), NULLIF("착공_예정_일", ''))
        ELSE "실제_착공_일"
    END AS 추정_착공일
FROM 건축_기본개요
"""
건축_기본일부 = con.sql(sql_건축_기본일부)

# %%
# 최근 10년 사용승인 데이터를 건축행위별로 세기
query_activity = """
SELECT
    "건축_구분_코드",
    "건축_구분_코드_명",
    COUNT(*) AS count
FROM 건축_기본일부
WHERE "사용승인_일" IS NOT NULL
    AND "건축_허가_일" >= '20150101'
    AND "건축_허가_일" <= '20241231'
GROUP BY "건축_구분_코드", "건축_구분_코드_명"
ORDER BY "건축_구분_코드"
"""
print("── 최근 10년 사용승인 데이터를 건축행위별로 세기 ──")
print(con.sql(query_activity))
print()
# 최근 10년(2015-2024) 건축인허가 데이터에는 건축 구분 코드 오류가 거의 없음(미기재된 경우 66건)

# 신축, 증축, 개축, 재축, 이전, 대수선만 집계
# (건축허가착공준공통계의 신축+증축/개축/재축/이전+대수선 구분과 연계)
query = """
SELECT
    *
FROM 건축_기본일부
WHERE "건축_구분_코드" IN ('0100', '0200', '0300', '0400', '0500', '0600')
"""
건축_신축증축대수선 = con.sql(query)

# 건축_신축증축대수선 준공 건수 확인
query_count = """
SELECT COUNT(*) AS count
FROM 건축_신축증축대수선
WHERE "사용승인_일" >= '20150101'
    AND "사용승인_일" <= '20241231'
"""
print("── 최근 10년 준공, 신축+증축/개축/재축/이전+대수선 건수 ──")
print(con.sql(query_count))
print()
# 최근 10년 준공, 신축+증축/개축/재축/이전+대수선 1,200,980 건

# %%
# 동별개요 LEFT JOIN
query = """
SELECT
    a.*,
    b."관리_동별_개요_PK",
    b."연면적(㎡)" AS 동별_연면적
FROM 건축_신축증축대수선 a
LEFT JOIN 건축_동별개요 b
    ON a."관리_허가대장_PK" = b."관리_허가대장_PK"
"""
건축_join_동별 = con.sql(query)

# 층별개요 LEFT JOIN
query = """
SELECT
    a.*,
    c."관리_층별_개요_PK",
    c."층_구분_코드",
    c."층_구분_코드_명",
    c."주_용도_코드",
    c."주_용도_코드_명",
    c."층_면적(㎡)" AS 층_면적
FROM 건축_join_동별 a
LEFT JOIN 건축_층별개요 c
    ON a."관리_동별_개요_PK" = c."관리_동별_개요_PK"
"""
건축_join_동별층별 = con.sql(query)

# %%
# 층별 면적 극단값 확인 (50만㎡ 초과 오류 데이터 식별용 백분위수 출력)
area_df = con.sql(
    'SELECT "층_면적(㎡)" FROM 건축_층별개요 WHERE "층_면적(㎡)" IS NOT NULL'
).df()
print("── 건축 층별 면적 분포 백분위수 (극단값 확인) ──")
print(f"Max value:           {area_df['층_면적(㎡)'].max(): 15.2f}")
print(f"99.9999% percentile: {area_df['층_면적(㎡)'].quantile(0.999999): 15.2f}")
print(f"99.999% percentile:  {area_df['층_면적(㎡)'].quantile(0.99999): 15.2f}")
print(f"99.99% percentile:   {area_df['층_면적(㎡)'].quantile(0.9999): 15.2f}")
print(f"99.9% percentile:    {area_df['층_면적(㎡)'].quantile(0.999): 15.2f}")
print(f"99.0% percentile:    {area_df['층_면적(㎡)'].quantile(0.99): 15.2f}")
print()

# %%
# 층별 연면적 집계 (옥탑층 제외 및 50만㎡ 초과 극단값 제외)
query = """
SELECT
    관리_허가대장_PK,
    관리_동별_개요_PK,
    시군구_코드,
    주_용도_코드,
    주_용도_코드_명,
    MAX("연면적(㎡)") AS 건별_연면적,
    MAX(동별_연면적) AS 동별_연면적,
    SUM(층_면적) AS 층별_연면적,
    COALESCE(SUM(층_면적), MAX(동별_연면적)) AS 연면적
FROM 건축_join_동별층별
WHERE "층_구분_코드" NOT IN ('30')  -- 옥탑층 제외
    AND 층_면적 <= 500000  -- 층 면적이 500,000㎡ 이하인 경우만
GROUP BY 관리_허가대장_PK, 관리_동별_개요_PK, 시군구_코드, 주_용도_코드, 주_용도_코드_명
"""
건축_group_동별 = con.sql(query)

# 건별 최종 연면적 산출
query = """
WITH grouped AS (
    SELECT
        관리_허가대장_PK,
        시군구_코드,
        주_용도_코드,
        주_용도_코드_명,
        MAX(건별_연면적) AS 건별_연면적,
        SUM(동별_연면적) AS 동별_연면적,
        SUM(층별_연면적) AS 층별_연면적,
        COALESCE(SUM(연면적), MAX(건별_연면적)) AS 연면적
    FROM 건축_group_동별
    GROUP BY 관리_허가대장_PK, 시군구_코드, 주_용도_코드, 주_용도_코드_명
)
SELECT
    *
FROM grouped
LEFT JOIN 건축_신축증축대수선
    USING (관리_허가대장_PK)
"""
건축_group_건별 = con.sql(query)

# %%
# 최근 10년 건축물 면적 합계 확인 (건별, 동별, 층별, 최종 연면적 비교)
query_compare = """
SELECT
    건축_구분_코드,
    건축_구분_코드_명,
    SUM(건별_연면적) AS "SUM(건별_연면적)",
    SUM(동별_연면적) AS "SUM(동별_연면적)",
    SUM(층별_연면적) AS "SUM(층별_연면적)",
    SUM(연면적) AS "SUM(연면적)"
FROM 건축_group_건별
WHERE "사용승인_일" >= '20150101'
    AND "사용승인_일" <= '20241231'
GROUP BY 건축_구분_코드, 건축_구분_코드_명
ORDER BY 건축_구분_코드
"""
print("── 최근 10년 건축물 면적 합계 확인 (건별 vs 동별 vs 층별 vs 최종연면적) ──")
print(con.sql(query_compare))
print()
# 단순히 층별 면적 집계한 것과 동일한 결과

# %%
# final_건축 정의
query = """
SELECT
    시군구_코드,
    주_용도_코드,
    주_용도_코드_명,
    건축_구분_코드,
    건축_구분_코드_명,
    연면적,
    건축_허가_일 AS 허가일,
    추정_착공일 AS 착공일,
    사용승인_일 AS 준공일
FROM 건축_group_건별
"""
final_건축 = con.sql(query)

# %% [markdown]
# ## 2. 주택인허가 데이터 전처리 및 Relation 체이닝

# %%
# 주택 기본일부 처리 (추정 착공일 산출)
# [추정 착공일 로직 설명]
# 준공일(사용_검사_일)이 존재하는 경우(NULL 또는 빈 문자열이 아닌 경우) 건물이 준공되었으므로 착공도 반드시 이루어졌다고 봅니다.
# 착공_일 → 착공_예정_일 순으로 우선순위를 부여하여 NULL이 아닌 첫 번째 값을 추정 착공일로 선택합니다.
# 준공일이 없는 경우에는 착공_일(기본값)을 그대로 사용합니다.
sql_주택_기본일부 = """
SELECT
    "관리_주택대장_PK",
    "시군구_코드",
    "연면적(㎡)",
    "승인_일",
    "착공_예정_일",
    "착공_일",
    "사용_검사_예정_일",
    "사용_검사_일",
    CASE
        WHEN "사용_검사_일" IS NOT NULL AND "사용_검사_일" != ''
            THEN COALESCE(NULLIF("착공_일", ''), NULLIF("착공_예정_일", ''))
        ELSE "착공_일"
    END AS 추정_착공일
FROM 주택_기본개요
"""
주택_기본일부 = con.sql(sql_주택_기본일부)

# %%
# 층별 면적 극단값 확인 (50만㎡ 초과 오류 데이터 식별용 백분위수 출력 - 주택)
area_df_주택 = con.sql(
    'SELECT "층_면적(㎡)" FROM 주택_층별개요 WHERE "층_면적(㎡)" IS NOT NULL'
).df()
print("── 주택 층별 면적 분포 백분위수 (극단값 확인) ──")
print(f"Max value:           {area_df_주택['층_면적(㎡)'].max(): 15.2f}")
print(f"99.9999% percentile: {area_df_주택['층_면적(㎡)'].quantile(0.999999): 15.2f}")
print(f"99.999% percentile:  {area_df_주택['층_면적(㎡)'].quantile(0.99999): 15.2f}")
print(f"99.99% percentile:   {area_df_주택['층_면적(㎡)'].quantile(0.9999): 15.2f}")
print(f"99.9% percentile:    {area_df_주택['층_면적(㎡)'].quantile(0.999): 15.2f}")
print(f"99.0% percentile:    {area_df_주택['층_면적(㎡)'].quantile(0.99): 15.2f}")

# %% [markdown]
# - 기본개요 연면적을 사용할 수는 없음:
#   - 검토 결과 신축인 경우만 층별개요에 면적이 부기됨. 층별개요 면적, 주용도코드 집계 가능.
# - 지상, 지하, 복수층(하층), 복수층(상층) (현재는 없음) 해당하는 경우만 집계함.

# %%
# 동별 및 층별개요 연계 (지상/지하/복수층 및 50만㎡ 초과 극단값 제외)
query = """
SELECT
    a.관리_주택대장_PK,
    a.관리_동별_개요_PK,
    b.관리_층별_개요_PK,
    b.시군구_코드,
    b.층_구분_코드,
    b.층_구분_코드_명,
    b."층_면적(㎡)",
    b.용도_코드 AS 주_용도_코드,
    b.용도_코드_명 AS 주_용도_코드_명
FROM 주택_동별개요 a
JOIN 주택_층별개요 b
    ON a."관리_동별_개요_PK" = b."관리_동별_개요_PK"
WHERE b."층_구분_코드" IN ('10', '20', '21', '22')  -- 지상, 지하, 복수층만
    AND b."층_면적(㎡)" <= 500000  -- 층 면적이 500,000㎡ 이하인 경우만
"""
주택_동별층별 = con.sql(query)

# 층별 연면적 집계
query = """
SELECT
    관리_주택대장_PK,
    시군구_코드,
    주_용도_코드,
    주_용도_코드_명,
    SUM("층_면적(㎡)") AS 연면적
FROM 주택_동별층별
GROUP BY 관리_주택대장_PK, 시군구_코드, 주_용도_코드, 주_용도_코드_명
"""
주택_grouped = con.sql(query)

# 주택 일자 정보
query = """
SELECT
    관리_주택대장_PK,
    승인_일 AS 허가일,
    추정_착공일 AS 착공일,
    사용_검사_일 AS 준공일
FROM 주택_기본일부
"""
주택_date = con.sql(query)

# final_주택 정의
# (주택인허가 데이터 검토 결과, 신축인 경우에만 층별개요에 면적이 부기되어 있으므로
# 이를 근거로 해당 주택 데이터들을 신축('0100')으로 추정하여 집계합니다.)
query = """
SELECT
    a.관리_주택대장_PK,
    a.시군구_코드,
    a.주_용도_코드,
    a.주_용도_코드_명,
    '0100' AS 건축_구분_코드,
    '신축' AS 건축_구분_코드_명,
    a.연면적,
    b.허가일,
    b.착공일,
    b.준공일
FROM 주택_grouped a
JOIN 주택_date b
    USING (관리_주택대장_PK)
"""
final_주택 = con.sql(query)

# %% [markdown]
# ## 3. 최근 10년 준공 실적 집계 (전체)

# %%
print(f"=== {lower[:4]}~{upper[:4]}년 준공 실적 집계 (전체) ===")

query_통합_10년 = f"""
-- 10개년 준공 실적 통합 집계 (건축인허가와 주택인허가를 UNION ALL 한 후 최종 GROUP BY)
SELECT
    준공_년월,
    시도_코드,
    용도_대분류_코드,
    건축_구분_코드,
    건축_구분_코드_명,
    SUM(연면적) AS "sum(""연면적"")"
FROM (
    -- [1] 건축인허가 준공 데이터 추출
    SELECT
        LEFT(준공일, 6) AS 준공_년월,
        LEFT(시군구_코드, 2) AS 시도_코드,
        LEFT(주_용도_코드, 2) AS 용도_대분류_코드,
        건축_구분_코드,
        건축_구분_코드_명,
        연면적
    FROM final_건축
    WHERE 준공일 IS NOT NULL
        AND LENGTH(준공일) = 8
        AND 준공일 >= '{lower}'
        AND 준공일 <= '{upper}'

    UNION ALL

    -- [2] 주택인허가 준공 데이터 추출
    SELECT
        LEFT(준공일, 6) AS 준공_년월,
        LEFT(시군구_코드, 2) AS 시도_코드,
        LEFT(주_용도_코드, 2) AS 용도_대분류_코드,
        건축_구분_코드,
        건축_구분_코드_명,
        연면적
    FROM final_주택
    WHERE 준공일 IS NOT NULL
        AND LENGTH(준공일) = 8
        AND 준공일 >= '{lower}'
        AND 준공일 <= '{upper}'
) AS combined
-- [3] 두 개방데이터의 동일 범주(Key) 행들을 병합하여 연면적 합산
GROUP BY 준공_년월,
    시도_코드,
    용도_대분류_코드,
    건축_구분_코드,
    건축_구분_코드_명
ORDER BY ALL
"""
df_준공_10년_통합 = con.sql(query_통합_10년).df()
print(f"통합 완료 (전체, 중복 제거됨): {len(df_준공_10년_통합):,} 건")

# %% [markdown]
# ## 4. 최근 10년 준공 실적 집계 (주택)

# %%
print(f"=== {lower[:4]}~{upper[:4]}년 준공 실적 집계 (주택/주거용만) ===")

query_통합_10년_주택 = f"""
-- 10개년 주택/주거용 준공 실적 통합 집계 (주_용도_코드 <= '02999' 필터 및 최종 GROUP BY)
SELECT
    준공_년월,
    시도_코드,
    주_용도_코드,
    건축_구분_코드,
    건축_구분_코드_명,
    SUM(연면적) AS "sum(""연면적"")"
FROM (
    -- [1] 건축인허가 주거용 준공 데이터 추출
    SELECT
        LEFT(준공일, 6) AS 준공_년월,
        LEFT(시군구_코드, 2) AS 시도_코드,
        주_용도_코드,
        건축_구분_코드,
        건축_구분_코드_명,
        연면적
    FROM final_건축
    WHERE 준공일 IS NOT NULL
        AND LENGTH(준공일) = 8
        AND 준공일 >= '{lower}'
        AND 준공일 <= '{upper}'
        AND 주_용도_코드 <= '02999'

    UNION ALL

    -- [2] 주택인허가 주거용 준공 데이터 추출
    SELECT
        LEFT(준공일, 6) AS 준공_년월,
        LEFT(시군구_코드, 2) AS 시도_코드,
        주_용도_코드,
        건축_구분_코드,
        건축_구분_코드_명,
        연면적
    FROM final_주택
    WHERE 준공일 IS NOT NULL
        AND LENGTH(준공일) = 8
        AND 준공일 >= '{lower}'
        AND 준공일 <= '{upper}'
        AND 주_용도_코드 <= '02999'
) AS combined
-- [3] 두 개방데이터의 동일 범주(Key) 행들을 병합하여 연면적 합산
GROUP BY 준공_년월,
    시도_코드,
    주_용도_코드,
    건축_구분_코드,
    건축_구분_코드_명
ORDER BY ALL
"""
df_준공_10년_주택_통합 = con.sql(query_통합_10년_주택).df()
print(f"통합 완료 (주택, 중복 제거됨): {len(df_준공_10년_주택_통합):,} 건")

# %% [markdown]
# ## 5. Excel 파일 저장

# %%
output_dir = base_dir / "results" / "stat_permit_completion" / f"{YEAR}{int(MONTH):02d}"
output_dir.mkdir(parents=True, exist_ok=True)

path_통합 = output_dir / "준공_10년_통합.xlsx"
path_주택_통합 = output_dir / "준공_10년_주택_통합.xlsx"

df_준공_10년_통합.to_excel(path_통합, index=False)
df_준공_10년_주택_통합.to_excel(path_주택_통합, index=False)

print(f"Saved: {path_통합}")
print(f"Saved: {path_주택_통합}")
