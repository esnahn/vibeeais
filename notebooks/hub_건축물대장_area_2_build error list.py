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
from tabulate import tabulate

data_root = Path("D:\\데이터\\건축데이터 건축허브 개방데이터")
path_DB = data_root / "건축물대장_2025년_02월.db"

# %% [markdown]
# 건축허브에서 제공하는 건축물대장 데이터를 활용하여 분석
#
# 2025년 2월 이전 데이터는 위반건축물 등 일부 건축물이 빠져있는 문제가 있어 부득이 2025년 2월 말 데이터로 2024년 말 기준 건축물 현황을 집계

# %%
# Open a DuckDB connection
con = duckdb.connect(database=path_DB, read_only=True)

# print the list of tables in the database
tables = con.execute("SHOW TABLES").fetchall()
print("Tables in the database:")
for table in tables:
    print(table[0])
    print()

# show heads of the tables
for table in tables:
    table_name = table[0]
    print(f"Head of {table_name}:")
    df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
    print(df)
    print()

# %%
# Get the list of tables in the database
tables = con.execute("SHOW TABLES").fetchall()

print("=== COLUMN COUNT SUMMARY ===")
column_counts = []

for table in tables:
    table_name = table[0]
    columns_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
    column_count = len(columns_info)
    column_names = list(columns_info["column_name"])

    column_counts.append(
        {
            "Table Name": table_name,
            "Column Count": column_count,
            "Column Names": ", ".join(column_names),
        }
    )

column_counts_df = pd.DataFrame(column_counts)
# Use context manager to temporarily set pandas display options
with pd.option_context(
    "display.max_columns",
    None,
    "display.max_colwidth",
    None,
    "display.width",
    None,
    "display.max_rows",
    None,
):
    display(column_counts_df)


# %%
# Create a list to store the table names and their record counts
table_counts = []

# Iterate through the tables and count the records
for table in tables:
    table_name = table[0]
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    table_counts.append({"Table Name": table_name, "Record Count": count})

# Convert the list to a DataFrame
record_counts_df = pd.DataFrame(table_counts)

# Display the DataFrame
display(record_counts_df)

# %%
table_name = "표제부"


def get_column_counts_duckdb(con: duckdb.DuckDBPyConnection, table_name: str):
    # 0) print the statement
    print(f"Counting columns in table: {table_name}")
    # 1) 테이블 스키마에서 컬럼 이름을 가져오고
    cols_df = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
    cols = cols_df["name"].tolist()

    # 2) 각 컬럼에 대한 COUNT 문을 생성
    # 표제부에는 컬럼명에 %가 들어간 경우가 있어, 오류가 발생함
    # 이를 피하기 위해, 컬럼명을 따옴표로 감싸줌
    count_exprs = [f'COUNT("{col}") AS "{table_name}_{col}_count"' for col in cols]

    # 3) 최종 쿼리 조립
    query = """
    SELECT
        {exprs}
    FROM "{table_name}"
    """.format(exprs=",\n    ".join(count_exprs), table_name=table_name)

    # 4) 실행 및 결과 반환
    result = con.execute(query).fetchdf().T

    return result


with pd.option_context("display.max_rows", None, "display.max_columns", None):
    # Display the result with all rows and columns
    print(
        tabulate(get_column_counts_duckdb(con, table_name), intfmt=",", floatfmt=",.0f")
    )

# %%
table_name = "표제부"
query = f"""SELECT
    *
FROM
    (SUMMARIZE "{table_name}")
WHERE
    column_name IN (
        '시군구_코드',
        '법정동_코드',
        '주_부속_구분_코드',
        '주_부속_구분_코드_명',
        '대지_면적(㎡)',
        '건축_면적(㎡)',
        '건폐_율(%)',
        '연면적(㎡)',
        '용적_률_산정_연면적(㎡)',
        '용적_률(%)',
        '구조_코드',
        '구조_코드_명',
        '기타_구조',
        '주_용도_코드',
        '주_용도_코드_명',
        '기타_용도',
        '높이(m)',
        '지상_층_수',
        '지하_층_수',
        '부속_건축물_수',
        '부속_건축물_면적(㎡)',
        '총_동_연면적(㎡)',
        '허가_일',
        '착공_일',
        '사용승인_일',
    );
"""
con.sql(query).show(max_rows=1_000)

# %% [markdown]
# 표제부 면적 관련 데이터는 미기재율 1% 미만.
# 그러나 면적, 건폐율, 용적률 등이 음수인 경우, 이론상 100%를 초과할 수 없는 건폐율이 1,000,000,000인 경우 등 이상값 발견됨.
#
# 시군구, 법정동, 사용승인일 등은 미기재율은 낮으나 이상값이 발견됨 (0, 99999, 9990408 등)

# %%
with pd.option_context("display.max_rows", None, "display.max_columns", None):
    # Display the result with all rows and columns
    print(
        tabulate(
            get_column_counts_duckdb(con, "총괄표제부"), intfmt=",", floatfmt=",.0f"
        )
    )

# %%
table_name = "총괄표제부"
query = f"""SELECT
    *
FROM
    (SUMMARIZE "{table_name}")
WHERE
    column_name IN (
        '시군구_코드',
        '법정동_코드',
        '주_부속_구분_코드',
        '주_부속_구분_코드_명',
        '대지_면적(㎡)',
        '건축_면적(㎡)',
        '건폐_율(%)',
        '연면적(㎡)',
        '용적_률_산정_연면적(㎡)',
        '용적_률(%)',
        '주_용도_코드',
        '주_용도_코드_명',
        '기타_용도',
        '세대_수(세대)',
        '가구_수(가구)',
        '주_건축물_수',
        '부속_건축물_수',
        '부속_건축물_면적(㎡)',
        '허가_일',
        '착공_일',
        '사용승인_일',
    );
"""
con.sql(query).show(max_rows=1_000)

# %%
query = """
SELECT
    m.관리_건축물대장_PK,
    m.관리_상위_건축물대장_PK,
    m.관리_상위_건축물대장_PK IS NULL AS 관리_상위_건축물대장_PK_null,
    m.대지_위치,
    m.건물_명,
    b.동_명,
    b."시군구_코드",
    b."법정동_코드",
    b."주_부속_구분_코드",
    b."주_부속_구분_코드_명",
    b."대지_면적(㎡)",
    b."건축_면적(㎡)",
    b."건폐_율(%)",
    b."연면적(㎡)",
    b."용적_률_산정_연면적(㎡)",
    b."용적_률(%)",
    b."주_용도_코드",
    b."주_용도_코드_명",
    b."기타_용도",
    b."높이(m)",
    b."지상_층_수",
    b."지하_층_수",
    b."부속_건축물_수",
    b."부속_건축물_면적(㎡)",
    b."총_동_연면적(㎡)",
    b."허가_일",
    b."착공_일",
    b."사용승인_일",
    b.생성_일자,
    r."대지_면적(㎡)"               AS "총괄_대지_면적(㎡)",
    r."건축_면적(㎡)"               AS "총괄_건축_면적(㎡)",
    r."건폐_율(%)"                  AS "총괄_건폐_율(%)",
    r."연면적(㎡)"                  AS "총괄_연면적(㎡)",
    r."용적_률_산정_연면적(㎡)"     AS "총괄_용적_률_산정_연면적(㎡)",
    r."용적_률(%)"                  AS "총괄_용적_률(%)",
    r."주_용도_코드"                AS "총괄_주_용도_코드",
    r."주_용도_코드_명"             AS "총괄_주_용도_코드_명",
    r."기타_용도"                   AS "총괄_기타_용도",
    r."세대_수(세대)"               AS "총괄_세대_수(세대)",
    r."가구_수(가구)"               AS "총괄_가구_수(가구)",
    r."주_건축물_수"                AS "총괄_주_건축물_수",
    r."부속_건축물_수"              AS "총괄_부속_건축물_수",
    r."부속_건축물_면적(㎡)"        AS "총괄_부속_건축물_면적(㎡)",
FROM 기본개요 m
INNER JOIN 표제부 b
ON m.관리_건축물대장_PK = b.관리_건축물대장_PK
LEFT JOIN 총괄표제부 r  -- short for "recap"
ON m.관리_상위_건축물대장_PK = r.관리_건축물대장_PK
"""
기본개요_표제부_총괄표제부 = con.sql(query)
기본개요_표제부_총괄표제부.limit(5).show()

# %%
con.sql("""
        SELECT *
        FROM 기본개요_표제부_총괄표제부
        WHERE 대지_위치 LIKE '서울특별시 강남구 대치동 500번지' -- AND 동_명 LIKE ''
        """).show()

# %% [markdown]
# 대치동 500번지 개포우성2차 단지는 총괄표제부에 대지면적 0으로 기재되어, 건폐율, 용적률도 0으로 표시됨.
# (토지이용계획 상 실제 면적 36,959.8 ㎡)
#
# 표제부 대지면적, 건폐율, 용적률도 미기재(빈 칸)가 아니라 0으로 기재되어 있는데, 개방데이터 상에서는 동일하게 0으로 구분할 수 없음.

# %%
con.sql("""
        SELECT *
        FROM 기본개요_표제부_총괄표제부
        WHERE 대지_위치 LIKE '%한솔동 939%' AND 동_명 LIKE '101동'
        """).show()

# %% [markdown]
# 한솔동 939번지, 101동: 표제부 대장상에서 대지면적, 건폐율, 용적률이 0으로 기재가 아니라 미기재(빈 칸)된 경우.
# 개방데이터 상에서는 동일하게 0으로 구분할 수 없음.

# %%
con.sql("SELECT COUNT(*) FROM 기본개요_표제부_총괄표제부").show()

# %%
query = """
SELECT
  관리_상위_건축물대장_PK_null,
  주_부속_구분_코드,
  COUNT(*) AS 건축물_수,
  AVG("대지_면적(㎡)") AS 평균_대지_면적,
  AVG("건축_면적(㎡)") AS 평균_건축_면적,
  AVG("건폐_율(%)") AS 평균_건폐율,
  AVG("연면적(㎡)") AS 평균_연면적,
  AVG("용적_률_산정_연면적(㎡)") AS 평균_용적률산정용연면적,
  AVG("용적_률(%)") AS 평균_용적률,
  AVG(지상_층_수) AS 평균_지상층수,
  AVG(지하_층_수) AS 평균_지하층수,
  AVG("부속_건축물_수") AS 평균_부속_건축물_수,
  AVG("부속_건축물_면적(㎡)") AS 평균_부속_건축물_면적,
  AVG("총_동_연면적(㎡)") AS 평균_총_동_연면적,
  AVG("총괄_대지_면적(㎡)") AS 평균_총괄_대지_면적,
  AVG("총괄_건축_면적(㎡)") AS 평균_총괄_건축_면적,
  AVG("총괄_건폐_율(%)") AS 평균_총괄_건폐율,
  AVG("총괄_연면적(㎡)") AS 평균_총괄_연면적,
  AVG("총괄_용적_률_산정_연면적(㎡)") AS 평균_총괄_용적률산정용연면적,
  AVG("총괄_용적_률(%)") AS 평균_총괄_용적률,
  AVG("총괄_주_건축물_수") AS 평균_총괄_주_건축물_수,
FROM
  기본개요_표제부_총괄표제부
GROUP BY
  관리_상위_건축물대장_PK_null,
  주_부속_구분_코드
ORDER BY
  관리_상위_건축물대장_PK_null DESC,
  주_부속_구분_코드;
"""
con.sql(query).show()

# %% [markdown]
# 총괄표제부가 없는 단독 주건축물에서 평균 건폐율이 13,000%로 나타나는 등, 이상값의 영향이 매우 크다.

# %%
query = """
SELECT * FROM 기본개요_표제부_총괄표제부
WHERE
  주_부속_구분_코드 = 0
"""
주건축물 = con.sql(query)
주건축물.limit(5).show()

# %%
con.sql("SELECT COUNT(*) FROM 주건축물").show()

# %% [markdown]
# 건축물대장 표제부 중, 기본개요와 연계 성공하고, 주/부속 구분 코드가 0(주건축물)으로 기재된 경우는 7,364,967 건으로, 2024년 건축물 현황 통계 7,421,603동과 비교할 때 다소 작은 수치이다.
#
# 이러한 차이는 보안시설 등 일부 건축물 데이터가 개방데이터에서 제외된 것이 주 원인으로 판단되나, 기본개요 연계 실패, 주/부속 구분 코드 기재 오류 등의 경우도 배제할 수 없다.

# %%
query = """
SELECT * FROM 주건축물
WHERE
  관리_상위_건축물대장_PK_null IS true
"""
주건축물_단독 = con.sql(query)
주건축물_단독.limit(5).show()

# %%
query_columns = """
  관리_건축물대장_PK,
  관리_상위_건축물대장_PK,
  대지_위치,
  건물_명,
  동_명,
  주_부속_구분_코드,
  "대지_면적(㎡)",
  "건축_면적(㎡)",
  "건폐_율(%)",
  "연면적(㎡)",
  "용적_률_산정_연면적(㎡)",
  "용적_률(%)",
  지상_층_수,
  지하_층_수,
  "부속_건축물_수",
  "부속_건축물_면적(㎡)",
  "총_동_연면적(㎡)",
  생성_일자,
"""
query = f"""
SELECT
{query_columns}
FROM
  주건축물_단독
ORDER BY
  RANDOM();
"""
con.sql(query).limit(20).show()

# %% [markdown]
# 단독 주건축물의 경우, 대지면적, 건폐율, 용적률에 오류가 있는 경우가 많다.
#
# 건축면적, 연면적, 용적률 산정용 연면적, 지상/지하 층수의 오류율이 낮다.
#
# 총 동 연면적은 연면적과 동일하거나 0(미기재)인 경우가 많다.
#
# → 동별 연면적 파악을 위해서는 연면적 값 사용, 검증에는 건축면적, 층수 등 사용

# %% [markdown]
# 건축면적과 연면적이 0(미기재)인 건축물도 다수 존재. 용적률 산정 연면적만 기재됨.
#
# 사용승인일 기준 최근 10년 건축한 건축물의 경우에도 연면적이 음수인 등 오류 사례 존재함.
#
# - 광진리 168-1번지: 신축 내역 없이 -132.16 ㎡ 개축만 등재하여 연면적이 음수로 기재됨
# - 서부리 816-18번지: 증축 후 이기된 별도 대장과 별개로 감축만 등재하여 연면적 음수 기재
# - 본리리 108-2번지: 2004년 신축 사용승인 후 용도변경, 대장 정비 등 있었으나, 최종적으로 건축면적, 연면적 미기재, 용적률 산정용 연면적 음수 기재. 층별개요는 정상.

# %% [markdown]
# 여의도 면적(제방 안쪽 면적 2.9㎢ = 2,900,000㎡)의 수 배~수십 배에 달하는 연면적이 기재된 경우 다수로, 대지면적, 건축면적 등과 비교할 때 오류임이 명백함.
#
# 연면적이 지나치게 큰 오류가 있는 경우 총 동 연면적도 같은 값이어서 오류 정정에 도움이 되지 않음.
#
#

# %% [markdown]
# 집합건축물대장 표제부의 경우 대지면적, 건폐율, 용적률이 기재되지 않음. 또한, 해당 수치는 대지 단위로 산정되므로 동별 연면적 검증에는 도움이 되지 않음.
#
# 건축면적이 미기재된 사례도 있음.
#
# - 한솔동 939번지 상가동: 건축면적 미기재(대장상 빈 칸으로 나타남).
#   실제로는 상가동이 독립적으로 존재하는 것이 아니라 101동 하부에 위치하고 있으며,
#   주동과 그 사이 공간에 위치한 1층 건물로 옥상을 공원화하여 이용하고 있음.
#   실제로 1층 부분이 건축면적에 산입되지 않아 건축면적이 0이 된 것인지는 알 수 없으나,
#   건축물대장에서는 빈 칸으로 나타나고 있으므로, 미기재로 판단함.

# %% [markdown]
# 지상층수가 0으로 기재된 사례가 9,097건에 달함.
# 다만, 지하도상가 등 실제로 지상층이 존재하지 않는 경우도 포함된 수치임.
#
# 지상층수는 0이 반드시 미기재를 뜻하지는 않으므로 주의가 필요함.

# %% [markdown]
# ## 요약
#
# 연면적, 용적률 산정 연면적, 대지면적, 건축면적, 건폐율, 용적률 등 데이터는 미기재를 0으로 저장하고 있음.
#
# 이외에도 명백한 오류 사례가 많음.
#
# 수치 데이터 미기재(0) 및 오류 사례를 널(null)값으로 처리하고, 검증 규칙을 미기재 및 오류를 고려하여 엄밀하게 정의할 필요가 있음.

# %% [markdown]
# ---

# %%
주건축물.columns

# %%
주건축물.limit(1).show()

# %%
# 0 as null

query = """
SELECT
  "관리_건축물대장_PK",
  "관리_상위_건축물대장_PK",
  "관리_상위_건축물대장_PK_null" AS 단독,
  "대지_위치",
  "시군구_코드",
  "법정동_코드",
  "주_용도_코드",
  "주_용도_코드_명",
  "기타_용도",
  CAST(NULLIF("대지_면적(㎡)", 0) AS DOUBLE) AS "대지_면적(㎡)",
  CAST(NULLIF("건축_면적(㎡)", 0) AS DOUBLE) AS "건축_면적(㎡)",
  CAST(NULLIF("건폐_율(%)", 0) AS DOUBLE) AS "건폐_율(%)",
  CAST(NULLIF("연면적(㎡)", 0) AS DOUBLE) AS "연면적(㎡)",
  CAST(NULLIF("용적_률_산정_연면적(㎡)", 0) AS DOUBLE) AS "용적_률_산정_연면적(㎡)",
  CAST(NULLIF("용적_률(%)", 0) AS DOUBLE) AS "용적_률(%)",
  CAST(NULLIF("높이(m)", 0) AS DOUBLE) AS "높이(m)",
  "지상_층_수",
  "지하_층_수",
  "사용승인_일",
  CAST(NULLIF("총괄_대지_면적(㎡)", 0) AS DOUBLE) AS "총괄_대지_면적(㎡)",
  CAST(NULLIF("총괄_건축_면적(㎡)", 0) AS DOUBLE) AS "총괄_건축_면적(㎡)",
  CAST(NULLIF("총괄_건폐_율(%)", 0) AS DOUBLE) AS "총괄_건폐_율(%)",
  CAST(NULLIF("총괄_연면적(㎡)", 0) AS DOUBLE) AS "총괄_연면적(㎡)",
  CAST(NULLIF("총괄_용적_률_산정_연면적(㎡)", 0) AS DOUBLE) AS "총괄_용적_률_산정_연면적(㎡)",
  CAST(NULLIF("총괄_용적_률(%)", 0) AS DOUBLE) AS "총괄_용적_률(%)",
  NULLIF("총괄_주_건축물_수", 0) AS "총괄_주_건축물_수",
FROM
  주건축물
"""
rel = con.sql(query)
rel.limit(20).show()
rel.to_parquet("../data/processed/주건축물_표제부_기본개요_총괄표제부.parquet")

# %% [markdown]
# ---

# %%
rel_orig = con.from_parquet(
    "../data/processed/주건축물_표제부_기본개요_총괄표제부.parquet"
)

con.sql("DESCRIBE rel_orig").show(max_rows=100)

# %% [markdown]
# ## 기존 규칙
#
# ### 14. (대지면적) 일반건축물대장 및 표제부 내 대지면적의 값이 건축면적보다 작은 경우
#

# %%
query = """
SELECT
  "대지_면적(㎡)",
  "총괄_대지_면적(㎡)",
  "건축_면적(㎡)",
  COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)") < "건축_면적(㎡)" - 1 AS 기존14,
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  기존14 = true
"""
con.sql(query).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 기존14, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 16. (건축면적) 일반건축물대장 및 표제부의 연면적이 표제부의 건축면적보다 작은 경우

# %%
query = """
SELECT
  "건축_면적(㎡)",
  "연면적(㎡)",
  "연면적(㎡)" < "건축_면적(㎡)" - 1 AS 기존16,
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  기존16 = true
"""
con.sql(query).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 기존16, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 19. (건폐율) 일반건축물대장 및 표제부 건폐율 계산의 정확성
#
# → 소수점 이하 처리에 대한 명확한 법규 처리 없어 반올림 자리수가 불분명함(소숫점 이하 절사처리)
#

# %%
query = """
SELECT
  "대지_면적(㎡)",
  "건축_면적(㎡)",
  "건폐_율(%)",
  "건축_면적(㎡)" / "대지_면적(㎡)" * 100 AS 건폐율_재계산,
  ("건폐_율(%)" - 건폐율_재계산) AS 건폐율_차이,
  ABS(건폐율_차이) > 0.1 AS 기존19
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  건폐율_차이 is not null
"""
con.sql(query).limit(20).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  기존19 = true
"""
con.sql(query).limit(20).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 기존19, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 20. (용적률) 일반건축물대장 및 표제부 용적율 계산의 정확성
#

# %%
query = """
SELECT
  "대지_면적(㎡)",
  "용적_률_산정_연면적(㎡)",
  "용적_률(%)",
  "용적_률_산정_연면적(㎡)" / "대지_면적(㎡)" * 100 AS 용적률_재계산,
  ("용적_률(%)" - 용적률_재계산) AS 용적률_차이,
  ABS(용적률_차이) > 0.1 AS 기존20
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  용적률_차이 is not null
"""
con.sql(query).limit(20).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  기존20 = true
"""
con.sql(query).limit(20).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 기존20, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ## 추가 규칙
#
# ### 01. 연면적이 용적률 산정 연면적보다 작음

# %%
query = """
SELECT
  "연면적(㎡)",
  "용적_률_산정_연면적(㎡)",
  "연면적(㎡)" < "용적_률_산정_연면적(㎡)" - 1 AS 신규01
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  신규01 is not null
"""
con.sql(query).limit(20).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  신규01 = true
"""
con.sql(query).limit(20).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 신규01, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 02. 연면적이 (건축면적×지상층수)+(대지면적×지하층수)보다 큼

# %%
query = """
SELECT
  "대지_면적(㎡)",
  "총괄_대지_면적(㎡)",
  "건축_면적(㎡)",
  "연면적(㎡)",
  지상_층_수,
  지하_층_수,
  ("건축_면적(㎡)" * 지상_층_수) + (COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)") * 지하_층_수) AS 연면적_상한,
  "연면적(㎡)" > 연면적_상한 + 1 AS 신규02
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  신규02 is not null
"""
con.sql(query).limit(20).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  신규02 = true
"""
con.sql(query).limit(20).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 신규02, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 03. 연면적, 용적률 산정 연면적, 대지면적, 건축면적 중 음수 존재

# %%
query = """
SELECT
  "대지_면적(㎡)",
  "총괄_대지_면적(㎡)",
  "건축_면적(㎡)",
  "총괄_건축_면적(㎡)",
  "연면적(㎡)",
  "용적_률_산정_연면적(㎡)",
  지상_층_수,
  지하_층_수,
  COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)", 0) < 0
  OR COALESCE("건축_면적(㎡)", "총괄_건축_면적(㎡)", 0) < 0
  OR COALESCE("연면적(㎡)", 0) < 0
  OR COALESCE("지상_층_수", 0) < 0
  OR COALESCE("지하_층_수", 0) < 0
  AS 신규03,
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(5).show()

# %%
query = """
SELECT *
FROM
  rel
WHERE
  신규03 = true
"""
con.sql(query).limit(5).show()

# %%
# 컬럼의 값별 개수 집계
con.sql("""
    SELECT 신규03, COUNT(*) AS count
    FROM rel
    GROUP BY ALL
    ORDER BY ALL
""").show()

# %% [markdown]
# ### 결과 저장

# %%
# save the final result to parquet

query = """
SELECT
  *,
  COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)") < "건축_면적(㎡)" - 1 AS 기존14,
  "연면적(㎡)" < "건축_면적(㎡)" - 1 AS 기존16,
  "건축_면적(㎡)" / "대지_면적(㎡)" * 100 AS 건폐율_재계산,
  ("건폐_율(%)" - 건폐율_재계산) AS 건폐율_차이,
  ABS(건폐율_차이) > 0.1 AS 기존19,
  "용적_률_산정_연면적(㎡)" / "대지_면적(㎡)" * 100 AS 용적률_재계산,
  ("용적_률(%)" - 용적률_재계산) AS 용적률_차이,
  ABS(용적률_차이) > 0.1 AS 기존20,
  "연면적(㎡)" < "용적_률_산정_연면적(㎡)" - 1 AS 신규01,
  ("건축_면적(㎡)" * 지상_층_수) + (COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)") * 지하_층_수) AS 연면적_상한,
  "연면적(㎡)" > 연면적_상한 + 1 AS 신규02,
  COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)", 0) < 0
  OR COALESCE("건축_면적(㎡)", "총괄_건축_면적(㎡)", 0) < 0
  OR COALESCE("연면적(㎡)", 0) < 0
  OR COALESCE("지상_층_수", 0) < 0
  OR COALESCE("지하_층_수", 0) < 0
  AS 신규03,
FROM
  rel_orig
"""
rel = con.sql(query)
rel.limit(20).show()
rel.to_parquet("../data/processed/주건축물_검증규칙.parquet")

# %%
