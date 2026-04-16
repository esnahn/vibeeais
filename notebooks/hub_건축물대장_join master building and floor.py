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

# %% [markdown]
# 각 테이블별 레코드 수 확인

# %%
# Create a dictionary to store the results
max_lengths = {}

# Iterate through the tables
for table in tables:
    table_name = table[0]
    # Fetch the table schema
    schema = con.execute(f"DESCRIBE {table_name}").fetchdf()
    # display(schema)
    # Filter text columns
    text_columns = schema[
        schema["column_type"].str.contains("VARCHAR|TEXT", case=False)
    ]["column_name"]
    # Initialize a dictionary for the current table
    max_lengths[table_name] = {}
    # Calculate max length for each text column
    for column in text_columns:
        max_length = con.execute(
            f"SELECT MAX(LENGTH({column})) FROM {table_name}"
        ).fetchone()[0]
        max_lengths[table_name][column] = max_length

# Display the results
for table_name, columns in max_lengths.items():
    print(f"Table: {table_name}")
    for column, length in columns.items():
        print(f"  Column: {column}, Max Length: {length}")
    print()

# %% [markdown]
# 각 테이블별, 컬럼별 최대 문자열 길이 검사
#
# 데이터와 함께 제공된 스키마와 비교하여 데이터에 존재하는 최대 길이를 넘지 않는지 검증
#
# (데이터 로딩이 잘못된 경우 최대 길이를 넘는 컬럼이 존재할 수 있어 확인함)

# %% [markdown]
# (내부DB에서 표제부와 폐말소대장이 하나의 테이블로 합쳐진 것은 개방데이터와는 무관함 확인)

# %%
# Query to count the occurrences
query = """
    SELECT
        COUNT(관리_건축물대장_PK) AS 관리_건축물대장_PK_count,
        COUNT(관리_상위_건축물대장_PK) AS 관리_상위_건축물대장_PK_count
    FROM 기본개요
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()
print(result)

# %%
# 1) 테이블 스키마에서 컬럼 이름을 가져오고
cols_df = con.execute("PRAGMA table_info('기본개요')").fetchdf()
cols = cols_df["name"].tolist()

# 2) 각 컬럼에 대한 COUNT 문을 생성
count_exprs = [f"COUNT({col}) AS {col}_count" for col in cols]

# 3) 최종 쿼리 조립
query = """
SELECT
    {exprs}
FROM 기본개요
""".format(exprs=",\n    ".join(count_exprs))

# 4) 실행 및 결과 확인
result = con.execute(query).fetchdf()
print(result.T)


# %% [markdown]
# (기본개요에서 각 컬럼별 널이 아닌 값 수를 셈)

# %%
# 1) 테이블 스키마에서 컬럼 이름을 가져오고
cols_df = con.execute("PRAGMA table_info('표제부')").fetchdf()
cols = cols_df["name"].tolist()

# 2) 각 컬럼에 대한 COUNT 문을 생성
# 표제부에는 컬럼명에 %가 들어간 경우가 있어, 오류가 발생함
# 이를 피하기 위해, 컬럼명을 따옴표로 감싸줌
count_exprs = [f'COUNT("{col}") AS "{col}_count"' for col in cols]

# 3) 최종 쿼리 조립
query = """
SELECT
    {exprs}
FROM 표제부
""".format(exprs=",\n    ".join(count_exprs))

# 4) 실행 및 결과 확인
result = con.execute(query).fetchdf()

with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    # Display the result with all rows and columns
    print(result.T)

# %% [markdown]
# (표제부에서 각 컬럼별 널이 아닌 값 수를 셈)
# (표제부는 컬럼명에 특수문자(%)가 들어간 컬럼이 있어, 그대로 사용하면 에러가 발생함. 따옴표로 감싸줘야 함)

# %%
# 기본개요와 표제부 조인

# # Drop the view if it already exists
# con.execute("DROP VIEW IF EXISTS 기본개요_표제부")

# Create a view for the joined data
create_view_query = """
SELECT
    기본개요.*,
    표제부.\"동_명\",
    표제부.\"주_부속_구분_코드\",
    표제부.\"주_부속_구분_코드_명\",
    표제부.\"대지_면적(㎡)\",
    표제부.\"건축_면적(㎡)\",
    표제부.\"연면적(㎡)\",
    표제부.\"용적_률_산정_연면적(㎡)\",
    표제부.\"구조_코드\",
    표제부.\"주_용도_코드\",
    표제부.\"지붕_코드\",
    표제부.\"세대_수(세대)\",
    표제부.\"가구_수(가구)\",
    표제부.\"높이(m)\",
    표제부.\"지상_층_수\",
    표제부.\"지하_층_수\",
    표제부.\"부속_건축물_수\",
    표제부.\"부속_건축물_면적(㎡)\",
    표제부.\"총_동_연면적(㎡)\",
    표제부.\"허가_일\",
    표제부.\"착공_일\",
    표제부.\"사용승인_일\",
    표제부.\"허가번호_년\",
    표제부.\"호_수(호)\",
    표제부.\"생성_일자\"
FROM 기본개요
INNER JOIN 표제부
ON 기본개요.관리_건축물대장_PK = 표제부.관리_건축물대장_PK
"""
기본개요_표제부 = con.sql(create_view_query)

# Verify the view creation by querying it
view_result = con.execute("SELECT * FROM 기본개요_표제부 LIMIT 5").fetchdf()
print(view_result)

# %% [markdown]
# 표제부에서 일부 컬럼만 사용
#
# 동_명
# 주_부속_구분_코드
# 주_부속_구분_코드_명
# 대지_면적(㎡)
# 건축_면적(㎡)
# 연면적(㎡)
# 용적_률_산정_연면적(㎡)
# 구조_코드
# 주_용도_코드
# 지붕_코드
# 세대_수(세대)
# 가구_수(가구)
# 높이(m)
# 지상_층_수
# 지하_층_수
# 부속_건축물_수
# 부속_건축물_면적(㎡)
# 총_동_연면적(㎡)
# 허가_일
# 착공_일
# 사용승인_일
# 허가번호_년
# 호_수(호)
# 생성_일자

# %%
row_count = con.execute("SELECT COUNT(*) FROM 기본개요_표제부").fetchone()[0]
print(f"Row count of 기본개요_표제부: {row_count}")

# %% [markdown]
# 표제부 테이블에 있는 모든 데이터가 기본개요와 잘 연계됨

# %%
# 1) 기본개요_표제부 컬럼 이름을 가져오고
# DuckDB에서는 PRAGMA table_info는 테이블이나 뷰에만 사용 가능하므로,
# 기본개요_표제부가 테이블/뷰가 아니라면, 쿼리 결과에서 컬럼명을 가져와야 합니다.
# 예시: LIMIT 0으로 쿼리 실행 후 컬럼명 추출

cols = con.execute("SELECT * FROM 기본개요_표제부 LIMIT 0").df().columns.tolist()

# 2) 각 컬럼에 대한 COUNT 문을 생성
# 표제부에는 컬럼명에 %가 들어간 경우가 있어, 오류가 발생함
# 이를 피하기 위해, 컬럼명을 따옴표로 감싸줌
count_exprs = [f'COUNT("{col}") AS "{col}"' for col in cols]

# 3) 최종 쿼리 조립
query = """
SELECT
    {exprs}
FROM 기본개요_표제부
""".format(exprs=",\n    ".join(count_exprs))

# 4) 실행 및 결과 확인
result = con.execute(query).fetchdf()

with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    # Display the result with all rows and columns
    print(result.T)

# %% [markdown]
# 기본개요와 잘 연계된 표제부에 기재된 컬럼별 데이터 기재 건수
#
# 총 8,027,067개 동 중 법정동 기재된 경우 8027063,
# 구조_코드 8006871,
# 주_용도_코드 7997938,
# 지붕_코드 7989535,
# 사용승인일 7401141 등
#
# 데이터가 충분히 기재된 컬럼만 데이터 정제 과정에 활용
#
#

# %%
# Query to count rows where 사용승인일 is not 8 characters long
query = """
SELECT COUNT(*) AS invalid_사용승인일_count
FROM 기본개요_표제부
WHERE LENGTH(사용승인_일) != 8
"""

# Execute the query and fetch the result
invalid_count = con.execute(query).fetchone()[0]
print(f"Number of rows where 사용승인일 is not 8 characters long: {invalid_count}")

# %% [markdown]
# 28819개 동은 사용승인일이 데이터 형식(8자리 숫자열)에 맞지 않게 기재되어 있음

# %%
# Query to count rows where 사용승인일 is larger than 20250229
query = """
SELECT COUNT(*) AS invalid_사용승인일_count
FROM 기본개요_표제부
WHERE 사용승인_일 > '20250229' and LENGTH(사용승인_일) = 8
"""

# Execute the query and fetch the result
invalid_count = con.execute(query).fetchone()[0]
print(f"Number of rows where 사용승인일 is larger than 20250229: {invalid_count}")

# %% [markdown]
# 54개 동은 사용승인일 데이터 기준 시점 이후로 기재되어 있음

# %%
# Query to fetch rows where 사용승인_일 is greater than '20250229'
query = """
SELECT *
FROM 기본개요_표제부
WHERE 사용승인_일 > '20250229' and LENGTH(사용승인_일) = 8
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(result)

# %% [markdown]
# 전라남도 여수시 여천중학교의 예를 들면, 사용승인일이 '21120323'로 기재되어 있는데, 착공일이 '20111212'인 점을 고려할 때 2011년 12월 모일의 오기로 의심되나, 모든 데이터를 이렇게 정확하게 파악할 수는 없음.

# %%
# Query to count rows where 사용승인일 is less than 19000101
query = """
SELECT COUNT(*) AS invalid_사용승인일_count
FROM 기본개요_표제부
WHERE 사용승인_일 < '19000101' and LENGTH(사용승인_일) = 8
"""

# Execute the query and fetch the result
invalid_count = con.execute(query).fetchone()[0]
print(f"Number of rows where 사용승인일 is less than 19000101: {invalid_count}")

# %% [markdown]
# 56477개 동은 사용승인일 데이터가 1900년 이전 시점으로 기재되어 있음

# %%
# Query to count rows where 사용승인일 is between 20241231 and 20250229
query = """
SELECT COUNT(*) AS invalid_사용승인일_count
FROM 기본개요_표제부
WHERE 사용승인_일 > '20241231' and 사용승인_일 <= '20250229' and LENGTH(사용승인_일) = 8
"""

# Execute the query and fetch the result
invalid_count = con.execute(query).fetchone()[0]
print(
    f"Number of rows where 사용승인일 is between 20241231 and 20250229: {invalid_count}"
)

# %% [markdown]
# 11585개 동은 사용승인일이 2024년 말 이후로, 2024년 말 기준 분석에서는 제외가 필요함

# %% [markdown]
# 위와 같은 여러 오류를 제외하여 사용승인일이 정상적으로 기재된 경우만 선별함

# %%
# Query to count rows where 사용승인일 is between 19000101 and 20241231
query = """
SELECT COUNT(*) AS valid_사용승인일_count
FROM 기본개요_표제부
WHERE 사용승인_일 > '19000101' and 사용승인_일 <= '20241231' and LENGTH(사용승인_일) = 8
"""

# Execute the query and fetch the result
valid_count = con.execute(query).fetchone()[0]
print(
    f"Number of rows where 사용승인일 is between 19000101 and 20241231: {valid_count}"
)

# %% [markdown]
# 1900년 이후 2024년 말까지 사용승인된 건축물 수는 7303104 동
#
# 1차로 이 건축물만 사용

# %%
# # Drop the view if it already exists
# con.execute("DROP VIEW IF EXISTS 기본개요_표제부_사용승인일_정상")

# Create a view with the specified conditions
create_view_query = """
SELECT *
FROM 기본개요_표제부
WHERE 사용승인_일 > '19000101'
    AND 사용승인_일 <= '20241231'
    AND LENGTH(사용승인_일) = 8
"""
기본개요_표제부_사용승인일_정상 = con.sql(create_view_query)

# Verify the view creation by querying it
view_result = con.execute(
    "SELECT * FROM 기본개요_표제부_사용승인일_정상 LIMIT 5"
).fetchdf()

with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(view_result)

# %%
# Query to fetch unique values for 구조_코드
query = """
SELECT 구조_코드, COUNT(*) AS count
FROM 기본개요_표제부_사용승인일_정상
GROUP BY 구조_코드
ORDER BY count DESC
"""

# Execute the query and fetch the result
unique_values_구조_코드 = con.execute(query).fetchdf()

# Display the result
print(unique_values_구조_코드)

# %% [markdown]
# 구조코드: 2자리 숫자열
#
# 구조코드 없는 경우 4022, 빈 문자열인 경우 3, 특수문자 1건
#
# 구조코드가 없으면 뺄 것인지?

# %%
# Query to fetch unique values for 주_용도_코드
query = """
SELECT 주_용도_코드, COUNT(*) AS count
FROM 기본개요_표제부_사용승인일_정상
GROUP BY 주_용도_코드
ORDER BY count DESC
"""

# Execute the query and fetch the result
unique_values_주_용도_코드 = con.execute(query).fetchdf()
# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    # Display the result with all rows and columns
    print(unique_values_주_용도_코드)

# %% [markdown]
# 주용도코드: 5자리 숫자열, 뒷 3자리 000: 대분류 의미
#
# 주용도코드 없는 경우 5171
#
# 구 4자리 용도 코드 (Z로 시작) 별도 처리 필요
#
# 5자리 숫자열에 맞지 않는 경우 존재(JY)
#
# 어차피 층별개요의 주용도코드 사용 예정

# %%
# Query to fetch unique values for 지붕_코드
query = """
SELECT 지붕_코드, COUNT(*) AS count
FROM 기본개요_표제부_사용승인일_정상
GROUP BY 지붕_코드
ORDER BY count DESC
"""

# Execute the query and fetch the result
unique_values_지붕_코드 = con.execute(query).fetchdf()
# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    # Display the result with all rows and columns
    print(unique_values_지붕_코드)

# %% [markdown]
# 지붕 코드: 2자리 숫자열
#
# 지붕코드 없는 경우 15733
#
# 구조코드가 없으면 뺄 것인지?

# %% [markdown]
# 층별개요 검증 시작

# %%
# # Drop the view if it already exists
# con.execute("DROP VIEW IF EXISTS 층별개요_필요컬럼")

# Create a view with the specified columns
create_view_query = """
SELECT
    관리_건축물대장_PK,
    대지_위치,
    건물_명,
    층_구분_코드,
    층_구분_코드_명,
    층_번호,
    층_번호_명,
    구조_코드,
    주_용도_코드,
    주_용도_코드_명,
    "면적(㎡)",
    주_부속_구분_코드,
    면적_제외_여부
FROM 층별개요
"""
층별개요_필요컬럼 = con.sql(create_view_query)

# Verify the view creation by querying it
view_result = con.execute("SELECT * FROM 층별개요_필요컬럼 LIMIT 5").fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(view_result)

# %%
# Query to count values of 면적_제외_여부
query = """
SELECT 면적_제외_여부, COUNT(*) AS count
FROM 층별개요_필요컬럼
GROUP BY 면적_제외_여부
ORDER BY count DESC
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(result)

# %% [markdown]
# 면적 제외 여부는 기재되지 않은 경우가 12816630,
# 미제외(0) 7954285,
# 제외(1) 277800
#
# 기재되지 않은 경우 미제외로 판단하여 진행, 제외로 기재된 경우만 제외함

# %%
# Query to count values of 주_용도_코드
query = """
SELECT 주_용도_코드, COUNT(*) AS count
FROM 층별개요_필요컬럼
GROUP BY 주_용도_코드
ORDER BY count DESC
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(result)

# %% [markdown]
# 주용도코드: 5자리 숫자열, 소분류
#
# 주용도코드 없는 경우 25487
#
# 4자리 코드 (b, k, Z로 시작) 별도 처리 필요
#
# 5자리 숫자열에 맞지 않는 경우 존재(창, 홀, 점)

# %%
# Query to count rows where 면적(㎡) is less than 0
query = """
SELECT COUNT(*) AS negative_area_count
FROM 층별개요_필요컬럼
WHERE "면적(㎡)" < 0
"""

# Execute the query and fetch the result
negative_area_count = con.execute(query).fetchone()[0]
print(f"Number of rows where 면적(㎡) is less than 0: {negative_area_count}")

# %% [markdown]
# 면적이 0보다 작은 경우 존재. 제외 필요

# %%
# Query to fetch the "면적(㎡)" column
query = """
SELECT "면적(㎡)"
FROM 층별개요_필요컬럼
WHERE "면적(㎡)" IS NOT NULL
"""

# Fetch the data
area_data = con.execute(query).fetchdf()

# Calculate statistics
max_value = area_data["면적(㎡)"].max()
mean_value = area_data["면적(㎡)"].mean()
percentile_1m = area_data["면적(㎡)"].quantile(0.999999)  # 1 - 1/1,000,000
percentile_100k = area_data["면적(㎡)"].quantile(0.99999)
percentile_10k = area_data["면적(㎡)"].quantile(0.9999)
percentile_1k = area_data["면적(㎡)"].quantile(0.999)  # 1 - 1/1,000
percentile_1h = area_data["면적(㎡)"].quantile(0.99)  # 1 - 1/100

# Display the results
print(f"Max value:           {max_value: 15.2f}")
print(f"Mean value:          {mean_value: 15.2f}")
print(f"0.999999 percentile: {percentile_1m: 15.2f}")
print(f"0.99999 percentile:  {percentile_100k: 15.2f}")
print(f"0.9999 percentile:   {percentile_10k: 15.2f}")
print(f"0.999 percentile:    {percentile_1k: 15.2f}")
print(f"0.99 percentile:     {percentile_1h: 15.2f}")


# %%
# Query to fetch data sorted by 면적(㎡) in descending order
query = """
SELECT *
FROM 층별개요_필요컬럼
ORDER BY "면적(㎡)" DESC
LIMIT 100
"""

# Execute the query and fetch the result
sorted_data = con.execute(query).fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
    "display.float_format",
    "{:,.2f}".format,  # Format floats as non-scientific with 2 decimal places
):
    print(sorted_data)

# %% [markdown]
# 지하 3층 면적이 188,957,549 으로 기재된 경기 김포 한강신도시 롯데캐슬의 경우, 대지 면적이 65,940 에 불과하여 층별개요에 기재된 면적은 현실적으로 불가능한 수치
#
# 이러한 오류 사례를 거르기 위하여 개별 사례를 검토 후 층별 면적이 500,000 이상인 경우 (0.001% 미만 사례) 제외하도록 하였다.
#

# %%
# # Drop the view if it already exists
# con.execute("DROP VIEW IF EXISTS 층별개요_필터링")

# Create a view with the specified conditions
create_view_query = """
SELECT *
FROM 층별개요_필요컬럼
WHERE (면적_제외_여부 IS NULL OR 면적_제외_여부 != 1)
  AND "면적(㎡)" > 0
  AND "면적(㎡)" <= 500000
"""
층별개요_필터링 = con.sql(create_view_query)

# Verify the view creation by querying it
view_result = con.execute("SELECT * FROM 층별개요_필터링 LIMIT 5").fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(view_result)

# %% [markdown]
#

# %%
# # Drop the view if it already exists
# con.execute("DROP VIEW IF EXISTS 기본개요_표제부_층별개요_조인")

# Create a view for the joined data
create_view_query = """
SELECT
    a.*,
    b.층_구분_코드,
    b.층_구분_코드_명,
    b.층_번호,
    b.층_번호_명,
    b.구조_코드 AS 층_구조_코드,
    b.주_용도_코드 AS 층_주_용도_코드,
    b.주_용도_코드_명 AS 층_주_용도_코드_명,
    b."면적(㎡)" AS 층_면적,
    b.주_부속_구분_코드 AS 층_주_부속_구분_코드,
    b.면적_제외_여부 AS 층_면적_제외_여부
FROM 기본개요_표제부_사용승인일_정상 a
INNER JOIN 층별개요_필터링 b
ON a.관리_건축물대장_PK = b.관리_건축물대장_PK
"""
기본개요_표제부_층별개요_조인 = con.sql(create_view_query)

# Verify the view creation by querying it
view_result = con.execute(
    "SELECT * FROM 기본개요_표제부_층별개요_조인 LIMIT 5"
).fetchdf()

# Display the result
with pd.option_context(
    "display.max_rows",
    None,
    "display.max_columns",
    None,
):
    print(view_result)

# %%
# Query to count rows in 기본개요_표제부_층별개요_조인
query = """
SELECT COUNT(*) AS row_count
FROM 기본개요_표제부_층별개요_조인
"""

# Execute the query and fetch the result
row_count = con.execute(query).fetchone()[0]
print(f"Row count of 기본개요_표제부_층별개요_조인: {row_count}")

# %%
# Query to calculate the sum of 층_면적
query = """
SELECT SUM("층_면적") AS total_floor_area
FROM 기본개요_표제부_층별개요_조인
"""

# Execute the query and fetch the result
total_floor_area = con.execute(query).fetchone()[0]
print(f"Total floor area: {total_floor_area:,.0f}")

# %% [markdown]
# 분석 대상 건축물 층별 면적 합계: 4,372,971,283
#
# 2024년 건축물 현황 시도별 건축물 연면적 합계: 4,314,987,939
#
# 일부 오차가 있으며, 통계 집계 기준이나 오류 정정 규칙 등 차이로 인한 것일 수 있음
#
# 건축물 현황은 부속 건축물을 집계하지 않으나, 본 연구에서는 재조달원가의 정확한 산정을 위하여 부속 건축물 면적도 포함하였음

# %%
# Query to calculate the total area by 층_주_용도_코드 and 층_주_용도_코드명
query = """
SELECT
    층_주_용도_코드,
    층_주_용도_코드_명,
    SUM("층_면적") AS total_area
FROM 기본개요_표제부_층별개요_조인
GROUP BY 층_주_용도_코드, 층_주_용도_코드_명
ORDER BY total_area DESC
"""

# Execute the query and fetch the result
area_summary = con.execute(query).fetchdf()

# Save the result to a CSV file
output_file = data_root / "floor_usage_area_summary.csv"
area_summary.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Summary saved to {output_file}")

# %%
# Query to calculate the total area by 시군구_코드
query = """
SELECT
    시군구_코드,
    SUM("층_면적") AS total_area
FROM 기본개요_표제부_층별개요_조인
GROUP BY 시군구_코드
ORDER BY total_area DESC
"""

# Execute the query and fetch the result
area_summary = con.execute(query).fetchdf()

# Load the 시군구 코드 to 시군구명 mapping from the CSV file
sgg_mapping = pd.read_csv(
    "../data/processed/code_sgg.csv", dtype=str, encoding="utf-8-sig"
)

# Merge the area summary with the 시군구명 mapping
area_summary = area_summary.merge(
    sgg_mapping, how="left", left_on="시군구_코드", right_on="시군구코드"
)

# Drop the redundant 시군구코드 column after the merge
area_summary = area_summary.drop(columns=["시군구코드"])
# Save the result to a CSV file
output_file = data_root / "floor_area_by_region_summary.csv"
area_summary.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Summary saved to {output_file}")

# %%
# Query to calculate the total area by 구조_코드
query = """
SELECT
    구조_코드,
    SUM("층_면적") AS total_area
FROM 기본개요_표제부_층별개요_조인
GROUP BY 구조_코드
ORDER BY total_area DESC
"""

# Execute the query and fetch the result
area_by_structure_code = con.execute(query).fetchdf()

# Save the result to a CSV file
output_file = data_root / "area_by_structure_code.csv"
area_by_structure_code.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Result saved to {output_file}")


# %%
# Query to calculate the total area by 지붕_코드
query = """
SELECT
    지붕_코드,
    SUM("층_면적") AS total_area
FROM 기본개요_표제부_층별개요_조인
GROUP BY 지붕_코드
ORDER BY total_area DESC
"""

# Execute the query and fetch the result
area_by_roof_code = con.execute(query).fetchdf()

# Save the result to a CSV file
output_file = data_root / "area_by_roof_code.csv"
area_by_roof_code.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Result saved to {output_file}")

# %%
# Define the output file path for the Parquet file
output_parquet_file = data_root / "기본개요_표제부_층별개요_조인.parquet"

# Query to fetch all data from the view
query = """
SELECT *
FROM 기본개요_표제부_층별개요_조인
"""

# Execute the query and fetch the result as a DataFrame
data = con.execute(query).fetchdf()

# Save the DataFrame to a Parquet file
data.to_parquet(output_parquet_file, index=False)

print(f"Data saved to {output_parquet_file}")

# %%
# # List of intermediate views to drop
# views_to_drop = [
#     "기본개요_표제부",
#     "기본개요_표제부_사용승인일_정상",
#     "기본개요_표제부_층별개요_조인",
#     "층별개요_필요컬럼",
#     "층별개요_필터링",
# ]

# # Drop each view
# for view in views_to_drop:
#     con.execute(f"DROP VIEW IF EXISTS {view}")
#     print(f"Dropped view: {view}")

# %%
