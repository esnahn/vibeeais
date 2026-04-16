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
import matplotlib.pyplot as plt
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

# %%
# Create a temporary view with the selected columns
표제부_필터링 = con.sql("""
    SELECT
        "주_용도_코드",
        "주_용도_코드_명",
        "기타_용도"
    FROM 표제부
""")

# %%
# Display the first few rows
표제부_필터링_df = con.execute("SELECT * FROM 표제부_필터링 LIMIT 10000").fetchdf()
# Process the DataFrame
filtered_sampled_df = (
    표제부_필터링_df.drop_duplicates()
    .sort_values(by=["주_용도_코드"])
    .groupby("주_용도_코드")
    .filter(lambda x: len(x) >= 100)  # Drop groups with less rows
    .reset_index(drop=True)
    .groupby("주_용도_코드")
    .apply(lambda x: x.sample(n=10, replace=False, random_state=42))
    .reset_index(drop=True)
)

# Display the processed DataFrame
display(filtered_sampled_df)

# Save the processed DataFrame to a CSV file in the results directory
results_dir = Path("../results")
filtered_sampled_df.to_csv(
    results_dir / "표제부_용도_기재내용_예시.csv", index=False, encoding="utf-8-sig"
)

# %%
# Perform value count using SQL
value_counts_sql = con.sql("""
    SELECT
        "주_용도_코드",
        "주_용도_코드_명",
        "기타_용도",
        COUNT(*) AS "Count"
    FROM 표제부_필터링
    GROUP BY "주_용도_코드", "주_용도_코드_명", "기타_용도"
    ORDER BY "Count" DESC
""")

# Fetch and display the result
value_counts_df = value_counts_sql.fetchdf()
display(value_counts_df.head(30))
# Save the top 30 rows to a CSV file
value_counts_df.head(30).to_csv(
    results_dir / "표제부_용도_기재내용_top30.csv", index=False, encoding="utf-8-sig"
)

# %%
# Calculate the sum of counts grouped by 주_용도_코드
sum_by_code_sql = con.sql("""
    SELECT
        "주_용도_코드",
        "주_용도_코드_명",
        SUM("Count") AS "Total Count"
    FROM value_counts_sql
    GROUP BY "주_용도_코드", "주_용도_코드_명"
    ORDER BY "Total Count" DESC
""")

# Fetch and display the result
sum_by_code_df = sum_by_code_sql.fetchdf().astype({"Total Count": "int"})
display(sum_by_code_df)

# Save the result to a CSV file
sum_by_code_df.to_csv(
    results_dir / "동_주_용도_코드별_합계.csv", index=False, encoding="utf-8-sig"
)

# %%

# Apply the custom style
plt.style.use("./auri.mplstyle")

head_df = sum_by_code_df.head(20)

# Plot the bar chart
plt.bar(
    (head_df["주_용도_코드"] + " " + head_df["주_용도_코드_명"]).fillna("(미기재)"),
    head_df["Total Count"],
)
for i, value in enumerate(head_df["Total Count"]):
    if i in [0]:
        plt.text(
            i,
            value + 40000,  # Move text slightly up
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=6,
            # rotation=45,
        )
    if i in range(1, 21):
        plt.text(
            i + 0.03,  # Move text slightly to the right
            value + 100000,  # Move text slightly up
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=6,
            rotation=90,
        )
plt.xlabel("주 용도 코드")
plt.ylabel("동 수")
plt.title("주 용도 코드별 동 수")
plt.ticklabel_format(
    style="plain", axis="y"
)  # Use plain style for full numbers on y-axis
plt.gca().yaxis.set_major_formatter(
    plt.FuncFormatter(lambda x, _: f"{int(x):,}")
)  # Format y-axis with commas
plt.xticks(rotation=90, ha="center", va="bottom", fontsize=6)
for lbl in plt.gca().get_xticklabels():
    lbl.set_y(-0.40)
# Save the plot to a file
plt.savefig(results_dir / "동_주_용도_코드별_동_수_막대그래프.png")
# Show the plot
plt.show()

# %%
value_counts_df

# %%
# Create a temporary view to split the '기타_용도' column into arrays using a regular expression
value_counts_split = con.sql("""
    SELECT
        *,
        regexp_split_to_array(lower("기타_용도"), '[^\\p{L}\\d]+') AS "기타_용도_분리"
    FROM value_counts_sql
""")

# Fetch and display the result
value_counts_split.fetchdf()

# %%
# Create a temporary view to split the '기타_용도' column into rows using a regular expression
value_counts_split_table = con.sql("""
    SELECT
        *,
        regexp_split_to_table(lower("기타_용도"), '[^\\p{L}\\d]+') AS "기타_용도_분리"
    FROM value_counts_sql
""")

# Fetch and display the result
value_counts_split_table.fetchdf()

# %%
# Group by 주_용도_코드, 주_용도_코드_명, and 기타_용도_분리 and calculate the sum of Count
sum_by_words = con.sql("""
    SELECT
        "기타_용도_분리",
        SUM("Count") AS "Total Count"
    FROM value_counts_split_table
    GROUP BY "기타_용도_분리"
    ORDER BY "Total Count" DESC
""")
# Drop rows where "기타_용도_분리" is empty
sum_by_words = sum_by_words.filter(
    "기타_용도_분리 IS NOT NULL AND 기타_용도_분리 != ''"
)
# Fetch and display the result
sum_by_words_df = sum_by_words.fetchdf().astype({"Total Count": "int"})
display(sum_by_words_df.head(30))

# Save the result to a CSV file
sum_by_words_df.to_csv(
    results_dir / "동_기타_용도_단어별_합계_중복포함.csv",
    index=False,
    encoding="utf-8-sig",
)

# %%
value_counts_split_table.fetchdf()

# %%
# Select one distinct record for each 기타_용도_분리
distinct_record_sql = con.sql("""
    SELECT DISTINCT ON ("기타_용도_분리")
        "기타_용도_분리",
        "기타_용도",
        "주_용도_코드",
        "주_용도_코드_명"
    FROM value_counts_split_table
    WHERE "기타_용도_분리" IS NOT NULL AND "기타_용도_분리" != ''
    ORDER BY "Count" DESC
""")

# Fetch and display the result
distinct_record_df = distinct_record_sql.fetchdf()
display(distinct_record_df)


# %%
# top 30 from grouped_sum
top_words_df = sum_by_words_df.head(30)

# join it with distinct_record_sql in sql
usage_sample_sql = con.sql("""
    SELECT
        t."기타_용도_분리",
        t."Total Count",
        d."기타_용도",
        d."주_용도_코드",
        d."주_용도_코드_명"
    FROM top_words_df t
    JOIN distinct_record_sql d
    ON t."기타_용도_분리" = d."기타_용도_분리"
    ORDER BY t."Total Count" DESC
""")
# Fetch and display the result
usage_sample_df = usage_sample_sql.fetchdf()
display(usage_sample_df)

# Save the result to a CSV file
usage_sample_df.to_csv(
    results_dir / "동_기타_용도_단어별_합계_중복포함_예시.csv",
    index=False,
    encoding="utf-8-sig",
)

# %%
value_counts_split_table

# %%
# Create a temporary view grouped_df by grouping and summing the counts
sum_by_code_words_sql = con.sql("""
    SELECT
        "주_용도_코드",
        FIRST("주_용도_코드_명") AS "주_용도_코드_명",
        "기타_용도_분리",
        SUM("Count") AS "Total Count"
    FROM value_counts_split_table
    WHERE "기타_용도_분리" IS NOT NULL AND "기타_용도_분리" != ''
    GROUP BY "주_용도_코드", "기타_용도_분리"
    ORDER BY "Total Count" DESC;
""")

# Fetch and display the result
sum_by_code_words_df = sum_by_code_words_sql.fetchdf().astype({"Total Count": "int"})
display(sum_by_code_words_df)

# Save the result to a CSV file
sum_by_code_words_df.to_csv(
    results_dir / "동_주_용도_코드별_기타_용도_단어별_합계.csv",
    index=False,
    encoding="utf-8-sig",
)

# %%
# Pivot the data
pivot_df = sum_by_code_words_df.pivot_table(
    index="주_용도_코드",
    columns="기타_용도_분리",
    values="Total Count",
    aggfunc="sum",
    fill_value=0,
)
# Limit the pivot table to the top 30 rows and columns based on Total Count
top_columns = (
    sum_by_code_words_df.groupby("기타_용도_분리")["Total Count"]
    .sum()
    .nlargest(10)
    .index
)

pivot_df = pivot_df.loc[:, top_columns]

# Sort the pivot table by the index
pivot_df = pivot_df.sort_index()
pivot_df = pivot_df.loc[
    :,
    pivot_df.sum(axis=0).sort_values(ascending=False).index,
]

# select rows with sum > 1000
pivot_df = pivot_df.loc[pivot_df.sum(axis=1) > 1000]

# Add the 주_용도_코드_명 as a label column
pivot_df.insert(
    0,
    "주_용도_코드_명",
    sum_by_code_words_df.drop_duplicates("주_용도_코드").set_index("주_용도_코드")[
        "주_용도_코드_명"
    ],
)

# Display the pivot table
display(pivot_df)

# Save the pivot table to a CSV file
pivot_df.to_csv(
    results_dir / "동_주_용도_코드별_상위_단어별_빈도_pivot.csv",
    index=True,
    encoding="utf-8-sig",
)

# %%

# Apply the custom style
plt.style.use("./auri.mplstyle")

head_df = sum_by_code_df.head(20)

# Plot the bar chart
plt.bar(
    (head_df["주_용도_코드"] + " " + head_df["주_용도_코드_명"]).fillna("(미기재)"),
    head_df["Total Count"],
)
for i, value in enumerate(head_df["Total Count"]):
    if i in [0]:
        plt.text(
            i,
            value + 40000,  # Move text slightly up
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=6,
            # rotation=45,
        )
    if i in range(1, 21):
        plt.text(
            i + 0.03,  # Move text slightly to the right
            value + 100000,  # Move text slightly up
            f"{value:,}",
            ha="center",
            va="bottom",
            fontsize=6,
            rotation=90,
        )
plt.xlabel("주 용도 코드")
plt.ylabel("동 수")
plt.title("주 용도 코드별 동 수")
plt.ticklabel_format(
    style="plain", axis="y"
)  # Use plain style for full numbers on y-axis
plt.gca().yaxis.set_major_formatter(
    plt.FuncFormatter(lambda x, _: f"{int(x):,}")
)  # Format y-axis with commas
plt.xticks(rotation=90, ha="center", va="bottom", fontsize=6)
for lbl in plt.gca().get_xticklabels():
    lbl.set_y(-0.40)
# Save the plot to a file
plt.savefig(results_dir / "동_주_용도_코드별_동_수_막대그래프.png")
# Show the plot
plt.show()
