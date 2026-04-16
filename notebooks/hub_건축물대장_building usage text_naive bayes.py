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
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB

data_root = Path("D:\\데이터\\건축데이터 건축허브 개방데이터")
path_DB = data_root / "건축물대장_2025년_02월.db"

results_dir = Path("../results")

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
    # print(con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf())
    print(con.sql(f"SELECT * FROM {table_name} LIMIT 5"))
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
    WHERE "기타_용도" IS NOT NULL AND "기타_용도" != ''
        AND "주_용도_코드_명" IS NOT NULL AND "주_용도_코드_명" != ''
""")
# Count the records in the filtered view
con.sql("SELECT COUNT(*) FROM 표제부_필터링").fetchone()[0]

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
display(value_counts_df.head(5))
# Save the top 30 rows to a CSV file
# value_counts_df.head(30).to_csv(
#     results_dir / "표제부_용도_기재내용_top30.csv", index=False, encoding="utf-8-sig"
# )

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
# Group by valid (5 digit) '주_용도_코드',
# sum the 'Count' column, and filter where the sum is >= 100
class_counts_sql = con.sql("""
    SELECT
        "주_용도_코드",
        "주_용도_코드_명",
        SUM("Count") AS "Total_Count"
    FROM value_counts_sql
    WHERE LENGTH("주_용도_코드") = 5 AND "주_용도_코드" ~ '^\\d+$'
    GROUP BY "주_용도_코드", "주_용도_코드_명"
    HAVING SUM("Count") >= 100
    ORDER BY "Total_Count" DESC
""")

# Fetch and display the result
class_counts_df = class_counts_sql.fetchdf().astype({"Total_Count": int})
display(class_counts_df)
# Save to a CSV file
class_counts_df.to_csv(
    results_dir / "naive_bayes_클래스별_문서빈도.csv",
    index=False,
    encoding="utf-8-sig",
)

# %%
# Group by '기타_용도_분리',
# sum the 'Count' column, and filter where the sum is >= 100
word_counts_sql = con.sql("""
    SELECT
        "기타_용도_분리",
        SUM("Count") AS "Total_Count"
    FROM value_counts_split_table
    WHERE "기타_용도_분리" IS NOT NULL AND "기타_용도_분리" != ''
    GROUP BY "기타_용도_분리"
    HAVING SUM("Count") >= 100
    ORDER BY "Total_Count" DESC
""")

# Fetch and display the result
word_counts_df = word_counts_sql.fetchdf().astype({"Total_Count": int})
display(word_counts_df)
# Save to a CSV file
word_counts_df.to_csv(
    results_dir / "naive_bayes_단어별_문서빈도.csv",
    index=False,
    encoding="utf-8-sig",
)

# %%
# Construct training data with '주_용도_코드' and '기타_용도_분리'
# Filter with class_counts_sql and word_counts_sql
train_data_sql = con.sql("""
    SELECT
        t."주_용도_코드" AS class,
        t."기타_용도_분리" AS word,
        t."Count" AS weight,
    FROM value_counts_split_table t
    JOIN class_counts_sql c
        ON t."주_용도_코드" = c."주_용도_코드"
    JOIN word_counts_sql w
        ON t."기타_용도_분리" = w."기타_용도_분리"
    WHERE t.Count >= 100
    ORDER BY t."Count" DESC
""")
train_df = train_data_sql.fetchdf()
train_df

# %%


# 2. Vectorize & train

# The multinomial Naive Bayes classifier is suitable for classification with
# discrete features (e.g., word counts for text classification). The multinomial
# distribution normally requires integer feature counts. However, in practice,
# fractional counts such as tf-idf may also work.

# word is already a word, but no harm from count vectorization
# regex does not support unicode, so we need to customize the pattern
vectorizer = CountVectorizer(token_pattern=r"[\\w\\d가-힣]+")
X = vectorizer.fit_transform(train_df["word"])
y = train_df["class"]
weights = train_df["weight"]
clf = MultinomialNB(alpha=1.0)
clf.fit(X, y, sample_weight=weights)

# %%
vectorizer.vocabulary_

# %%
# test the classifier
test_text = "도시형생활주택(단지형다세대주택)다세대주택(8세대)외 1"
X_test = vectorizer.transform([test_text])
X_test.toarray()

# %%
clf.predict(X_test)[0]


# %%
# 3. Register a prediction UDF
def nb_predict(text: str) -> str:
    x = vectorizer.transform([text])
    return clf.predict(x)[0]


# Remove the function if it already exists
try:
    con.remove_function("nb_predict")
except Exception as e:
    if "No function by the name of" not in str(e):
        raise

# Register the function

con.create_function("nb_predict", nb_predict)

# %%
preds = con.sql("""
  SELECT
    *,
    nb_predict("기타_용도") AS predicted_label,
    CASE
      WHEN "주_용도_코드" = predicted_label THEN 1
      ELSE 0
    END AS is_correct
  FROM value_counts_sql
""")
preds_label = con.sql("""
  SELECT
    preds.*,
    class_counts_sql."주_용도_코드_명" AS "predicted_label_name",
  FROM preds
  LEFT JOIN class_counts_sql
    ON preds."predicted_label" = class_counts_sql."주_용도_코드"
""")
preds_df = preds_label.fetchdf()
preds_df

# %%
# Save preds to a CSV file
preds_df.to_csv(
    results_dir / "naive_bayes_예측결과.csv",
    index=False,
    encoding="utf-8-sig",
)
