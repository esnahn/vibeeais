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
import duckdb

# Load the CSV file into DuckDB
con = duckdb.connect()
results = con.sql("SELECT * FROM '../results/층_naive_bayes_예측결과.csv'")
# Display the first 5 rows of the results
display(results.df().head())

# %%
# Calculate the overall accuracy
overall_accuracy = con.sql("""
    SELECT
        SUM(Count * is_correct) / SUM(Count) AS overall_accuracy
    FROM results
""")
print("Overall Accuracy: {:.2f}%".format(overall_accuracy.df().iloc[0, 0] * 100))

# calculate the accuracy by 주_용도_코드
accuracy = con.sql("""
    SELECT
        주_용도_코드,
        주_용도_코드_명,
        SUM(Count) AS total_count,
        SUM(Count * is_correct) AS correct_count,
        SUM(Count * is_correct) / SUM(Count) AS accuracy
    FROM results
    WHERE LENGTH("주_용도_코드") = 5 AND "주_용도_코드" ~ '^\\d+$'
    GROUP BY 주_용도_코드, 주_용도_코드_명
    HAVING SUM(Count) >= 100
    ORDER BY SUM(Count) DESC
""")
accuracy_df = (
    accuracy.df()
    .astype({"total_count": "int", "correct_count": "int"})
    .head(20)
    .sort_values(by="주_용도_코드", ascending=True)
)
accuracy_df["accuracy"] = accuracy_df["accuracy"].apply(
    lambda x: "{:.2f}%".format(x * 100)
)
display(accuracy_df)
# Save the accuracy results to a CSV file
accuracy_df.to_csv(
    "../results/층_naive_bayes_accuracy.csv", index=False, encoding="utf-8-sig"
)
accuracy.df().astype({"total_count": "int", "correct_count": "int"}).to_csv(
    "../results/층_naive_bayes_accuracy_orig.csv", index=False, encoding="utf-8-sig"
)

# %%
# # top and bottom 5 by accuracy
# top_5_accuracy = accuracy.df().nlargest(5, "accuracy")
# bottom_5_accuracy = accuracy.df().query("accuracy > 0").nsmallest(5, "accuracy")
# print("Top 5 Accuracy:")
# display(top_5_accuracy)
# print("Bottom 5 Accuracy:")
# display(bottom_5_accuracy)

# %%
# Calculate the average Count for is_correct values 0 and 1 grouped by 주_용도_코드 and 주_용도_코드_명
average_count = con.sql("""
	SELECT
		주_용도_코드,
		주_용도_코드_명,
		AVG(CASE WHEN is_correct = 1 THEN Count ELSE NULL END) AS avg_count_correct,
		AVG(CASE WHEN is_correct = 0 THEN Count ELSE NULL END) AS avg_count_incorrect,
	FROM results
    WHERE LENGTH("주_용도_코드") = 5 AND "주_용도_코드" ~ '^\\d+$'
	GROUP BY 주_용도_코드, 주_용도_코드_명
    HAVING SUM(Count) >= 100000
    ORDER BY 주_용도_코드
""")
average_count_df = average_count.df()
# format the average counts to 2 decimal places
average_count_df["avg_count_correct"] = average_count_df["avg_count_correct"].apply(
    lambda x: "{:.2f}".format(x)
)
average_count_df["avg_count_incorrect"] = average_count_df["avg_count_incorrect"].apply(
    lambda x: "{:.2f}".format(x)
)
display(average_count_df)
# Save the average counts to a CSV file
average_count_df.to_csv(
    "../results/층_naive_bayes_average_count.csv", index=False, encoding="utf-8-sig"
)

# %%
