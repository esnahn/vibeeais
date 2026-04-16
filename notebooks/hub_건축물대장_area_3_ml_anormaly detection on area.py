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
import time
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.kernel_approximation import Nystroem
from sklearn.linear_model import SGDOneClassSVM
from sklearn.pipeline import make_pipeline
from tabulate import tabulate

# from sklearn.neighbors import LocalOutlierFactor


plt.style.use("../auri.mplstyle")

pd.options.display.unicode.east_asian_width = True

# %%
parquet_path = Path("../data/processed/주건축물_검증규칙.parquet")
df_orig = pd.read_parquet(parquet_path)
print(f"Loaded shape: {df_orig.shape}")
df_orig.head()

# %%
con = duckdb.connect(database=":memory:")
query = """
SELECT
  *,
FROM
  df_orig
WHERE
  COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)", 0)
    >= COALESCE("건축_면적(㎡)", 0)                             -- not 기존14
  AND COALESCE("연면적(㎡)", 0) >= COALESCE("건축_면적(㎡)", 0) -- not 기존16
  AND COALESCE("연면적(㎡)", 0)                                 -- not 신규01
        >= COALESCE("용적_률_산정_연면적(㎡)", 0)
  AND COALESCE("대지_면적(㎡)", "총괄_대지_면적(㎡)", 0) >= 0   -- not 신규03
  AND COALESCE("건축_면적(㎡)", "총괄_건축_면적(㎡)", 0) >= 0
  AND COALESCE("연면적(㎡)", 0) >= 0
  AND COALESCE("지상_층_수", 0) >= 0
  AND COALESCE("지하_층_수", 0) >= 0
;
"""
rel = con.sql(query)
con.sql("SELECT COUNT(*) FROM rel").show()

# 검증규칙 확실히 통과한 건축물만 선택
df_filtered = rel.df()
with pd.option_context("display.max_columns", None):
    print(tabulate(df_filtered.head(), headers="keys", tablefmt="psql"))

# %%
df_filtered.shape

# %%
df_filtered.dtypes

# %% [markdown]
# ### 무차원 변수 산출
#
# 건축물의 규모에 비례하지 않는(무차원) 개별 건축물의 특성을 반영하는 변수를 산출하기 위하여 면적 관련 변수의 차원 분석을 시행.
#
# | 이름                 | 차원         |
# | -------------------- | ------------ |
# | 대지면적             | [면적]       |
# | 건축면적             | [면적]       |
# | 연면적               | [면적][층수] |
# | 용적률 산정용 연면적 | [면적][층수] |
# | 지상 층수            | [층수]       |
#
# <!-- | 건폐율               | [면적]/[면적] = [비율]             |
# | 용적률               | [면적][층수]/[면적] = [비율][층수] | -->
# <!-- | 지하 층수            | [층수]                             | -->
#
# 물리학의 차원분석은 길이, 질량 등을 다루며, 개수, 비율 등은 무차원수로 보나, 건축물 대상 차원분석에서는 건축법에 따라 건축물의 특성을 통제하는 변수인 면적, 층수, 비율 등을 별도의 차원으로 두었음.
#
# 1차적으로 비율 변수를 산출한 후, 각 비율 변수의 상관관계를 검토한 후 비례 관계가 나타나지 않는 무차원 변수를 최종 산출하고, 클러스터링 및 기계학습에 활용.
#
# | 이름            | 정의                          | 차원                               |
# | --------------- | ----------------------------- | ---------------------------------- |
# | 건폐율 (재산출) | 건축면적/대지면적             | [면적]/[면적] = [비율]             |
# | 용적률 (재산출) | 용적률 산정용 연면적/대지면적 | [면적][층수]/[면적] = [비율][층수] |
# | 지상 유효층수   | 용적률 산정용 연면적/건축면적 | [면적][층수]/[면적] = [비율][층수] |
# | 용적 산정률     | 용적률 산정용 연면적/연면적   | [면적][층수]/[면적][층수] = [비율] |
#
# <!-- | 실질 용적률     | 연면적/대지면적               | [면적][층수]/[면적] = [비율][층수] |
# | 유효층수        | 연면적/건축면적               | [면적][층수]/[면적] = [비율][층수] | -->
#
# 용적률, 유효층수 등 연면적이 적용된 비율 변수를 층수로 나누어 층별 비율 변수를 도출.
#
# | 이름            | 정의                                   | 차원                              |
# | --------------- | -------------------------------------- | --------------------------------- |
# | 지상층별 건폐율 | 용적률 산정용 연면적/지상층수/대지면적 | [면적][층수]/층수/[면적] = [비율] |
# | 지상층별 충만률 | 용적률 산정용 연면적/지상층수/건축면적 | [면적][층수]/층수/[면적] = [비율] |
#

# %%
# 우선 COALESCE 적용
대지면적 = df_filtered["대지_면적(㎡)"].fillna(df_filtered["총괄_대지_면적(㎡)"])
# 건축면적 = df_filtered["건축_면적(㎡)"].fillna(df_filtered["총괄_건축_면적(㎡)"])
지상층수 = df_filtered["지상_층_수"].fillna(0)
지하층수 = df_filtered["지하_층_수"].fillna(0)
총층수 = 지상층수 + 지하층수

df_dimensionless = pd.DataFrame(
    {
        "관리_건축물대장_PK": df_filtered["관리_건축물대장_PK"],
        "대지면적": 대지면적,
        "건축면적": df_filtered["건축_면적(㎡)"],
        "연면적": df_filtered["연면적(㎡)"],
        "용적률_산정용_연면적": df_filtered["용적_률_산정_연면적(㎡)"],
        "지상층수": 지상층수,
        "건폐율": df_filtered["건축_면적(㎡)"] / 대지면적,
        "용적률": df_filtered["용적_률_산정_연면적(㎡)"] / 대지면적,
        # "실질_용적률": df_filtered["연면적(㎡)"] / 대지면적,
        # "유효층수": df_filtered["연면적(㎡)"] / df_filtered["건축_면적(㎡)"],
        "지상_유효층수": df_filtered["용적_률_산정_연면적(㎡)"]
        / df_filtered["건축_면적(㎡)"],
        "용적_산정률": df_filtered["용적_률_산정_연면적(㎡)"]
        / df_filtered["연면적(㎡)"],
        # "층별_건폐율": df_filtered["연면적(㎡)"] / 총층수 / 대지면적,
        "지상층별_건폐율": df_filtered["용적_률_산정_연면적(㎡)"] / 지상층수 / 대지면적,
        # "층별_충만률": df_filtered["연면적(㎡)"]
        # / 총층수
        # / df_filtered["건축_면적(㎡)"],
        "지상층별_충만률": df_filtered["용적_률_산정_연면적(㎡)"]
        / 지상층수
        / df_filtered["건축_면적(㎡)"],
    }
)

# 무한대/NaN 정리
df_dimensionless = df_dimensionless.replace(
    [pd.NA, float("inf"), -float("inf")], np.nan
)

# %%
df_dimensionless.dtypes

# %%
df_dimensionless.head()

# %%
with pd.option_context("display.float_format", "{:.4f}".format):
    display(df_dimensionless.describe())

# %%
sampled = df_dimensionless.sample(n=3000, random_state=1106).copy()


# %%
sampled.head()

# %%
sns.pairplot(sampled.iloc[:, 1:6], corner=True)

# %%
sns.pairplot(sampled.iloc[:, 6:], corner=True)

# %% [markdown]
# ### Anormaly Detection
#
# Isolation Forest, One-Class SVM using SGD
#

# %%
df = (
    df_dimensionless.iloc[:, 6:12]  # select only the dimensionless columns
    .dropna()
    # .sample(n=1_000_000, random_state=1106)
    .copy()
)

df.head()

# %%
df.shape

# %%

# settings
outliers_fraction = 0.01

anomaly_algorithms = [
    (
        "One-Class SVM (SGD)",
        make_pipeline(
            Nystroem(gamma=0.1, random_state=1106, n_components=1_000),
            SGDOneClassSVM(
                nu=outliers_fraction,
                fit_intercept=True,
                random_state=1106,
            ),
        ),
    ),
    (
        "Isolation Forest",
        IsolationForest(contamination=outliers_fraction, random_state=1106),
    ),
]

# Choose sizes to test
sizes = [10_000, 100_000, 1_000_000]  # 4_703_097 failed on 64GB RAM machine
results = []

for algo_name, clf in anomaly_algorithms:
    for size in sizes:
        X = df.sample(n=size, random_state=1106).to_numpy()

        start = time.perf_counter()
        clf.fit(X)
        elapsed = time.perf_counter() - start

        results.append((size, elapsed))
        print(f"clf={algo_name} n={size:<6}  time={elapsed:.4f} sec")


# %%
X = df.to_numpy()
X_sampled = df.sample(n=1_000_000, random_state=1106).to_numpy()

# settings
outliers_fraction = 0.01


# %%
clf = make_pipeline(
    Nystroem(gamma=0.1, random_state=1106, n_components=1_000),
    SGDOneClassSVM(
        nu=outliers_fraction,
        fit_intercept=True,
        random_state=1106,
    ),
)
clf.fit(X_sampled)
y_pred = clf.predict(X)
# align by index when assigning back
df_dimensionless["pred_one-class_svm"] = pd.Series(y_pred, index=df.index)

# runtime ~6 min.

# %%
clf = IsolationForest(contamination=outliers_fraction, random_state=1106)
clf.fit(X_sampled)
y_pred = clf.predict(X)

# align by index when assigning back
df_dimensionless["pred_isolation_forest"] = pd.Series(y_pred, index=df.index)

# runtime <1 min.

# %%
# 1: inlier, -1: outlier
df_dimensionless.head()

# %%
df_orig.loc[1]

# %% [markdown]
# 숭인동아파트(숭인모범) 아파트 한 동에 대해, SVM은 inlier로 IF는 outlier로 판정함.
#

# %%
with pd.option_context("display.float_format", "{:.4f}".format):
    display(df_dimensionless.describe())

# %%
stat_func_names = ["count", "mean", "std", "min", "max"]

stats = df_dimensionless.groupby("pred_one-class_svm")[
    [
        "대지면적",
        "건축면적",
        "연면적",
        "용적률_산정용_연면적",
        "지상층수",
        "건폐율",
        "용적률",
        "지상_유효층수",
        "용적_산정률",
        "지상층별_건폐율",
        "지상층별_충만률",
    ]
].agg(stat_func_names)  # type: ignore
stacked = stats.stack(level=1, future_stack=True)  # columns → index
stacked = stacked.swaplevel(0, 1)  # flip the first two index levels
stacked.index = stacked.index.set_names(["통계", *stats.index.names])
stacked = stacked.reindex(index=stat_func_names, level=0)

with pd.option_context(
    "display.float_format", "{:.4f}".format, "display.max_columns", None
):
    display(stacked)


# %% [markdown]
# ---
#

# %%
stat_func_names = ["count", "mean", "std", "min", "max"]

stats = df_dimensionless.groupby("pred_isolation_forest")[
    [
        "대지면적",
        "건축면적",
        "연면적",
        "용적률_산정용_연면적",
        "지상층수",
        "건폐율",
        "용적률",
        "지상_유효층수",
        "용적_산정률",
        "지상층별_건폐율",
        "지상층별_충만률",
    ]
].agg(stat_func_names)  # type: ignore
stacked = stats.stack(level=1, future_stack=True)  # columns → index
stacked = stacked.swaplevel(0, 1)  # flip the first two index levels
stacked.index = stacked.index.set_names(["통계", *stats.index.names])
stacked = stacked.reindex(index=stat_func_names, level=0)

with pd.option_context(
    "display.float_format", "{:.4f}".format, "display.max_columns", None
):
    display(stacked)


# %%
cols = ["pred_one-class_svm", "pred_isolation_forest"]
pred_sum = df_dimensionless[cols].sum(axis=1, min_count=2)
# -2 for outlier by both, 0 for outlier by one

pred_both = pd.Series(pred_sum == -2, index=df_dimensionless.index)
df_dimensionless["pred_both"] = pred_both.mask(pred_sum.isna())

df_dimensionless.head()

# %%
outliers = df_dimensionless[df_dimensionless["pred_both"] == True]  # noqa: E712
inliers = df_dimensionless[df_dimensionless["pred_both"] == False]  # noqa: E712

# Choose how many to keep
n_out = min(len(outliers), 1000)  # keep up to 1k outliers
n_in = min(len(inliers), 3000)  # keep up to 3k inliers

out_sample = outliers.sample(n=n_out, random_state=1106)
in_sample = inliers.sample(n=n_in, random_state=1106)

plot_df = pd.concat([out_sample, in_sample]).sort_index()
plot_df["prediction"] = plot_df["pred_both"].map({True: "Outlier", False: "Inlier"})
# Attach only the features + predictions
plot_df = plot_df.iloc[:, [*range(6, 12), -1]]
plot_df.head()

# %%
plot_df["prediction"].value_counts()

# %%
sns.pairplot(
    plot_df, hue="prediction", corner=True, palette={"Outlier": "red", "Inlier": "blue"}
)

# %% [markdown]
# ---
#
# 시도별, 연도별 표를 만들어보자.
#

# %%
df_results = df_orig.copy()
df_results["이상값"] = df_dimensionless["pred_both"]
# 무한대/NaN 정리
df_results = df_results.replace([np.nan, pd.NA, float("inf"), -float("inf")], pd.NA)
df_results["시도_코드"] = df_results["시군구_코드"].str[:2]
df_results["사용승인_년"] = df_results["사용승인_일"].str[:4]
df_results = df_results[
    [
        "관리_건축물대장_PK",
        "시도_코드",
        "사용승인_년",
        "주_용도_코드",
        "기존14",
        "기존16",
        "기존19",
        "기존20",
        "신규01",
        "신규02",
        "이상값",
    ]
]
with pd.option_context("display.max_columns", None):
    print(tabulate(df_results.head(30), headers="keys", tablefmt="psql"))

# %%
df_results.to_parquet("../results/area_data_anomaly.parquet", index=False)

# %%
