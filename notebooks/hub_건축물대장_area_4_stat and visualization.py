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

import matplotlib.pyplot as plt
import pandas as pd
from tabulate import tabulate

plt.style.use("../auri.mplstyle")
pd.options.display.unicode.east_asian_width = True

# %%
parquet_path = Path("../data/processed/주건축물_검증규칙.parquet")
df_orig = pd.read_parquet(parquet_path)
print(f"Loaded shape: {df_orig.shape}")
with pd.option_context("display.max_columns", None):
    print(tabulate(df_orig.head(30), headers="keys", tablefmt="psql"))

# %%
df_results = pd.read_parquet("../results/area_data_anomaly.parquet")
df_results = df_results.convert_dtypes()
print(f"Loaded shape: {df_results.shape}")
with pd.option_context("display.max_columns", None):
    print(tabulate(df_results.head(30), headers="keys", tablefmt="psql"))

# %%
df_results.dtypes

# %%
df_results.head()

# %% [markdown]
# ---
#
# 시도별, 연도별 표를 만들어보자.
#

# %%
df_year = pd.DataFrame(
    {
        "사용승인_년": [f"{year:04d}" for year in range(1950, 2025)],
    }
)
df_year

# %%
df_usage = df_orig[["주_용도_코드", "주_용도_코드_명"]].drop_duplicates()
df_usage = df_usage.set_index("주_용도_코드").sort_index()
df_usage = df_usage.reindex(
    index=[f"{i:02d}000" for i in range(1, 34)]
)  # 01000, 02000, …
df_usage

# %%
df_kcad = pd.read_csv(
    "../data/processed/code_kcad_sgg_2024.csv", dtype="string"
).sort_values("행정구역분류")
df_kcad["시도_코드"] = df_kcad["시군구코드"].str[:2]
df_sido = df_kcad[["시도_코드", "시도"]].drop_duplicates()
df_sido

# %%
df_results.shape

# %%
df_results["기존16"].value_counts(dropna=False)

# %%
# mean()은 NA 제외하고 계산

print(df_results["기존16"].mean())
print(373505 / (7364967 - 343908))

# %%
# NA → False로 처리

print(df_results["기존16"].astype("boolean").fillna(False).mean())
print(373505 / (7364967))

# %%
# NA → False로 처리한 df 준비
# Convert columns to the best possible dtypes using dtypes supporting pd.NA
df_gb = df_results.iloc[:, 1:].copy().convert_dtypes()
df_gb.iloc[:, 3:] = df_gb.iloc[:, 3:].fillna(False)  # NA → False
df_gb

# %%
df_gb.dtypes

# %%
gb = (
    df_gb.iloc[:, [0, *range(3, df_gb.shape[1])]]
    .groupby("시도_코드")
    .mean()
    .astype(float)
    .mul(100)
    .round(2)
)
df_ratio = df_sido.set_index("시도_코드").join(gb)
df_ratio

# %%
df_ratio.to_csv(
    "../results/area_data_anomaly_ratio_by_sido.csv", index=True, encoding="utf-8-sig"
)

# %%
df_orig["기존14"].value_counts(dropna=False)

# %%
df_orig[df_orig["시군구_코드"].str.startswith("38")]

# %% [markdown]
# ---
#
# 연도
#

# %%
# Convert columns to the best possible dtypes using dtypes supporting pd.NA
df_gb = df_results.iloc[:, 1:].copy().convert_dtypes()
df_gb.iloc[:, 3:] = df_gb.iloc[:, 3:].fillna(False)  # NA → False

key_col = "사용승인_년"

gb = (
    df_gb.iloc[:, [1, *range(3, df_gb.shape[1])]]
    .groupby(key_col)
    .mean()
    .astype(float)
    .mul(100)
    .round(2)
)

df_ratio_year = df_year.set_index(key_col).join(gb).fillna("-")
with pd.option_context("display.max_rows", None):
    display(df_ratio_year)

# %%
df_ratio_year.to_csv(
    "../results/area_data_anomaly_ratio_by_year.csv", index=True, encoding="utf-8-sig"
)

# %% [markdown]
# ---
#
# 용도
#

# %%
# Convert columns to the best possible dtypes using dtypes supporting pd.NA
df_gb = df_results.iloc[:, 1:].copy().convert_dtypes()
df_gb.iloc[:, 3:] = df_gb.iloc[:, 3:].fillna(False)  # NA → False

key_col = "주_용도_코드"

gb = (
    df_gb.iloc[:, [2, *range(3, df_gb.shape[1])]]
    .groupby(key_col)
    .mean()
    .astype(float)
    .mul(100)
    .round(2)
)

df_ratio_usage = df_usage.join(gb).fillna("-")
with pd.option_context("display.max_rows", None):
    display(df_ratio_usage)

# %%
df_ratio_usage.to_csv(
    "../results/area_data_anomaly_ratio_by_usage.csv", index=True, encoding="utf-8-sig"
)
