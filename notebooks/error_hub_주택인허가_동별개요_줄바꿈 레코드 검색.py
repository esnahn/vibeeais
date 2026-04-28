# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%

import polars as pl
from IPython.display import display

# %% [markdown]
# ### 파케이 파일 경로 설정

# %%
parquet_path = "data/parquet/주택인허가_동별개요.parquet"

# %% [markdown]
# ### '관리_동별_개요_PK' 컬럼 값이 '1017100005635'인 레코드 검색
# before:
# > 1017100005635|1017100007809|서울특별시 강서구 마곡동 블록|SH아파트(10-1BL)|11500|10500|2|0000|0000|마곡도시개발사업지구|10-1단지||1|부속건축물|관리사무소(2F), 주민공동시설(2F), 작은도서관, 보육시설, 경로당, 커뮤니티시설, 게스트룸
# > 				|02000|공동주택|0|0|0|0|0|0|0|0|0|0|21|철근콘크리트구조|10|(철근)콘크리트|503.26|1267.08|0|341.37|0|1|5.2|0|0|5.1|3.0|0|0|15|0|20220813

# %%
result = (
    pl.scan_parquet(parquet_path)
    .filter(pl.col("관리_동별_개요_PK") == "1017100005635")
    .collect()
)

# %% [markdown]
# ### 검색 결과 전체 출력

# %%
with pl.Config(tbl_cols=-1, tbl_rows=-1):
    display(result.transpose())

# before: null after linebreak
# ┌─────────────────────────────────┐
# │ column_0                        │
# │ ---                             │
# │ str                             │
# ╞═════════════════════════════════╡
# │ 1017100005635                   │
# │ 1017100007809                   │
# │ 서울특별시 강서구 마곡동 블록   │
# │ SH아파트(10-1BL)                │
# │ …                               │
# │ 부속건축물                      │
# │ 관리사무소(2F),                 │
# │ 주민공동시설(2F), 작은도서관, … │
# │ null                            │
# │ null                            │
# │ null                            │
# │ …                               │
#
# after: all data are loaded
# ┌─────────────────────────────────┐
# │ column_0                        │
# │ ---                             │
# │ str                             │
# ╞═════════════════════════════════╡
# │ 1017100005635                   │
# │ …                               │
# │ 1017100007809                   │
# │ 주민공동시설(2F), 작은도서관, … │
# │ 02000                           │
# │ 공동주택                        │
# │ 0                               │
# │ …                               │
# │ 15.000000000                    │
# │ 0.000000000                     │
# │ 20220813                        │
# └─────────────────────────────────┘
