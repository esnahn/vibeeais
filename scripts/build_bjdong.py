from pathlib import Path
import pandas as pd
from tabulate import tabulate

base_dir = Path(__file__).resolve().parent.parent
data_from = base_dir / "data/region/original"
data_to = base_dir / "data/region"

df = pd.read_csv(
    data_from
    / "법정동코드 전체자료.txt",  # from https://www.code.go.kr/stdcode/regCodeL.do
    encoding="cp949",
    sep="\t",
    index_col=0,
    dtype={"법정동코드": str},
)
print(tabulate(df.head(), headers="keys"))
df.to_csv(data_to / "code_bjd.csv", encoding="utf-8-sig")

df_sgg = df.loc[df.index.str.endswith("00000")]
df_sgg.index = df_sgg.index.str[:5]
df_sgg.index.name = "시군구코드"
df_sgg = df_sgg.rename(columns={"법정동명": "시군구명"})
print(tabulate(df_sgg.head(), headers="keys"))
df_sgg.to_csv(data_to / "code_sgg.csv", encoding="utf-8-sig")

# assume that sido doesn't have space in the name
df_sido = df.loc[~df.법정동명.str.contains(" ")]
df_sido.index = df_sido.index.str[:2]
df_sido.index.name = "시도코드"
df_sido = df_sido.rename(columns={"법정동명": "시도명"})
print(tabulate(df_sido.head(), headers="keys"))
df_sido.to_csv(data_to / "code_sido.csv", encoding="utf-8-sig")
