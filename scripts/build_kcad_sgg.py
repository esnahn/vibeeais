import re
from pathlib import Path

import pandas as pd
from tabulate import tabulate

# 통계분류포털 > 사회분류 > 일반분류 > 한국행정구역분류 > 자료실 > 최신개정
# https://kssc.mods.go.kr:8443/
# 통계청의 기관명이 국가데이터처로 변경됨에 따라,
# 2025년 12월 2일부터 통계분류포털의 도메인주소가 변경되었습니다.
# Deep link:
# https://kssc.mods.go.kr:8443/ksscNew_web/kssc/common/IndexedSearchList.do?gubun=2&strCategoryNameCode=019&top_menu=19&main_menu=626&sub_menu=6261&cntGugun=N&searchGugun=N&categoryMenu=006&addGubun=no


def build_kcad_sgg():
    file_path = Path(__file__).resolve()
    root_path = file_path.parent.parent

    data_from = root_path / "data/region/original"
    data_to = root_path / "data/region"
    # data_to.mkdir(parents=True, exist_ok=True)

    excel_files = list(data_from.glob("*.xlsx"))

    for excel_from in excel_files:
        print(f"Processing: {excel_from.name}")

        # data\region\original\한국행정구역분류_2025.12.31.기준_20251231031320.xlsx
        # Extract date from filename (e.g., ...2025.12.31.기준... -> 20251231)
        # Handle formats like 2025.12.31 or 2026. 2. 1.
        match = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.기준", excel_from.name)
        if not match:
            print(f"Skipping {excel_from.name}: Could not find date in filename.")
            continue

        year, month, day = match.groups()
        date_str = f"{year}{int(month):02d}{int(day):02d}"

        csv_sido = data_to / f"code_kcad_sido_{date_str}.csv"
        csv_sgg = data_to / f"code_kcad_sgg_{date_str}.csv"

        # Read the Excel file
        try:
            df = pd.read_excel(
                excel_from,
                sheet_name="2-2. 연계표_행정동 및 법정동(기준시점)",
                header=1,  # Row 2 (0-based index) contains column names
                usecols="A:L",
                dtype=str,  # codes are strings consisted of numbers
                # If keep_default_na is False, and na_values are specified,
                # only the NaN values specified na_values are used for parsing.
                # TODO: but why?
                keep_default_na=False,
                na_values=[
                    "#N/A",
                    "#NA",
                    "<NA>",
                    "N/A",
                    "NA",
                    "NULL",
                    "NaN",
                    "None",
                    "n/a",
                    "nan",
                    "null",
                ],
            )
        except Exception as e:
            print(f"Error reading {excel_from.name}: {e}")
            continue

        # replace newline characters in column names with spaces
        # to transform multiline column names to single line
        df.columns = [col.replace("\n", " ") for col in df.columns]

        # 1. Generate Sido CSV (KCAD length 2)
        # Filter rows where 행정구역분류 length is 2
        sido_df = df[df["행정구역분류"].str.len() == 2].copy()
        # add a column 시도코드 as the first 2 characters of 법정동코드
        sido_df["시도코드"] = sido_df["법정동코드"].str[:2]
        # make it index
        sido_df = sido_df.set_index("시도코드")

        print(f"Saving Sido to {csv_sido.name}")
        sido_df.to_csv(csv_sido, index=True, encoding="utf-8-sig")

        # 2. Generate SGG CSV (KCAD length 5)
        # Filter rows where 행정구역분류 length is 5
        sgg_df = df[df["행정구역분류"].str.len() == 5].copy()
        # add a column 시군구코드 as the first 5 characters of 법정동코드
        sgg_df["시군구코드"] = sgg_df["법정동코드"].str[:5]
        # make it index
        sgg_df = sgg_df.set_index("시군구코드")

        print(f"Saving SGG to {csv_sgg.name}")
        sgg_df.to_csv(csv_sgg, index=True, encoding="utf-8-sig")

        # Print samples
        print("\nSido Sample:")
        print(tabulate(sido_df.head(3), headers="keys"))
        print("\nSGG Sample:")
        print(tabulate(sgg_df.head(3), headers="keys"))


if __name__ == "__main__":
    build_kcad_sgg()
