from pathlib import Path
import pandas as pd


def verify_kcad_assumptions():
    # Setup paths
    script_path = Path(__file__).resolve()
    root_path = script_path.parent.parent
    data_dir = root_path / "data/region/original"
    results_dir = root_path / "results"
    results_dir.mkdir(exist_ok=True)
    results_file = results_dir / "kcad_verify_results.txt"

    # Dual output logger
    def log(message=""):
        print(message)
        with open(results_file, "a", encoding="utf-8") as f:
            f.write(str(message) + "\n")

    # Clear previous results
    with open(results_file, "w", encoding="utf-8") as f:
        f.write("=== KOSTAT SGG Extraction Verification Results ===\n")

    log("Step 1: Contextualizing SGG Count (Total 268)")
    log("- 226 basic/legal Si-Gun-Gu (시군구)")
    log("- 41 non-autonomous districts (자치구가 아닌 구, e.g. 수원시 장안구)")
    log("- 1 special self-governing city without separate SGGs (세종특별자치시)")
    log("-" * 55)

    # Find the latest Excel file
    excel_files = sorted(list(data_dir.glob("한국행정구역분류_*.xlsx")), reverse=True)
    if not excel_files:
        log("No Excel files found.")
        return

    latest_excel = excel_files[0]
    log(f"Analyzing latest file: {latest_excel.name}")

    # Load the data
    try:
        df = pd.read_excel(
            latest_excel,
            sheet_name="2-2. 연계표_행정동 및 법정동(기준시점)",
            header=1,
            usecols="A:L",
            dtype=str,
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
        log(f"Error reading file: {e}")
        return

    # Clean column names
    df.columns = [col.replace("\n", " ") for col in df.columns]

    # Metrics
    total_rows = len(df)

    # Venn-like categorization using KCAD length as requested
    df_sido = df[df["행정구역분류"].str.len() == 2]
    df_sgg_final = df[df["행정구역분류"].str.len() == 5]
    df_dong = df[df["행정구역분류"].str.len() > 5]
    df_missing = df[df["행정구역분류"].isna() | (df["행정구역분류"] == "")]
    
    # We still care about rows that might have SGG-like BJD suffix (00000)
    cond_00000 = df["법정동코드"].str.endswith("00000")
    df_00000 = df[cond_00000]

    # Summary Table
    summary = [
        ["Total Rows in Excel", total_rows],
        ["Sido Level (KCAD len 2)", len(df_sido)],
        ["SGG Level (KCAD len 5)", len(df_sgg_final)],
        ["Dong/Sub-SGG Level (KCAD len > 5)", len(df_dong)],
        ["Missing/Unknown (KCAD NaN)", len(df_missing)],
        ["Rows with MOIS SGG-suffix (00000)", len(df_00000)],
    ]
    
    log("\n### Categorization Summary (Venn Breakdown) ###")
    log(f"{'Category':<40} | {'Count':<10}")
    log("-" * 55)
    for label, count in summary:
        log(f"{label:<40} | {count:<10}")

    # Verification Asserts
    log("\n### Verification Checks ###")
    
    # 1. KCAD Coverage (Sido + SGG + Dong + Missing = Total)
    is_coverage_correct = (len(df_sido) + len(df_sgg_final) + len(df_dong) + len(df_missing)) == total_rows
    log(f"Check: KCAD coverage correct? {'PASS' if is_coverage_correct else 'FAIL'}")
    
    # 2. SGG vs BJD-Suffix Alignment
    # Do all len-5 SGGs have a 00000 suffix in MOIS BJD?
    sgg_without_00000 = df_sgg_final[~df_sgg_final["법정동코드"].str.endswith("00000")]
    log(f"Check: All SGGs have BJD-00000 suffix? {'PASS' if len(sgg_without_00000) == 0 else 'FAIL'} (Mismatches: {len(sgg_without_00000)})")
    
    # 3. Known SGG check
    known_sggs = ["강남구", "수원시", "세종특별자치시"]
    found_sgg_names = df_sgg_final[
        df_sgg_final["시군구"].str.contains("|".join(known_sggs), na=False)
    ]["시군구"].unique()
    log(
        f"Check: Known SGGs captured? {'PASS' if len(found_sgg_names) >= 2 else 'FAIL'} (Found: {', '.join(found_sgg_names)})"
    )

    # 4. Code Distinction (MOIS BJD vs. KOSTAT KCAD)
    log("\n### Code Distinction Summary (MOIS BJD vs. KOSTAT KCAD) ###")
    sample_diff = df_sgg_final[
        df_sgg_final["법정동코드"].str[:5] != df_sgg_final["행정구역분류"]
    ].head()
    if not sample_diff.empty:
        log("BJD codes (MOIS) and KCAD codes (KOSTAT) are different as expected.")
        log("Showing sample of differences:")
        sample_to_show = sample_diff[
            ["시도", "시군구", "행정구역분류", "법정동코드"]
        ].copy()
        sample_to_show.columns = ["시도", "시군구", "KCAD(KOSTAT)", "BJD_Full(MOIS)"]
        sample_to_show["BJD_SGG(MOIS)"] = sample_to_show["BJD_Full(MOIS)"].str[:5]
        log(
            sample_to_show[
                ["시도", "시군구", "KCAD(KOSTAT)", "BJD_SGG(MOIS)"]
            ].to_string(index=False)
        )
    else:
        log("All BJD[:5] and KCAD codes match (unlikely for full dataset).")

    # 5. SGG Sub-categorization (226 + 41 + 1 = 268)
    log("\n### SGG Sub-categorization Details ###")
    log("Logic: Filter by '행정구역분류' length 5.")
    log("Categories: Special (Sejong), Non-autonomous (Space or Jeju), Autonomous (Legal SGG).")
    
    # Category Identification
    def classify(row):
        name = row["시군구"]
        sido = row["시도"]
        
        # 1. Sejong Special Case
        if sido == "세종특별자치시" and name == "세종특별자치시":
            return "Special"
            
        # 2. Non-autonomous: Space in name OR Jeju admin cities
        if " " in name or sido == "제주특별자치도":
            return "Non-autonomous"
            
        # 3. Autonomous: The rest (Legal Sigungu)
        return "Autonomous"

    df_sgg_final["Category"] = df_sgg_final.apply(classify, axis=1)
    
    df_sejong = df_sgg_final[df_sgg_final["Category"] == "Special"]
    df_non_auto = df_sgg_final[df_sgg_final["Category"] == "Non-autonomous"]
    df_auto = df_sgg_final[df_sgg_final["Category"] == "Autonomous"]
    
    log(f"{'Category':<45} | {'Count':<10} | {'Expected':<10}")
    log("-" * 75)
    log(f"{'Autonomous SGG (Basic Si-Gun-Gu)':<45} | {len(df_auto):<10} | {'226':<10}")
    log(f"{'Non-autonomous Districts (Gu with space/Jeju)':<45} | {len(df_non_auto):<10} | {'41':<10}")
    log(f"{'Special City (Sejong)':<45} | {len(df_sejong):<10} | {'1':<10}")
    log("-" * 75)
    log(f"{'Total SGG Count (Len 5)':<45} | {len(df_sgg_final):<10} | {'268':<10}")

    if len(df_non_auto) > 0:
        log("\nSample of Non-autonomous districts:")
        log(
            df_non_auto[["시도", "시군구", "행정구역분류", "법정동코드"]]
            .head(5)
            .to_string(index=False)
        )

    log(f"\nFull results saved to: {results_file}")


if __name__ == "__main__":
    verify_kcad_assumptions()
