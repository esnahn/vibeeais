import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Compare two Excel files instantly using pandas describe stats and schema-independent sorting."
    )
    parser.add_argument(
        "file_ref", type=str, help="Path to the reference (old) Excel file."
    )
    parser.add_argument(
        "file_tgt", type=str, help="Path to the target (new) Excel file."
    )
    parser.add_argument(
        "--atol",
        type=float,
        default=1e-2,
        help="Absolute tolerance for float comparison (default: 1e-2).",
    )
    parser.add_argument(
        "--rtol",
        type=float,
        default=1e-6,
        help="Relative tolerance for float comparison (default: 1e-6).",
    )

    args = parser.parse_args()

    ref_path = Path(args.file_ref)
    tgt_path = Path(args.file_tgt)

    print("=" * 80)
    print(
        f"Comparing Excel Spreadsheets:\nReference: {ref_path}\nTarget:    {tgt_path}"
    )
    print("=" * 80)

    if not ref_path.exists():
        print(f"[ERROR] Reference file '{ref_path}' does not exist.")
        sys.exit(1)
    if not tgt_path.exists():
        print(f"[ERROR] Target file '{tgt_path}' does not exist.")
        sys.exit(1)

    # Load spreadsheets
    try:
        df_ref = pd.read_excel(ref_path)
        df_tgt = pd.read_excel(tgt_path)
    except Exception as e:
        print(f"[ERROR] Failed to read Excel files: {e}")
        sys.exit(1)

    print(f"Reference shape: {df_ref.shape}")
    print(f"Target shape:    {df_tgt.shape}")

    # 1. Enforce Column Schema Equality immediately
    if list(df_ref.columns) != list(df_tgt.columns):
        print("[ERROR] Column schemas differ! Spreadsheets cannot be compared.")
        print(f"  Reference columns ({len(df_ref.columns)}): {df_ref.columns.tolist()}")
        print(f"  Target columns    ({len(df_tgt.columns)}): {df_tgt.columns.tolist()}")
        sys.exit(1)

    if df_ref.shape[0] != df_tgt.shape[0]:
        print(
            f"[WARNING] Row counts differ: Ref={df_ref.shape[0]} vs Tgt={df_tgt.shape[0]} (Column schemas are identical)."
        )
    else:
        print("[SUCCESS] Column schemas and shapes are identical.")
    cols = list(df_ref.columns)

    # -------------------------------------------------------------
    # STEP 1: Comparing Summary Statistics
    # -------------------------------------------------------------
    print("\n--- STEP 1: Comparing Summary Statistics ---")

    # Identify numeric columns for statistical checks
    numeric_cols = [
        col
        for col in cols
        if pd.api.types.is_numeric_dtype(df_ref[col])
        and pd.api.types.is_numeric_dtype(df_tgt[col])
    ]

    # 1.1 Sum Check
    sum_match = True
    mismatched_sums = []
    if numeric_cols:
        for col in numeric_cols:
            ref_sum = df_ref[col].sum()
            tgt_sum = df_tgt[col].sum()
            if not np.isclose(
                ref_sum, tgt_sum, atol=args.atol, rtol=args.rtol, equal_nan=True
            ):
                mismatched_sums.append((col, ref_sum, tgt_sum))
                sum_match = False

        print("\n  [1.1] Column Sums:")
        if sum_match:
            print("    All column sums match perfectly.")
        else:
            print("    Mismatch detected in column sums:")
            for col, ref_sum, tgt_sum in mismatched_sums:
                print(
                    f"      - '{col}': Ref={ref_sum:.4f}, Tgt={tgt_sum:.4f} (Diff: {abs(ref_sum - tgt_sum):.4f})"
                )
    else:
        print("\n  [1.1] Column Sums: No numeric columns to check.")

    # 1.2 Count Check
    count_match = True
    mismatched_counts = []
    for col in cols:
        ref_cnt = int(df_ref[col].count())
        tgt_cnt = int(df_tgt[col].count())
        if ref_cnt != tgt_cnt:
            mismatched_counts.append((col, ref_cnt, tgt_cnt))
            count_match = False

    print("\n  [1.2] Non-Null Record Counts:")
    if count_match:
        print("    All non-null record counts match perfectly.")
    else:
        print("    Mismatch detected in record counts:")
        for col, ref_cnt, tgt_cnt in mismatched_counts:
            print(f"      - '{col}': Ref={ref_cnt}, Tgt={tgt_cnt}")

    # 1.3 Distribution Check (mean, std, min, percentiles, max)
    dist_match = True
    if numeric_cols:
        print("\n  [1.3] Distribution Statistics (mean, std, min, percentiles, max):")
        desc_ref = df_ref[numeric_cols].describe()
        desc_tgt = df_tgt[numeric_cols].describe()

        # Exclude 'count' row since it was checked in 1.2
        dist_rows = ["mean", "std", "min", "25%", "50%", "75%", "max"]
        rows_to_compare = [r for r in dist_rows if r in desc_ref.index]

        try:
            pd.testing.assert_frame_equal(
                desc_ref.loc[rows_to_compare],
                desc_tgt.loc[rows_to_compare],
                check_dtype=False,
                atol=args.atol,
                rtol=args.rtol,
            )
            print("    All distributions match perfectly.")
        except AssertionError:
            print("    Distribution statistics differ.")
            dist_match = False
    else:
        print("\n  [1.3] Distribution Statistics: No numeric columns to check.")

    shape_match = df_ref.shape == df_tgt.shape

    if shape_match and sum_match and count_match and dist_match:
        print(
            "\n[SUCCESS] Fast-check passed! Descriptive statistics and shapes are identical."
        )
        print("Proceeding to row-level sorting comparison to verify all rows...")
    else:
        print(
            "\n[FAIL] Fast-check failed! Proceeding to row-level sorting comparison..."
        )

    # -------------------------------------------------------------
    # STEP 2: Direct Sorted DataFrame Comparison
    # -------------------------------------------------------------
    print("\n--- STEP 2: Running Direct Sorted Comparison ---")

    # Define keys as all non-numeric columns and any integer-like numeric columns (e.g. 시도_코드, 준공_년월)
    keys = []
    for col in cols:
        series = df_ref[col]
        if not pd.api.types.is_numeric_dtype(series):
            keys.append(col)
        else:
            # If numeric, check if it behaves like integers (no actual fractional parts)
            non_null = series.dropna()
            if not non_null.empty and np.all(non_null % 1 == 0):
                keys.append(col)

    if not keys:
        print(
            "[WARNING] No key columns (non-float) found to sort by. Skipping sorting and comparing directly."
        )
        df_ref_sorted = df_ref
        df_tgt_sorted = df_tgt
    else:
        print(f"Sorting files by keys: {keys}")
        # Sort and align based on non-float keys
        df_ref_sorted = df_ref.sort_values(by=keys).reset_index(drop=True)
        df_tgt_sorted = df_tgt.sort_values(by=keys).reset_index(drop=True)

    # Compare directly using pandas built-in tolerance logic
    try:
        pd.testing.assert_frame_equal(
            df_ref_sorted[cols],
            df_tgt_sorted[cols],
            check_dtype=False,
            atol=args.atol,
            rtol=args.rtol,
        )
        print(
            "\n[SUCCESS] Both spreadsheets are 100% identical! All values match perfectly under designated tolerances."
        )
        sys.exit(0)
    except AssertionError:
        print("\n[FAIL] Mismatches detected! Isolating differences...")

        if not keys:
            # If no keys, merge on index to align rows 1-to-1
            merged = pd.merge(
                df_ref_sorted,
                df_tgt_sorted,
                left_index=True,
                right_index=True,
                how="outer",
                suffixes=("_ref", "_tgt"),
                indicator=True,
            )
            keys_to_print = []
        else:
            # Sort by all columns and add a sequence number to duplicate keys to match 1-to-1
            df_ref_sorted = df_ref.sort_values(by=cols).reset_index(drop=True)
            df_tgt_sorted = df_tgt.sort_values(by=cols).reset_index(drop=True)
            df_ref_sorted["_seq"] = df_ref_sorted.groupby(keys).cumcount()
            df_tgt_sorted["_seq"] = df_tgt_sorted.groupby(keys).cumcount()

            merged = pd.merge(
                df_ref_sorted,
                df_tgt_sorted,
                on=keys + ["_seq"],
                how="outer",
                suffixes=("_ref", "_tgt"),
                indicator=True,
            )
            keys_to_print = keys

        # 1. Rows unique to Reference (missing in Target)
        only_ref = merged[merged["_merge"] == "left_only"]
        if not only_ref.empty:
            print(
                f"\n[!] Found {len(only_ref)} rows unique to REFERENCE (missing in TARGET):"
            )
            ref_val_cols = [f"{c}_ref" for c in cols if c not in keys_to_print]
            print(
                only_ref[keys_to_print + ref_val_cols].head(10).to_string(index=False)
            )

        # 2. Rows unique to Target (missing in Reference)
        only_tgt = merged[merged["_merge"] == "right_only"]
        if not only_tgt.empty:
            print(
                f"\n[!] Found {len(only_tgt)} rows unique to TARGET (missing in REFERENCE):"
            )
            tgt_val_cols = [f"{c}_tgt" for c in cols if c not in keys_to_print]
            print(
                only_tgt[keys_to_print + tgt_val_cols].head(10).to_string(index=False)
            )

        # 3. Value differences in overlapping rows
        both = merged[merged["_merge"] == "both"]
        value_cols = [c for c in cols if c not in keys_to_print]

        if not both.empty and value_cols:
            mismatch_mask = pd.Series(False, index=both.index)
            for col in value_cols:
                ref_vals = both[f"{col}_ref"]
                tgt_vals = both[f"{col}_tgt"]

                null_mismatch = ref_vals.isna() != tgt_vals.isna()

                ref_float = pd.to_numeric(ref_vals, errors="coerce")
                tgt_float = pd.to_numeric(tgt_vals, errors="coerce")

                numeric_mismatch = ~np.isclose(
                    ref_float, tgt_float, atol=args.atol, rtol=args.rtol, equal_nan=True
                )
                numeric_mismatch &= ref_float.notna() & tgt_float.notna()

                str_mismatch = (
                    (ref_vals.astype(str) != tgt_vals.astype(str))
                    & (ref_float.isna() | tgt_float.isna())
                    & ref_vals.notna()
                    & tgt_vals.notna()
                )

                mismatch_mask |= null_mismatch | numeric_mismatch | str_mismatch

            mismatched_rows = both[mismatch_mask]

            if not mismatched_rows.empty:
                print(f"\n[!] Found {len(mismatched_rows)} rows with value mismatches:")
                limit = min(10, len(mismatched_rows))
                for idx in range(limit):
                    row = mismatched_rows.iloc[idx]
                    print(f"  Mismatch #{idx + 1}:")
                    if keys_to_print:
                        print(f"    Keys: {row[keys_to_print].to_dict()}")
                    else:
                        print(f"    Row Index: {row.name}")
                    for col in value_cols:
                        val_ref = row[f"{col}_ref"]
                        val_tgt = row[f"{col}_tgt"]

                        ref_null = pd.isna(val_ref)
                        tgt_null = pd.isna(val_tgt)
                        if ref_null != tgt_null:
                            print(
                                f"      Column '{col}': Ref={val_ref}, Tgt={val_tgt} (Null mismatch)"
                            )
                        elif not ref_null and not tgt_null:
                            try:
                                is_close = np.isclose(
                                    float(val_ref),
                                    float(val_tgt),
                                    atol=args.atol,
                                    rtol=args.rtol,
                                )
                            except Exception:
                                is_close = str(val_ref) == str(val_tgt)
                            if not is_close:
                                print(
                                    f"      Column '{col}': Ref={val_ref}, Tgt={val_tgt}"
                                )
            else:
                print("\nNo value mismatches found in overlapping rows.")

        sys.exit(1)


if __name__ == "__main__":
    main()
