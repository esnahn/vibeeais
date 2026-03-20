import zipfile
import tempfile
import shutil
from pathlib import Path
import polars as pl
import gc


def load_schema(schema_path):
    column_names = []
    read_dtypes = {}
    with open(schema_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith("컬럼한글명"):
            start_idx = i + 1
            break
    for line in lines[start_idx:]:
        parts = line.strip().split("\t")
        if len(parts) >= 2:
            col_name = parts[0].strip()
            column_names.append(col_name)
            read_dtypes[col_name] = pl.String
    return column_names, read_dtypes


def test_full_polars_cleanup():
    base_dir = Path(__file__).resolve().parent.parent
    zip_path = (
        base_dir / "data/original/국토교통부_건축물대장_총괄표제부+(2025년+12월).zip"
    )
    schema_path = base_dir / "data/schema/schema_총괄표제부.txt"
    parquet_path = base_dir / "data/parquet/총괄표제부_test.parquet"

    columns, read_dtypes = load_schema(schema_path)

    print("\n--- Testing extraction, Polars conversion, and deletion ---")
    temp_dir = tempfile.mkdtemp(dir=base_dir / "data")
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            target_file = z.namelist()[0]
            print(f"  [1] Extracting {target_file} to {temp_dir}...")
            z.extract(target_file, path=temp_dir)
            extracted_txt_path = Path(temp_dir) / target_file

            print("  [2] Setting up Polars lazy frame...")
            lazy_df = pl.scan_csv(
                extracted_txt_path,
                separator="|",
                has_header=False,
                encoding="utf8-lossy",
                new_columns=columns,
                schema_overrides=read_dtypes,
                truncate_ragged_lines=True,
                quote_char=None,
            )

            print("  [3] Sinking to Parquet...")
            lazy_df.sink_parquet(parquet_path)

            print("  [4] Cleaning up Polars references...")
            del lazy_df
            gc.collect()

            print("  [5] Attempting to delete extracted txt within zip context...")
            try:
                extracted_txt_path.unlink()
                print("      -> Successfully deleted the txt file!")
            except Exception as e:
                print(f"      -> FAILED to delete txt file: {e}")

    except Exception as e:
        print(f"  Test failed: {e}")

    print("\n  [6] Attempting to delete the temporary folder...")
    try:
        shutil.rmtree(temp_dir)
        print("  -> SUCCESSFULLY deleted the temp folder!")
    except Exception as e:
        print(f"  -> FAILED to delete the temp folder: {e}")


if __name__ == "__main__":
    test_full_polars_cleanup()
