# TODO: polars???

from pathlib import Path

import polars as pl


def show_parquet(file_path: Path):
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    print(f"\n{'=' * 50}")
    print(f"File: {file_path.name}")
    print(f"{'=' * 50}")

    # Handle large files efficiently using lazy evaluation
    lf = pl.scan_parquet(file_path)

    # Get schema and column count
    columns = lf.columns
    dtypes = lf.collect_schema().dtypes()

    # Get row count (Polars optimizes this using parquet metadata when possible)
    row_count = lf.select(pl.len()).collect().item()

    # Show shape
    print(f"\n[Shape]: {row_count:,} rows x {len(columns)} columns")

    # Show schema
    print("\n[Schema]")
    for col_name, dtype in zip(columns, dtypes):
        print(f"  - {col_name}: {dtype}")

    # Show sample data
    print("\n[Preview (first row)]")

    df = lf.head(1).collect()

    # print it all but in group of 5 columns
    columns = df.columns
    chunk_size = 5
    with pl.Config(tbl_cols=-1, tbl_width_chars=1000):
        for i in range(0, len(columns), chunk_size):
            chunk_cols = columns[i : i + chunk_size]
            print(f"\n[Columns {i + 1} to {min(i + chunk_size, len(columns))}]")
            print(df.select(chunk_cols))

    print("\n")


def main():
    base_dir = Path(__file__).resolve().parent.parent
    parquet_dir = base_dir / "data" / "parquet"

    if not parquet_dir.exists():
        print(f"Directory not found: {parquet_dir}")
        return

    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return

    print(f"Found {len(parquet_files)} parquet files. Showing all of them.")
    for f in parquet_files:
        show_parquet(f)


if __name__ == "__main__":
    main()
