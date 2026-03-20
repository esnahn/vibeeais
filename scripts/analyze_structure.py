import zipfile
import polars as pl
import io
import re
from pathlib import Path


def load_schema(schema_path):
    """
    Reads a schema text file and extracts the column names and types.
    """
    column_names = []
    read_dtypes = {}
    cast_dtypes = {}

    with open(schema_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Find the start line
    start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith("컬럼한글명"):
            start_idx = i + 1
            break

    for line in lines[start_idx:]:
        parts = line.strip().split("\t")
        if len(parts) >= 2:
            col_name = parts[0].strip()
            data_type = parts[1].strip()
            column_names.append(col_name)

            # Read all columns as string first to prevent parsing errors on corrupted numeric fields
            read_dtypes[col_name] = pl.String

            if "NUMERIC" in data_type:
                match = re.search(r"NUMERIC\((\d+)(?:,(\d+))?\)", data_type)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2)) if match.group(2) else 0

                    # Store the target decimal type for casting
                    # Polars Decimal supports fixed precision and scale
                    cast_dtypes[col_name] = pl.Decimal(precision=precision, scale=scale)

    return column_names, read_dtypes, cast_dtypes


def analyze_with_polars():
    base_dir = Path(__file__).resolve().parent.parent
    schema_file = base_dir / "data/schema/schema_총괄표제부.txt"
    zip_path = (
        base_dir / "data/original/국토교통부_건축물대장_총괄표제부+(2025년+12월).zip"
    )

    print(f"Loading schema from {schema_file}")
    columns, read_dtypes, cast_dtypes = load_schema(schema_file)
    print(
        f"Mapped {len(columns)} columns. Scheduled {len(cast_dtypes)} for Decimal casting."
    )

    print("Extracting a small portion for out-of-core simulation...")
    with zipfile.ZipFile(zip_path, "r") as z:
        file_list = z.namelist()
        target = file_list[0]

        with z.open(target) as f:
            # Read first 10MB to analyze structure
            head_bytes = f.read(10 * 1024 * 1024)

    # Load into Polars
    df = pl.read_csv(
        io.BytesIO(head_bytes),
        separator="|",
        has_header=False,
        encoding="utf8-lossy",
        new_columns=columns,
        schema_overrides=read_dtypes,
        truncate_ragged_lines=True,
    )

    # Cast NUMERIC columns to pl.Decimal
    print("Casting numeric columns to fixed precision Decimal...")
    exprs = []
    for col, dtype in cast_dtypes.items():
        # strict=False ensures that parsing errors resulting from corrupted numbers
        # (like '1000000000000000516565') are replaced with nulls.
        exprs.append(pl.col(col).cast(dtype, strict=False))

    if exprs:
        df = df.with_columns(exprs)

    print("Data Shape (Sample):", df.shape)

    print("\nData Schema:")
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"  {col}: {dtype}")

    print("\nData Head:")
    print(df.head())


if __name__ == "__main__":
    analyze_with_polars()
