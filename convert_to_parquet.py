import json
import zipfile
import polars as pl
import os
import re
from pathlib import Path
import tempfile
import shutil
import gc
import time

def load_schema(schema_path):
    column_names = []
    read_dtypes = {}
    cast_dtypes = {}
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith("컬럼한글명"):
            start_idx = i + 1
            break
            
    for line in lines[start_idx:]:
        parts = line.strip().split('\t')
        if len(parts) >= 2:
            col_name = parts[0].strip()
            data_type = parts[1].strip()
            column_names.append(col_name)
            
            read_dtypes[col_name] = pl.String
            
            if 'NUMERIC' in data_type:
                match = re.search(r'NUMERIC\((\d+)(?:,(\d+))?\)', data_type)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2)) if match.group(2) else 0
                    cast_dtypes[col_name] = pl.Decimal(precision=precision, scale=scale)
                
    return column_names, read_dtypes, cast_dtypes

def _sink_csv_to_parquet(csv_path, parquet_path, columns, read_dtypes, cast_dtypes):
    """Scoped helper so lazy_df is released when this function returns, freeing file handles."""
    lazy_df = pl.scan_csv(
        csv_path,
        separator='|',
        has_header=False,
        encoding='utf8-lossy',
        new_columns=columns,
        schema_overrides=read_dtypes,
        quote_char=None # Data contains unescaped quotes like "나"동
    )
    # Apply Decimal casts lazily
    exprs = [pl.col(col).cast(dtype, strict=False) for col, dtype in cast_dtypes.items()]
    if exprs:
        lazy_df = lazy_df.with_columns(exprs)
    # Sink to Parquet. This streams the file chunk by chunk without entirely loading into memory!
    lazy_df.sink_parquet(parquet_path)

    # Polars holds the file handle open past sink_parquet()
    del lazy_df
    gc.collect()
    time.sleep(10)  # Allow Polars Rust threads to release file handles on Windows

def convert_to_parquet():
    base_dir = Path(r'e:/국건위-auri/2026 국건위 업무(안의순)/10. 기획단 업무/도심형 블록주택(도심주택)/0223_세움터 데이터 분석')
    catalog_path = base_dir / 'data' / 'dataset_catalog.json'
    parquet_dir = base_dir / 'data' / 'parquet'
    
    parquet_dir.mkdir(parents=True, exist_ok=True)
    
    with open(catalog_path, 'r', encoding='utf-8') as f:
        catalog = json.load(f)
        
    print(f"Loaded catalog with {len(catalog)} datasets.")
    
    for dataset_name, info in catalog.items():
        zip_path = base_dir / info['zip_path']
        schema_path = base_dir / info['schema_path']
        out_parquet_path = parquet_dir / f"{dataset_name}.parquet"
        
        if out_parquet_path.exists():
            print(f"Skipping {dataset_name}, parquet already exists.")
            continue
            
        print(f"\nProcessing {dataset_name}...")
        columns, read_dtypes, cast_dtypes = load_schema(schema_path)
        
        # Polars Lazy API out-of-core processing works best on uncompressed files
        # We extract the file to a temporary location, process it with scan_csv(), sink_parquet()
        with tempfile.TemporaryDirectory(dir=base_dir/'data') as temp_dir:
            try:
                with zipfile.ZipFile(zip_path, 'r') as z:
                    target_file = z.namelist()[0]
                    print(f"  Extracting {target_file} to temporary folder...")
                    z.extract(target_file, path=temp_dir)
                    extracted_txt_path = Path(temp_dir) / target_file
                    
                print(f"  Converting to parquet out-of-core...")
                _sink_csv_to_parquet(extracted_txt_path, out_parquet_path, columns, read_dtypes, cast_dtypes)
                print(f"  Successfully saved -> {out_parquet_path.name}")
                
            except Exception as e:
                print(f"  Failed processing {dataset_name}: {e}")
            
if __name__ == "__main__":
    convert_to_parquet()
