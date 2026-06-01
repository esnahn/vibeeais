import json
import re
import sys
from pathlib import Path

# 출력 인코딩을 UTF-8로 강제하여 윈도우 터미널 한글 깨짐 방지
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

_PERIOD_RE = re.compile(r"^\d{6}$")
_ZIP_RE = re.compile(r"국토교통부_(.*?)_(.*?)\+\((\d{4})년\+(\d{2})월\)\.zip")


def build_catalog():
    base_dir = Path(__file__).resolve().parent.parent
    original_base = base_dir / "data" / "original"
    schema_base = base_dir / "data" / "schema"
    catalog_path = base_dir / "data" / "dataset_catalog.json"

    # Discover all YYYYMM period subdirectories under data/original/
    period_dirs = sorted(
        d for d in original_base.iterdir() if d.is_dir() and _PERIOD_RE.match(d.name)
    )

    if not period_dirs:
        print("No YYYYMM subdirectories found under data/original/")
        return

    # catalog shape: { "202512_건축물대장_기본개요": { "period": "202512", ... } }
    catalog: dict[str, dict] = {}

    for period_dir in period_dirs:
        period = period_dir.name  # e.g. "202512"
        schema_dir = schema_base / period

        zip_files = sorted(period_dir.glob("*.zip"))
        schema_files = list(schema_dir.glob("*.txt")) if schema_dir.exists() else []

        if not zip_files:
            print(f"[{period}] WARNING: No zip files found, skipping.")
            continue
        if not schema_files:
            print(
                f"[{period}] WARNING: No schema files found in {schema_dir}, skipping."
            )
            continue

        period_count = 0
        for zip_file in zip_files:
            match = _ZIP_RE.match(zip_file.name)
            if not match:
                print(f"[{period}] WARNING: Unexpected filename: {zip_file.name}")
                continue

            data_category = match.group(1)
            dataset_name = match.group(2)
            catalog_key = f"{period}_{data_category}_{dataset_name}"

            expected_schema = f"schema_{data_category}_{dataset_name}.txt"
            matched_schema = next(
                (s for s in schema_files if s.name == expected_schema), None
            )

            if matched_schema:
                print(f"[{period}] Matched: {data_category}_{dataset_name}")
                catalog[catalog_key] = {
                    "period": period,
                    "data_category": data_category,
                    "dataset_name": dataset_name,
                    "zip_path": str(zip_file.relative_to(base_dir)).replace("\\", "/"),
                    "schema_path": str(matched_schema.relative_to(base_dir)).replace(
                        "\\", "/"
                    ),
                    "zip_size_bytes": zip_file.stat().st_size,
                }
                period_count += 1
            else:
                print(
                    f"[{period}] WARNING: Schema not found for {data_category}_{dataset_name}"
                )

        print(f"[{period}] {period_count} datasets matched.\n")

    print(f"Total: {len(catalog)} dataset entries across {len(period_dirs)} period(s).")

    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=4)
        f.write("\n")
    print(f"Saved catalog to {catalog_path.relative_to(base_dir)}")


if __name__ == "__main__":
    build_catalog()
