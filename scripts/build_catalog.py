import json
import re
from pathlib import Path

# 분석 데이터 연월에 맞추어 수정
YEAR = "2025"
MONTH = "12"


def build_catalog():
    base_dir = Path(__file__).resolve().parent.parent
    original_dir = base_dir / "data" / "original"
    schema_dir = base_dir / "data" / "schema"
    catalog_path = base_dir / "data" / "dataset_catalog.json"

    # 1. List all zip files and extract the dataset name
    # Example filename: 국토교통부_건축물대장_총괄표제부+(2025년+12월).zip
    catalog = {}
    zip_pattern = re.compile(
        rf"국토교통부_건축물대장_(.*?)\+\({YEAR}년\+{MONTH}월\)\.zip"
    )

    zip_files = list(original_dir.glob("*.zip"))
    schema_files = list(schema_dir.glob("*.txt"))

    for zip_file in zip_files:
        match = zip_pattern.match(zip_file.name)
        if match:
            dataset_name = match.group(1)

            # 2. Find the corresponding schema file
            # Handled naming variations: schema_총괄표제부.txt OR 표제부.txt
            expected_schema_name1 = f"schema_{dataset_name}.txt"
            expected_schema_name2 = f"{dataset_name}.txt"

            matched_schema = None
            for schema_file in schema_files:
                if (
                    schema_file.name == expected_schema_name1
                    or schema_file.name == expected_schema_name2
                ):
                    matched_schema = schema_file
                    break

            if matched_schema:
                print(f"Matched: {dataset_name}")
                print(f"  Zip: {zip_file.name}")
                print(f"  Schema: {matched_schema.name}")

                catalog[dataset_name] = {
                    "dataset_name": dataset_name,
                    "zip_path": str(zip_file.relative_to(base_dir)).replace("\\", "/"),
                    "schema_path": str(matched_schema.relative_to(base_dir)).replace(
                        "\\", "/"
                    ),
                    "zip_size_bytes": zip_file.stat().st_size,
                }
            else:
                print(f"WARNING: Schema not found for {dataset_name}")
        else:
            print(f"WARNING: File {zip_file.name} does not match expected pattern.")

    print(f"\nTotal datasets matched: {len(catalog)}")

    # 3. Save matching results to JSON
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=4)
        print(f"Saved catalog to {catalog_path.relative_to(base_dir)}")


if __name__ == "__main__":
    build_catalog()
