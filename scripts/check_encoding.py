import argparse
import sys
import zipfile
from pathlib import Path

import chardet
import pandas as pd
from tabulate import tabulate

# Force stdout encoding to utf-8 on Windows to prevent Korean character corruption
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# Ensure Korean text columns align correctly in terminal printing
pd.options.display.unicode.east_asian_width = True


def detect_encoding_from_bytes(raw_data):
    """Detect encoding of raw bytes using chardet."""
    try:
        detection = chardet.detect(raw_data)
        encoding = detection["encoding"]
        confidence = detection["confidence"]
        if not encoding:
            encoding = "utf-8"
        if encoding.lower() == "ascii":
            encoding = "utf-8"
        return encoding, confidence
    except Exception as e:
        print(f"Error in chardet: {e}")
        return "utf-8", 0.0


def detect_encoding(file_or_path):
    """Detect encoding of a file path or a binary stream."""
    try:
        if isinstance(file_or_path, (str, Path)):
            with open(file_or_path, "rb") as f:
                raw_data = f.read(10000)
        else:
            raw_data = file_or_path.read(10000)
            if hasattr(file_or_path, "seek"):
                file_or_path.seek(0)
        return detect_encoding_from_bytes(raw_data)
    except Exception:
        return "utf-8", 0.0


def check_csv(file_or_path, num_rows=5, name=None):
    """Read and preview a CSV file or stream."""
    if isinstance(file_or_path, (str, Path)):
        path = Path(file_or_path)
        name = name or path.name
    else:
        name = name or "CSV Stream"

    print(f"=== CSV File: {name} ===")
    detected_enc, confidence = detect_encoding(file_or_path)
    print(f"Detected: {detected_enc} (Confidence: {confidence:.2f})")

    # Priority list of encodings
    if confidence >= 0.5:
        encodings = [detected_enc, "utf-8", "cp949", "euc-kr", "iso-8859-1"]
    else:
        encodings = ["utf-8", "cp949", "euc-kr", detected_enc, "iso-8859-1"]
    encodings = [enc for enc in dict.fromkeys(encodings) if enc]

    # Auto-detect separator for all CSV files
    sep = None
    for enc in encodings:
        try:
            # If it's a stream, seek back to start before reading
            if not isinstance(file_or_path, (str, Path)) and hasattr(
                file_or_path, "seek"
            ):
                file_or_path.seek(0)

            df = pd.read_csv(
                file_or_path,
                encoding=enc,
                sep=sep,
                nrows=num_rows,
                engine="python",
            )
            print(
                f"Read success: {enc} (Previewing {df.shape[0]:,} rows, {df.shape[1]:,} cols)"
            )
            print(tabulate(df, headers="keys", tablefmt="psql"))
            print("\n" + "=" * 60 + "\n")
            return enc
        except Exception as e:
            print(f"Failed to read CSV with encoding {enc}: {e}")
            continue
    print("Failed to read with all encodings.\n" + "=" * 60 + "\n")
    return "Error"


def check_txt(file_or_path, num_rows=5, name=None):
    """Read and preview a TXT file or stream line-by-line without tabular parsing."""
    if isinstance(file_or_path, (str, Path)):
        path = Path(file_or_path)
        name = name or path.name
    else:
        name = name or "Text Stream"

    print(f"=== Text File: {name} ===")
    detected_enc, confidence = detect_encoding(file_or_path)
    print(f"Detected: {detected_enc} (Confidence: {confidence:.2f})")

    # Priority list of encodings
    if confidence >= 0.5:
        encodings = [detected_enc, "utf-8", "cp949", "euc-kr", "iso-8859-1"]
    else:
        encodings = ["utf-8", "cp949", "euc-kr", detected_enc, "iso-8859-1"]
    encodings = [enc for enc in dict.fromkeys(encodings) if enc]

    for enc in encodings:
        try:
            lines = []
            if isinstance(file_or_path, (str, Path)):
                with open(file_or_path, "rb") as f:
                    for _ in range(num_rows):
                        line_bytes = f.readline()
                        if not line_bytes:
                            break
                        lines.append(line_bytes.decode(enc))
            else:
                if hasattr(file_or_path, "seek"):
                    file_or_path.seek(0)
                for _ in range(num_rows):
                    line_bytes = file_or_path.readline()
                    if not line_bytes:
                        break
                    lines.append(line_bytes.decode(enc))

            print(f"Read success: {enc} (Previewing {len(lines)} lines)")
            print("-" * 60)
            for i, line in enumerate(lines):
                print(f"{i + 1:3d}: {line.rstrip()}")
            print("-" * 60)
            print("\n" + "=" * 60 + "\n")
            return enc
        except Exception as e:
            print(f"Failed to read TXT with encoding {enc}: {e}")
            continue
    print("Failed to read with all encodings.\n" + "=" * 60 + "\n")
    return "Error"


def check_excel(file_or_path, num_rows=5, name=None):
    """Read and preview an Excel file or stream."""
    if isinstance(file_or_path, (str, Path)):
        path = Path(file_or_path)
        name = name or path.name
    else:
        name = name or "Excel Stream"

    print(f"=== Excel File: {name} ===")
    try:
        if not isinstance(file_or_path, (str, Path)) and hasattr(file_or_path, "seek"):
            file_or_path.seek(0)

        excel_file = pd.ExcelFile(file_or_path)
        print(f"Sheets: {', '.join(excel_file.sheet_names)}")
        for sheet_name in excel_file.sheet_names[:1]:  # Preview first sheet only
            df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=num_rows)
            print(
                f"--- Sheet: {sheet_name} (Previewing {df.shape[0]:,} rows, {df.shape[1]:,} cols) ---"
            )
            print(tabulate(df, headers="keys", tablefmt="psql"))
        print("\n" + "=" * 60 + "\n")
        return excel_file.sheet_names
    except Exception as e:
        print(f"Failed to read Excel: {e}\n" + "=" * 60 + "\n")
        return "Error"


def check_zip(file_path, num_rows=5):
    """Read and preview the first entry inside a ZIP file by reusing check_csv/check_txt/check_excel."""
    path = Path(file_path)
    print(f"=== ZIP File: {path.name} ===")
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            file_list = z.namelist()
            print(f"Files inside ZIP: {', '.join(file_list)}")
            if not file_list:
                print("ZIP is empty.\n" + "=" * 60 + "\n")
                return "Empty"

            target = file_list[0]
            print(f"\n--- Previewing first entry inside ZIP: {target} ---")
            suffix = Path(target).suffix.lower()

            if suffix == ".csv":
                with z.open(target) as f:
                    enc = check_csv(f, num_rows, name=target)
                return f"{target} ({enc})"
            elif suffix == ".txt":
                with z.open(target) as f:
                    enc = check_txt(f, num_rows, name=target)
                return f"{target} ({enc})"
            elif suffix in [".xls", ".xlsx", ".xlsm"]:
                with z.open(target) as f:
                    sheets = check_excel(f, num_rows, name=target)
                return f"{target} (Excel sheets: {sheets})"
            else:
                print(f"Unsupported preview type: {suffix}")

        print("\n" + "=" * 60 + "\n")
        return target
    except Exception as e:
        print(f"Failed to read ZIP: {e}\n" + "=" * 60 + "\n")
        return "Error"


def run_checks(target_path, file_type="all", num_rows=5):
    target = Path(target_path)
    if not target.exists():
        print(f"Error: Path does not exist - {target_path}")
        return

    csv_encodings = {}
    txt_encodings = {}
    excel_sheets = {}
    zip_encodings = {}

    if target.is_file():
        suffix = target.suffix.lower()
        if suffix == ".csv" and file_type in ["csv", "all"]:
            csv_encodings[target.as_posix()] = check_csv(target, num_rows)
        elif suffix == ".txt" and file_type in ["txt", "all"]:
            txt_encodings[target.as_posix()] = check_txt(target, num_rows)
        elif suffix in [".xls", ".xlsx", ".xlsm"] and file_type in ["excel", "all"]:
            excel_sheets[target.as_posix()] = check_excel(target, num_rows)
        elif suffix == ".zip" and file_type in ["zip", "all"]:
            zip_encodings[target.as_posix()] = check_zip(target, num_rows)
    elif target.is_dir():
        print(f"Scanning directory: {target.as_posix()} (Filter: {file_type})")
        if file_type in ["csv", "all"]:
            for f in sorted(list(target.rglob("*.csv"))):
                csv_encodings[f.as_posix()] = check_csv(f, num_rows)
        if file_type in ["txt", "all"]:
            for f in sorted(list(target.rglob("*.txt"))):
                if f.name != "checksums.blake3":
                    txt_encodings[f.as_posix()] = check_txt(f, num_rows)
        if file_type in ["excel", "all"]:
            for f in sorted(list(target.rglob("*.xls*"))):
                excel_sheets[f.as_posix()] = check_excel(f, num_rows)
        if file_type in ["zip", "all"]:
            for f in sorted(list(target.rglob("*.zip"))):
                zip_encodings[f.as_posix()] = check_zip(f, num_rows)

        # Summary
        print("\n" + "#" * 60 + "\nSUMMARY REPORT\n" + "#" * 60)
        if csv_encodings:
            print("CSV Encodings:")
            for p, enc in csv_encodings.items():
                print(f"  - {Path(p).name}: {enc}")
        if txt_encodings:
            print("\nTXT Encodings:")
            for p, enc in txt_encodings.items():
                print(f"  - {Path(p).name}: {enc}")
        if excel_sheets:
            print("\nExcel Sheets:")
            for p, sheets in excel_sheets.items():
                print(f"  - {Path(p).name}: {sheets}")
        if zip_encodings:
            print("\nZIP Entries:")
            for p, z_enc in zip_encodings.items():
                print(f"  - {Path(p).name}: {z_enc}")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] in ["-h", "--help"]:
        print(
            "Usage: python scripts/check_encoding.py <path_to_file_or_directory> [--type {csv|txt|excel|zip|all}] [--rows N]"
        )
        print("\nExamples:")
        print("  python scripts/check_encoding.py data/region")
        print("  python scripts/check_encoding.py data/region/code_sido.csv --rows 10")
        print(
            '  python scripts/check_encoding.py "data/original/202512/국토교통부_건축물대장_총괄표제부+(2025년+12월).zip"'
        )
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Check encodings and preview files.")
    parser.add_argument("path", help="Path to file or directory to inspect")
    parser.add_argument(
        "--type",
        choices=["csv", "txt", "excel", "zip", "all"],
        default="all",
        help="File type filter",
    )
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to preview")

    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    target = Path(args.path)
    if not target.is_absolute():
        target = project_root / target

    run_checks(target, args.type, args.rows)
