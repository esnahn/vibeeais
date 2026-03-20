import zipfile
import chardet
from pathlib import Path

base_dir = Path(__file__).resolve().parent.parent
zip_path = (
    base_dir
    / "data"
    / "original"
    / "국토교통부_건축물대장_총괄표제부+(2025년+12월).zip"
)

try:
    with zipfile.ZipFile(zip_path, "r") as z:
        file_list = z.namelist()
        if not file_list:
            print("Zip empty.")
            exit()

        target = file_list[0]
        print(f"Reading {target}")

        with z.open(target) as f:
            raw_data = f.read(5000)

            # check chardet
            detection = chardet.detect(raw_data)
            print("Chardet detection:", detection)

            # try to decode with detected or common encodings
            encodings = [
                detection["encoding"],
                "utf-8",
                "cp949",  # windows 확장완성형
                "euc-kr",  # 완성형
                "iso-8859-1",  # Latin-1
            ]
            for enc in dict.fromkeys(encodings):
                if enc:
                    try:
                        decoded = raw_data[:500].decode(enc)
                        print(f"--- Decoded with {enc} ---")
                        print(decoded[:200])
                    except Exception as e:
                        print(f"Failed to decode with {enc}: {e}")

except Exception as e:
    print("Error:", e)
