import shutil
from pathlib import Path


def main():
    base_dir = Path("data")
    for item in base_dir.glob("tmp*"):
        if item.is_dir():
            print(f"\nAttempting to delete directory {item}...")
            try:
                shutil.rmtree(item)
                print(f"Successfully deleted {item}")
            except Exception as e:
                print(f"Failed to delete {item}: {e}")
        elif item.is_file():
            print(f"\nAttempting to delete file {item}...")
            try:
                item.unlink()
                print(f"Successfully deleted {item}")
            except Exception as e:
                print(f"Failed to delete {item}: {e}")


if __name__ == "__main__":
    main()
