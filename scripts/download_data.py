"""
Utility script to acquire the Brazilian E-Commerce Public Dataset by Olist
and place all raw CSVs into data/raw/.
"""
import os
import sys
import requests
from tqdm import tqdm

RAW_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

BASE_URL = "https://raw.githubusercontent.com/vishalkirtaniya/e-com-data-analysis/main/"
DEST_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")


def download_file(file_name: str, target_dir: str):
    url = BASE_URL + file_name
    dest_path = os.path.join(target_dir, file_name)

    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        print(f"[SKIP] {file_name} already exists ({os.path.getsize(dest_path):,} bytes).")
        return True

    print(f"[DOWNLOADING] {file_name} from {url} ...")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024 * 1024  # 1 MB chunk

    with open(dest_path, "wb") as f, tqdm(
        total=total_size, unit="B", unit_scale=True, desc=file_name, ncols=80
    ) as pbar:
        for chunk in response.iter_content(chunk_size=block_size):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))

    print(f"[SUCCESS] Downloaded {file_name} -> {dest_path} ({os.path.getsize(dest_path):,} bytes)")
    return True


def main():
    os.makedirs(DEST_DIR, exist_ok=True)
    print(f"Target directory for raw data: {DEST_DIR}")
    failed = []

    for file_name in RAW_FILES:
        try:
            download_file(file_name, DEST_DIR)
        except Exception as e:
            print(f"[ERROR] Failed to download {file_name}: {e}", file=sys.stderr)
            failed.append(file_name)

    if failed:
        print(f"\nFailed to download {len(failed)} files: {failed}", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll 9 Olist CSV datasets downloaded successfully into data/raw/!")


if __name__ == "__main__":
    main()
