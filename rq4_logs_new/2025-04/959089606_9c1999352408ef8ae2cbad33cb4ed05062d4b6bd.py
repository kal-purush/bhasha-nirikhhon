import pandas as pd
from pathlib import Path
from db_config import get_engine

# Step 1: Connect to the database
engine = get_engine()

# Step 2: Choose which folder to load from
data_path = Path("D:/2023")  # or D:/2022, etc.

# Step 3: Find all CSV files
csv_files = list(data_path.rglob("*.csv"))
print(f"📄 Found {len(csv_files)} CSV files.")

# Step 4: Loop through and upload in chunks
for csv_file in csv_files:
    print(f"\n📥 Uploading {csv_file.name}")

    try:
        for chunk in pd.read_csv(csv_file, chunksize=5000):
            # 👇 change this to your actual table name (e.g. five_minutes)
            chunk.to_sql("five_minutes", engine, if_exists="append", index=False)

        print(f"✅ Done: {csv_file.name}")

    except Exception as e:
        print(f"❌ Error with {csv_file.name}: {e}")