import csv
import json
import os
from upstash_redis import Redis

# -----------------------------
# Load Upstash credentials from env
UPSTASH_URL = "https://romantic-monarch-26720.upstash.io"
UPSTASH_TOKEN =  "AWhgAAIncDI5NzkyZmIzMTkxOGY0OTg4OTAxYjJhODM2MWUyZjJhN3AyMjY3MjA"
BATCH_SIZE = 500  # number of records per batch (tune if needed)

if not UPSTASH_URL or not UPSTASH_TOKEN:
    raise RuntimeError("Set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN in environment")

client = Redis(url=UPSTASH_URL, token=UPSTASH_TOKEN)
print("[Upstash] client initialized")

# -----------------------------
# CSV import function
def import_csv(file_path, key_column, value_column=None):
    """
    Imports CSV rows into Upstash Redis.
    key_column: name of the column to use as Redis key
    value_column: name of column to use as value; if None, store entire row JSON
    """
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        count = 0
        for row in reader:
            key = str(row[key_column])
            if value_column:
                value = row[value_column]
            else:
                value = json.dumps(row, ensure_ascii=False)
            batch.append((key, value))
            if len(batch) >= BATCH_SIZE:
                _push_batch(batch)
                count += len(batch)
                print(f"[CSV] Imported {count} rows...")
                batch = []
        if batch:
            _push_batch(batch)
            count += len(batch)
            print(f"[CSV] Imported {count} rows total")


# -----------------------------
# SQL file import function (for simple INSERT INTO statements)
def import_sql(file_path):
    """
    Imports a SQL dump with simple INSERT INTO statements like:
    INSERT INTO cache (key, value) VALUES ('k1','v1'),('k2','v2');
    """
    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    import re
    pattern = re.compile(r"INSERT INTO .*? \((.*?)\) VALUES (.*?);", re.IGNORECASE | re.DOTALL)
    matches = pattern.findall(content)
    total = 0
    for cols, values in matches:
        # parse column names
        cols = [c.strip().strip("`") for c in cols.split(",")]
        # parse each tuple of values
        tuples = re.findall(r"\((.*?)\)", values)
        batch = []
        for t in tuples:
            vals = [v.strip().strip("'") for v in t.split(",")]
            row = dict(zip(cols, vals))
            key = row.get("key")
            value = row.get("value") or json.dumps(row)
            if key:
                batch.append((key, value))
            if len(batch) >= BATCH_SIZE:
                _push_batch(batch)
                total += len(batch)
                print(f"[SQL] Imported {total} rows...")
                batch = []
        if batch:
            _push_batch(batch)
            total += len(batch)
            print(f"[SQL] Imported {total} rows total")


# -----------------------------
# Batch push helper
def _push_batch(batch):
    # Upstash mset expects a flat dict
    data = {k: v for k, v in batch}
    client.mset(data)


# -----------------------------
# Example usage
if __name__ == "__main__":
    # CSV import
    csv_file = "cache_rows.csv"          # replace with your CSV path
    import_csv(csv_file, key_column="key")  # replace "id" with your key column

    # SQL import
    # sql_file = "data.sql"
    # import_sql(sql_file)
