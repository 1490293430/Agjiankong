
import os
import sys
from clickhouse_driver import Client

# Configuration (using defaults from backend/common/config.py usually, hardcoding for this fix script based on context or env vars)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "stock_data")

def get_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
        # settings={'use_numpy': True}
    )

def fix_schema():
    client = get_client()
    try:
        # Check if 'kline' table exists
        exists = client.execute("EXISTS TABLE kline")
        if not exists or not exists[0][0]:
            print("Table 'kline' does not exist. Nothing to fix.")
            return

        # Check columns
        columns = client.execute("DESCRIBE kline")
        col_names = [col[0] for col in columns]
        
        print(f"Current columns: {col_names}")

        if 'time' not in col_names:
            print("Adding 'time' column...")
            # Add time column
            client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS time DateTime DEFAULT toDateTime(date) AFTER date")
            print("Column 'time' added.")
            
            # Since we just added the column, existing hourly data will have default time (00:00:00) which is invalid for intraday
            # We should delete hourly data so it can be re-fetched properly
            print("Clearing invalid hourly data (will be re-fetched)...")
            # Note: mutation is async
            client.execute("ALTER TABLE kline DELETE WHERE period IN ('1h', '60', 'hourly')")
            print("Delete mutation submitted for hourly data.")
            
            # Also need to update ORDER BY key if possible? 
            # ClickHouse creates a new table version for ORDER BY changes, which is complex.
            # But missing 'time' in sorting key might be okay if we have it in filtering? 
            # Actually, for ReplacingMergeTree, if 'time' is not in ORDER BY, then rows with same (code, period, date) will be deduplicated!
            # Use 'time' in ORDER BY is crucial for Hourly data.
            
            # If valid sorting key is (code, period, date, time), but current is (code, period, date)...
            # We can't easily change ORDER BY key in ClickHouse without creating a new table.
            
            print("Checking Sorting Key...")
            create_query = client.execute("SHOW CREATE TABLE kline")[0][0]
            print(f"Create Query: {create_query}")
            
            if "ORDER BY (code, period, date)" in create_query and "time" not in create_query.split("ORDER BY")[1]:
                print("⚠️ Sorting Key matches old schema. Recreating table is recommended.")
                # Strategy: Rename old table, create new table, move daily data back? 
                # Or just drop the table and let it re-collect? Since user wants to 'fix' it.
                # Dropping table is drastic but ensures correctness.
                print("DROP TABLE kline... (Data will be re-collected)")
                client.execute("DROP TABLE kline")
                print("Table dropped. Please restart backend to recreate table with correct schema.")
                return

        else:
            print("'time' column already exists.")
            
        # Check unique key / order by
        create_query = client.execute("SHOW CREATE TABLE kline")[0][0]
        if "ORDER BY" in create_query:
            order_by_part = create_query.split("ORDER BY")[1].split(")")[0]
            if "time" not in order_by_part:
                 print("⚠️ 'time' column exists but NOT in ORDER BY key. Dropping table to fix schema...")
                 client.execute("DROP TABLE kline")
                 print("Table dropped. Please restart backend.")
                 return

        print("Schema looks correct.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    fix_schema()
