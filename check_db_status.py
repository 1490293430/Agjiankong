
from common.db import _create_clickhouse_client
import json

def check_db():
    client = _create_clickhouse_client()
    try:
        # Check all periods for a sample stock (the one from logs)
        code = "300308"
        print(f"Checking data for stock {code}...")
        
        counts = client.execute("SELECT period, COUNT(*) FROM kline WHERE code = %(code)s GROUP BY period", {'code': code})
        print(f"Counts by period: {counts}")
        
        # Check some samples of period '1h' or 'hourly'
        samples = client.execute("SELECT period, date, time FROM kline WHERE code = %(code)s AND (period = '1h' OR period = 'hourly') ORDER BY date DESC, time DESC LIMIT 5")
        print(f"Sample hourly data: {samples}")
        
        # Check table structure
        cols = client.execute("DESCRIBE kline")
        print(f"Columns: {[c[0] for c in cols]}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    check_db()
