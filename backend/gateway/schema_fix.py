
import logging
from clickhouse_driver import Client
from common.config import settings

logger = logging.getLogger(__name__)

def get_client():
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db
    )

def fix_kline_schema():
    """Check and fix kline table schema"""
    try:
        logger.info("Checking kline table schema...")
        client = get_client()
        
        # Check if 'kline' table exists
        exists = client.execute("EXISTS TABLE kline")
        if not exists or not exists[0][0]:
            logger.info("Table 'kline' does not exist. Skipping schema fix.")
            return

        # Check columns
        columns = client.execute("DESCRIBE kline")
        col_names = [col[0] for col in columns]
        
        if 'time' not in col_names:
            logger.warning("Column 'time' missing in kline table. Adding it...")
            # Add time column
            client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS time DateTime DEFAULT toDateTime(date) AFTER date")
            logger.info("Column 'time' added successfully.")
            
            # Clear invalid hourly data
            logger.warning("Clearing potentially corrupted hourly data (missing timestamps)...")
            # Note: mutation is async, but we send it off
            try:
                client.execute("ALTER TABLE kline DELETE WHERE period IN ('1h', '60', 'hourly')")
                logger.info("Hourly data cleanup mutation submitted.")
            except Exception as e:
                logger.error(f"Failed to clear hourly data: {e}")
        else:
            logger.info("Schema check passed: 'time' column exists.")
            
        client.disconnect()

    except Exception as e:
        logger.error(f"Failed to fix kline schema: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fix_kline_schema()
