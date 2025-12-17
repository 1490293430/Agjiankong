"""
数据库连接模块（ClickHouse）
"""
from clickhouse_driver import Client
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)

_client: Client = None


def get_clickhouse() -> Client:
    """获取ClickHouse连接"""
    global _client
    
    if _client is None:
        try:
            _client = Client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_db,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password
            )
            # 测试连接
            _client.execute("SELECT 1")
            logger.info(f"ClickHouse连接成功: {settings.clickhouse_host}:{settings.clickhouse_port}")
        except Exception as e:
            logger.error(f"ClickHouse连接失败: {e}")
            raise
    
    return _client


def init_tables():
    """初始化数据表"""
    client = get_clickhouse()
    
    # K线表
    client.execute("""
        CREATE TABLE IF NOT EXISTS kline
        (
            code String,
            date Date,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            amount Float64
        )
        ENGINE = MergeTree()
        ORDER BY (code, date)
    """)
    
    logger.info("数据表初始化完成")

