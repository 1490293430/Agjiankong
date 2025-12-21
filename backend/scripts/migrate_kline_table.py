#!/usr/bin/env python3
"""
K线表自动迁移脚本
将 kline 表从 MergeTree 迁移到 ReplacingMergeTree，避免 mutation 堆积问题

使用方法：
    python scripts/migrate_kline_table.py
    或
    python -m scripts.migrate_kline_table
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from clickhouse_driver import Client
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)


def create_clickhouse_client() -> Client:
    """创建 ClickHouse 连接"""
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=10,
        send_receive_timeout=30
    )


def check_table_exists(client: Client) -> bool:
    """检查表是否存在"""
    try:
        result = client.execute("""
            SELECT count() 
            FROM system.tables 
            WHERE database = %(db)s AND name = 'kline'
        """, {'db': settings.clickhouse_db})
        return result[0][0] > 0 if result else False
    except Exception as e:
        logger.error(f"检查表是否存在失败: {e}")
        return False


def get_table_engine(client: Client) -> str | None:
    """获取表的引擎类型"""
    try:
        result = client.execute("""
            SELECT engine 
            FROM system.tables 
            WHERE database = %(db)s AND name = 'kline'
        """, {'db': settings.clickhouse_db})
        return result[0][0] if result and len(result) > 0 else None
    except Exception as e:
        logger.error(f"获取表引擎失败: {e}")
        return None


def get_table_columns(client: Client) -> list[str]:
    """获取表的所有列名"""
    try:
        result = client.execute("DESCRIBE kline")
        return [row[0] for row in result] if result else []
    except Exception as e:
        logger.error(f"获取表列信息失败: {e}")
        return []


def get_table_row_count(client: Client) -> int:
    """获取表的行数"""
    try:
        result = client.execute("SELECT count() FROM kline")
        return result[0][0] if result and len(result) > 0 else 0
    except Exception as e:
        logger.error(f"获取表行数失败: {e}")
        return 0


def add_missing_columns(client: Client) -> bool:
    """添加缺失的字段"""
    columns = get_table_columns(client)
    success = True
    
    # 添加 update_time 字段
    if "update_time" not in columns:
        logger.info("正在添加 update_time 字段...")
        try:
            client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS update_time DateTime DEFAULT now()")
            logger.info("✓ update_time 字段添加成功")
        except Exception as e:
            logger.error(f"添加 update_time 字段失败: {e}")
            success = False
    
    # 添加 period 字段
    if "period" not in columns:
        logger.info("正在添加 period 字段...")
        try:
            client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS period String DEFAULT 'daily'")
            logger.info("✓ period 字段添加成功")
        except Exception as e:
            logger.error(f"添加 period 字段失败: {e}")
            success = False
    
    return success


def migrate_table_engine(client: Client) -> bool:
    """迁移表引擎到 ReplacingMergeTree"""
    logger.info("=" * 60)
    logger.info("开始迁移表引擎...")
    logger.info("=" * 60)
    
    # 检查当前引擎
    current_engine = get_table_engine(client)
    logger.info(f"当前表引擎: {current_engine}")
    
    if current_engine == 'ReplacingMergeTree':
        logger.info("✓ 表已经是 ReplacingMergeTree 引擎，无需迁移")
        return True
    
    # 获取表行数
    row_count = get_table_row_count(client)
    logger.info(f"表当前行数: {row_count:,}")
    
    if row_count == 0:
        logger.info("表为空，直接重建表...")
        try:
            # 先创建新表结构
            create_table_sql = """
            CREATE TABLE kline_new
            (
                code String,
                period String,
                date Date,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Float64,
                amount Float64,
                update_time DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (code, period, date)
            """
            client.execute(create_table_sql)
            logger.info("✓ 新表创建成功")
            
            # 删除旧表
            client.execute("DROP TABLE kline")
            logger.info("✓ 旧表已删除")
            
            # 重命名新表
            client.execute("RENAME TABLE kline_new TO kline")
            logger.info("✓ 表重建完成")
            return True
        except Exception as e:
            logger.error(f"重建空表失败: {e}")
            return False
    
    # 获取表结构
    columns = get_table_columns(client)
    logger.info(f"表字段: {', '.join(columns)}")
    
    # 确保有必要的字段
    if "update_time" not in columns or "period" not in columns:
        logger.warning("缺少必要字段，先添加字段...")
        if not add_missing_columns(client):
            logger.error("添加字段失败，无法继续迁移")
            return False
    
    # 重建表（使用 ReplacingMergeTree）
    logger.info("开始重建表...")
    logger.info("步骤 1/4: 创建新表 kline_new...")
    
    try:
        # 创建新表
        create_table_sql = """
        CREATE TABLE kline_new
        (
            code String,
            period String,
            date Date,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            amount Float64,
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, period, date)
        """
        client.execute(create_table_sql)
        logger.info("✓ 新表创建成功")
    except Exception as e:
        logger.error(f"创建新表失败: {e}")
        return False
    
    # 迁移数据
    logger.info("步骤 2/4: 迁移数据到新表...")
    try:
        # 检查是否有 period 字段
        columns = get_table_columns(client)
        if "period" in columns:
            migrate_sql = """
            INSERT INTO kline_new (code, period, date, open, high, low, close, volume, amount, update_time)
            SELECT code, period, date, open, high, low, close, volume, amount, 
                   COALESCE(update_time, now()) as update_time
            FROM kline
            """
        else:
            migrate_sql = """
            INSERT INTO kline_new (code, period, date, open, high, low, close, volume, amount, update_time)
            SELECT code, 'daily' as period, date, open, high, low, close, volume, amount,
                   COALESCE(update_time, now()) as update_time
            FROM kline
            """
        
        client.execute(migrate_sql)
        
        # 验证数据迁移
        new_count = client.execute("SELECT count() FROM kline_new")[0][0]
        logger.info(f"✓ 数据迁移完成，新表行数: {new_count:,}")
        
        if new_count != row_count:
            logger.warning(f"⚠️ 数据行数不匹配！旧表: {row_count:,}, 新表: {new_count:,}")
            logger.warning("请检查数据迁移是否完整")
            
            # 询问是否继续
            response = input("是否继续替换旧表？(yes/no): ").strip().lower()
            if response != 'yes':
                logger.info("迁移已取消，新表 kline_new 已保留，请手动检查")
                return False
        
    except Exception as e:
        logger.error(f"数据迁移失败: {e}")
        logger.error("新表 kline_new 已创建，请手动检查后删除")
        return False
    
    # 备份旧表
    logger.info("步骤 3/4: 备份旧表为 kline_backup...")
    try:
        client.execute("RENAME TABLE kline TO kline_backup")
        logger.info("✓ 旧表已备份为 kline_backup")
    except Exception as e:
        logger.error(f"备份旧表失败: {e}")
        logger.error("请手动处理，新表 kline_new 已创建")
        return False
    
    # 重命名新表
    logger.info("步骤 4/4: 将新表重命名为 kline...")
    try:
        client.execute("RENAME TABLE kline_new TO kline")
        logger.info("✓ 新表已重命名为 kline")
    except Exception as e:
        logger.error(f"重命名新表失败: {e}")
        logger.error("请手动执行: RENAME TABLE kline_new TO kline")
        return False
    
    logger.info("=" * 60)
    logger.info("✓ 表引擎迁移完成！")
    logger.info("=" * 60)
    logger.info("建议：")
    logger.info("  1. 验证数据完整性")
    logger.info("  2. 确认无误后，可以删除备份表: DROP TABLE kline_backup")
    logger.info("  3. 重启应用，让 init_tables() 确认表结构")
    
    return True


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("K线表自动迁移脚本")
    logger.info("=" * 60)
    
    try:
        client = create_clickhouse_client()
        logger.info(f"✓ ClickHouse 连接成功: {settings.clickhouse_host}:{settings.clickhouse_port}")
    except Exception as e:
        logger.error(f"✗ ClickHouse 连接失败: {e}")
        logger.error("请检查配置和网络连接")
        return 1
    
    # 检查表是否存在
    if not check_table_exists(client):
        logger.warning("表 kline 不存在，将在应用启动时自动创建")
        return 0
    
    # 检查并添加缺失字段
    logger.info("检查表结构...")
    if not add_missing_columns(client):
        logger.error("添加字段失败，请检查错误信息")
        return 1
    
    # 迁移表引擎
    if not migrate_table_engine(client):
        logger.error("表引擎迁移失败")
        return 1
    
    logger.info("=" * 60)
    logger.info("✓ 迁移完成！")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        logger.error(f"未预期的错误: {e}", exc_info=True)
        sys.exit(1)

