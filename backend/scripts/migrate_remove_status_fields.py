"""
数据库迁移脚本：移除indicators表中的状态判断字段

这些字段原本用于预计算状态，现在改为让AI自己判断，因此不再需要：
- ma5_trend, ma10_trend, ma20_trend, ma60_trend
- macd_dif_trend
- boll_expanding, boll_contracting
- adx_rising
- cci_rising, cci_status
- ichimoku_above_cloud, ichimoku_below_cloud, ichimoku_in_cloud
- ichimoku_tk_cross_up, ichimoku_tk_cross_down
- fib_trend, fib_current_level

运行方式：
    python -m scripts.migrate_remove_status_fields
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import _create_clickhouse_client
from common.logger import get_logger

logger = get_logger(__name__)

# 需要删除的字段列表
FIELDS_TO_DROP = [
    # MA趋势字段
    "ma5_trend",
    "ma10_trend", 
    "ma20_trend",
    "ma60_trend",
    # MACD趋势字段
    "macd_dif_trend",
    # 布林带状态字段
    "boll_expanding",
    "boll_contracting",
    # ADX状态字段
    "adx_rising",
    # CCI状态字段
    "cci_rising",
    "cci_status",
    # 一目均衡表状态字段
    "ichimoku_above_cloud",
    "ichimoku_below_cloud",
    "ichimoku_in_cloud",
    "ichimoku_tk_cross_up",
    "ichimoku_tk_cross_down",
    # 斐波那契状态字段
    "fib_trend",
    "fib_current_level",
]


def migrate():
    """执行迁移：删除状态字段"""
    client = None
    try:
        client = _create_clickhouse_client()
        
        print("=" * 60)
        print("开始迁移：移除indicators表中的状态判断字段")
        print("=" * 60)
        
        success_count = 0
        skip_count = 0
        fail_count = 0
        
        for field in FIELDS_TO_DROP:
            try:
                # ClickHouse的DROP COLUMN语法
                sql = f"ALTER TABLE indicators DROP COLUMN IF EXISTS {field}"
                client.execute(sql)
                print(f"✓ 已删除字段: {field}")
                success_count += 1
            except Exception as e:
                error_msg = str(e)
                if "doesn't exist" in error_msg.lower() or "not found" in error_msg.lower():
                    print(f"- 字段不存在，跳过: {field}")
                    skip_count += 1
                else:
                    print(f"✗ 删除字段失败 {field}: {e}")
                    fail_count += 1
        
        print("=" * 60)
        print(f"迁移完成: 成功={success_count}, 跳过={skip_count}, 失败={fail_count}")
        print("=" * 60)
        
        # 优化表（可选，清理已删除列的空间）
        try:
            print("\n正在优化表结构...")
            client.execute("OPTIMIZE TABLE indicators FINAL")
            print("✓ 表优化完成")
        except Exception as e:
            print(f"表优化失败（不影响使用）: {e}")
        
        return fail_count == 0
        
    except Exception as e:
        logger.error(f"迁移失败: {e}", exc_info=True)
        print(f"\n迁移失败: {e}")
        return False
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
