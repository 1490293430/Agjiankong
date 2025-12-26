"""
迁移脚本：为 indicators 表添加 period 字段
支持日线和小时线指标分别存储

使用方法：
docker exec stock_api python /app/scripts/migrate_indicators_add_period.py
"""
import os
import sys

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_driver import Client


def migrate():
    """执行迁移"""
    # 连接 ClickHouse
    client = Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
        database=os.getenv("CLICKHOUSE_DB", "stock"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )
    
    print("开始迁移 indicators 表...")
    
    # 1. 检查是否已经有 period 字段
    try:
        result = client.execute("DESCRIBE TABLE indicators")
        columns = [row[0] for row in result]
        if "period" in columns:
            print("✅ indicators 表已经有 period 字段，无需迁移")
            return True
    except Exception as e:
        print(f"检查表结构失败: {e}")
        return False
    
    print("检测到 indicators 表没有 period 字段，开始迁移...")
    
    # 2. 创建新表（带 period 字段）
    print("创建新表 indicators_new...")
    client.execute("""
    CREATE TABLE IF NOT EXISTS indicators_new
    (
        code String,
        market String,
        date Date,
        period String DEFAULT 'daily',
        ma5 Float64,
        ma10 Float64,
        ma20 Float64,
        ma60 Float64,
        ma5_trend String,
        ma10_trend String,
        ma20_trend String,
        ma60_trend String,
        ema12 Float64,
        ema26 Float64,
        macd_dif Float64,
        macd_dea Float64,
        macd Float64,
        macd_dif_trend String,
        macd_prev Float64,
        rsi Float64,
        bias6 Float64,
        bias12 Float64,
        bias24 Float64,
        boll_upper Float64,
        boll_middle Float64,
        boll_lower Float64,
        boll_expanding UInt8,
        boll_contracting UInt8,
        boll_width Float64,
        boll_width_prev Float64,
        kdj_k Float64,
        kdj_d Float64,
        kdj_j Float64,
        williams_r Float64,
        williams_r_prev Float64,
        adx Float64,
        plus_di Float64,
        minus_di Float64,
        adx_prev Float64,
        adx_rising UInt8,
        cci Float64,
        cci_prev Float64,
        cci_rising UInt8,
        cci_status String,
        ichimoku_tenkan Float64,
        ichimoku_kijun Float64,
        ichimoku_senkou_a Float64,
        ichimoku_senkou_b Float64,
        ichimoku_above_cloud UInt8,
        ichimoku_below_cloud UInt8,
        ichimoku_in_cloud UInt8,
        ichimoku_tk_cross_up UInt8,
        ichimoku_tk_cross_down UInt8,
        fib_swing_high Float64,
        fib_swing_low Float64,
        fib_236 Float64,
        fib_382 Float64,
        fib_500 Float64,
        fib_618 Float64,
        fib_786 Float64,
        fib_trend String,
        fib_current_level String,
        vol_ratio Float64,
        high_20d Float64,
        break_high_20d UInt8,
        recent_low Float64,
        current_price Float64,
        current_open Float64,
        current_high Float64,
        current_low Float64,
        current_close Float64,
        update_time DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(update_time)
    ORDER BY (code, market, date, period)
    """)
    
    # 3. 迁移数据（旧数据默认为 daily）
    print("迁移数据到新表...")
    client.execute("""
    INSERT INTO indicators_new
    SELECT 
        code, market, date, 'daily' as period,
        ma5, ma10, ma20, ma60,
        ma5_trend, ma10_trend, ma20_trend, ma60_trend,
        ema12, ema26,
        macd_dif, macd_dea, macd, macd_dif_trend, macd_prev, rsi,
        bias6, bias12, bias24,
        boll_upper, boll_middle, boll_lower, boll_expanding, boll_contracting, boll_width, boll_width_prev,
        kdj_k, kdj_d, kdj_j, williams_r, williams_r_prev,
        adx, plus_di, minus_di, adx_prev, adx_rising,
        cci, cci_prev, cci_rising, cci_status,
        ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, ichimoku_senkou_b,
        ichimoku_above_cloud, ichimoku_below_cloud, ichimoku_in_cloud, ichimoku_tk_cross_up, ichimoku_tk_cross_down,
        fib_swing_high, fib_swing_low, fib_236, fib_382, fib_500, fib_618, fib_786, fib_trend, fib_current_level,
        vol_ratio, high_20d, break_high_20d, recent_low,
        current_price, current_open, current_high, current_low, current_close,
        update_time
    FROM indicators
    """)
    
    # 4. 获取迁移的数据量
    result = client.execute("SELECT count() FROM indicators_new")
    new_count = result[0][0]
    result = client.execute("SELECT count() FROM indicators")
    old_count = result[0][0]
    print(f"迁移完成：旧表 {old_count} 条 -> 新表 {new_count} 条")
    
    # 5. 重命名表
    print("重命名表...")
    client.execute("RENAME TABLE indicators TO indicators_old")
    client.execute("RENAME TABLE indicators_new TO indicators")
    
    # 6. 删除旧表
    print("删除旧表...")
    client.execute("DROP TABLE IF EXISTS indicators_old")
    
    print("✅ 迁移完成！indicators 表现在支持 period 字段")
    return True


if __name__ == "__main__":
    try:
        success = migrate()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ 迁移失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
