"""
修复指标数据脚本

检查并修复数据库中 current_high < current_low 或 recent_low = 0 的异常数据
"""
import sys
import os

# 添加backend目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))


def check_and_fix_indicators():
    """检查并修复指标数据"""
    try:
        from common.db import _create_clickhouse_client
        from common.logger import get_logger
        
        logger = get_logger(__name__)
        client = _create_clickhouse_client()
        
        print("=" * 80)
        print("检查指标数据中的异常")
        print("=" * 80)
        
        # 1. 检查 current_high < current_low 的记录
        query1 = """
            SELECT code, date, period, current_high, current_low, current_open, current_close
            FROM indicators FINAL
            WHERE current_high < current_low AND current_high > 0 AND current_low > 0
            ORDER BY date DESC
            LIMIT 100
        """
        result1 = client.execute(query1)
        
        print(f"\n发现 {len(result1)} 条 current_high < current_low 的异常记录:")
        for row in result1[:10]:
            print(f"  {row[0]} {row[1]} {row[2]}: high={row[3]}, low={row[4]}, open={row[5]}, close={row[6]}")
        
        if len(result1) > 10:
            print(f"  ... 还有 {len(result1) - 10} 条")
        
        # 2. 检查 recent_low = 0 的记录
        query2 = """
            SELECT code, date, period, recent_low, current_low
            FROM indicators FINAL
            WHERE recent_low = 0 AND current_low > 0
            ORDER BY date DESC
            LIMIT 100
        """
        result2 = client.execute(query2)
        
        print(f"\n发现 {len(result2)} 条 recent_low = 0 的异常记录:")
        for row in result2[:10]:
            print(f"  {row[0]} {row[1]} {row[2]}: recent_low={row[3]}, current_low={row[4]}")
        
        if len(result2) > 10:
            print(f"  ... 还有 {len(result2) - 10} 条")
        
        # 3. 检查K线数据中是否有 high < low 的记录
        query3 = """
            SELECT code, date, period, high, low, open, close
            FROM kline FINAL
            WHERE high < low AND high > 0 AND low > 0
            ORDER BY date DESC
            LIMIT 100
        """
        result3 = client.execute(query3)
        
        print(f"\n发现 {len(result3)} 条K线数据 high < low 的异常记录:")
        for row in result3[:10]:
            print(f"  {row[0]} {row[1]} {row[2]}: high={row[3]}, low={row[4]}, open={row[5]}, close={row[6]}")
        
        if len(result3) > 10:
            print(f"  ... 还有 {len(result3) - 10} 条")
        
        print("\n" + "=" * 80)
        print("修复建议")
        print("=" * 80)
        
        if len(result1) > 0 or len(result2) > 0:
            print("""
1. 如果K线数据正常但指标数据异常，需要重新计算指标：
   - 在系统中触发"批量计算指标"任务
   - 或者删除异常的指标记录，让系统重新计算

2. 如果K线数据本身就有问题，需要重新采集K线数据：
   - 删除异常的K线记录
   - 重新运行K线采集任务
""")
        
        if len(result3) > 0:
            print("""
3. K线数据中存在 high < low 的异常记录，这些数据需要修复：
   - 可能是数据源返回的数据有问题
   - 或者是数据解析时列顺序错误
   
   建议：删除这些异常K线记录，重新从数据源获取
""")
        
        # 询问是否要修复
        print("\n" + "=" * 80)
        print("是否要尝试修复？")
        print("=" * 80)
        print("1. 交换 current_high 和 current_low（如果它们被颠倒了）")
        print("2. 重新计算 recent_low（从K线数据）")
        print("3. 交换K线数据中的 high 和 low")
        print("0. 不修复，只查看")
        
        choice = input("\n请输入选项 (0-3): ").strip()
        
        if choice == "1":
            # 交换 current_high 和 current_low
            fix_query = """
                ALTER TABLE indicators UPDATE 
                    current_high = current_low,
                    current_low = current_high
                WHERE current_high < current_low AND current_high > 0 AND current_low > 0
            """
            client.execute(fix_query)
            print("已交换 current_high 和 current_low")
            
        elif choice == "2":
            print("重新计算 recent_low 需要从K线数据计算，建议运行批量指标计算任务")
            
        elif choice == "3":
            # 交换K线数据中的 high 和 low
            fix_query = """
                ALTER TABLE kline UPDATE 
                    high = low,
                    low = high
                WHERE high < low AND high > 0 AND low > 0
            """
            client.execute(fix_query)
            print("已交换K线数据中的 high 和 low")
        
        client.disconnect()
        
    except Exception as e:
        import traceback
        print(f"检查失败: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    check_and_fix_indicators()
