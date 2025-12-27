"""
测试指标计算逻辑
"""
import pandas as pd
import sys
import os

# 添加backend目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

# 模拟K线数据（使用东方财富返回的真实数据格式）
# 格式：日期,开盘,收盘,最高,最低,成交量,成交额,...
raw_data = [
    "2025-12-24,6.72,6.79,6.81,6.66,28609,19373512.00",
    "2025-12-25,6.79,6.82,6.85,6.77,32642,22262503.00",
    "2025-12-26,6.87,7.05,7.31,6.82,180736,128306329.70",
]

# 解析数据（模拟 eastmoney_source.py 的解析逻辑）
kline_data = []
for line in raw_data:
    parts = line.split(",")
    item = {
        "code": "300107",
        "date": parts[0],
        "open": float(parts[1]),
        "close": float(parts[2]),
        "high": float(parts[3]),
        "low": float(parts[4]),
        "volume": float(parts[5]),
        "amount": float(parts[6]),
    }
    kline_data.append(item)

print("=" * 80)
print("模拟K线数据（解析后）:")
print("=" * 80)
for item in kline_data:
    print(f"  日期: {item['date']}")
    print(f"    open: {item['open']}, close: {item['close']}")
    print(f"    high: {item['high']}, low: {item['low']}")
    print(f"    high >= low? {item['high'] >= item['low']}")
    print()

# 转换为DataFrame
df = pd.DataFrame(kline_data)

print("=" * 80)
print("DataFrame 内容:")
print("=" * 80)
print(df[['date', 'open', 'close', 'high', 'low']])
print()

# 检查最后一行
latest = df.iloc[-1]
print("=" * 80)
print("最后一行数据:")
print("=" * 80)
print(f"  date: {latest['date']}")
print(f"  open: {latest['open']}")
print(f"  close: {latest['close']}")
print(f"  high: {latest['high']}")
print(f"  low: {latest['low']}")
print(f"  high >= low? {latest['high'] >= latest['low']}")
print()

# 模拟 calculate_all_indicators 中的计算
print("=" * 80)
print("模拟指标计算:")
print("=" * 80)

# 最近5天的最低点（这里只有3天数据，用全部）
recent_lows = df["low"].tail(5)
recent_low = float(recent_lows.min())
current_low = float(latest["low"])
current_high = float(latest["high"])
current_open = float(latest["open"])
current_close = float(latest["close"])

print(f"  recent_low (最近5天最低): {recent_low}")
print(f"  current_low: {current_low}")
print(f"  current_high: {current_high}")
print(f"  current_open: {current_open}")
print(f"  current_close: {current_close}")
print()

# 验证
if current_high < current_low:
    print("⚠️ 错误: current_high < current_low!")
else:
    print("✓ 正确: current_high >= current_low")

if recent_low == 0:
    print("⚠️ 错误: recent_low = 0!")
else:
    print(f"✓ 正确: recent_low = {recent_low}")

print()
print("=" * 80)
print("结论:")
print("=" * 80)
print("如果上面的计算结果正确，说明问题不在指标计算逻辑，")
print("而是在数据库中存储的数据本身有问题。")
print()
print("可能的原因:")
print("1. 数据库中的K线数据在保存时就已经错误")
print("2. 有其他数据源（如akshare）的数据格式不同")
print("3. 数据库表结构的列顺序与代码不匹配")
