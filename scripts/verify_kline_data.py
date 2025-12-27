"""
验证K线数据格式的脚本

用于检查东方财富返回的数据格式是否与代码中的解析逻辑一致
以及验证数据库中的数据是否正确
"""
import requests
from datetime import datetime
import sys
import os
import pandas as pd

# 添加backend目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))


def fetch_eastmoney_kline_raw(code: str, limit: int = 5):
    """直接获取东方财富K线数据（原始格式）"""
    code = str(code).strip().zfill(6)
    
    # 确定市场代码
    if code.startswith(('6',)):
        secid = f"1.{code}"
    else:
        secid = f"0.{code}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }
    
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",  # 日线
        "fqt": "0",    # 不复权
        "lmt": limit,
        "end": "20500101",
        "_": int(datetime.now().timestamp() * 1000),
    }
    
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    response = requests.get(url, params=params, headers=headers, timeout=30)
    data = response.json()
    
    if data.get("rc") != 0:
        print(f"接口返回错误: rc={data.get('rc')}")
        return None
    
    klines = data.get("data", {}).get("klines", [])
    name = data.get("data", {}).get("name", "")
    
    print(f"\n股票: {code} {name}")
    print(f"返回 {len(klines)} 条K线数据")
    print("\n原始数据格式分析:")
    print("=" * 80)
    
    for kline in klines[-3:]:  # 只显示最后3条
        parts = kline.split(",")
        print(f"\n原始数据: {kline}")
        print(f"  parts[0] (日期): {parts[0]}")
        print(f"  parts[1]: {parts[1]}")
        print(f"  parts[2]: {parts[2]}")
        print(f"  parts[3]: {parts[3]}")
        print(f"  parts[4]: {parts[4]}")
        print(f"  parts[5] (成交量): {parts[5]}")
        print(f"  parts[6] (成交额): {parts[6]}")
        print(f"  parts[7] (振幅): {parts[7]}")
        print(f"  parts[8] (涨跌幅): {parts[8]}")
        print(f"  parts[9] (涨跌额): {parts[9]}")
        print(f"  parts[10] (换手率): {parts[10]}")
        
        # 分析数据
        p1, p2, p3, p4 = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4])
        pct = float(parts[8])
        
        print(f"\n  数据分析:")
        print(f"    涨跌幅: {pct}%")
        
        # 根据涨跌幅推断收盘价
        # 如果 p2 是收盘价，那么昨收 = p2 / (1 + pct/100)
        if pct != 0:
            prev_close_if_p2 = p2 / (1 + pct/100)
            print(f"    如果 parts[2]={p2} 是收盘价，昨收应该是: {prev_close_if_p2:.2f}")
            
            prev_close_if_p4 = p4 / (1 + pct/100)
            print(f"    如果 parts[4]={p4} 是收盘价，昨收应该是: {prev_close_if_p4:.2f}")
        
        # 检查 high >= low
        print(f"\n    检查 high >= low:")
        print(f"      如果格式是 日期,开盘,收盘,最高,最低: high={p3}, low={p4}, high>=low? {p3 >= p4}")
        print(f"      如果格式是 日期,开盘,最高,最低,收盘: high={p2}, low={p3}, high>=low? {p2 >= p3}")
    
    return klines


def verify_db_kline_data(code: str):
    """验证数据库中的K线数据"""
    try:
        from common.db import get_kline_from_db
        
        print(f"\n验证数据库中的K线数据: {code}")
        print("=" * 80)
        
        kline_data = get_kline_from_db(code, None, None, "daily")
        
        if not kline_data:
            print(f"  数据库中没有K线数据")
            return
        
        print(f"  数据库中有 {len(kline_data)} 条K线数据")
        
        # 显示最后3条
        for item in kline_data[-3:]:
            print(f"\n  日期: {item.get('date')}")
            print(f"    open: {item.get('open')}")
            print(f"    high: {item.get('high')}")
            print(f"    low: {item.get('low')}")
            print(f"    close: {item.get('close')}")
            
            high = item.get('high', 0)
            low = item.get('low', 0)
            if high < low:
                print(f"    ⚠️ 警告: high < low，数据异常！")
            else:
                print(f"    ✓ high >= low，数据正常")
        
        # 测试 DataFrame 转换
        print(f"\n  测试 DataFrame 转换:")
        df = pd.DataFrame(kline_data)
        print(f"    DataFrame 列名: {list(df.columns)}")
        print(f"    最后一行数据:")
        last_row = df.iloc[-1]
        print(f"      open: {last_row['open']}")
        print(f"      high: {last_row['high']}")
        print(f"      low: {last_row['low']}")
        print(f"      close: {last_row['close']}")
        
        if last_row['high'] < last_row['low']:
            print(f"    ⚠️ DataFrame 中 high < low，数据异常！")
        else:
            print(f"    ✓ DataFrame 中 high >= low，数据正常")
            
    except Exception as e:
        import traceback
        print(f"  验证失败: {e}")
        traceback.print_exc()


def verify_db_indicator_data(code: str):
    """验证数据库中的指标数据"""
    try:
        from common.db import get_indicator
        
        print(f"\n验证数据库中的指标数据: {code}")
        print("=" * 80)
        
        indicator = get_indicator(code, "A", None, "daily")
        
        if not indicator:
            print(f"  数据库中没有指标数据")
            return
        
        print(f"  日期: {indicator.get('date')}")
        print(f"  current_price: {indicator.get('current_price')}")
        print(f"  current_open: {indicator.get('current_open')}")
        print(f"  current_high: {indicator.get('current_high')}")
        print(f"  current_low: {indicator.get('current_low')}")
        print(f"  current_close: {indicator.get('current_close')}")
        print(f"  recent_low: {indicator.get('recent_low')}")
        
        high = indicator.get('current_high', 0)
        low = indicator.get('current_low', 0)
        if high < low:
            print(f"  ⚠️ 警告: current_high < current_low，数据异常！")
        else:
            print(f"  ✓ current_high >= current_low，数据正常")
            
        recent_low = indicator.get('recent_low', 0)
        if recent_low == 0:
            print(f"  ⚠️ 警告: recent_low = 0，数据异常！")
        else:
            print(f"  ✓ recent_low = {recent_low}，数据正常")
            
    except Exception as e:
        import traceback
        print(f"  验证失败: {e}")
        traceback.print_exc()


def verify_indicator_calculation(code: str):
    """验证指标计算过程"""
    try:
        from common.db import get_kline_from_db
        from market.indicator.ta import calculate_all_indicators
        
        print(f"\n验证指标计算过程: {code}")
        print("=" * 80)
        
        # 获取K线数据
        kline_data = get_kline_from_db(code, None, None, "daily")
        
        if not kline_data or len(kline_data) < 60:
            print(f"  K线数据不足")
            return
        
        # 转换为DataFrame
        df = pd.DataFrame(kline_data)
        print(f"  DataFrame 形状: {df.shape}")
        print(f"  DataFrame 列名: {list(df.columns)}")
        
        # 显示最后一行的原始数据
        last_row = df.iloc[-1]
        print(f"\n  最后一行原始数据:")
        print(f"    date: {last_row.get('date', 'N/A')}")
        print(f"    open: {last_row['open']}")
        print(f"    high: {last_row['high']}")
        print(f"    low: {last_row['low']}")
        print(f"    close: {last_row['close']}")
        
        # 计算指标
        indicators = calculate_all_indicators(df)
        
        print(f"\n  计算后的指标:")
        print(f"    current_open: {indicators.get('current_open')}")
        print(f"    current_high: {indicators.get('current_high')}")
        print(f"    current_low: {indicators.get('current_low')}")
        print(f"    current_close: {indicators.get('current_close')}")
        print(f"    recent_low: {indicators.get('recent_low')}")
        
        # 验证
        if indicators.get('current_high', 0) < indicators.get('current_low', 0):
            print(f"  ⚠️ 计算后 current_high < current_low，问题出在指标计算！")
        else:
            print(f"  ✓ 计算后 current_high >= current_low，指标计算正常")
            
        if indicators.get('recent_low', 0) == 0:
            print(f"  ⚠️ 计算后 recent_low = 0，问题出在指标计算！")
        else:
            print(f"  ✓ 计算后 recent_low = {indicators.get('recent_low')}，指标计算正常")
            
    except Exception as e:
        import traceback
        print(f"  验证失败: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # 测试几只股票
    test_codes = ["300107", "600519", "000001"]
    
    print("=" * 80)
    print("第一部分：验证东方财富返回的数据格式")
    print("=" * 80)
    
    for code in test_codes:
        fetch_eastmoney_kline_raw(code)
        print("\n" + "=" * 80 + "\n")
    
    print("\n" + "=" * 80)
    print("第二部分：验证数据库中的K线数据")
    print("=" * 80)
    
    for code in test_codes:
        verify_db_kline_data(code)
    
    print("\n" + "=" * 80)
    print("第三部分：验证数据库中的指标数据")
    print("=" * 80)
    
    for code in test_codes:
        verify_db_indicator_data(code)
    
    print("\n" + "=" * 80)
    print("第四部分：验证指标计算过程")
    print("=" * 80)
    
    for code in test_codes:
        verify_indicator_calculation(code)
