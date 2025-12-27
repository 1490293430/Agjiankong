"""
采集上证指数历史K线并写入ClickHouse
用法: python -m scripts.fetch_sh_index_kline [--days 365]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import datetime, timedelta
from common.db import get_client
from common.logger import get_logger

logger = get_logger(__name__)


def fetch_sh_index_kline(days: int = 365):
    """采集上证指数历史K线（东方财富接口）
    
    Args:
        days: 获取最近多少天的数据，默认365天
    """
    print(f"正在采集上证指数历史K线（最近{days}天）...")
    
    try:
        # 东方财富K线接口
        # secid: 1.000001 = 上证指数
        # klt: 101=日线, 102=周线, 103=月线, 60=60分钟
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        params = {
            "secid": "1.000001",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",  # 日线
            "fqt": "1",    # 前复权
            "beg": start_date,
            "end": end_date,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "lmt": days
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        data = resp.json()
        
        if data.get("rc") != 0 or not data.get("data"):
            print(f"接口返回错误: {data}")
            return False
        
        klines = data["data"].get("klines", [])
        if not klines:
            print("未获取到K线数据")
            return False
        
        print(f"获取到 {len(klines)} 条K线数据")
        
        # 解析K线数据
        # 格式: 日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
        records = []
        for kline in klines:
            parts = kline.split(",")
            if len(parts) >= 7:
                records.append({
                    "code": "1A0001",
                    "market": "A",
                    "date": parts[0],
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]),
                    "pct": float(parts[8]) if len(parts) > 8 else 0,
                })
        
        if not records:
            print("解析K线数据失败")
            return False
        
        print(f"解析成功，准备写入数据库...")
        print(f"日期范围: {records[0]['date']} ~ {records[-1]['date']}")
        print(f"最新收盘: {records[-1]['close']:.2f}点")
        
        # 写入ClickHouse
        client = get_client()
        
        # 先删除旧数据
        client.command(f"ALTER TABLE kline_daily DELETE WHERE code = '1A0001' AND market = 'A'")
        print("已清除旧数据")
        
        # 插入新数据
        df = pd.DataFrame(records)
        client.insert_df("kline_daily", df)
        
        print(f"已写入 {len(records)} 条K线数据到 kline_daily 表")
        return True
        
    except Exception as e:
        print(f"采集失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def fetch_sh_index_kline_hourly(days: int = 30):
    """采集上证指数小时K线
    
    Args:
        days: 获取最近多少天的数据，默认30天
    """
    print(f"正在采集上证指数小时K线（最近{days}天）...")
    
    try:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        params = {
            "secid": "1.000001",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "60",   # 60分钟线
            "fqt": "1",
            "beg": start_date,
            "end": end_date,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "lmt": days * 4  # 每天约4根小时K线
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        data = resp.json()
        
        if data.get("rc") != 0 or not data.get("data"):
            print(f"接口返回错误: {data}")
            return False
        
        klines = data["data"].get("klines", [])
        if not klines:
            print("未获取到小时K线数据")
            return False
        
        print(f"获取到 {len(klines)} 条小时K线数据")
        
        records = []
        for kline in klines:
            parts = kline.split(",")
            if len(parts) >= 7:
                # 小时K线的时间格式: 2024-01-01 10:00
                time_str = parts[0]
                date_str = time_str.split(" ")[0] if " " in time_str else time_str
                
                records.append({
                    "code": "1A0001",
                    "market": "A",
                    "time": time_str,
                    "date": date_str,
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]),
                })
        
        if not records:
            print("解析小时K线数据失败")
            return False
        
        print(f"解析成功，准备写入数据库...")
        print(f"时间范围: {records[0]['time']} ~ {records[-1]['time']}")
        
        # 写入ClickHouse
        client = get_client()
        
        # 先删除旧数据
        client.command(f"ALTER TABLE kline_1h DELETE WHERE code = '1A0001' AND market = 'A'")
        print("已清除旧的小时K线数据")
        
        # 插入新数据
        df = pd.DataFrame(records)
        client.insert_df("kline_1h", df)
        
        print(f"已写入 {len(records)} 条小时K线数据到 kline_1h 表")
        return True
        
    except Exception as e:
        print(f"采集小时K线失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="采集上证指数历史K线")
    parser.add_argument("--days", type=int, default=365, help="获取最近多少天的日线数据")
    parser.add_argument("--hourly", action="store_true", help="同时获取小时K线")
    parser.add_argument("--hourly-days", type=int, default=30, help="小时K线获取天数")
    args = parser.parse_args()
    
    # 获取日线
    fetch_sh_index_kline(args.days)
    
    # 获取小时线
    if args.hourly:
        print("\n" + "="*50 + "\n")
        fetch_sh_index_kline_hourly(args.hourly_days)
