"""
单独采集上证指数并写入Redis（使用东方财富接口）
用法: python -m scripts.fetch_sh_index
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from datetime import datetime
from common.redis import get_json, set_json

def fetch_sh_index():
    """采集上证指数并写入Redis（东方财富接口）"""
    print("正在采集上证指数（东方财富）...")
    
    try:
        # 东方财富上证指数实时行情接口
        # 1.000001 = 上证指数
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "secid": "1.000001",  # 上证指数
            "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f60,f169,f170",
            "ut": "fa5fd1943c7b386f172d6893dbfba10b"
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://quote.eastmoney.com/"
        }
        
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        
        if data.get("rc") != 0 or not data.get("data"):
            print(f"接口返回错误: {data}")
            return False
        
        d = data["data"]
        # f43=最新价(需除100), f44=最高, f45=最低, f46=今开, f60=昨收
        # f47=成交量, f48=成交额, f57=代码, f58=名称
        # f169=涨跌额, f170=涨跌幅
        
        sh_index = {
            "code": "1A0001",
            "name": d.get("f58", "上证指数"),
            "price": d.get("f43", 0) / 100 if d.get("f43") else 0,
            "pct": d.get("f170", 0) / 100 if d.get("f170") else 0,
            "change": d.get("f169", 0) / 100 if d.get("f169") else 0,
            "high": d.get("f44", 0) / 100 if d.get("f44") else 0,
            "low": d.get("f45", 0) / 100 if d.get("f45") else 0,
            "open": d.get("f46", 0) / 100 if d.get("f46") else 0,
            "pre_close": d.get("f60", 0) / 100 if d.get("f60") else 0,
            "volume": d.get("f47", 0),
            "amount": d.get("f48", 0),
            "update_time": datetime.now().isoformat(),
            "market": "A",
            "sec_type": "index"
        }
        
        print(f"上证指数: {sh_index['price']:.2f}点, 涨跌: {sh_index['pct']:.2f}%")
        
        # 读取现有数据
        a_spot = get_json("market:a:spot") or []
        
        # 移除旧的上证指数数据（如果有）
        a_spot = [item for item in a_spot if str(item.get('code', '')) != '1A0001']
        
        # 添加新的上证指数数据
        a_spot.append(sh_index)
        
        # 写回Redis
        set_json("market:a:spot", a_spot, ex=30 * 24 * 3600)
        
        print(f"已写入Redis, 当前market:a:spot共 {len(a_spot)} 条数据")
        return True
        
    except Exception as e:
        print(f"采集失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fetch_sh_index()
