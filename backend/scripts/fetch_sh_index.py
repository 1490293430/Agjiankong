"""
单独采集上证指数并写入Redis
用法: python -m scripts.fetch_sh_index
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
from datetime import datetime
from common.redis import get_json, set_json

def fetch_sh_index():
    """采集上证指数并写入Redis"""
    print("正在采集上证指数...")
    
    try:
        # 获取指数实时行情
        index_df = ak.stock_zh_index_spot_em()
        
        # 找到上证指数 (代码1A0001)
        sh_row = index_df[index_df['代码'] == '1A0001']
        
        if sh_row.empty:
            print("未找到上证指数数据")
            return False
        
        row = sh_row.iloc[0]
        sh_index = {
            "code": "1A0001",
            "name": row.get("名称", "上证指数"),
            "price": float(row.get("最新价", 0) or 0),
            "pct": float(row.get("涨跌幅", 0) or 0),
            "change": float(row.get("涨跌额", 0) or 0),
            "volume": float(row.get("成交量", 0) or 0),
            "amount": float(row.get("成交额", 0) or 0),
            "high": float(row.get("最高", 0) or 0),
            "low": float(row.get("最低", 0) or 0),
            "open": float(row.get("今开", 0) or 0),
            "pre_close": float(row.get("昨收", 0) or 0),
            "update_time": datetime.now().isoformat(),
            "market": "A",
            "sec_type": "index"
        }
        
        print(f"上证指数: {sh_index['price']}点, 涨跌: {sh_index['pct']}%")
        
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
        return False

if __name__ == "__main__":
    fetch_sh_index()
