"""
诊断搜索结果无价格问题
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common.redis import get_json
from common.logger import get_logger

logger = get_logger(__name__)

def diagnose():
    """诊断搜索结果无价格问题"""
    print("=" * 60)
    print("诊断搜索结果无价格问题")
    print("=" * 60)
    
    # 1. 检查Redis中的A股数据
    print("\n[1] 检查Redis中的A股数据...")
    a_stocks = get_json("market:a:spot") or []
    print(f"   A股数据总数: {len(a_stocks)}")
    
    if a_stocks:
        # 检查前10只股票的price字段
        print("\n   前10只股票的price字段:")
        for i, stock in enumerate(a_stocks[:10]):
            code = stock.get("code", "")
            name = stock.get("name", "")
            price = stock.get("price")
            print(f"   [{i+1}] {code} {name}: price={price} (类型: {type(price).__name__})")
        
        # 统计有price和无price的股票数量
        with_price = sum(1 for s in a_stocks if s.get("price") is not None and s.get("price") != 0)
        without_price = len(a_stocks) - with_price
        print(f"\n   有价格的股票: {with_price}")
        print(f"   无价格的股票: {without_price}")
    else:
        print("   ⚠️ Redis中没有A股数据！")
    
    # 2. 检查Redis中的港股数据
    print("\n[2] 检查Redis中的港股数据...")
    hk_stocks = get_json("market:hk:spot") or []
    print(f"   港股数据总数: {len(hk_stocks)}")
    
    if hk_stocks:
        # 检查前10只股票的price字段
        print("\n   前10只股票的price字段:")
        for i, stock in enumerate(hk_stocks[:10]):
            code = stock.get("code", "")
            name = stock.get("name", "")
            price = stock.get("price")
            print(f"   [{i+1}] {code} {name}: price={price} (类型: {type(price).__name__})")
        
        # 统计有price和无price的股票数量
        with_price = sum(1 for s in hk_stocks if s.get("price") is not None and s.get("price") != 0)
        without_price = len(hk_stocks) - with_price
        print(f"\n   有价格的股票: {with_price}")
        print(f"   无价格的股票: {without_price}")
    else:
        print("   ⚠️ Redis中没有港股数据！")
    
    # 3. 测试搜索"000777"
    print("\n[3] 测试搜索'000777'...")
    keyword = "000777"
    all_stocks = a_stocks + hk_stocks
    
    results = []
    for stock in all_stocks:
        code = str(stock.get("code", "")).strip()
        if keyword in code:
            results.append(stock)
    
    print(f"   匹配到 {len(results)} 只股票:")
    for stock in results:
        code = stock.get("code", "")
        name = stock.get("name", "")
        price = stock.get("price")
        pct = stock.get("pct")
        volume = stock.get("volume")
        print(f"   - {code} {name}: price={price}, pct={pct}, volume={volume}")
    
    # 4. 检查数据采集时间
    print("\n[4] 检查数据采集时间...")
    from common.redis import get_redis
    redis = get_redis()
    
    a_time = redis.get("market:a:time")
    if a_time:
        a_time = a_time.decode('utf-8') if isinstance(a_time, bytes) else a_time
        print(f"   A股数据采集时间: {a_time}")
    else:
        print("   ⚠️ 没有A股数据采集时间记录")
    
    hk_time = redis.get("market:hk:time")
    if hk_time:
        hk_time = hk_time.decode('utf-8') if isinstance(hk_time, bytes) else hk_time
        print(f"   港股数据采集时间: {hk_time}")
    else:
        print("   ⚠️ 没有港股数据采集时间记录")
    
    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)

if __name__ == "__main__":
    diagnose()
