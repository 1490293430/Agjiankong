"""检查数据库数据"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clickhouse_driver import Client
from common.config import settings
from common.redis import get_json

client = Client(
    host=settings.clickhouse_host,
    port=settings.clickhouse_port,
    database=settings.clickhouse_db,
    user=settings.clickhouse_user,
    password=settings.clickhouse_password
)

print("=== Redis 缓存 ===")
a_spot = get_json('market:a:spot') or []
hk_spot = get_json('market:hk:spot') or []

a_stock = len([i for i in a_spot if i.get('sec_type') == 'stock'])
hk_stock = len([i for i in hk_spot if i.get('sec_type') == 'stock'])

print(f"A股: {len(a_spot)} 条, 其中股票 {a_stock} 条")
print(f"港股: {len(hk_spot)} 条, 其中股票 {hk_stock} 条")

print("\n=== kline 表 ===")
result = client.execute("SELECT count(), count(DISTINCT code) FROM kline")
print(f"总记录: {result[0][0]}, 股票数: {result[0][1]}")

# 按代码长度统计
result = client.execute("SELECT length(code) as len, count() FROM kline GROUP BY len ORDER BY len")
print("\n按代码长度:")
for row in result:
    print(f"  {row[0]}位: {row[1]}")

# 6位代码前缀
result = client.execute("SELECT substring(code,1,3) as p, count() FROM kline WHERE length(code)=6 GROUP BY p ORDER BY count() DESC LIMIT 15")
print("\n6位代码前缀(A股):")
for row in result:
    print(f"  {row[0]}: {row[1]}")

# 5位代码前缀
result = client.execute("SELECT substring(code,1,2) as p, count() FROM kline WHERE length(code)=5 GROUP BY p ORDER BY count() DESC LIMIT 15")
print("\n5位代码前缀(港股):")
for row in result:
    print(f"  {row[0]}: {row[1]}")
