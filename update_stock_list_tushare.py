#!/usr/bin/env python3
"""
使用Tushare数据源更新股票列表到数据库
在Docker容器内运行，使用系统已有的Tushare配置

注意：Tushare有频率限制（每小时1次），建议分开采集A股和港股

使用方法：
  python update_stock_list_tushare.py          # 采集A股和港股（需要2次API调用）
  python update_stock_list_tushare.py A        # 只采集A股
  python update_stock_list_tushare.py HK       # 只采集港股
"""
import sys
import os
import time

# 检测运行环境
if os.path.exists('/app'):
    # Docker容器内
    sys.path.insert(0, '/app')
    os.chdir('/app')
else:
    # 宿主机
    sys.path.insert(0, '/opt/Agjiankong/backend')
    os.chdir('/opt/Agjiankong/backend')

from datetime import datetime
from market_collector.tushare_source import fetch_stock_list_tushare, get_tushare_api
from market_collector.eastmoney_source import _classify_a_stock, _classify_hk_stock

# 解析命令行参数
market_param = sys.argv[1].upper() if len(sys.argv) > 1 else "ALL"
if market_param not in ["A", "HK", "ALL"]:
    print("错误：参数必须是 A、HK 或不传参数（采集全部）")
    print("使用方法：")
    print("  python update_stock_list_tushare.py          # 采集A股和港股")
    print("  python update_stock_list_tushare.py A        # 只采集A股")
    print("  python update_stock_list_tushare.py HK       # 只采集港股")
    sys.exit(1)

print("=" * 70)
if market_param == "ALL":
    print("开始更新股票列表到数据库（A股 + 港股）")
elif market_param == "A":
    print("开始更新股票列表到数据库（仅A股）")
else:
    print("开始更新股票列表到数据库（仅港股）")
print("=" * 70)

# ==================== 检查Tushare配置 ====================
print("\n[0/5] 检查Tushare配置...")
api = get_tushare_api()
if not api:
    print("  ✗ Tushare未配置或Token无效")
    print("  请在.env文件中配置TUSHARE_TOKEN")
    sys.exit(1)
print("  ✓ Tushare配置正常")
print("  ⚠ 注意：Tushare有频率限制，建议分开采集A股和港股")

# ==================== 获取A股列表 ====================
all_a_stocks = []
tushare_limited = False

if market_param in ["A", "ALL"]:
    print("\n[1/5] 使用Tushare获取A股列表...")
    
    try:
        # 使用Tushare获取A股列表
        tushare_stocks = fetch_stock_list_tushare()
        
        if tushare_stocks:
            print(f"  获取到 {len(tushare_stocks)} 条数据")
            
            # 遍历数据并过滤
            count_before = len(tushare_stocks)
            count_after = 0
            
            for stock in tushare_stocks:
                code = str(stock.get('code', ''))
                name = str(stock.get('name', ''))
                
                if not code or not name:
                    continue
                
                # 使用与K线采集相同的分类函数
                sec_type = _classify_a_stock(code, name)
                
                # 只保留股票类型
                if sec_type != "stock":
                    continue
                
                all_a_stocks.append({
                    "code": code,
                    "name": name,
                    "market": "A",
                    "sec_type": "stock"
                })
                count_after += 1
            
            print(f"  ✓ A股: {count_before}条（过滤后: {count_after}只股票）")
        else:
            print("  ✗ Tushare返回数据为空")
        
    except Exception as e:
        error_msg = str(e)
        if "抱歉，您每分钟最多访问该接口" in error_msg or "每小时" in error_msg or "频率限制" in error_msg:
            print(f"  ⚠ Tushare频率限制: {error_msg}")
            print("  提示：请等待一段时间后再试")
            tushare_limited = True
        else:
            print(f"  ✗ A股获取失败: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n  总计: {len(all_a_stocks)} 只A股")
else:
    print("\n[1/5] 跳过A股采集（参数指定只采集港股）")

# ==================== 获取港股列表 ====================
all_hk_stocks = []

if market_param in ["HK", "ALL"]:
    print("\n[2/5] 使用Tushare获取港股列表...")
    
    # 如果A股已经遇到频率限制，跳过港股
    if tushare_limited:
        print("  ⚠ 由于Tushare频率限制，跳过港股获取")
    else:
        try:
            # Tushare获取港股列表
            print("  正在获取港股数据...")
            
            # 如果是采集全部，添加延迟避免频率限制
            if market_param == "ALL":
                time.sleep(1)
            
            # 获取港股列表
            df = api.hk_basic(list_status='L', fields='ts_code,name')
            
            if df is not None and len(df) > 0:
                print(f"  获取到 {len(df)} 条数据")
                
                count_before = len(df)
                count_after = 0
                
                for _, row in df.iterrows():
                    ts_code = str(row.get('ts_code', ''))
                    name = str(row.get('name', ''))
                    
                    # 提取代码（去掉.HK后缀）
                    code = ts_code.split('.')[0] if '.' in ts_code else ts_code
                    
                    if not code or not name:
                        continue
                    
                    # 使用与K线采集相同的分类函数
                    sec_type = _classify_hk_stock(code, name)
                    
                    # 只保留股票类型
                    if sec_type != "stock":
                        continue
                    
                    all_hk_stocks.append({
                        "code": code,
                        "name": name,
                        "market": "HK",
                        "sec_type": "stock"
                    })
                    count_after += 1
                
                print(f"  ✓ 港股: {count_before}条（过滤后: {count_after}只股票）")
            else:
                print("  ✗ Tushare返回港股数据为空")
            
        except Exception as e:
            error_msg = str(e)
            if "抱歉，您每分钟最多访问该接口" in error_msg or "每小时" in error_msg or "频率限制" in error_msg:
                print(f"  ⚠ Tushare频率限制: {error_msg}")
                print("  提示：请等待一段时间后再试")
            else:
                print(f"  ✗ 港股获取失败: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n  总计: {len(all_hk_stocks)} 只港股")
else:
    print("\n[2/5] 跳过港股采集（参数指定只采集A股）")

# ==================== 保存到数据库 ====================
print("\n[3/5] 保存到ClickHouse数据库...")

try:
    from common.db import save_stock_info_batch
    
    if all_a_stocks:
        count = save_stock_info_batch(all_a_stocks, "A")
        print(f"  ✓ A股保存成功: {count}只")
    else:
        print("  - A股无数据，跳过保存")
    
    if all_hk_stocks:
        count = save_stock_info_batch(all_hk_stocks, "HK")
        print(f"  ✓ 港股保存成功: {count}只")
    else:
        print("  - 港股无数据，跳过保存")
        
except Exception as e:
    print(f"  ✗ 保存失败: {e}")
    import traceback
    traceback.print_exc()

# ==================== 验证 ====================
print("\n[4/5] 验证数据库...")

try:
    from common.db import get_clickhouse
    client = get_clickhouse()
    
    result_a = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'A'")
    a_count = result_a[0][0] if result_a else 0
    
    result_hk = client.execute("SELECT COUNT(DISTINCT code) FROM stock_info WHERE market = 'HK'")
    hk_count = result_hk[0][0] if result_hk else 0
    
    print(f"  ✓ 数据库中A股: {a_count}只")
    print(f"  ✓ 数据库中港股: {hk_count}只")
    
except Exception as e:
    print(f"  ✗ 验证失败: {e}")

# ==================== 提示信息 ====================
print("\n[5/5] 完成提示...")

if tushare_limited:
    print("\n" + "!" * 70)
    print("⚠ 遇到Tushare频率限制，部分数据未获取")
    print("!" * 70)
    print("\n解决方案：")
    print("1. 等待1小时后重新运行此脚本")
    print("2. 分开采集A股和港股：")
    print("   docker exec stock_api python update_stock_list_tushare.py A   # 先采集A股")
    print("   # 等待1小时后...")
    print("   docker exec stock_api python update_stock_list_tushare.py HK  # 再采集港股")
    print("3. 当前数据库中已有 {0} 只A股，{1} 只港股".format(a_count if 'a_count' in locals() else 0, hk_count if 'hk_count' in locals() else 0))
elif len(all_a_stocks) == 0 and len(all_hk_stocks) == 0:
    print("\n" + "!" * 70)
    print("⚠ 未获取到任何股票数据")
    print("!" * 70)
    print("\n可能原因：")
    print("1. Tushare Token无效或已过期")
    print("2. 网络连接问题")
    print("3. Tushare服务暂时不可用")
else:
    print("\n" + "=" * 70)
    print("✓ 股票列表更新成功！")
    print("=" * 70)
    if market_param == "ALL":
        print("\n提示：如果遇到频率限制，建议下次分开采集：")
        print("  docker exec stock_api python update_stock_list_tushare.py A   # 只采集A股")
        print("  docker exec stock_api python update_stock_list_tushare.py HK  # 只采集港股")

print("\n" + "=" * 70)
print("更新完成！")
print("=" * 70)
