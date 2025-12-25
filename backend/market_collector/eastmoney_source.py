"""
东方财富实时行情数据源（并发版本）
直接请求东方财富接口，支持并发采集
"""
import requests
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Tuple
from common.logger import get_logger

logger = get_logger(__name__)

# 东方财富行情接口
EASTMONEY_API_URL = "http://push2.eastmoney.com/api/qt/clist/get"


def fetch_eastmoney_a_stock_spot(max_retries: int = 2) -> List[Dict[str, Any]]:
    """
    从东方财富获取A股实时行情（并发版本）
    
    Args:
        max_retries: 最大重试次数
    
    Returns:
        股票行情列表
    """
    for attempt in range(max_retries):
        try:
            logger.info("[东方财富] 开始并发获取A股实时行情...")
            
            # A股分为：沪市主板(fs=m:1+t:2)、深市主板(fs=m:0+t:6)、创业板(fs=m:0+t:80)、科创板(fs=m:1+t:23)、北交所(fs=m:0+t:81)
            markets = [
                ("沪市主板", "m:1+t:2"),  # 沪市主板
                ("科创板", "m:1+t:23"),  # 科创板
                ("深市主板", "m:0+t:6"),  # 深市主板
                ("中小板", "m:0+t:13"),  # 中小板
                ("创业板", "m:0+t:80"),  # 创业板
                ("北交所", "m:0+t:81"),  # 北交所
            ]
            
            all_results = []
            
            # 并发请求各市场数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                futures = {
                    executor.submit(_fetch_eastmoney_market, name, fs): name 
                    for name, fs in markets
                }
                
                for future in concurrent.futures.as_completed(futures, timeout=120):
                    market_name = futures[future]
                    try:
                        results = future.result()
                        if results:
                            all_results.extend(results)
                            logger.info(f"[东方财富] {market_name}获取成功: {len(results)}只")
                    except Exception as e:
                        logger.warning(f"[东方财富] {market_name}获取失败: {e}")
            
            if all_results:
                # 去重（按code）
                seen = set()
                unique_results = []
                for item in all_results:
                    if item['code'] not in seen:
                        seen.add(item['code'])
                        unique_results.append(item)
                
                logger.info(f"[东方财富] A股获取完成，共 {len(unique_results)} 只股票（去重前: {len(all_results)}）")
                return unique_results
            else:
                raise Exception("所有市场数据获取失败")
                
        except Exception as e:
            logger.warning(f"[东方财富] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[东方财富] 获取A股行情失败，已重试{max_retries}次")
                raise
    
    return []


def fetch_eastmoney_hk_stock_spot(max_retries: int = 2) -> Tuple[List[Dict[str, Any]], str]:
    """
    从东方财富获取港股实时行情（并发版本）
    
    Args:
        max_retries: 最大重试次数
    
    Returns:
        (股票行情列表, 数据源名称)
    """
    for attempt in range(max_retries):
        try:
            logger.info("[东方财富] 开始并发获取港股实时行情...")
            
            # 港股：fs=m:128+t:3（港股主板）
            # 东方财富港股接口实际每页最多返回约100条，需要多页获取
            all_results = []
            page_size = 1000  # 请求1000条，实际可能返回更少
            
            # 先获取总数
            total = _get_eastmoney_hk_total()
            if total == 0:
                logger.warning("[东方财富] 获取港股总数为0，尝试直接获取第一页数据")
                # 尝试直接获取第一页，看看能拿到多少
                first_page = _fetch_eastmoney_hk_page(1, page_size)
                if first_page:
                    logger.info(f"[东方财富] 直接获取第一页成功: {len(first_page)}只")
                    return first_page, "东方财富(并发)"
                raise Exception("获取港股总数失败且第一页也为空")
            
            # 由于接口实际返回数量有限制，需要多次请求
            # 估算需要的页数（假设每页实际返回100条左右）
            actual_page_size = 100  # 实际每页返回数量
            pages = (total + actual_page_size - 1) // actual_page_size
            pages = min(pages, 50)  # 最多50页，避免请求过多
            logger.info(f"[东方财富] 港股总数: {total}，需要 {pages} 页")
            
            # 并发请求各页数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(_fetch_eastmoney_hk_page, page, page_size): page 
                    for page in range(1, pages + 1)
                }
                
                for future in concurrent.futures.as_completed(futures, timeout=120):
                    page = futures[future]
                    try:
                        results = future.result()
                        if results:
                            all_results.extend(results)
                            logger.debug(f"[东方财富] 港股第{page}页获取成功: {len(results)}只")
                    except Exception as e:
                        logger.warning(f"[东方财富] 港股第{page}页获取失败: {e}")
            
            if all_results:
                # 去重（按code）
                seen = set()
                unique_results = []
                for item in all_results:
                    if item['code'] not in seen:
                        seen.add(item['code'])
                        unique_results.append(item)
                
                logger.info(f"[东方财富] 港股获取完成，共 {len(unique_results)} 只股票（去重前: {len(all_results)}）")
                return unique_results, "东方财富(并发)"
            else:
                raise Exception("所有页面数据获取失败")
                
        except Exception as e:
            logger.warning(f"[东方财富] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[东方财富] 获取港股行情失败，已重试{max_retries}次")
                raise
    
    return [], ""


def _fetch_eastmoney_market(market_name: str, fs: str) -> List[Dict[str, Any]]:
    """获取单个市场的数据（支持分页）"""
    all_results = []
    page = 1
    page_size = 5000  # 请求5000条，实际可能返回更少
    max_pages = 30  # 最多30页
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }
    
    # 先获取总数
    try:
        params = {
            "pn": 1,
            "pz": 1,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs,
            "fields": "f12",
        }
        response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=15)
        data = response.json()
        total = data.get("data", {}).get("total", 0)
        logger.debug(f"[东方财富] {market_name}总数: {total}")
    except Exception as e:
        logger.warning(f"[东方财富] {market_name}获取总数失败: {e}")
        total = 5000  # 默认值
    
    if total == 0:
        return []
    
    # 计算需要的页数（假设每页实际返回约100-500条）
    # 保守估计，按每页100条计算
    estimated_pages = min((total + 99) // 100, max_pages)
    
    while page <= estimated_pages:
        params = {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": fs,
            "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f62,f115,f128,f140,f141,f152",
        }
        
        try:
            response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=30)
            data = response.json()
            
            if data.get("rc") != 0:
                logger.warning(f"[东方财富] {market_name}第{page}页接口返回错误: rc={data.get('rc')}")
                break
            
            items = data.get("data", {}).get("diff", [])
            
            if not items:
                break
            
            page_results = []
            for item in items:
                try:
                    result = _parse_eastmoney_item(item, "A")
                    if result:
                        page_results.append(result)
                except Exception as e:
                    continue
            
            all_results.extend(page_results)
            logger.debug(f"[东方财富] {market_name}第{page}页获取成功: {len(page_results)}只, 累计: {len(all_results)}只")
            
            # 如果已经获取到足够数据，退出
            if len(all_results) >= total:
                break
            
            # 如果本页返回数据很少（小于50），可能已经没有更多数据了
            if len(items) < 50:
                break
            
            page += 1
                
        except Exception as e:
            logger.warning(f"[东方财富] {market_name}第{page}页请求失败: {e}")
            break
    
    return all_results


def _get_eastmoney_hk_total() -> int:
    """获取港股总数"""
    params = {
        "pn": 1,
        "pz": 1,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2",  # 港股所有类型
        "fields": "f12",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }
    
    try:
        response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=15)
        data = response.json()
        total = data.get("data", {}).get("total", 0)
        logger.info(f"[东方财富] 港股总数查询结果: {total}")
        return total
    except Exception as e:
        logger.warning(f"[东方财富] 获取港股总数失败: {e}")
        return 0


def _fetch_eastmoney_hk_page(page: int, page_size: int) -> List[Dict[str, Any]]:
    """获取港股单页数据"""
    params = {
        "pn": page,
        "pz": page_size,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2",  # 港股所有类型
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f62,f115,f128,f140,f141,f152",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }
    
    try:
        logger.debug(f"[东方财富] 请求港股第{page}页，page_size={page_size}")
        response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") != 0:
            logger.warning(f"[东方财富] 港股第{page}页返回错误: rc={data.get('rc')}")
            return []
        
        items = data.get("data", {}).get("diff", [])
        total_in_response = data.get("data", {}).get("total", 0)
        logger.debug(f"[东方财富] 港股第{page}页返回: items={len(items) if items else 0}, total={total_in_response}")
        
        if not items:
            return []
        
        results = []
        for item in items:
            try:
                result = _parse_eastmoney_item(item, "HK")
                if result:
                    results.append(result)
            except Exception as e:
                continue
        
        logger.debug(f"[东方财富] 港股第{page}页解析成功: {len(results)}只")
        return results
        
    except Exception as e:
        logger.warning(f"[东方财富] 港股第{page}页请求失败: {e}")
        return []


def _parse_eastmoney_item(item: dict, market: str) -> Dict[str, Any]:
    """解析东方财富数据项"""
    # 字段映射：
    # f2: 最新价, f3: 涨跌幅, f4: 涨跌额, f5: 成交量, f6: 成交额
    # f7: 振幅, f8: 换手率, f9: 市盈率, f10: 量比
    # f12: 代码, f14: 名称, f15: 最高, f16: 最低, f17: 今开, f18: 昨收
    # f20: 总市值, f21: 流通市值
    
    code = str(item.get("f12", ""))
    if not code:
        return None
    
    # 注意：不再过滤 price == "-" 的数据
    # 港股在非交易时间可能返回 "-" 作为价格，但这些仍然是有效的股票
    # 只过滤完全没有代码的数据
    
    def safe_float(val):
        if val == "-" or val is None:
            return 0.0
        try:
            return float(val)
        except:
            return 0.0
    
    result = {
        "code": code,
        "name": item.get("f14", ""),
        "price": safe_float(item.get("f2")),
        "pct": safe_float(item.get("f3")),
        "change": safe_float(item.get("f4")),
        "volume": safe_float(item.get("f5")),
        "amount": safe_float(item.get("f6")),
        "amplitude": safe_float(item.get("f7")),
        "turnover": safe_float(item.get("f8")),
        "pe": safe_float(item.get("f9")),
        "volume_ratio": safe_float(item.get("f10")),
        "high": safe_float(item.get("f15")),
        "low": safe_float(item.get("f16")),
        "open": safe_float(item.get("f17")),
        "pre_close": safe_float(item.get("f18")),
        "market_cap": safe_float(item.get("f20")),
        "circulating_market_cap": safe_float(item.get("f21")),
        "update_time": datetime.now().isoformat(),
        "market": market,
    }
    
    # A股分类
    if market == "A":
        result["sec_type"] = _classify_a_stock(code, result.get("name", ""))
    
    return result


def _classify_a_stock(code: str, name: str) -> str:
    """A股证券分类"""
    code = str(code or '').strip()
    name = str(name or '')
    name_upper = name.upper()
    
    # ETF
    if "ETF" in name_upper:
        return "etf"
    if code.startswith("51") or code.startswith("159"):
        return "etf"
    
    # 基金
    if "LOF" in name_upper or "基金" in name:
        return "fund"
    if code.startswith("501") or code.startswith("502"):
        return "fund"
    
    # 指数
    if code.startswith("399") or code.startswith("000"):
        index_keywords = ["指数", "综指", "成指", "上证", "深证", "沪深", "中证"]
        for kw in index_keywords:
            if kw in name:
                return "index"
    
    return "stock"
