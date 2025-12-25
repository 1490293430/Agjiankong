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
                ("沪市主板", "m:1+t:2,m:1+t:23"),  # 沪市主板+科创板
                ("深市", "m:0+t:6,m:0+t:13,m:0+t:80"),  # 深市主板+中小板+创业板
                ("北交所", "m:0+t:81"),  # 北交所
            ]
            
            all_results = []
            
            # 并发请求各市场数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(_fetch_eastmoney_market, name, fs): name 
                    for name, fs in markets
                }
                
                for future in concurrent.futures.as_completed(futures, timeout=60):
                    market_name = futures[future]
                    try:
                        results = future.result()
                        if results:
                            all_results.extend(results)
                            logger.info(f"[东方财富] {market_name}获取成功: {len(results)}只")
                    except Exception as e:
                        logger.warning(f"[东方财富] {market_name}获取失败: {e}")
            
            if all_results:
                logger.info(f"[东方财富] A股获取完成，共 {len(all_results)} 只股票")
                return all_results
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
            # 东方财富港股接口一次最多返回5000条，需要分页
            all_results = []
            page_size = 5000
            
            # 先获取总数
            total = _get_eastmoney_hk_total()
            if total == 0:
                raise Exception("获取港股总数失败")
            
            # 计算需要的页数
            pages = (total + page_size - 1) // page_size
            logger.info(f"[东方财富] 港股总数: {total}，需要 {pages} 页")
            
            # 并发请求各页数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
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
                    except Exception as e:
                        logger.warning(f"[东方财富] 港股第{page}页获取失败: {e}")
            
            if all_results:
                logger.info(f"[东方财富] 港股获取完成，共 {len(all_results)} 只股票")
                return all_results, "东方财富(并发)"
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
    """获取单个市场的数据"""
    params = {
        "pn": 1,
        "pz": 10000,  # 每页数量，设置足够大
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": fs,
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f62,f115,f128,f140,f141,f152",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://quote.eastmoney.com/",
    }
    
    try:
        response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") != 0:
            logger.warning(f"[东方财富] {market_name}接口返回错误: {data}")
            return []
        
        items = data.get("data", {}).get("diff", [])
        if not items:
            return []
        
        results = []
        for item in items:
            try:
                result = _parse_eastmoney_item(item, "A")
                if result:
                    results.append(result)
            except Exception as e:
                continue
        
        return results
        
    except Exception as e:
        logger.warning(f"[东方财富] {market_name}请求失败: {e}")
        return []


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
        return data.get("data", {}).get("total", 0)
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
        response = requests.get(EASTMONEY_API_URL, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") != 0:
            return []
        
        items = data.get("data", {}).get("diff", [])
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
    
    # 过滤无效数据
    price = item.get("f2")
    if price == "-" or price is None:
        return None
    
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
