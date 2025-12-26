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
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
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
    
    name = item.get("f14", "")
    
    # 过滤退市和清退股票
    if name:
        # 带"退"字的股票（退市、清退等）
        if "退" in name:
            return None
        # PT股票（已退市的特别转让股票）
        if name.startswith("PT"):
            return None
        # ST股票中已经退市的
        if name.startswith("*ST") and ("退" in name or "清" in name):
            return None
    
    def safe_float(val):
        if val == "-" or val is None:
            return 0.0
        try:
            return float(val)
        except:
            return 0.0
    
    price = safe_float(item.get("f2"))
    market_cap = safe_float(item.get("f20"))
    
    # 过滤无效数据：价格为0且市值为0的股票（可能已退市或停牌很久）
    # 但保留价格为0但市值不为0的股票（可能是临时停牌）
    if price == 0 and market_cap == 0:
        # 检查是否有成交量，有成交量说明还在交易
        volume = safe_float(item.get("f5"))
        if volume == 0:
            return None
    
    result = {
        "code": code,
        "name": name,
        "price": price,
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
        "market_cap": market_cap,
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


# ============ 东方财富K线数据接口 ============

# 东方财富K线接口
EASTMONEY_KLINE_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get"


def fetch_eastmoney_a_kline(code: str, period: str = "daily", adjust: str = "", 
                            start_date: str = None, end_date: str = None,
                            limit: int = 500) -> List[Dict[str, Any]]:
    """
    从东方财富获取A股K线数据
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly, 1h/60）
        adjust: 复权类型（"": 不复权, "qfq": 前复权, "hfq": 后复权）
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）
        limit: 获取数量限制
    
    Returns:
        K线数据列表
    """
    try:
        code = str(code).strip().zfill(6)
        
        # 确定市场代码
        # 沪市：600、601、603、605、688开头 -> secid=1.code
        # 深市：000、001、002、003、300开头 -> secid=0.code
        # 北交所：4、8开头 -> secid=0.code
        if code.startswith(('6',)):
            secid = f"1.{code}"
        else:
            secid = f"0.{code}"
        
        # 周期映射
        # 东方财富 klt 参数：1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟, 101=日, 102=周, 103=月
        period_map = {
            "daily": "101",
            "d": "101",
            "day": "101",
            "weekly": "102",
            "w": "102",
            "week": "102",
            "monthly": "103",
            "m": "103",
            "month": "103",
            "1h": "60",
            "hourly": "60",
            "60": "60",
            "30m": "30",
            "15m": "15",
            "5m": "5",
            "1m": "1",
        }
        klt = period_map.get(period.lower(), "101")
        
        # 复权类型映射
        # fqt: 0=不复权, 1=前复权, 2=后复权
        fqt_map = {
            "": "0",
            "qfq": "1",
            "hfq": "2",
        }
        fqt = fqt_map.get(adjust, "0")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/",
        }
        
        params = {
            "secid": secid,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": klt,
            "fqt": fqt,
            "lmt": limit,
            "end": "20500101",  # 结束日期设置为未来，获取最新数据
            "_": int(datetime.now().timestamp() * 1000),
        }
        
        # 如果指定了结束日期
        if end_date:
            params["end"] = end_date
        
        response = requests.get(EASTMONEY_KLINE_URL, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") != 0:
            logger.warning(f"[东方财富K线] {code} 接口返回错误: rc={data.get('rc')}")
            return []
        
        klines = data.get("data", {}).get("klines", [])
        if not klines:
            logger.debug(f"[东方财富K线] {code} 无K线数据")
            return []
        
        result = []
        for kline in klines:
            # 格式: "日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率"
            parts = kline.split(",")
            if len(parts) < 7:
                continue
            
            try:
                item = {
                    "code": code,
                    "date": parts[0],  # 日期或时间
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]) if len(parts) > 6 else 0,
                }
                
                # 对于分钟/小时数据，日期格式是 "2024-01-01 09:30"
                # 对于日线数据，日期格式是 "2024-01-01"
                # 统一转换为 YYYYMMDD 格式（取日期部分）
                date_str = parts[0].split(" ")[0].replace("-", "")
                item["date"] = date_str
                
                # 添加周期标识
                if klt == "60":
                    item["period"] = "1h"
                elif klt in ["1", "5", "15", "30"]:
                    item["period"] = f"{klt}m"
                else:
                    item["period"] = "daily"
                
                result.append(item)
            except Exception as e:
                continue
        
        # 按日期过滤
        if start_date:
            start_date_str = start_date.replace("-", "")
            result = [r for r in result if r["date"] >= start_date_str]
        if end_date:
            end_date_str = end_date.replace("-", "")
            result = [r for r in result if r["date"] <= end_date_str]
        
        logger.info(f"[东方财富K线] {code} 获取成功: {len(result)}条 (周期={period})")
        return result
        
    except Exception as e:
        logger.warning(f"[东方财富K线] {code} 获取失败: {e}")
        return []


def fetch_eastmoney_hk_kline(code: str, period: str = "daily", adjust: str = "",
                             start_date: str = None, end_date: str = None,
                             limit: int = 500) -> List[Dict[str, Any]]:
    """
    从东方财富获取港股K线数据
    
    Args:
        code: 股票代码（如：00700, 03690）
        period: 周期（daily, weekly, monthly, 1h/60）
        adjust: 复权类型（"": 不复权, "qfq": 前复权, "hfq": 后复权）
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）
        limit: 获取数量限制
    
    Returns:
        K线数据列表
    """
    try:
        code = str(code).strip().zfill(5)
        
        # 港股 secid: 116.code
        secid = f"116.{code}"
        
        # 周期映射（同A股）
        period_map = {
            "daily": "101",
            "d": "101",
            "day": "101",
            "weekly": "102",
            "w": "102",
            "week": "102",
            "monthly": "103",
            "m": "103",
            "month": "103",
            "1h": "60",
            "hourly": "60",
            "60": "60",
            "30m": "30",
            "15m": "15",
            "5m": "5",
            "1m": "1",
        }
        klt = period_map.get(period.lower(), "101")
        
        # 复权类型映射
        fqt_map = {
            "": "0",
            "qfq": "1",
            "hfq": "2",
        }
        fqt = fqt_map.get(adjust, "0")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "http://quote.eastmoney.com/",
        }
        
        params = {
            "secid": secid,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": klt,
            "fqt": fqt,
            "lmt": limit,
            "end": "20500101",
            "_": int(datetime.now().timestamp() * 1000),
        }
        
        if end_date:
            params["end"] = end_date
        
        response = requests.get(EASTMONEY_KLINE_URL, params=params, headers=headers, timeout=30)
        data = response.json()
        
        if data.get("rc") != 0:
            logger.warning(f"[东方财富港股K线] {code} 接口返回错误: rc={data.get('rc')}")
            return []
        
        klines = data.get("data", {}).get("klines", [])
        if not klines:
            logger.debug(f"[东方财富港股K线] {code} 无K线数据")
            return []
        
        result = []
        for kline in klines:
            parts = kline.split(",")
            if len(parts) < 7:
                continue
            
            try:
                date_str = parts[0].split(" ")[0].replace("-", "")
                item = {
                    "code": code,
                    "date": date_str,
                    "open": float(parts[1]),
                    "close": float(parts[2]),
                    "high": float(parts[3]),
                    "low": float(parts[4]),
                    "volume": float(parts[5]),
                    "amount": float(parts[6]) if len(parts) > 6 else 0,
                }
                
                if klt == "60":
                    item["period"] = "1h"
                elif klt in ["1", "5", "15", "30"]:
                    item["period"] = f"{klt}m"
                else:
                    item["period"] = "daily"
                
                result.append(item)
            except Exception as e:
                continue
        
        if start_date:
            start_date_str = start_date.replace("-", "")
            result = [r for r in result if r["date"] >= start_date_str]
        if end_date:
            end_date_str = end_date.replace("-", "")
            result = [r for r in result if r["date"] <= end_date_str]
        
        logger.info(f"[东方财富港股K线] {code} 获取成功: {len(result)}条 (周期={period})")
        return result
        
    except Exception as e:
        logger.warning(f"[东方财富港股K线] {code} 获取失败: {e}")
        return []
