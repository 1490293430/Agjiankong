"""
新浪财经实时行情数据源
"""
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from common.logger import get_logger

logger = get_logger(__name__)

# 新浪财经行情接口
SINA_API_URL = "http://hq.sinajs.cn/list="


def fetch_sina_stock_spot(codes: List[str] = None, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    从新浪财经获取A股实时行情
    
    Args:
        codes: 股票代码列表，如果为None则获取全部A股
        max_retries: 最大重试次数
    
    Returns:
        股票行情列表
    """
    import concurrent.futures
    
    for attempt in range(max_retries):
        try:
            # 如果没有指定代码，先获取股票列表
            if not codes:
                codes = _get_all_a_stock_codes()
                if not codes:
                    raise Exception("无法获取A股代码列表")
            
            logger.info(f"[新浪] 开始获取 {len(codes)} 只股票行情...")
            
            # 分批获取（每批最多800只）
            batch_size = 800
            all_results = []
            batches = []
            
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i + batch_size]
                batches.append(batch_codes)
            
            # 使用线程池并发请求（最多4个并发）
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(_fetch_batch_sina, batch): i for i, batch in enumerate(batches)}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        batch_results = future.result(timeout=60)
                        all_results.extend(batch_results)
                    except Exception as e:
                        logger.warning(f"[新浪] 批次请求失败: {e}")
                
            logger.info(f"[新浪] 获取完成，共 {len(all_results)} 只股票")
            return all_results
            
        except Exception as e:
            logger.warning(f"[新浪] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[新浪] 获取A股行情失败，已重试{max_retries}次")
                raise
    
    return []


def _get_all_a_stock_codes() -> List[str]:
    """获取所有A股代码列表"""
    try:
        # 使用akshare获取股票列表
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        codes = df['代码'].tolist()
        return codes
    except Exception as e:
        logger.error(f"[新浪] 获取A股代码列表失败: {e}")
        return []


def _fetch_batch_sina(codes: List[str]) -> List[Dict[str, Any]]:
    """批量获取新浪行情数据"""
    results = []
    
    # 转换代码格式：600519 -> sh600519, 000001 -> sz000001
    sina_codes = []
    for code in codes:
        code = str(code).zfill(6)
        if code.startswith(('6', '9')):
            sina_codes.append(f"sh{code}")
        elif code.startswith(('0', '3', '2')):
            sina_codes.append(f"sz{code}")
        elif code.startswith(('4', '8')):
            sina_codes.append(f"bj{code}")  # 北交所
    
    if not sina_codes:
        return results
    
    # 请求新浪接口
    url = SINA_API_URL + ",".join(sina_codes)
    headers = {
        "Referer": "http://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'gbk'
        
        # 解析返回数据
        # 格式: var hq_str_sh600519="贵州茅台,1800.00,1795.00,1810.50,...";
        pattern = r'var hq_str_(\w+)="([^"]+)";'
        matches = re.findall(pattern, response.text)
        
        for sina_code, data_str in matches:
            if not data_str:
                continue
            
            try:
                fields = data_str.split(',')
                if len(fields) < 32:
                    continue
                
                # 提取代码
                code = sina_code[2:]  # 去掉sh/sz/bj前缀
                
                # 解析数据
                result = {
                    "code": code,
                    "name": fields[0],
                    "open": _safe_float(fields[1]),
                    "pre_close": _safe_float(fields[2]),
                    "price": _safe_float(fields[3]),
                    "high": _safe_float(fields[4]),
                    "low": _safe_float(fields[5]),
                    "volume": _safe_float(fields[8]) / 100,  # 转换为手
                    "amount": _safe_float(fields[9]),
                    "update_time": datetime.now().isoformat(),
                    "market": "A",
                    "sec_type": _classify_security(code, fields[0])
                }
                
                # 计算涨跌幅
                if result["pre_close"] and result["pre_close"] > 0:
                    result["pct"] = round((result["price"] - result["pre_close"]) / result["pre_close"] * 100, 2)
                    result["change"] = round(result["price"] - result["pre_close"], 2)
                else:
                    result["pct"] = 0
                    result["change"] = 0
                
                results.append(result)
                
            except Exception as e:
                logger.debug(f"[新浪] 解析股票数据失败: {sina_code}, {e}")
                continue
                
    except Exception as e:
        logger.error(f"[新浪] 请求失败: {e}")
        raise
    
    return results


def _safe_float(value: str) -> float:
    """安全转换为浮点数"""
    try:
        return float(value) if value else 0.0
    except (ValueError, TypeError):
        return 0.0


def _classify_security(code: str, name: str) -> str:
    """
    根据代码和名称判断证券类型
    返回: 'stock', 'etf', 'index', 'fund'
    """
    code = str(code or '').strip()
    name = str(name or '')
    name_upper = name.upper()
    
    # ETF 优先判断（名称含 ETF）
    if "ETF" in name_upper:
        return "etf"
    
    # LOF 和基金
    if "LOF" in name_upper or "基金" in name:
        return "fund"
    
    # 债券
    if "债" in name or "可转债" in name:
        return "fund"
    
    # 代码规则判断
    if code.startswith("399"):
        return "index"
    if code.startswith("51") or code.startswith("159"):
        return "etf"
    if code.startswith("501") or code.startswith("502"):
        return "fund"
    
    # 688 开头是科创板股票
    if code.startswith("688"):
        return "stock"
    
    # 指数关键词判断（放在公司特征词之前！）
    index_keywords = [
        # 交易所/市场前缀
        "上证", "深证", "沪深", "中证", "沪", "深", "创业板", "科创",
        # 指数类型
        "指数", "综指", "成指", "等权", "全指", "红利", "价值", "成长",
        "基本", "波动", "稳定", "动态", "治理", "高贝", "低贝", "分层",
        "优选", "领先", "百强", "央视", "腾讯", "济安", "丝路",
        # 特殊指数
        "股通", "互联", "龙头", "央企", "国企", "民企", "地企", "沪企",
        "海外", "周期", "非周期", "上游", "中游", "下游", "投资品",
        "中小", "大盘", "小盘", "中盘", "超大盘", "流通",
        # 行业指数后缀
        "TMT", "ESG", "碳中和", "新丝路", "一带一路", "持续产业",
        "中国造", "高端装备", "内地资源", "A股资源"
    ]
    
    for kw in index_keywords:
        if kw in name:
            return "index"
    
    # 000 开头的特殊处理：更多行业指数关键词（只对000开头生效，避免误判普通股票）
    if code.startswith("000"):
        index_000_keywords = [
            "有色", "资源", "消费", "医药", "优势", "百发", "细分", "主题",
            "HK", "CS", "农业", "精准", "金融", "材料", "能源", "信息",
            "电信", "可选", "必需", "公用", "工业", "地产"
        ]
        for kw in index_000_keywords:
            if kw in name:
                return "index"
        # 数字开头的名称 = 指数
        import re
        if re.match(r'^[\d]+', name):
            return "index"
    
    # 名称以 A/B 结尾的通常是股票（如 "万科A"）
    if name.endswith("Ａ") or name.endswith("Ｂ") or name.endswith("A") or name.endswith("B"):
        return "stock"
    
    # 公司名称特征词
    company_patterns = ["股份", "集团", "控股", "实业", "科技", "电子", 
                        "生物", "新材", "智能", "网络", "软件",
                        "环境", "建设", "工程", "制造", "机械", "设备",
                        "汽车", "电气", "服饰", "家居", "文化", "教育", 
                        "物流", "运输", "船舶", "置业", "租赁", "信托"]
    
    for pattern in company_patterns:
        if pattern in name:
            return "stock"
    
    return "stock"


def fetch_sina_hk_stock_spot(codes: List[str] = None, max_retries: int = 1) -> List[Dict[str, Any]]:
    """
    从新浪财经获取港股实时行情
    
    Args:
        codes: 股票代码列表，如果为None则获取全部港股
        max_retries: 最大重试次数
    
    Returns:
        股票行情列表
    """
    import concurrent.futures
    
    for attempt in range(max_retries):
        try:
            # 如果没有指定代码，先获取港股列表
            if not codes:
                codes = _get_all_hk_stock_codes()
                if not codes:
                    raise Exception("无法获取港股代码列表")
            
            logger.info(f"[新浪港股] 开始获取 {len(codes)} 只股票行情...")
            
            # 分批获取（每批最多200只，港股接口限制更严格）
            batch_size = 200
            all_results = []
            batches = []
            
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i + batch_size]
                batches.append(batch_codes)
            
            # 使用线程池并发请求（最多4个并发，避免被限流）
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(_fetch_batch_sina_hk, batch): i for i, batch in enumerate(batches)}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        batch_results = future.result(timeout=120)
                        all_results.extend(batch_results)
                    except Exception as e:
                        logger.warning(f"[新浪港股] 批次请求失败: {e}")
                
            logger.info(f"[新浪港股] 获取完成，共 {len(all_results)} 只股票")
            return all_results
            
        except Exception as e:
            logger.warning(f"[新浪港股] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[新浪港股] 获取港股行情失败，已重试{max_retries}次")
                raise
    
    return []


def _get_all_hk_stock_codes() -> List[str]:
    """获取所有港股代码列表"""
    try:
        # 使用akshare获取港股列表
        import akshare as ak
        df = ak.stock_hk_spot_em()
        codes = df['代码'].tolist()
        return codes
    except Exception as e:
        logger.error(f"[新浪港股] 获取港股代码列表失败: {e}")
        return []


def _fetch_batch_sina_hk(codes: List[str]) -> List[Dict[str, Any]]:
    """批量获取新浪港股行情数据"""
    results = []
    
    # 转换代码格式：00700 -> hk00700
    sina_codes = []
    for code in codes:
        code = str(code).zfill(5)  # 港股代码5位
        sina_codes.append(f"hk{code}")
    
    if not sina_codes:
        return results
    
    # 请求新浪接口
    url = SINA_API_URL + ",".join(sina_codes)
    headers = {
        "Referer": "http://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.encoding = 'gbk'
        
        # 解析返回数据
        # 港股格式: var hq_str_hk00700="TENCENT,腾讯控股,368.200,369.600,372.000,366.000,...";
        pattern = r'var hq_str_(\w+)="([^"]+)";'
        matches = re.findall(pattern, response.text)
        
        for sina_code, data_str in matches:
            if not data_str:
                continue
            
            try:
                fields = data_str.split(',')
                if len(fields) < 15:
                    continue
                
                # 提取代码（去掉hk前缀）
                code = sina_code[2:]
                
                # 港股新浪数据格式：
                # 0: 英文名, 1: 中文名, 2: 开盘价, 3: 昨收, 4: 最高, 5: 最低
                # 6: 最新价, 7: 涨跌额, 8: 涨跌幅, 9: 买入价, 10: 卖出价
                # 11: 成交量, 12: 成交额, 13: 市盈率, 14: 52周最高, 15: 52周最低
                # 16: 日期, 17: 时间
                result = {
                    "code": code,
                    "name": fields[1] if len(fields) > 1 else fields[0],
                    "open": _safe_float(fields[2]),
                    "pre_close": _safe_float(fields[3]),
                    "high": _safe_float(fields[4]),
                    "low": _safe_float(fields[5]),
                    "price": _safe_float(fields[6]),
                    "change": _safe_float(fields[7]),
                    "pct": _safe_float(fields[8]),
                    "volume": _safe_float(fields[11]),
                    "amount": _safe_float(fields[12]),
                    "pe": _safe_float(fields[13]) if len(fields) > 13 else 0,
                    "update_time": datetime.now().isoformat(),
                    "market": "HK"
                }
                
                results.append(result)
                
            except Exception as e:
                logger.debug(f"[新浪港股] 解析股票数据失败: {sina_code}, {e}")
                continue
                
    except Exception as e:
        logger.error(f"[新浪港股] 请求失败: {e}")
        raise
    
    return results


# ============ K线数据采集 ============

# 新浪K线数据接口
SINA_KLINE_API = "http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"


def fetch_sina_kline(
    code: str,
    period: str = "daily",
    adjust: str = "",
    start_date: str = None,
    end_date: str = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    从新浪财经获取A股K线数据
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly, 1h）
        adjust: 复权类型（暂不支持，新浪接口返回不复权数据）
        start_date: 开始日期（YYYYMMDD，新浪接口不支持，通过limit控制）
        end_date: 结束日期（YYYYMMDD，新浪接口不支持）
        limit: 获取的K线数量（最大1023）
    
    Returns:
        K线数据列表
    """
    try:
        # 转换周期参数
        # 新浪接口支持的周期：5, 15, 30, 60（分钟）, day, week, month
        period_map = {
            "daily": "day",
            "day": "day",
            "weekly": "week",
            "week": "week",
            "monthly": "month",
            "month": "month",
            "1h": "60",
            "hourly": "60",
            "60": "60",
        }
        sina_period = period_map.get(period, "day")
        
        # 转换代码格式：600519 -> sh600519, 000001 -> sz000001
        code_str = str(code).zfill(6)
        if code_str.startswith(('6', '9')):
            sina_code = f"sh{code_str}"
        elif code_str.startswith(('0', '3', '2')):
            sina_code = f"sz{code_str}"
        elif code_str.startswith(('4', '8')):
            sina_code = f"bj{code_str}"  # 北交所
        else:
            sina_code = f"sh{code_str}"  # 默认沪市
        
        # 限制最大数量
        limit = min(limit, 1023)
        
        # 构建请求URL
        params = {
            "symbol": sina_code,
            "scale": sina_period if sina_period in ["5", "15", "30", "60"] else "",
            "ma": "no",
            "datalen": limit
        }
        
        # 日线/周线/月线使用不同的参数
        if sina_period in ["day", "week", "month"]:
            url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=240&ma=no&datalen={limit}"
            if sina_period == "week":
                url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=1680&ma=no&datalen={limit}"
            elif sina_period == "month":
                url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=7200&ma=no&datalen={limit}"
        else:
            # 分钟级别
            url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale={sina_period}&ma=no&datalen={limit}"
        
        headers = {
            "Referer": "http://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        logger.info(f"[新浪K线] 获取 {code} {period} K线数据...")
        
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        
        # 解析JSON数据
        # 返回格式: [{"day":"2024-01-02","open":"1800.00","high":"1810.00","low":"1795.00","close":"1805.00","volume":"12345678"},...]
        import json
        data = json.loads(response.text)
        
        if not data:
            logger.warning(f"[新浪K线] {code} 返回数据为空")
            return []
        
        results = []
        
        # 转换日期格式用于过滤
        filter_start = None
        filter_end = None
        if start_date:
            # 支持 YYYYMMDD 和 YYYY-MM-DD 格式
            filter_start = start_date.replace("-", "") if "-" in start_date else start_date
        if end_date:
            filter_end = end_date.replace("-", "") if "-" in end_date else end_date
        
        for item in data:
            try:
                # 解析日期/时间
                date_str = item.get("day", "")
                
                # 获取日期并转换格式用于比较
                item_date = date_str[:10] if len(date_str) >= 10 else date_str
                item_date_cmp = item_date.replace("-", "") if "-" in item_date else item_date
                
                # 根据 start_date 和 end_date 过滤数据
                if filter_start and item_date_cmp < filter_start:
                    continue
                if filter_end and item_date_cmp > filter_end:
                    continue
                
                result = {
                    "date": item_date,
                    "time": date_str,
                    "open": _safe_float(item.get("open", 0)),
                    "high": _safe_float(item.get("high", 0)),
                    "low": _safe_float(item.get("low", 0)),
                    "close": _safe_float(item.get("close", 0)),
                    "volume": _safe_float(item.get("volume", 0)),
                    "amount": 0,  # 新浪接口不返回成交额
                    "code": code,
                    "market": "A"
                }
                results.append(result)
            except Exception as e:
                logger.debug(f"[新浪K线] 解析数据失败: {e}")
                continue
        
        # 按日期排序（从旧到新）
        results.sort(key=lambda x: x.get("date", ""))
        
        logger.info(f"[新浪K线] {code} 获取成功，共 {len(results)} 条K线数据")
        return results
        
    except Exception as e:
        logger.warning(f"[新浪K线] 获取 {code} K线数据失败: {e}")
        return []
