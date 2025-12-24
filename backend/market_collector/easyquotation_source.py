"""
Easyquotation 实时行情数据源
基于 easyquotation 库获取实时行情
"""
from datetime import datetime
from typing import List, Dict, Any
from common.logger import get_logger

logger = get_logger(__name__)


def fetch_easyquotation_stock_spot(max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    使用 easyquotation 获取A股实时行情
    
    Args:
        max_retries: 最大重试次数
    
    Returns:
        股票行情列表
    """
    for attempt in range(max_retries):
        try:
            import easyquotation
            
            logger.info("[Easyquotation] 开始获取A股实时行情...")
            
            # 使用腾讯数据源（更稳定）
            quotation = easyquotation.use('tencent')
            
            # 获取所有股票行情
            all_data = quotation.market_snapshot(prefix=True)
            
            if not all_data:
                raise Exception("获取行情数据为空")
            
            results = []
            for code, data in all_data.items():
                try:
                    # 提取纯代码（去掉sh/sz前缀）
                    pure_code = code[2:] if code.startswith(('sh', 'sz')) else code
                    
                    result = {
                        "code": pure_code,
                        "name": data.get("name", ""),
                        "price": _safe_float(data.get("now", 0)),
                        "open": _safe_float(data.get("open", 0)),
                        "pre_close": _safe_float(data.get("close", 0)),
                        "high": _safe_float(data.get("high", 0)),
                        "low": _safe_float(data.get("low", 0)),
                        "volume": _safe_float(data.get("volume", 0)) / 100,  # 转换为手
                        "amount": _safe_float(data.get("turnover", 0)),
                        "pct": _safe_float(data.get("涨跌(%)", 0)),
                        "change": _safe_float(data.get("涨跌", 0)),
                        "update_time": datetime.now().isoformat(),
                        "market": "A",
                        "sec_type": _classify_security(pure_code, data.get("name", ""))
                    }
                    
                    # 如果没有涨跌幅，手动计算
                    if result["pct"] == 0 and result["pre_close"] > 0:
                        result["pct"] = round((result["price"] - result["pre_close"]) / result["pre_close"] * 100, 2)
                        result["change"] = round(result["price"] - result["pre_close"], 2)
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.debug(f"[Easyquotation] 解析股票数据失败: {code}, {e}")
                    continue
            
            logger.info(f"[Easyquotation] 获取完成，共 {len(results)} 只股票")
            return results
            
        except ImportError:
            logger.error("[Easyquotation] 未安装 easyquotation 库，请运行: pip install easyquotation")
            raise
        except Exception as e:
            logger.warning(f"[Easyquotation] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[Easyquotation] 获取A股行情失败，已重试{max_retries}次")
                raise
    
    return []


def _safe_float(value) -> float:
    """安全转换为浮点数"""
    try:
        if value is None:
            return 0.0
        return float(value)
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


# ============ K线数据采集 ============

def fetch_easyquotation_kline(
    code: str,
    period: str = "daily",
    adjust: str = "",
    start_date: str = None,
    end_date: str = None,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    使用 easyquotation 获取A股K线数据
    
    注意：easyquotation 主要用于实时行情，K线数据功能有限
    这里使用其底层的腾讯数据源获取K线
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly, 1h）
        adjust: 复权类型（暂不支持）
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        limit: 获取的K线数量
    
    Returns:
        K线数据列表
    """
    import requests
    
    try:
        # 转换周期参数
        # 腾讯接口支持的周期：day, week, month, m1, m5, m15, m30, m60
        period_map = {
            "daily": "day",
            "day": "day",
            "weekly": "week",
            "week": "week",
            "monthly": "month",
            "month": "month",
            "1h": "m60",
            "hourly": "m60",
            "60": "m60",
        }
        qq_period = period_map.get(period, "day")
        
        # 转换代码格式：600519 -> sh600519, 000001 -> sz000001
        code_str = str(code).zfill(6)
        if code_str.startswith(('6', '9')):
            qq_code = f"sh{code_str}"
        elif code_str.startswith(('0', '3', '2')):
            qq_code = f"sz{code_str}"
        elif code_str.startswith(('4', '8')):
            qq_code = f"bj{code_str}"  # 北交所
        else:
            qq_code = f"sh{code_str}"  # 默认沪市
        
        # 限制最大数量
        limit = min(limit, 1000)
        
        # 腾讯K线接口
        # 日线/周线/月线
        if qq_period in ["day", "week", "month"]:
            url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={qq_code},{qq_period},,,{limit},qfq"
        else:
            # 分钟级别
            url = f"http://web.ifzq.gtimg.cn/appstock/app/minute/query?code={qq_code}"
        
        headers = {
            "Referer": "http://web.ifzq.gtimg.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        logger.debug(f"[Easyquotation K线] 获取 {code} {period} K线数据，URL: {url[:80]}...")
        
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        
        import json
        data = json.loads(response.text)
        
        if not data:
            logger.debug(f"[Easyquotation K线] {code} 返回数据为空")
            return []
        
        results = []
        
        # 解析日线/周线/月线数据
        if qq_period in ["day", "week", "month"]:
            # 数据格式: {"code":0,"msg":"","data":{"sh600519":{"day":[["2024-01-02","1800.00","1810.00","1795.00","1805.00","12345678"],...],...}}}
            stock_data = data.get("data", {}).get(qq_code, {})
            
            # 尝试多种可能的数据键名
            kline_data = stock_data.get(qq_period, []) or stock_data.get("qfq" + qq_period, []) or stock_data.get("hfq" + qq_period, [])
            
            # 如果还是空，记录详细信息帮助调试
            if not kline_data:
                available_keys = list(stock_data.keys()) if stock_data else []
                logger.debug(f"[Easyquotation K线] {code} 无K线数据，可用键: {available_keys}")
            
            # 转换日期格式用于过滤
            filter_start = None
            filter_end = None
            if start_date:
                # 支持 YYYYMMDD 和 YYYY-MM-DD 格式
                filter_start = start_date.replace("-", "") if "-" in start_date else start_date
            if end_date:
                filter_end = end_date.replace("-", "") if "-" in end_date else end_date
            
            for item in kline_data:
                try:
                    if len(item) < 6:
                        continue
                    
                    # 获取日期并转换格式用于比较
                    item_date = item[0]
                    item_date_cmp = item_date.replace("-", "") if "-" in item_date else item_date
                    
                    # 根据 start_date 和 end_date 过滤数据
                    if filter_start and item_date_cmp < filter_start:
                        continue
                    if filter_end and item_date_cmp > filter_end:
                        continue
                    
                    result = {
                        "date": item[0],
                        "time": item[0],
                        "open": _safe_float(item[1]),
                        "close": _safe_float(item[2]),
                        "high": _safe_float(item[3]),
                        "low": _safe_float(item[4]),
                        "volume": _safe_float(item[5]),
                        "amount": 0,
                        "code": code,
                        "market": "A"
                    }
                    results.append(result)
                except Exception as e:
                    logger.debug(f"[Easyquotation K线] 解析数据失败: {e}")
                    continue
        else:
            # 分钟级别数据（腾讯接口返回当日分钟数据）
            # 对于小时K线，需要使用其他接口
            # 这里尝试使用新浪接口作为备选
            logger.info(f"[Easyquotation K线] 分钟级别数据，尝试使用新浪接口...")
            try:
                from market_collector.sina_source import fetch_sina_kline
                return fetch_sina_kline(code, period, adjust, start_date, end_date, limit)
            except Exception as e:
                logger.warning(f"[Easyquotation K线] 新浪接口也失败: {e}")
                return []
        
        # 按日期排序（从旧到新）
        results.sort(key=lambda x: x.get("date", ""))
        
        if results:
            logger.debug(f"[Easyquotation K线] {code} 获取成功，共 {len(results)} 条K线数据")
        
        return results
        
    except ImportError:
        logger.error("[Easyquotation K线] 未安装 easyquotation 库")
        return []
    except Exception as e:
        logger.warning(f"[Easyquotation K线] 获取 {code} K线数据失败: {e}")
        return []
