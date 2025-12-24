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
