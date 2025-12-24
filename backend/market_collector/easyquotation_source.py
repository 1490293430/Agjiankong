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
        return "etf"  # 51/159 开头的大多是 ETF
    if code.startswith("501") or code.startswith("502"):
        return "fund"  # 501/502 开头的是分级基金
    
    # 688 开头是科创板股票
    if code.startswith("688"):
        return "stock"
    
    # 60/00/30 开头的普通股票代码，如果名称像公司名就是股票
    company_patterns = ["股份", "集团", "控股", "实业", "科技", "电子", "医药", 
                        "生物", "新材", "智能", "网络", "软件", "信息", "通信",
                        "能源", "环境", "建设", "工程", "制造", "机械", "设备",
                        "汽车", "电气", "电力", "化工", "材料", "食品", "饮料",
                        "服饰", "家居", "传媒", "文化", "教育", "医疗", "健康",
                        "物流", "运输", "航空", "船舶", "港口", "地产", "置业",
                        "投资", "证券", "保险", "银行", "金融", "租赁", "信托"]
    
    # 如果名称包含公司特征词，判定为股票
    for pattern in company_patterns:
        if pattern in name:
            return "stock"
    
    # 名称以 A/B 结尾的通常是股票（如 "万科A"、"招商银行"）
    if name.endswith("Ａ") or name.endswith("Ｂ") or name.endswith("A") or name.endswith("B"):
        return "stock"
    
    # 000 开头的特殊处理：很多是指数
    if code.startswith("000"):
        # 指数名称特征词
        index_name_patterns = [
            "综指", "成指", "等权", "全指", "红利", "价值", "成长", "基本",
            "波动", "稳定", "动态", "治理", "高贝", "低贝", "分层", "优选",
            "领先", "百强", "央视", "腾讯", "济安", "丝路", "AH", "R价值",
            "R成长", "新兴", "中型", "小型", "大型", "市值", "细分", "主题"
        ]
        for pattern in index_name_patterns:
            if pattern in name:
                return "index"
        # 名称是数字开头+中文的短名称，很可能是指数（如"180金融"、"50等权"、"300工业"）
        import re
        if re.match(r'^[\d]+', name) and len(name) <= 10:
            return "index"
    
    # 指数名称关键词（更严格的匹配）
    strict_index_keywords = [
        "指数", "综指", "成指", "等权", "全指"
    ]
    
    for kw in strict_index_keywords:
        if kw in name:
            return "index"
    
    return "stock"
