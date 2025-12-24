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
    code = str(code or '')
    name = str(name or '')
    
    # ETF 判断
    if 'ETF' in name or 'LOF' in name:
        return 'etf'
    
    # 指数判断
    if '指数' in name:
        return 'index'
    if code.startswith('399'):  # 深证指数
        return 'index'
    if code.startswith('000') and ('上证' in name or '深证' in name or '中证' in name or '沪' in name):
        return 'index'
    
    # ETF 代码段判断
    if len(code) == 6:
        if code[:2] in ['51', '52', '56', '58'] or code[:3] == '159':
            return 'etf'
        if code.startswith('5') and not code.startswith('60'):
            return 'fund'
    
    return 'stock'
