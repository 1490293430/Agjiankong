"""
自动选股引擎
"""
from typing import List, Dict, Any, Optional
from strategy.scorer import score_stock
from common.logger import get_logger

logger = get_logger(__name__)


def select_stocks(
    stocks: List[Dict[str, Any]],
    indicators_map: Dict[str, Dict[str, Any]],
    threshold: int = 65,
    max_count: int = 30,
    exclude_st: bool = True,
    min_volume: float = 1000000  # 最小成交额（万元）
) -> List[Dict[str, Any]]:
    """自动选股
    
    Args:
        stocks: 股票列表
        indicators_map: 技术指标字典 {code: indicators}
        threshold: 分数阈值（默认65）
        max_count: 最大选择数量（默认30）
        exclude_st: 是否排除ST股票
        min_volume: 最小成交额（万元）
    
    Returns:
        选中的股票列表（按分数降序）
    """
    result = []
    
    try:
        for stock in stocks:
            code = str(stock.get("code", ""))
            name = str(stock.get("name", ""))
            
            # 过滤ST股票
            if exclude_st and ("ST" in name or "*" in name):
                continue
            
            # 过滤成交额过小的股票
            amount = stock.get("amount", 0)
            if isinstance(amount, (int, float)) and amount < min_volume:
                continue
            
            # 过滤停牌股票（涨跌幅为0且成交量为0）
            pct = stock.get("pct", 0)
            volume = stock.get("volume", 0)
            if abs(pct) < 0.01 and volume == 0:
                continue
            
            # 获取技术指标
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            # 打分
            score = score_stock(stock, indicators)
            
            # 达到阈值则加入
            if score >= threshold:
                stock_copy = stock.copy()
                stock_copy["score"] = score
                result.append(stock_copy)
        
        # 按分数降序排序
        result.sort(key=lambda x: x.get("score", 0), reverse=True)
        
        # 限制数量
        result = result[:max_count]
        
        logger.info(f"选股完成：阈值{threshold}，选中{len(result)}只股票")
        
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
    
    return result


def filter_by_sector(
    stocks: List[Dict[str, Any]],
    sectors: List[str]
) -> List[Dict[str, Any]]:
    """按板块筛选股票
    
    Args:
        stocks: 股票列表
        sectors: 板块列表（需要股票数据中包含sector字段）
    
    Returns:
        筛选后的股票列表
    """
    if not sectors:
        return stocks
    
    return [
        stock for stock in stocks
        if stock.get("sector") in sectors
    ]

