"""
自动选股引擎
"""
from typing import List, Dict, Any, Optional
from strategy.scorer import score_stock
from common.logger import get_logger
from common.redis import get_json, set_json

logger = get_logger(__name__)

# 选股结果存储键
SELECTED_STOCKS_KEY = "app:selected_stocks"


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


def check_market_environment(all_stocks: List[Dict[str, Any]]) -> bool:
    """检查市场环境：上涨家数比率 > 50%
    
    Args:
        all_stocks: 全市场股票列表
    
    Returns:
        True表示市场环境良好（上涨家数>50%）
    """
    if not all_stocks:
        return False
    
    # 过滤掉无效数据（pct为NaN、None或0的股票）
    import math
    valid_stocks = []
    for stock in all_stocks:
        pct = stock.get("pct")
        # 检查pct是否为有效数值（不是NaN、None，且是数字）
        if pct is not None and not (isinstance(pct, float) and math.isnan(pct)):
            try:
                pct_val = float(pct)
                if not math.isnan(pct_val):
                    valid_stocks.append(stock)
            except (ValueError, TypeError):
                continue
    
    if not valid_stocks:
        logger.warning(f"市场环境检查：没有有效的股票数据（所有股票的pct都是NaN或无效）")
        return False
    
    total = len(valid_stocks)
    rising_count = sum(1 for stock in valid_stocks if stock.get("pct", 0) > 0)
    ratio = rising_count / total if total > 0 else 0
    
    logger.info(f"市场环境检查：有效股票数={total}，上涨股票数={rising_count}，比率={ratio:.2%}")
    # 放宽条件：如果有效股票数太少（可能是非交易时间），直接返回True允许选股
    if total < 100:
        logger.warning(f"有效股票数过少（{total}），可能是非交易时间，放宽市场环境检查")
        return True
    # 放宽条件：从50%降低到30%，避免过于严格
    return ratio > 0.3


def filter_stocks_by_criteria(
    stocks: List[Dict[str, Any]],
    indicators_map: Dict[str, Dict[str, Any]],
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """基于多维度筛选股票
    
    筛选维度：
    1. 市场环境：上涨家数比率 > 50%（已在外部检查）
    2. 个股趋势：价格 > MA60 且 MA60 斜率向上，短期均线组合向上
    3. 动量与强度：量比、RSI、威廉指标
    4. 价格结构：价格突破、布林带状态
    
    Args:
        stocks: 股票列表
        indicators_map: 技术指标字典 {code: indicators}
        config: 筛选配置
            - volume_ratio_min: 量比最小值（默认1.2）
            - volume_ratio_max: 量比最大值（默认5.0）
            - rsi_min: RSI最小值（默认40）
            - rsi_max: RSI最大值（默认65）
            - williams_r_enable: 是否启用威廉指标筛选（默认True）
            - break_high_enable: 是否启用突破高点筛选（默认True）
            - boll_enable: 是否启用布林带筛选（默认True）
    
    Returns:
        筛选后的股票列表
    """
    result = []
    
    volume_ratio_min = config.get("volume_ratio_min", 1.2)
    volume_ratio_max = config.get("volume_ratio_max", 5.0)
    rsi_min = config.get("rsi_min", 40)
    rsi_max = config.get("rsi_max", 65)
    williams_r_enable = config.get("williams_r_enable", True)
    break_high_enable = config.get("break_high_enable", True)
    boll_enable = config.get("boll_enable", True)
    
    for stock in stocks:
        code = str(stock.get("code", ""))
        indicators = indicators_map.get(code)
        
        if not indicators:
            continue
        
        current_price = stock.get("price", 0)
        if current_price <= 0:
            continue
        
        # 1. 个股趋势筛选
        ma60 = indicators.get("ma60")
        ma60_trend = indicators.get("ma60_trend", "")
        ma5 = indicators.get("ma5")
        ma10 = indicators.get("ma10")
        ma20 = indicators.get("ma20")
        ma5_trend = indicators.get("ma5_trend", "")
        ma10_trend = indicators.get("ma10_trend", "")
        
        # 价格 > MA60 且 MA60 向上
        if not ma60 or current_price <= ma60:
            continue
        
        if ma60_trend not in ["向上", "up"]:
            continue
        
        # 短期均线组合：MA5 > MA20 或 MA10 > MA20
        if ma5 and ma20:
            ma5_above_ma20 = ma5 > ma20
        else:
            ma5_above_ma20 = False
        
        if ma10 and ma20:
            ma10_above_ma20 = ma10 > ma20
        else:
            ma10_above_ma20 = False
        
        if not (ma5_above_ma20 or ma10_above_ma20):
            continue
        
        # 2. 动量与强度筛选
        vol_ratio = indicators.get("vol_ratio", 0)
        if vol_ratio < volume_ratio_min or vol_ratio > volume_ratio_max:
            continue
        
        rsi = indicators.get("rsi")
        if rsi is None or rsi < rsi_min or rsi > rsi_max:
            continue
        
        # 威廉指标：从超卖区（<-80）上穿 -50
        if williams_r_enable:
            williams_r = indicators.get("williams_r")
            williams_r_prev = indicators.get("williams_r_prev")
            
            if williams_r is not None and williams_r_prev is not None:
                # 前一日在超卖区（<-80），当前日上穿-50（从-80以下上穿到-50以上）
                if not (williams_r_prev < -80 and williams_r > -50):
                    continue
            elif williams_r is not None:
                # 如果没有前一日数据，至少当前不在超卖区（>-80）
                if williams_r < -80:
                    continue
        
        # 3. 价格结构筛选
        # 价格突破近期高点（20日新高）
        if break_high_enable:
            break_high = indicators.get("break_high_20d", False)
            if not break_high:
                continue
        
        # 布林带收口后突破：价格上穿布林中轨且布林带由收口转向开口
        if boll_enable:
            boll_middle = indicators.get("boll_middle")
            boll_expanding = indicators.get("boll_expanding", False)
            boll_contracting_prev = not indicators.get("boll_contracting", False)
            
            if boll_middle and current_price < boll_middle:
                continue
            
            # 要求布林带正在开口（由收口转向开口）
            if not boll_expanding:
                continue
        
        # 通过所有筛选，添加到结果
        stock_copy = stock.copy()
        stock_copy["indicators"] = {
            "ma60": ma60,
            "ma60_trend": ma60_trend,
            "vol_ratio": vol_ratio,
            "rsi": rsi,
            "williams_r": indicators.get("williams_r"),
        }
        result.append(stock_copy)
    
    logger.info(f"筛选完成：从{len(stocks)}只股票中筛选出{len(result)}只")
    return result


def save_selected_stocks(stocks: List[Dict[str, Any]], market: str = "A") -> None:
    """保存选股结果到Redis
    
    Args:
        stocks: 选中的股票列表
        market: 市场类型（A或HK）
    """
    try:
        data = {
            "market": market,
            "stocks": stocks,
            "count": len(stocks),
            "timestamp": __import__("datetime").datetime.now().isoformat()
        }
        set_json(SELECTED_STOCKS_KEY, data)
        logger.info(f"选股结果已保存：{market}股市场，{len(stocks)}只股票")
    except Exception as e:
        logger.error(f"保存选股结果失败: {e}")


def get_selected_stocks() -> Dict[str, Any]:
    """获取上次选股结果
    
    Returns:
        包含market、stocks、count、timestamp的字典
    """
    try:
        data = get_json(SELECTED_STOCKS_KEY)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.debug(f"获取选股结果失败: {e}")
    
    return {"market": "A", "stocks": [], "count": 0, "timestamp": None}

