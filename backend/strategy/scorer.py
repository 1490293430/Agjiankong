"""
股票打分器
"""
from typing import Dict, Any
from common.logger import get_logger

logger = get_logger(__name__)


def score_stock(stock: Dict[str, Any], indicators: Dict[str, Any]) -> int:
    """对股票进行综合打分（0-100分）
    
    Args:
        stock: 股票数据
        indicators: 技术指标
    
    Returns:
        分数（0-100）
    """
    score = 0
    
    try:
        # 1. 趋势分析（30分）
        ma5 = indicators.get("ma5")
        ma10 = indicators.get("ma10")
        ma20 = indicators.get("ma20")
        
        if ma5 and ma10 and ma20:
            if ma5 > ma10 > ma20:
                score += 30  # 完美多头排列
            elif ma5 > ma20:
                score += 15  # 短期在长期之上
            elif ma5 > ma10:
                score += 8   # 短期趋势向上
        
        # 2. MACD分析（20分）
        macd_dif = indicators.get("macd_dif")
        macd_dea = indicators.get("macd_dea")
        
        if macd_dif is not None and macd_dea is not None:
            if macd_dif > macd_dea > 0:
                score += 20  # 金叉且在零轴上方
            elif macd_dif > macd_dea:
                score += 10  # 金叉
            elif macd_dif > 0:
                score += 5   # 在零轴上方
        
        # 3. RSI分析（15分）
        rsi = indicators.get("rsi")
        if rsi is not None:
            if 40 < rsi < 70:
                score += 15  # 健康区间
            elif 30 < rsi <= 40:
                score += 8   # 偏弱但可关注
            elif 70 < rsi < 80:
                score += 5   # 偏强但需注意
            elif rsi < 30:
                score += 3   # 超卖
            elif rsi > 80:
                score -= 5   # 超买（扣分）
        
        # 4. 成交量分析（20分）
        vol_ratio = indicators.get("vol_ratio")
        if vol_ratio:
            if vol_ratio > 1.5:
                score += 20  # 明显放量
            elif vol_ratio > 1.1:
                score += 10  # 温和放量
            elif vol_ratio > 0.8:
                score += 5   # 正常
            elif vol_ratio < 0.5:
                score -= 5   # 缩量（扣分）
        
        # 5. 当日强度（15分）
        pct = stock.get("pct", 0)
        if isinstance(pct, (int, float)):
            if 2 < pct < 6:
                score += 15  # 涨幅适中
            elif 0 < pct <= 2:
                score += 8   # 小幅上涨
            elif 6 < pct < 9:
                score += 5   # 涨幅较大
            elif pct < -3:
                score -= 5   # 大幅下跌（扣分）
        
        # 确保分数在0-100之间
        score = max(0, min(100, score))
        
    except Exception as e:
        logger.error(f"股票打分失败 {stock.get('code', '')}: {e}", exc_info=True)
        score = 0
    
    return score

