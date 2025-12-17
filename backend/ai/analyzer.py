"""
AI分析服务（可接入本地模型或API）
"""
from typing import Dict, Any, Optional
from common.logger import get_logger
from ai.prompt import build_stock_analysis_prompt

logger = get_logger(__name__)


def analyze_stock_simple(stock: dict, indicators: dict, news: list = None) -> Dict[str, Any]:
    """简单规则分析（无需AI模型）"""
    
    score = 0
    reasons = []
    
    # 趋势分析
    ma5 = indicators.get("ma5")
    ma10 = indicators.get("ma10")
    ma20 = indicators.get("ma20")
    
    if ma5 and ma10 and ma20:
        if ma5 > ma10 > ma20:
            score += 3
            reasons.append("多头排列，趋势向上")
        elif ma5 > ma20:
            score += 1
            reasons.append("短期均线在长期均线之上")
    
    # MACD分析
    macd_dif = indicators.get("macd_dif")
    macd_dea = indicators.get("macd_dea")
    
    if macd_dif and macd_dea:
        if macd_dif > macd_dea > 0:
            score += 2
            reasons.append("MACD金叉且在零轴上方")
        elif macd_dif > macd_dea:
            score += 1
            reasons.append("MACD金叉")
    
    # RSI分析
    rsi = indicators.get("rsi")
    if rsi:
        if 40 < rsi < 70:
            score += 1
            reasons.append("RSI处于健康区间")
        elif rsi > 80:
            score -= 1
            reasons.append("RSI超买")
        elif rsi < 20:
            score += 1
            reasons.append("RSI超卖")
    
    # 成交量分析
    vol_ratio = indicators.get("vol_ratio")
    if vol_ratio:
        if vol_ratio > 1.5:
            score += 1
            reasons.append("成交量明显放大")
        elif vol_ratio < 0.5:
            score -= 1
            reasons.append("成交量萎缩")
    
    # 涨跌幅分析
    pct = stock.get("pct", 0)
    if 2 < pct < 6:
        score += 1
        reasons.append("涨幅适中")
    elif pct > 9:
        score -= 1
        reasons.append("涨停或接近涨停，追高风险")
    
    # 综合判断
    if score >= 5:
        trend = "强势"
        risk = "中"
        advice = "可以考虑买入，注意止损"
    elif score >= 3:
        trend = "偏强"
        risk = "中"
        advice = "可以关注，回踩时考虑介入"
    elif score >= 1:
        trend = "震荡"
        risk = "中"
        advice = "观望为主"
    else:
        trend = "偏弱"
        risk = "高"
        advice = "建议回避"
    
    return {
        "trend": trend,
        "risk": risk,
        "confidence": min(abs(score) / 8, 1.0),
        "score": score,
        "key_factors": reasons[:5],
        "advice": advice,
        "summary": f"{stock.get('name', '')}当前趋势{trend}，{advice}"
    }


def analyze_stock_with_ai(stock: dict, indicators: dict, news: list = None) -> Dict[str, Any]:
    """使用AI模型分析（需要接入实际模型）
    
    你可以在这里接入：
    - OpenAI API
    - 本地大模型（Qwen, DeepSeek等）
    - 其他AI服务
    """
    
    # 如果没有配置AI模型，使用简单分析
    # 你可以在这里添加实际的AI调用代码
    # 例如：
    # prompt = build_stock_analysis_prompt(stock, indicators, news)
    # result = call_ai_model(prompt)
    # return parse_ai_response(result)
    
    return analyze_stock_simple(stock, indicators, news)


def analyze_stock(stock: dict, indicators: dict, news: list = None, use_ai: bool = False) -> Dict[str, Any]:
    """分析股票（统一入口）
    
    Args:
        stock: 股票数据
        indicators: 技术指标
        news: 相关资讯
        use_ai: 是否使用AI模型（默认False，使用规则分析）
    """
    try:
        if use_ai:
            return analyze_stock_with_ai(stock, indicators, news)
        else:
            return analyze_stock_simple(stock, indicators, news)
    except Exception as e:
        logger.error(f"股票分析失败 {stock.get('code', '')}: {e}", exc_info=True)
        return {
            "trend": "未知",
            "risk": "未知",
            "confidence": 0,
            "score": 0,
            "key_factors": [],
            "advice": "数据不足，无法分析",
            "summary": "分析失败"
        }

