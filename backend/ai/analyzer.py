；"""
AI分析服务（可接入本地模型或API）
"""
from typing import Dict, Any, Optional
import json

import requests

from common.logger import get_logger
from ai.prompt import build_stock_analysis_prompt
from common.runtime_config import get_runtime_config
from common.config import settings

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


def _get_ai_runtime_config() -> Optional[Dict[str, Any]]:
    """从运行时配置和环境变量获取 AI 配置"""
    cfg = get_runtime_config()

    api_key = cfg.openai_api_key or settings.openai_api_key
    api_base = cfg.openai_api_base or settings.openai_api_base
    model = cfg.openai_model or settings.openai_model

    if not api_key:
        return None

    return {
        "api_key": api_key,
        "api_base": api_base.rstrip("/"),
        "model": model,
    }


def analyze_stock_with_ai(stock: dict, indicators: dict, news: list = None) -> Dict[str, Any]:
    """使用 OpenAI 兼容接口进行 AI 分析"""

    ai_cfg = _get_ai_runtime_config()
    if not ai_cfg:
        # 未配置 AI，退回规则分析
        logger.warning("AI 配置缺失，使用简单规则分析")
        return analyze_stock_simple(stock, indicators, news)

    prompt = build_stock_analysis_prompt(stock, indicators, news)

    # 要求模型直接输出 JSON，方便前端消费
    json_instruction = """
请严格使用以下 JSON 格式回答，不要输出任何多余文字或注释：
{
  "trend": "上涨/下跌/震荡/未知",
  "risk": "低/中/高/未知",
  "confidence": 0-100之间的整数,
  "score": 一个整数评分（例如 -100 到 100，越高代表越看多）,
  "key_factors": ["关键因素1", "关键因素2", "..."],
  "advice": "一句话操作建议，如：短线可以逢低少量买入，控制仓位。",
  "summary": "100字以内的综合总结。"
}
"""

    full_prompt = prompt + "\n\n" + json_instruction

    try:
        url = f"{ai_cfg['api_base']}/chat/completions"
        headers = {
            "Authorization": f"Bearer {ai_cfg['api_key']}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": ai_cfg["model"],
            "messages": [
                {
                    "role": "system",
                    "content": "你是一名专业的中文股票分析师，请根据提供的数据给出客观理性的分析。",
                },
                {"role": "user", "content": full_prompt},
            ],
            "temperature": 0.2,
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )

        parsed: Dict[str, Any]
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            # 如果模型没有返回合法 JSON，则退回简单分析
            logger.warning("AI 返回内容不是合法 JSON，使用简单规则分析")
            return analyze_stock_simple(stock, indicators, news)

        # 与前端期望的结构对齐，缺失字段使用默认值补全
        fallback = analyze_stock_simple(stock, indicators, news)

        return {
            "trend": parsed.get("trend") or fallback.get("trend", "未知"),
            "risk": parsed.get("risk") or fallback.get("risk", "未知"),
            "confidence": int(parsed.get("confidence", fallback.get("confidence", 0))),
            "score": int(parsed.get("score", fallback.get("score", 0))),
            "key_factors": parsed.get("key_factors") or fallback.get("key_factors", []),
            "advice": parsed.get("advice") or fallback.get("advice", "暂无建议"),
            "summary": parsed.get("summary") or fallback.get("summary", "暂无总结"),
        }

    except Exception as e:
        logger.error(f"调用 AI 接口失败: {e}", exc_info=True)
        # 出现异常时退回简单分析，保证功能可用
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

