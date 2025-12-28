"""
AI分析服务（可接入本地模型或API）
"""
from typing import Dict, Any, Optional, List, Tuple
import json
from datetime import datetime

import requests

from common.logger import get_logger
from ai.prompt import build_stock_analysis_prompt
from ai.parameter_optimizer import get_dynamic_parameters, get_parameter_optimizer
from common.runtime_config import get_runtime_config
from common.config import settings

logger = get_logger(__name__)

# AI请求历史记录（只保留最近1次完整分析）
AI_REQUEST_HISTORY_KEY = "ai:request:history"
MAX_REQUEST_HISTORY = 1


def _save_ai_request_history(request_data: Dict[str, Any]):
    """保存AI请求数据到Redis（只保留最近1次完整分析）
    
    时间戳统一使用北京时间（Asia/Shanghai），格式为 ISO 8601
    """
    try:
        from common.redis import set_json
        from datetime import datetime, timezone, timedelta
        
        # 统一使用北京时间（UTC+8）
        beijing_tz = timezone(timedelta(hours=8))
        now_beijing = datetime.now(beijing_tz)
        
        # 确保时间戳使用北京时间
        if "timestamp" not in request_data or not request_data["timestamp"]:
            request_data["timestamp"] = now_beijing.isoformat()
        
        # 添加时区标识
        request_data["timezone"] = "Asia/Shanghai"
        request_data["timestamp_utc"] = datetime.now(timezone.utc).isoformat()
        
        # 只保留最近1次，直接覆盖
        set_json(AI_REQUEST_HISTORY_KEY, [request_data])
        logger.debug(f"AI请求历史已保存")
    except Exception as e:
        logger.warning(f"保存AI请求历史失败: {e}")


def get_ai_request_history() -> List[Dict[str, Any]]:
    """获取AI请求历史记录"""
    try:
        from common.redis import get_json
        return get_json(AI_REQUEST_HISTORY_KEY) or []
    except Exception as e:
        logger.warning(f"获取AI请求历史失败: {e}")
        return []


def validate_trading_signals(signal_data: Dict[str, Any], dynamic_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """更严格的交易信号验证逻辑
    
    Args:
        signal_data: 包含交易信号的字典
        dynamic_params: 动态参数字典（如果为None，使用默认参数）
    
    Returns:
        验证后的信号数据，如果验证失败则返回观望信号
    """
    signal = signal_data.get("signal", "观望")
    
    # 获取动态参数
    if dynamic_params is None:
        # 从信号数据中获取指标（如果可用）
        indicators = signal_data.get("indicators", {})
        dynamic_params = get_dynamic_parameters(indicators) if indicators else {}
    
    min_risk_reward = dynamic_params.get("min_risk_reward", 1.5)
    rsi_upper_limit = dynamic_params.get("rsi_upper_limit", 80)
    
    # 如果不是买入信号，直接返回（建议价格由AI返回，不自动计算）
    if signal != "买入":
        return signal_data
    
    # 检查必需字段
    required_fields = ['buy_price', 'sell_price', 'stop_loss']
    for field in required_fields:
        if field not in signal_data or signal_data[field] is None:
            logger.warning(f"买入信号但缺少{field}字段")
            return {
                **signal_data,
                "signal": "观望",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "reason": f"缺少{field}字段"
            }
    
    try:
        buy_price = float(signal_data['buy_price'])
        sell_price = float(signal_data['sell_price'])
        stop_loss = float(signal_data['stop_loss'])
    except (ValueError, TypeError):
        logger.warning("交易点位格式错误")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": "交易点位格式错误"
        }
    
    # 验证价格关系
    if not (stop_loss < buy_price < sell_price):
        logger.warning(f"价格关系不合理: stop_loss={stop_loss}, buy_price={buy_price}, sell_price={sell_price}")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": "价格关系不合理：止损价必须小于买入价，买入价必须小于止盈价"
        }
    
    # 验证价格必须为正数
    if buy_price <= 0 or sell_price <= 0 or stop_loss <= 0:
        logger.warning(f"交易点位必须为正数")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": "交易点位必须为正数"
        }
    
    # 验证风险回报比
    risk = buy_price - stop_loss
    reward = sell_price - buy_price
    
    if risk <= 0:
        logger.warning(f"风险计算错误: risk={risk}")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": "风险计算错误"
        }
    
    # 验证单笔亏损金额（总资金1万，单笔最大亏损3%即300元）
    TOTAL_CAPITAL = 10000.0
    MAX_LOSS_PCT = 0.03  # 3%
    MAX_LOSS_AMOUNT = TOTAL_CAPITAL * MAX_LOSS_PCT  # 300元
    
    if buy_price > 0:
        max_shares = TOTAL_CAPITAL / buy_price
        loss_per_share = risk
        total_loss = loss_per_share * max_shares
        
        if total_loss > MAX_LOSS_AMOUNT:
            logger.warning(f"单笔亏损金额过大: {total_loss:.2f}元，超过限制{MAX_LOSS_AMOUNT:.2f}元")
            return {
                **signal_data,
                "signal": "观望",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "reason": f"单笔亏损金额过大: {total_loss:.2f}元，超过限制{MAX_LOSS_AMOUNT:.2f}元（总资金的3%）"
            }
    
    risk_reward_ratio = reward / risk
    
    if risk_reward_ratio < min_risk_reward:
        logger.warning(f"风险回报比不足: {risk_reward_ratio:.2f}，要求>={min_risk_reward}")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": f"风险回报比不足: {risk_reward_ratio:.2f}，要求>={min_risk_reward}"
        }
    
    # 检查RSI超买（如果可用）
    rsi = signal_data.get("rsi")
    if rsi and rsi > rsi_upper_limit:
        logger.warning(f"RSI超买: {rsi:.2f}，上限={rsi_upper_limit}")
        return {
            **signal_data,
            "signal": "观望",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "reason": f"RSI超买: {rsi:.2f}，上限={rsi_upper_limit}"
        }
    
    # 验证通过，添加风险回报比信息
    signal_data["risk_reward_ratio"] = round(risk_reward_ratio, 2)
    signal_data["risk_pct"] = round((risk / buy_price) * 100, 2)
    signal_data["reward_pct"] = round((reward / buy_price) * 100, 2)
    
    # 量价配合验证（添加警告但不阻止交易）
    indicators = signal_data.get("indicators", {})
    vol_ratio = indicators.get("vol_ratio") or indicators.get("hourly_vol_ratio")
    if vol_ratio and vol_ratio < 0.8:
        signal_data["volume_warning"] = f"成交量偏低（量比{vol_ratio:.2f}），需关注量能配合"
        logger.info(f"量价配合警告: 量比={vol_ratio:.2f}，建议关注成交量变化")
    
    return signal_data


class TradingSystemMonitor:
    """交易系统性能监控"""
    
    def __init__(self):
        self.signals_history: list = []
        self.performance_metrics = {
            'total_signals': 0,
            'buy_signals': 0,
            'watch_signals': 0,
            'avg_risk_reward': 0.0,
            'avg_confidence': 0.0
        }
    
    def record_signal(self, signal_data: Dict[str, Any]):
        """记录交易信号"""
        self.signals_history.append({
            **signal_data,
            "timestamp": datetime.now().isoformat()
        })
        
        self.performance_metrics['total_signals'] += 1
        
        signal = signal_data.get("signal", "观望")
        if signal == "买入":
            self.performance_metrics['buy_signals'] += 1
            if signal_data.get("risk_reward_ratio"):
                # 更新平均风险回报比
                total = self.performance_metrics['buy_signals']
                current_avg = self.performance_metrics['avg_risk_reward']
                new_ratio = signal_data["risk_reward_ratio"]
                self.performance_metrics['avg_risk_reward'] = (
                    (current_avg * (total - 1) + new_ratio) / total
                )
        else:
            self.performance_metrics['watch_signals'] += 1
        
        # 更新平均置信度
        confidence = signal_data.get("confidence", 0)
        total = self.performance_metrics['total_signals']
        current_avg = self.performance_metrics['avg_confidence']
        self.performance_metrics['avg_confidence'] = (
            (current_avg * (total - 1) + confidence) / total
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        total = self.performance_metrics['total_signals']
        buy_count = self.performance_metrics['buy_signals']
        
        return {
            **self.performance_metrics,
            'signal_rate': round((buy_count / total * 100) if total > 0 else 0, 2)
        }


# 全局监控实例
_monitor = TradingSystemMonitor()


def get_system_metrics() -> Dict[str, Any]:
    """获取交易系统性能指标"""
    return _monitor.get_metrics()


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
    
    # 根据评分判断信号
    if score >= 5:
        signal = "买入"
    elif score >= 3:
        signal = "关注"
    elif score >= 1:
        signal = "观望"
    else:
        signal = "回避"
    
    return {
        "trend": trend,
        "risk": risk,
        "confidence": min(abs(score) / 8, 1.0),
        "score": score,
        "signal": signal,
        "key_factors": reasons[:5],
        "advice": advice,
        "summary": f"{stock.get('name', '')}当前趋势{trend}，{advice}",
        "buy_price": None,
        "sell_price": None,
        "stop_loss": None
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


def analyze_stock_with_ai(stock: dict, indicators: dict, news: list = None, include_trading_points: bool = False) -> Dict[str, Any]:
    """使用 OpenAI 兼容接口进行 AI 分析
    
    Args:
        stock: 股票数据
        indicators: 技术指标
        news: 相关资讯
        include_trading_points: 是否包含交易点位（买入价、卖出价、止损价）
    """

    ai_cfg = _get_ai_runtime_config()
    if not ai_cfg:
        # 未配置 AI，退回规则分析
        logger.warning("AI 配置缺失，使用简单规则分析")
        return analyze_stock_simple(stock, indicators, news)

    # 获取动态参数
    dynamic_params = get_dynamic_parameters(indicators)
    
    prompt = build_stock_analysis_prompt(
        stock, indicators, news, 
        include_trading_points=include_trading_points,
        dynamic_params=dynamic_params
    )

    # 如果不需要交易点位，添加JSON格式说明
    if not include_trading_points:
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
    else:
        # 包含交易点位的prompt已经包含了JSON格式说明
        full_prompt = prompt

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
            "temperature": 0.2 if not include_trading_points else 0.3,
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=None)  # 不限时
        resp.raise_for_status()
        data = resp.json()

        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        
        # 记录AI请求历史
        _save_ai_request_history({
            "timestamp": datetime.now().isoformat(),
            "type": "single",
            "stock": {
                "code": stock.get("code", ""),
                "name": stock.get("name", ""),
                "price": stock.get("price", 0),
                "pct": stock.get("pct", 0),
            },
            "indicators": indicators,
            "dynamic_params": dynamic_params,
            "request": {
                "url": url,
                "model": ai_cfg["model"],
                "temperature": payload["temperature"],
                "prompt_length": len(full_prompt),
            },
            "prompt": full_prompt,
            "response": content,
            "include_trading_points": include_trading_points,
        })

        parsed: Dict[str, Any]
        try:
            # 尝试提取JSON（可能包含markdown代码块）
            if "```json" in content:
                start = content.find("```json") + 7
                end = content.find("```", start)
                content = content[start:end].strip()
            elif "```" in content:
                start = content.find("```") + 3
                end = content.find("```", start)
                content = content[start:end].strip()
            
            parsed = json.loads(content)
        except json.JSONDecodeError:
            # 如果模型没有返回合法 JSON，返回错误信息
            logger.warning("AI 返回内容不是合法 JSON")
            return {
                "trend": "未知",
                "risk": "未知",
                "confidence": 0,
                "score": 0,
                "signal": "观望",
                "key_factors": [],
                "advice": "AI返回格式错误",
                "summary": "AI返回内容解析失败",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "ref_buy_price": None,
                "ref_sell_price": None,
                "ref_stop_loss": None,
                "wait_conditions": [],
                "reason": "AI返回格式错误"
            }

        # 与前端期望的结构对齐，缺失字段使用默认值补全
        result = {
            "trend": parsed.get("trend", "未知"),
            "risk": parsed.get("risk", "未知"),
            "confidence": int(parsed.get("confidence", 0)),
            "score": int(parsed.get("score", 0)),
            "key_factors": parsed.get("key_factors", []),
            "advice": parsed.get("advice", "暂无建议"),
            "summary": parsed.get("summary", "暂无总结"),
        }
        
        # 如果包含交易点位，添加相关字段并使用验证函数
        if include_trading_points:
            signal = parsed.get("signal", "观望")
            result["signal"] = signal
            
            # 买入信号或强烈看多信号都返回交易点位
            if signal in ["买入", "强烈看多"]:
                result["buy_price"] = parsed.get("buy_price")
                result["sell_price"] = parsed.get("sell_price")
                result["stop_loss"] = parsed.get("stop_loss")
                result["reason"] = parsed.get("reason", "符合三重过滤趋势波段系统" if signal == "买入" else "多周期共振，信号强烈")
                result["indicators"] = indicators  # 传递指标用于RSI检查
                result["wait_conditions"] = []  # 买入信号无需等待条件
                
                # 只有买入信号才进行严格验证，强烈看多信号保留AI返回的建议价格
                if signal == "买入":
                    validated = validate_trading_signals(result, dynamic_params=dynamic_params)
                    result = validated
            elif signal in ["关注", "观望"]:
                # 只有关注信号返回参考价格和等待条件，观望不返回
                result["buy_price"] = None
                result["sell_price"] = None
                result["stop_loss"] = None
                if signal == "关注":
                    result["ref_buy_price"] = parsed.get("ref_buy_price")
                    result["ref_sell_price"] = parsed.get("ref_sell_price")
                    result["ref_stop_loss"] = parsed.get("ref_stop_loss")
                    result["wait_conditions"] = parsed.get("wait_conditions", [])
                    result["reason"] = parsed.get("reason", "趋势向好但入场时机未到，需等待条件满足")
                else:
                    result["ref_buy_price"] = None
                    result["ref_sell_price"] = None
                    result["ref_stop_loss"] = None
                    result["wait_conditions"] = []
                    result["reason"] = parsed.get("reason", "不符合三重过滤系统入场条件")
            else:
                result["buy_price"] = None
                result["sell_price"] = None
                result["stop_loss"] = None
                result["ref_buy_price"] = None
                result["ref_sell_price"] = None
                result["ref_stop_loss"] = None
                result["wait_conditions"] = []
                result["reason"] = parsed.get("reason", "趋势向下，不符合做多条件")
            
            # 记录信号到监控系统
            _monitor.record_signal(result)
        
        return result

    except requests.exceptions.Timeout as e:
        logger.error(f"调用 AI 接口超时: {e}")
        # API超时，返回超时错误
        return {
            "trend": "未知",
            "risk": "未知",
            "confidence": 0,
            "score": 0,
            "signal": "观望",
            "key_factors": [],
            "advice": "API超时",
            "summary": "AI接口请求超时，请稍后重试",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "ref_buy_price": None,
            "ref_sell_price": None,
            "ref_stop_loss": None,
            "wait_conditions": [],
            "reason": "API超时"
        }
    except Exception as e:
        logger.error(f"调用 AI 接口失败: {e}", exc_info=True)
        # 出现异常时返回错误信息
        return {
            "trend": "未知",
            "risk": "未知",
            "confidence": 0,
            "score": 0,
            "signal": "观望",
            "key_factors": [],
            "advice": "AI分析失败",
            "summary": f"AI接口调用失败: {str(e)[:50]}",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None,
            "ref_buy_price": None,
            "ref_sell_price": None,
            "ref_stop_loss": None,
            "wait_conditions": [],
            "reason": "AI接口错误"
        }


def analyze_stocks_batch_with_ai(stocks_data: list, include_trading_points: bool = False, save_history: bool = False) -> List[Dict[str, Any]]:
    """使用 OpenAI 兼容接口批量分析多支股票（一次请求）
    
    Args:
        stocks_data: 股票数据列表，每个元素为 (stock, indicators, news) 的元组
        include_trading_points: 是否包含交易点位
        save_history: 是否保存到历史记录（默认False，由调用方统一保存）
    
    Returns:
        分析结果列表，顺序与输入一致
    """
    if not stocks_data:
        return []
    
    ai_cfg = _get_ai_runtime_config()
    if not ai_cfg:
        # 未配置 AI，返回配置缺失错误
        logger.warning("AI 配置缺失")
        return [
            {
                "code": stock.get('code', ''),
                "name": stock.get('name', ''),
                "trend": "未知",
                "risk": "未知",
                "confidence": 0,
                "score": 0,
                "signal": "观望",
                "key_factors": [],
                "advice": "AI未配置",
                "summary": "请先在配置页设置AI API Key",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "ref_buy_price": None,
                "ref_sell_price": None,
                "ref_stop_loss": None,
                "wait_conditions": [],
                "reason": "AI未配置"
            }
            for stock, indicators, news in stocks_data
        ]
    
    # 获取动态参数（使用第一支股票的数据）
    if stocks_data:
        dynamic_params = get_dynamic_parameters(stocks_data[0][1])
    else:
        dynamic_params = {}
    
    from ai.prompt import build_stocks_batch_analysis_prompt
    prompt = build_stocks_batch_analysis_prompt(
        stocks_data,
        include_trading_points=include_trading_points,
        dynamic_params=dynamic_params
    )
    
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
                    "content": "你是一名专业的中文股票分析师，请根据提供的数据给出客观理性的分析。返回的数据必须是有效的JSON格式。",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2 if not include_trading_points else 0.3,
        }
        
        resp = requests.post(url, headers=headers, json=payload, timeout=None)  # 不限时（批量分析需要更长时间）
        resp.raise_for_status()
        data = resp.json()
        
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        
        # 记录AI请求历史（批量分析）- 只在save_history=True时保存
        if save_history:
            stocks_summary = [
                {
                    "code": stock.get("code", ""),
                    "name": stock.get("name", ""),
                    "price": stock.get("price", 0),
                    "pct": stock.get("pct", 0),
                }
                for stock, indicators, news in stocks_data
            ]
            _save_ai_request_history({
                "timestamp": datetime.now().isoformat(),
                "type": "batch",
                "stocks_count": len(stocks_data),
                "stocks": stocks_summary,
                "indicators_sample": stocks_data[0][1] if stocks_data else {},  # 只保存第一只股票的指标作为样本
                "dynamic_params": dynamic_params,
                "request": {
                    "url": url,
                    "model": ai_cfg["model"],
                    "temperature": payload["temperature"],
                    "prompt_length": len(prompt),
                },
                "prompt": prompt,
                "response": content,
                "include_trading_points": include_trading_points,
            })
        
        # 解析JSON数组
        try:
            # 尝试提取JSON（可能包含markdown代码块）
            if "```json" in content:
                start = content.find("```json") + 7
                end = content.find("```", start)
                content = content[start:end].strip()
            elif "```" in content:
                start = content.find("```") + 3
                end = content.find("```", start)
                content = content[start:end].strip()
            
            parsed_list = json.loads(content)
            
            # 确保返回的是列表
            if not isinstance(parsed_list, list):
                logger.warning("AI 返回的不是数组格式，尝试转换")
                parsed_list = [parsed_list]
            
            # 验证数量是否匹配
            if len(parsed_list) != len(stocks_data):
                logger.warning(f"AI 返回结果数量({len(parsed_list)})与输入股票数量({len(stocks_data)})不匹配")
                # 补充缺失的结果
                while len(parsed_list) < len(stocks_data):
                    parsed_list.append({
                        "code": stocks_data[len(parsed_list)][0].get('code', ''),
                        "name": stocks_data[len(parsed_list)][0].get('name', ''),
                        "trend": "未知",
                        "risk": "未知",
                        "confidence": 0,
                        "score": 0,
                        "signal": "观望",
                        "key_factors": [],
                        "advice": "AI分析失败",
                        "summary": "批量分析结果缺失",
                        "buy_price": None,
                        "sell_price": None,
                        "stop_loss": None,
                        "ref_buy_price": None,
                        "ref_sell_price": None,
                        "ref_stop_loss": None,
                        "wait_conditions": [],
                        "reason": "AI返回结果数量不足"
                    })
                # 截断多余的结果
                parsed_list = parsed_list[:len(stocks_data)]
            
            # 为每个结果添加code和name（如果缺失）
            results = []
            for i, result in enumerate(parsed_list):
                stock = stocks_data[i][0]
                
                # 确保code和name存在
                if 'code' not in result or not result['code']:
                    result['code'] = stock.get('code', '')
                if 'name' not in result or not result['name']:
                    result['name'] = stock.get('name', '')
                
                # 补全缺失字段，确保格式与单次分析完全一致
                final_result = {
                    "code": result.get("code", stock.get('code', '')),
                    "name": result.get("name", stock.get('name', '')),
                    "trend": result.get("trend", "未知"),
                    "risk": result.get("risk", "未知"),
                    "confidence": int(result.get("confidence", 0)),
                    "score": int(result.get("score", 0)),
                    "signal": result.get("signal", "观望"),
                    "key_factors": result.get("key_factors", []),
                    "advice": result.get("advice", "暂无建议"),
                    "summary": result.get("summary", "暂无总结"),
                }
                
                if include_trading_points:
                    signal = final_result.get("signal", "观望")
                    # 买入信号或强烈看多信号都返回交易点位
                    if signal in ["买入", "强烈看多"]:
                        final_result.update({
                            "buy_price": result.get("buy_price"),
                            "sell_price": result.get("sell_price"),
                            "stop_loss": result.get("stop_loss"),
                            "reason": result.get("reason", "符合三重过滤趋势波段系统" if signal == "买入" else "多周期共振，信号强烈"),
                            "wait_conditions": [],
                        })
                        # 只有买入信号才进行严格验证，强烈看多信号保留AI返回的建议价格
                        if signal == "买入":
                            final_result = validate_trading_signals(final_result, dynamic_params)
                    elif signal in ["关注", "观望"]:
                        # 只有关注信号返回参考价格和等待条件，观望不返回
                        if signal == "关注":
                            final_result.update({
                                "buy_price": None,
                                "sell_price": None,
                                "stop_loss": None,
                                "ref_buy_price": result.get("ref_buy_price"),
                                "ref_sell_price": result.get("ref_sell_price"),
                                "ref_stop_loss": result.get("ref_stop_loss"),
                                "wait_conditions": result.get("wait_conditions", []),
                                "reason": result.get("reason", "趋势向好但入场时机未到，需等待条件满足"),
                            })
                        else:
                            final_result.update({
                                "buy_price": None,
                                "sell_price": None,
                                "stop_loss": None,
                                "ref_buy_price": None,
                                "ref_sell_price": None,
                                "ref_stop_loss": None,
                                "wait_conditions": [],
                                "reason": result.get("reason", "不符合三重过滤系统入场条件"),
                            })
                    else:
                        # 回避信号
                        final_result.update({
                            "buy_price": None,
                            "sell_price": None,
                            "stop_loss": None,
                            "ref_buy_price": None,
                            "ref_sell_price": None,
                            "ref_stop_loss": None,
                            "wait_conditions": [],
                            "reason": result.get("reason", "趋势向下，不符合做多条件"),
                        })
                else:
                    # 不包含交易点位时，也要有这些字段（设为None），保持格式一致
                    final_result.update({
                        "buy_price": None,
                        "sell_price": None,
                        "stop_loss": None,
                        "reason": None,
                    })
                
                results.append(final_result)
            
            return results
            
        except json.JSONDecodeError as e:
            logger.warning(f"AI 返回内容不是合法 JSON: {e}")
            # 返回API解析失败的结果
            return [
                {
                    "code": stock.get('code', ''),
                    "name": stock.get('name', ''),
                    "trend": "未知",
                    "risk": "未知",
                    "confidence": 0,
                    "score": 0,
                    "signal": "观望",
                    "key_factors": [],
                    "advice": "AI返回格式错误",
                    "summary": "AI返回内容解析失败",
                    "buy_price": None,
                    "sell_price": None,
                    "stop_loss": None,
                    "ref_buy_price": None,
                    "ref_sell_price": None,
                    "ref_stop_loss": None,
                    "wait_conditions": [],
                    "reason": "AI返回格式错误"
                }
                for stock, indicators, news in stocks_data
            ]
        
    except requests.exceptions.Timeout as e:
        logger.error(f"批量调用 AI 接口超时: {e}")
        # API超时，返回超时错误
        return [
            {
                "code": stock.get('code', ''),
                "name": stock.get('name', ''),
                "trend": "未知",
                "risk": "未知",
                "confidence": 0,
                "score": 0,
                "signal": "观望",
                "key_factors": [],
                "advice": "API超时",
                "summary": "AI接口请求超时，请稍后重试",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "ref_buy_price": None,
                "ref_sell_price": None,
                "ref_stop_loss": None,
                "wait_conditions": [],
                "reason": "API超时"
            }
            for stock, indicators, news in stocks_data
        ]
    except Exception as e:
        logger.error(f"批量调用 AI 接口失败: {e}", exc_info=True)
        # 出现异常时返回错误信息
        return [
            {
                "code": stock.get('code', ''),
                "name": stock.get('name', ''),
                "trend": "未知",
                "risk": "未知",
                "confidence": 0,
                "score": 0,
                "signal": "观望",
                "key_factors": [],
                "advice": "AI分析失败",
                "summary": f"AI接口调用失败: {str(e)[:50]}",
                "buy_price": None,
                "sell_price": None,
                "stop_loss": None,
                "ref_buy_price": None,
                "ref_sell_price": None,
                "ref_stop_loss": None,
                "wait_conditions": [],
                "reason": "AI接口错误"
            }
            for stock, indicators, news in stocks_data
        ]


def analyze_stock(stock: dict, indicators: dict, news: list = None, use_ai: bool = False, include_trading_points: bool = False) -> Dict[str, Any]:
    """分析股票（统一入口）
    
    Args:
        stock: 股票数据
        indicators: 技术指标
        news: 相关资讯
        use_ai: 是否使用AI模型（默认False，使用规则分析）
        include_trading_points: 是否包含交易点位（买入价、卖出价、止损价）
    """
    try:
        if use_ai:
            return analyze_stock_with_ai(stock, indicators, news, include_trading_points=include_trading_points)
        else:
            return analyze_stock_simple(stock, indicators, news)
    except Exception as e:
        logger.error(f"股票分析失败 {stock.get('code', '')}: {e}", exc_info=True)
        return {
            "trend": "未知",
            "risk": "未知",
            "confidence": 0,
            "score": 0,
            "signal": "观望",
            "key_factors": [],
            "advice": "数据不足，无法分析",
            "summary": "分析失败",
            "buy_price": None,
            "sell_price": None,
            "stop_loss": None
        }

