"""
AI分析提示词构建
"""
from typing import Optional, List, Dict, Any
from ai.parameter_optimizer import get_dynamic_parameters


def get_custom_prompt() -> Optional[str]:
    """从运行时配置获取自定义提示词"""
    try:
        from common.runtime_config import get_runtime_config
        cfg = get_runtime_config()
        return cfg.ai_custom_prompt if cfg.ai_custom_prompt else None
    except Exception:
        return None


def build_stock_analysis_prompt(stock: dict, indicators: dict, news: list = None, include_trading_points: bool = True, dynamic_params: dict = None) -> str:
    """构建股票分析提示词"""
    
    # 检查是否有自定义提示词
    custom_prompt = get_custom_prompt()
    if custom_prompt:
        indicators_str = "\n".join([f"- {k}: {v}" for k, v in indicators.items() if v is not None])
        prompt = custom_prompt
        prompt = prompt.replace("{stock_code}", str(stock.get('code', '')))
        prompt = prompt.replace("{stock_name}", str(stock.get('name', '')))
        prompt = prompt.replace("{current_price}", str(stock.get('price', 0)))
        prompt = prompt.replace("{pct}", str(stock.get('pct', 0)))
        prompt = prompt.replace("{indicators}", indicators_str)
        return prompt
    
    current_price = stock.get('price', 0)
    
    # 获取动态参数
    if dynamic_params is None:
        dynamic_params = get_dynamic_parameters(indicators)
    
    min_conditions = dynamic_params.get('min_conditions', 3)
    min_risk_reward = dynamic_params.get('min_risk_reward', 1.5)
    rsi_upper_limit = dynamic_params.get('rsi_upper_limit', 80)
    
    # 获取关键数据
    ma20 = indicators.get('ma20')
    ma60 = indicators.get('ma60')
    boll_upper = indicators.get('boll_upper')
    fib_382 = indicators.get('fib_382')
    fib_500 = indicators.get('fib_500')
    fib_618 = indicators.get('fib_618')
    recent_low = indicators.get('recent_low')
    
    prompt = f"""
你是专业的量化交易分析模型，使用"三重过滤趋势波段系统"进行交易决策。

【股票基本信息】
代码：{stock.get('code', '')}，名称：{stock.get('name', '')}
当前价：{current_price}元，涨跌幅：{stock.get('pct', 0)}%

【大盘】上证：{indicators.get('sh_index_price', 'N/A')}点，{indicators.get('sh_index_pct', 'N/A')}%

【日线均线】（当前值>前值为向上）
MA5：{indicators.get('ma5')}/{indicators.get('ma5_prev')}，MA10：{indicators.get('ma10')}/{indicators.get('ma10_prev')}
MA20：{ma20}/{indicators.get('ma20_prev')}，MA60：{ma60}/{indicators.get('ma60_prev')}

【日线MACD】
DIF：{indicators.get('macd_dif')}/{indicators.get('macd_dif_prev')}，MACD柱：{indicators.get('macd')}/{indicators.get('macd_prev')}

【日线指标】
RSI：{indicators.get('rsi')}，KDJ：K={indicators.get('kdj_k')}/D={indicators.get('kdj_d')}/J={indicators.get('kdj_j')}
CCI：{indicators.get('cci')}/{indicators.get('cci_prev')}，威廉%R：{indicators.get('williams_r')}/{indicators.get('williams_r_prev')}
ADX：{indicators.get('adx')}/{indicators.get('adx_prev')}，+DI：{indicators.get('plus_di')}，-DI：{indicators.get('minus_di')}
成交量比：{indicators.get('vol_ratio')}

【布林带】上轨：{boll_upper}，中轨：{indicators.get('boll_middle')}，下轨：{indicators.get('boll_lower')}

【斐波那契】高：{indicators.get('fib_swing_high')}，低：{indicators.get('fib_swing_low')}
38.2%：{fib_382}，50%：{fib_500}，61.8%：{fib_618}

【小时线】
MA5：{indicators.get('hourly_ma5')}，MA20：{indicators.get('hourly_ma20')}
MACD DIF：{indicators.get('hourly_macd_dif')}，MACD柱：{indicators.get('hourly_macd')}
RSI：{indicators.get('hourly_rsi')}，KDJ：K={indicators.get('hourly_kdj_k')}/D={indicators.get('hourly_kdj_d')}/J={indicators.get('hourly_kdj_j')}
成交量比：{indicators.get('hourly_vol_ratio')}

【止损参考】当前低：{indicators.get('current_low')}，近期低：{recent_low}

【风控】资金10000元，单笔最大亏损300元，风险回报比>={min_risk_reward}，RSI上限{rsi_upper_limit}

【信号规则】
- 买入/强烈看多：返回buy_price/sell_price/stop_loss
- 关注：返回ref_buy_price和wait_conditions
- 观望/回避：只返回基础字段

【价格参考】
买入价：MA20({ma20})或斐波那契38.2%({fib_382})/50%({fib_500})
止损价：斐波那契61.8%({fib_618})或近期低({recent_low})
止盈价：布林带上轨({boll_upper})

【reason要求】不超过30字，禁止"第一步""第二步"等分步描述

返回JSON：
{{
  "code": "{stock.get('code', '')}",
  "name": "{stock.get('name', '')}",
  "signal": "买入/强烈看多/关注/观望/回避",
  "trend": "上涨/下跌/震荡",
  "risk": "低/中/高",
  "confidence": 0-100,
  "score": -100到100,
  "key_factors": ["因素1", "因素2"],
  "advice": "一句话建议",
  "summary": "30字以内",
  "reason": "30字以内简洁理由",
  "buy_price": 数字或null,
  "sell_price": 数字或null,
  "stop_loss": 数字或null,
  "ref_buy_price": 数字或null,
  "wait_conditions": []
}}
"""
    return prompt



def build_stocks_batch_analysis_prompt(stocks_data: list, include_trading_points: bool = True, dynamic_params: dict = None) -> str:
    """构建批量股票分析提示词"""
    if not stocks_data:
        return ""
    
    # 大盘数据
    first_indicators = stocks_data[0][1] if stocks_data else {}
    sh_index_price = first_indicators.get('sh_index_price', 'N/A')
    sh_index_pct = first_indicators.get('sh_index_pct', 'N/A')
    
    # 构建股票信息列表
    stocks_info = []
    for i, (stock, indicators, news) in enumerate(stocks_data, 1):
        current_price = stock.get('price', 0)
        stock_info = f"""
【股票{i}】{stock.get('code', '')} {stock.get('name', '')}
价格：{current_price}元，涨跌：{stock.get('pct', 0)}%

日线均线：MA60={indicators.get('ma60')}/{indicators.get('ma60_prev')}，MA20={indicators.get('ma20')}/{indicators.get('ma20_prev')}，MA5={indicators.get('ma5')}/{indicators.get('ma5_prev')}
日线MACD：DIF={indicators.get('macd_dif')}/{indicators.get('macd_dif_prev')}，柱={indicators.get('macd')}/{indicators.get('macd_prev')}
日线指标：RSI={indicators.get('rsi')}，KDJ=K{indicators.get('kdj_k')}/D{indicators.get('kdj_d')}/J{indicators.get('kdj_j')}，CCI={indicators.get('cci')}/{indicators.get('cci_prev')}
ADX={indicators.get('adx')}/{indicators.get('adx_prev')}，+DI={indicators.get('plus_di')}，-DI={indicators.get('minus_di')}，量比={indicators.get('vol_ratio')}
布林带：上={indicators.get('boll_upper')}，中={indicators.get('boll_middle')}，下={indicators.get('boll_lower')}
斐波那契：38.2%={indicators.get('fib_382')}，50%={indicators.get('fib_500')}，61.8%={indicators.get('fib_618')}

小时线：MA5={indicators.get('hourly_ma5')}，MA20={indicators.get('hourly_ma20')}
小时MACD：DIF={indicators.get('hourly_macd_dif')}，柱={indicators.get('hourly_macd')}
小时指标：RSI={indicators.get('hourly_rsi')}，KDJ=K{indicators.get('hourly_kdj_k')}/D{indicators.get('hourly_kdj_d')}/J{indicators.get('hourly_kdj_j')}，量比={indicators.get('hourly_vol_ratio')}

止损参考：当前低={indicators.get('current_low')}，近期低={indicators.get('recent_low')}，斐波61.8%={indicators.get('fib_618')}
"""
        stocks_info.append(stock_info)
    
    stocks_info_str = "\n".join(stocks_info)
    
    # 获取动态参数
    if dynamic_params is None and stocks_data:
        dynamic_params = get_dynamic_parameters(stocks_data[0][1])
    
    min_conditions = dynamic_params.get('min_conditions', 3) if dynamic_params else 3
    min_risk_reward = dynamic_params.get('min_risk_reward', 1.5) if dynamic_params else 1.5
    rsi_upper_limit = dynamic_params.get('rsi_upper_limit', 80) if dynamic_params else 80
    
    prompt = f"""
你是专业的量化交易分析模型，使用"三重过滤趋势波段系统"分析以下{len(stocks_data)}支股票。

【大盘】上证：{sh_index_price}点，{sh_index_pct}%

{stocks_info_str}

【分析规则】
第一步（日线趋势）：价格>MA60 且 MA60向上 → 可做多，否则观望/回避
第二步（小时线入场）：需满足{min_conditions}个条件：MA5>MA20、MACD向上、量比>1.5、KDJ/RSI超卖回升
第三步（风控）：风险回报比>={min_risk_reward}，单笔亏损<=300元，RSI<{rsi_upper_limit}

【信号规则】
- 买入/强烈看多：多周期共振，返回buy_price/sell_price/stop_loss
- 关注：趋势好但时机未到，返回ref_buy_price和wait_conditions
- 观望/回避：不满足条件，只返回基础字段

【reason要求】不超过30字，禁止"第一步""第二步"等分步描述
正确："日线多头+小时线共振，MACD金叉"
错误："第一步趋势过滤器通过..."

返回JSON数组：
[
  {{
    "code": "代码",
    "name": "名称",
    "signal": "买入/强烈看多/关注/观望/回避",
    "trend": "上涨/下跌/震荡",
    "risk": "低/中/高",
    "confidence": 0-100,
    "score": -100到100,
    "key_factors": ["因素1", "因素2"],
    "advice": "一句话建议",
    "summary": "30字以内",
    "reason": "30字以内简洁理由",
    "buy_price": 数字或null,
    "sell_price": 数字或null,
    "stop_loss": 数字或null,
    "ref_buy_price": 数字或null,
    "wait_conditions": []
  }}
]

返回{len(stocks_data)}个对象，顺序与输入一致。
"""
    return prompt
