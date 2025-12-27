"""
AI分析提示词构建
"""
from typing import Optional
from ai.parameter_optimizer import get_dynamic_parameters


def get_custom_prompt() -> Optional[str]:
    """从运行时配置获取自定义提示词"""
    try:
        from common.runtime_config import get_runtime_config
        cfg = get_runtime_config()
        return cfg.ai_custom_prompt if cfg.ai_custom_prompt else None
    except Exception:
        return None


def build_stock_analysis_prompt(stock: dict, indicators: dict, news: list = None, include_trading_points: bool = False, dynamic_params: dict = None) -> str:
    """构建股票分析提示词
    
    Args:
        stock: 股票数据
        indicators: 技术指标
        news: 相关资讯
        include_trading_points: 是否包含交易点位（买入价、卖出价、止损价）
    """
    
    # 检查是否有自定义提示词
    custom_prompt = get_custom_prompt()
    if custom_prompt:
        # 使用自定义提示词，替换变量
        news_summary = ""
        if news:
            news_summary = "\n".join([f"- {n.get('title', '')}" for n in news[:5]])
        
        # 构建指标字符串
        indicators_str = "\n".join([f"- {k}: {v}" for k, v in indicators.items() if v is not None])
        
        # 替换变量
        prompt = custom_prompt
        prompt = prompt.replace("{stock_code}", str(stock.get('code', '')))
        prompt = prompt.replace("{stock_name}", str(stock.get('name', '')))
        prompt = prompt.replace("{current_price}", str(stock.get('price', 0)))
        prompt = prompt.replace("{pct}", str(stock.get('pct', 0)))
        prompt = prompt.replace("{volume}", str(stock.get('volume', 0)))
        prompt = prompt.replace("{amount}", str(stock.get('amount', 0)))
        prompt = prompt.replace("{indicators}", indicators_str)
        prompt = prompt.replace("{news}", news_summary if news_summary else "暂无相关资讯")
        
        return prompt
    
    # 使用默认提示词
    news_summary = ""
    if news:
        news_summary = "\n".join([f"- {n.get('title', '')}" for n in news[:5]])
    
    current_price = stock.get('price', 0)
    
    if include_trading_points:
        # 获取动态参数（如果未提供，则自动检测）
        if dynamic_params is None:
            dynamic_params = get_dynamic_parameters(indicators)
        
        min_conditions = dynamic_params.get('min_conditions', 3)
        min_risk_reward = dynamic_params.get('min_risk_reward', 1.5)
        vol_ratio_threshold = dynamic_params.get('vol_ratio_threshold', 1.5)
        rsi_upper_limit = dynamic_params.get('rsi_upper_limit', 80)
        
        # 获取关键数据
        ma60 = indicators.get('ma60')
        ma60_trend = indicators.get('ma60_trend', '未知')
        ma5 = indicators.get('ma5')
        ma5_trend = indicators.get('ma5_trend', '未知')
        ma10 = indicators.get('ma10')
        ma20 = indicators.get('ma20')
        ma20_trend = indicators.get('ma20_trend', '未知')
        macd_dif = indicators.get('macd_dif')
        macd_dif_trend = indicators.get('macd_dif_trend', '未知')
        macd_prev = indicators.get('macd_prev')
        macd_current = indicators.get('macd')
        vol_ratio = indicators.get('vol_ratio')
        rsi = indicators.get('rsi')
        recent_low = indicators.get('recent_low')
        current_low = indicators.get('current_low')
        boll_upper = indicators.get('boll_upper')
        kdj_j = indicators.get('kdj_j')
        
        # CCI顺势指标
        cci = indicators.get('cci')
        cci_status = indicators.get('cci_status', '未知')
        
        # 斐波那契回撤位
        fib_382 = indicators.get('fib_382')
        fib_500 = indicators.get('fib_500')
        fib_618 = indicators.get('fib_618')
        fib_trend = indicators.get('fib_trend', '未知')
        fib_current_level = indicators.get('fib_current_level', '未知')
        fib_swing_high = indicators.get('fib_swing_high')
        fib_swing_low = indicators.get('fib_swing_low')
        
        # 多周期分析数据（如果有）
        multi_tf_signal = indicators.get('multi_tf_signal')
        multi_tf_resonance = indicators.get('multi_tf_resonance')
        daily_trend = indicators.get('daily_trend_direction')
        hourly_trend = indicators.get('hourly_trend_direction')
        entry_timing = indicators.get('entry_timing')
        entry_signals = indicators.get('entry_signals', [])
        
        prompt = f"""
你是专业的量化交易分析模型，使用"三重过滤趋势波段系统"进行交易决策。

【股票基本信息】
股票代码：{stock.get('code', '')}
股票名称：{stock.get('name', '')}
当前价格：{current_price}元
涨跌幅：{stock.get('pct', 0)}%

【第一步：趋势判定（决定是否入场）】
核心规则：只在主要趋势方向上进行交易，顺势而为。

趋势判定数据：
- 当前价格：{current_price}元
- MA60（长期趋势线）：{ma60 if ma60 else 'N/A'}元
- MA60趋势方向：{ma60_trend}
- 价格与MA60关系：{"价格 > MA60" if ma60 and current_price > ma60 else "价格 < MA60" if ma60 and current_price < ma60 else "N/A"}

做多趋势条件：股价 > MA60 且 MA60方向向上倾斜

【第二步：入场信号（决定何时入场）- 基于小时线】
核心规则：日线趋势确立后，用小时线找回调企稳的精确入场点。

小时线入场信号数据：
- 小时线MA5：{indicators.get('hourly_ma5', 'N/A')}元，趋势：{indicators.get('hourly_ma5_trend', '未知')}
- 小时线MA20：{indicators.get('hourly_ma20', 'N/A')}元，趋势：{indicators.get('hourly_ma20_trend', '未知')}
- 小时线MACD DIF：{indicators.get('hourly_macd_dif', 'N/A')}，趋势：{indicators.get('hourly_macd_dif_trend', '未知')}
- 小时线MACD柱（当前）：{indicators.get('hourly_macd', 'N/A')}
- 小时线MACD柱（前一根）：{indicators.get('hourly_macd_prev', 'N/A')}
- 小时线RSI：{indicators.get('hourly_rsi', 'N/A')}
- 小时线KDJ J值：{indicators.get('hourly_kdj_j', 'N/A')}（<20为超卖区）
- 小时线成交量比：{indicators.get('hourly_vol_ratio', 'N/A')}（>1.5为放量）

日线辅助参考：
- 日线MA5：{ma5 if ma5 else 'N/A'}元，趋势：{ma5_trend}
- 日线MA20：{ma20 if ma20 else 'N/A'}元，趋势：{ma20_trend}
- 日线MACD柱：{macd_current if macd_current else 'N/A'}
- 日线成交量比：{vol_ratio if vol_ratio else 'N/A'}

做多入场条件（基于小时线，需同时满足）：
1. 日线趋势确认：日线MA60向上（已在第一步确认）
2. 小时线回调企稳：价格回调至小时线MA20附近后企稳
3. 小时线动能确认：小时线MACD的DIF拐头向上，或MACD绿柱显著缩短
4. 小时线均线信号：小时线MA5向上金叉MA20，或MA5拐头向上
5. 放量启动：小时线成交量比 > 1.5

【第三步：风险控制（决定买多少、亏多少）】
核心规则：严格止损，让利润奔跑。

重要：总资金设置为10000元，所有仓位计算和决策都基于此资金量。
- 单笔交易最大可接受亏损：总资金的3%（即300元）
- 单只股票仓位：总资金的100%（即10000元，全仓买入）
- 可以同时持有多只股票，每只股票都是全仓买入（不限制交易股票数量）
- 只做多，不做空（只买入，不卖出做空）

风险控制数据：
- MA10（移动止损参考）：{ma10 if ma10 else 'N/A'}元
- MA20（移动止损参考）：{ma20 if ma20 else 'N/A'}元
- 当前K线最低价：{current_low if current_low else 'N/A'}元
- 近期5日最低价：{recent_low if recent_low else 'N/A'}元
- 布林带上轨（压力位）：{boll_upper if boll_upper else 'N/A'}元
- RSI：{rsi if rsi else 'N/A'}（>80时需谨慎，避免超买区入场）

止损点设置规则：
- 初始止损：设在入场K线最低价下方2%-3%，或设在近期调整的波段低点（{recent_low if recent_low else 'N/A'}元）下方
- 单笔最大亏损不超过300元（总资金的3%）
- 移动止损（止盈）：当股价上涨后，将止损位移动至MA10或MA20下方

止盈点设置规则：
- 基于技术压力位（布林带上轨：{boll_upper if boll_upper else 'N/A'}元）
- 或基于固定收益目标（建议收益3-10%）

仓位计算示例（基于10000元总资金，全仓买入）：
- 如果买入价10元，止损价9.7元（风险0.3元/股）
- 单笔最大亏损300元 ÷ 0.3元/股 = 1000股（10手）
- 实际买入金额：10元 × 1000股 = 10000元（占总资金100%，全仓）
- 如果风险更大，例如止损价9.5元（风险0.5元/股），则：300元 ÷ 0.5元/股 = 600股（6手），买入金额6000元
- 注意：可以同时持有多只股票，每只都是全仓买入，不限制持仓数量

【辅助参考】
- RSI：{rsi if rsi else 'N/A'}（>80时入场需谨慎，避免超买）
- 布林带上轨：{boll_upper if boll_upper else 'N/A'}元（观察是否过热）
- CCI顺势指标：{cci if cci else 'N/A'}（状态：{cci_status}，>100超买，<-100超卖）

【斐波那契回撤分析】
- 波段高点：{fib_swing_high if fib_swing_high else 'N/A'}元
- 波段低点：{fib_swing_low if fib_swing_low else 'N/A'}元
- 波段趋势：{fib_trend}（up=上涨趋势，down=下跌趋势）
- 当前价格所处回撤区间：{fib_current_level}
- 38.2%回撤位：{fib_382 if fib_382 else 'N/A'}元（浅回撤支撑/阻力）
- 50%回撤位：{fib_500 if fib_500 else 'N/A'}元（中度回撤支撑/阻力）
- 61.8%回撤位：{fib_618 if fib_618 else 'N/A'}元（黄金分割位，重要支撑/阻力）

斐波那契使用说明：
- 上涨趋势中：价格回撤到38.2%-61.8%区间是较好的买入区域
- 下跌趋势中：价格反弹到38.2%-61.8%区间是较好的卖出区域
- 61.8%是黄金分割位，突破此位可能趋势反转

【多周期分析】（日线定趋势，小时线定进场）
- 日线趋势方向：{daily_trend if daily_trend else 'N/A'}（决定是否可以做多）
- 小时线趋势方向：{hourly_trend if hourly_trend else 'N/A'}（决定入场时机）
- 多周期共振信号：{multi_tf_signal if multi_tf_signal else 'N/A'}
- 是否共振：{multi_tf_resonance if multi_tf_resonance is not None else 'N/A'}
- 入场时机评估：{entry_timing if entry_timing else 'N/A'}
- 入场信号：{', '.join(entry_signals) if entry_signals else 'N/A'}

多周期分析说明：
- 日线多头 + 小时线多头 = 强烈看多，立即入场
- 日线多头 + 小时线回调企稳 = 最佳入场时机（回调买入）
- 日线多头 + 小时线下跌中 = 等待小时线企稳再入场
- 日线空头 = 观望或回避，不做多

【三重过滤系统分析步骤】

【第一步：趋势过滤器 - 是否符合大趋势？】
检查条件：当前价格 > MA60 且 MA60_trend == "向上"
- 如果不符合 → 直接输出 signal="观望"，reason="不符合趋势要求：股价未站上MA60或MA60趋势向下"，不返回交易点位
- 如果符合 → 进入第二步

【第二步：入场信号过滤器 - 基于小时线判断入场时机】
日线趋势确认后，用小时线找回调企稳的精确入场点。

必须同时满足以下条件（至少满足{min_conditions}个以上）：
1. hourly_ma5_trend == "向上" AND hourly_ma20_trend == "向上"  # 小时线均线向上
2. hourly_macd_dif_trend == "向上"  # 小时线MACD DIF线向上
3. hourly_macd > hourly_macd_prev AND hourly_macd < 0  # 小时线MACD绿柱缩短
4. hourly_vol_ratio > {vol_ratio_threshold}  # 小时线放量启动（阈值：{vol_ratio_threshold}）
5. hourly_kdj_j < 30 或 hourly_rsi < 40  # 小时线超卖区拐头向上（可选）

入场信号评分：
- 满足5个条件：信号强度100%
- 满足4个条件：信号强度80%
- 满足3个条件：信号强度60%
- 少于{min_conditions}个条件：信号不足

- 如果满足{min_conditions}个以上条件 → 进入第三步
- 否则 → 输出 signal="观望"，reason="小时线入场信号不足：等待回调企稳"，不返回交易点位

【第三步：风险控制过滤器 - 盈亏比是否合理？】
重要：总资金设置为10000元，所有仓位计算和决策都基于此资金量。

仓位管理规则：
- 单笔交易最大可接受亏损：总资金的3%（即300元）
- 单只股票仓位：总资金的100%（即10000元，全仓买入）
- 可以同时持有多只股票，每只股票都是全仓买入（不限制交易股票数量）
- 只做多，不做空（只买入，不卖出做空）

必须计算并验证：

1. 买入价（buy_price）：
   - 建议为当前价格的99%（考虑滑点）或MA20支撑位附近
   - 参考值：{ma20 if ma20 else 'N/A'}元

2. 止损价（stop_loss）：
   - 设在 min(current_low={current_low if current_low else 'N/A'}, recent_low={recent_low if recent_low else 'N/A'}) * 0.98
   - 或设在买入价下方2-3%
   - 单笔最大亏损不超过300元（总资金的3%）

3. 止盈价（sell_price）：
   - 基于布林带上轨（{boll_upper if boll_upper else 'N/A'}元）
   - 或基于风险回报比1:2（即 (sell_price - buy_price) / (buy_price - stop_loss) >= 2）
   - 建议收益3-10%

4. 仓位计算示例（基于10000元总资金，全仓买入）：
   - 如果买入价10元，止损价9.7元（风险0.3元/股）
   - 单笔最大亏损300元 ÷ 0.3元/股 = 1000股（10手）
   - 实际买入金额：10元 × 1000股 = 10000元（占总资金100%，全仓）
   - 如果风险更大，例如止损价9.5元（风险0.5元/股），则：300元 ÷ 0.5元/股 = 600股（6手），买入金额6000元
   - 注意：可以同时持有多只股票，每只都是全仓买入，不限制持仓数量
   - 如果资金已部分使用，按剩余资金计算，但目标是全仓买入

5. 必须验证：
   - stop_loss < buy_price < sell_price
   - 风险回报比 >= {min_risk_reward}（即盈利/亏损 >= {min_risk_reward}）
   - RSI < {rsi_upper_limit}（避免超买区入场，当前上限：{rsi_upper_limit}）
   - 单笔亏损金额 <= 300元（总资金的3%）

6. 如果验证通过 → 输出 signal="买入"，并返回完整的 buy_price、sell_price、stop_loss
7. 如果验证不通过 → 输出 signal="观望"，reason="风险回报比不足或RSI超买或单笔亏损超过限制（超过300元）"，不返回交易点位

请严格返回 JSON 格式，不要输出任何多余文字：
{{
  "code": "{stock.get('code', '')}",
  "name": "{stock.get('name', '')}",
  "trend": "上涨/下跌/震荡/未知",
  "risk": "低/中/高/未知",
  "confidence": 0-100之间的整数,
  "score": 一个整数评分（-100到100，越高代表越看多）,
  "signal": "买入/关注/观望/回避",
  "buy_price": 买入价（数字，signal为买入时必填，否则为null）,
  "sell_price": 止盈价（数字，signal为买入时必填，否则为null）,
  "stop_loss": 止损价（数字，signal为买入时必填，否则为null）,
  "key_factors": ["关键因素1", "关键因素2"],
  "advice": "一句话操作建议",
  "summary": "100字以内的综合总结",
  "reason": "给出交易点位的理由（30字以内，说明为何符合三重过滤系统）"
}}
"""
    else:
        prompt = f"""
你是一名专业的股票分析师，请对以下股票进行综合分析：

【股票信息】
股票代码：{stock.get('code', '')}
股票名称：{stock.get('name', '')}
当前价格：{current_price}元
涨跌幅：{stock.get('pct', 0)}%
成交量：{stock.get('volume', 0)}
成交额：{stock.get('amount', 0)}万元

【技术指标】
均线系统：
- MA5：{indicators.get('ma5', 'N/A')}
- MA10：{indicators.get('ma10', 'N/A')}
- MA20：{indicators.get('ma20', 'N/A')}
- MA60：{indicators.get('ma60', 'N/A')}

MACD指标：
- DIF：{indicators.get('macd_dif', 'N/A')}
- DEA：{indicators.get('macd_dea', 'N/A')}
- MACD柱：{indicators.get('macd', 'N/A')}

RSI相对强弱指标：{indicators.get('rsi', 'N/A')}（超买>70，超卖<30）

布林带（压力支撑位）：
- 上轨：{indicators.get('boll_upper', 'N/A')}
- 中轨：{indicators.get('boll_middle', 'N/A')}
- 下轨：{indicators.get('boll_lower', 'N/A')}

KDJ指标（超买超卖）：
- K值：{indicators.get('kdj_k', 'N/A')}
- D值：{indicators.get('kdj_d', 'N/A')}
- J值：{indicators.get('kdj_j', 'N/A')}

成交量分析：
- 成交量比：{indicators.get('vol_ratio', 'N/A')}（>1.5为放量，<0.5为缩量）

【相关资讯】
{news_summary if news_summary else '暂无相关资讯'}

请给出：
1. 趋势判断（短期/中期趋势）
2. 风险评级（低/中/高）
3. 操作建议（买入/持有/观望/止盈卖出，只做多不做空）
4. 关键支撑位和压力位（如果有）
5. 简要总结（100字以内）

请用专业但易懂的语言回答。
"""
    
    return prompt


def build_stocks_batch_analysis_prompt(stocks_data: list, include_trading_points: bool = False, dynamic_params: dict = None) -> str:
    """构建批量股票分析提示词（一次分析多支股票）
    
    Args:
        stocks_data: 股票数据列表，每个元素为 (stock, indicators, news) 的元组
        include_trading_points: 是否包含交易点位
        dynamic_params: 动态参数
    """
    if not stocks_data:
        return ""
    
    # 构建股票信息列表
    stocks_info = []
    for i, (stock, indicators, news) in enumerate(stocks_data, 1):
        news_summary = ""
        if news:
            news_summary = "\n".join([f"  - {n.get('title', '')}" for n in news[:3]])
        
        current_price = stock.get('price', 0)
        stock_info = f"""
【股票 {i}】
股票代码：{stock.get('code', '')}
股票名称：{stock.get('name', '')}
当前价格：{current_price}元
涨跌幅：{stock.get('pct', 0)}%

日线指标（定趋势）：
- MA60：{indicators.get('ma60', 'N/A')}，趋势：{indicators.get('ma60_trend', '未知')}
- MA20：{indicators.get('ma20', 'N/A')}，趋势：{indicators.get('ma20_trend', '未知')}
- 日线RSI：{indicators.get('rsi', 'N/A')}
- 布林带上轨：{indicators.get('boll_upper', 'N/A')}元

小时线指标（定进场）：
- 小时MA5：{indicators.get('hourly_ma5', 'N/A')}，趋势：{indicators.get('hourly_ma5_trend', '未知')}
- 小时MA20：{indicators.get('hourly_ma20', 'N/A')}，趋势：{indicators.get('hourly_ma20_trend', '未知')}
- 小时MACD DIF：{indicators.get('hourly_macd_dif', 'N/A')}，趋势：{indicators.get('hourly_macd_dif_trend', '未知')}
- 小时MACD柱：{indicators.get('hourly_macd', 'N/A')}
- 小时RSI：{indicators.get('hourly_rsi', 'N/A')}
- 小时KDJ J值：{indicators.get('hourly_kdj_j', 'N/A')}
- 小时成交量比：{indicators.get('hourly_vol_ratio', 'N/A')}

止损参考：
- 当前最低价：{indicators.get('current_low', 'N/A')}元
- 近期最低价：{indicators.get('recent_low', 'N/A')}元

相关资讯：
{news_summary if news_summary else '  暂无相关资讯'}
"""
        stocks_info.append(stock_info)
    
    stocks_info_str = "\n".join(stocks_info)
    
    if include_trading_points:
        # 获取动态参数
        if dynamic_params is None and stocks_data:
            from ai.parameter_optimizer import get_dynamic_parameters
            dynamic_params = get_dynamic_parameters(stocks_data[0][1])
        
        min_conditions = dynamic_params.get('min_conditions', 3) if dynamic_params else 3
        min_risk_reward = dynamic_params.get('min_risk_reward', 1.5) if dynamic_params else 1.5
        vol_ratio_threshold = dynamic_params.get('vol_ratio_threshold', 1.5) if dynamic_params else 1.5
        rsi_upper_limit = dynamic_params.get('rsi_upper_limit', 80) if dynamic_params else 80
        
        prompt = f"""
你是专业的量化交易分析模型，使用"三重过滤趋势波段系统"对以下{len(stocks_data)}支股票进行批量分析。

{stocks_info_str}

【分析要求 - 日线定趋势，小时线定进场】
请对每支股票分别进行分析，使用三重过滤系统：

1. 趋势过滤器（日线）：股价 > MA60 且 MA60趋势向上 → 确认可以做多
2. 入场信号过滤器（小时线）：需满足{min_conditions}个以上条件：
   - 小时MA5/MA20向上
   - 小时MACD DIF向上或绿柱缩短
   - 小时成交量比 > {vol_ratio_threshold}
   - 小时KDJ J值<30或RSI<40后拐头（超卖回升）
3. 风险控制过滤器：风险回报比 >= {min_risk_reward}，单笔最大亏损300元

【多周期配合说明】
- 日线多头 + 小时线回调企稳 = 最佳入场时机（回调买入）
- 日线多头 + 小时线多头共振 = 强烈看多，可立即入场
- 日线多头 + 小时线下跌中 = 等待小时线企稳
- 日线空头 = 观望或回避

【重要规则】
- 总资金10000元，每只股票全仓买入（不限制持仓数量）
- 单笔最大亏损：总资金的3%（300元）
- 只做多，不做空

请严格返回 JSON 格式，返回一个数组，每支股票一个对象：
[
  {{
    "code": "股票代码",
    "name": "股票名称",
    "trend": "上涨/下跌/震荡/未知",
    "risk": "低/中/高/未知",
    "confidence": 0-100之间的整数,
    "score": -100到100的整数评分,
    "signal": "买入/关注/观望/回避",
    "buy_price": 买入价（数字或null）,
    "sell_price": 止盈价（数字或null）,
    "stop_loss": 止损价（数字或null）,
    "key_factors": ["关键因素1", "关键因素2"],
    "advice": "一句话操作建议",
    "summary": "100字以内的综合总结",
    "reason": "给出交易点位的理由（30字以内）"
  }},
  ...
]

请确保返回的数组包含{len(stocks_data)}个对象，顺序与输入的股票顺序一致。
"""
    else:
        prompt = f"""
你是专业的股票分析师，请对以下{len(stocks_data)}支股票进行综合分析。

{stocks_info_str}

请对每支股票分别进行分析，严格返回 JSON 格式数组：
[
  {{
    "code": "股票代码",
    "trend": "上涨/下跌/震荡/未知",
    "risk": "低/中/高/未知",
    "confidence": 0-100之间的整数,
    "score": -100到100的整数评分,
    "signal": "买入/持有/观望/回避",
    "key_factors": ["关键因素1", "关键因素2"],
    "advice": "一句话操作建议",
    "summary": "100字以内的综合总结"
  }},
  ...
]

请确保返回的数组包含{len(stocks_data)}个对象，顺序与输入的股票顺序一致。
"""
    
    return prompt


def build_market_summary_prompt(stocks: list, indicators_map: dict) -> str:
    """构建市场总结提示词"""
    
    prompt = f"""
你是股票市场分析师，请对当前市场情况进行分析：

【市场概况】
共分析 {len(stocks)} 只股票

【技术指标概况】
- 处于多头排列的股票比例
- RSI处于健康区间的股票比例
- 放量的股票比例

请给出：
1. 市场整体趋势判断
2. 主要关注板块
3. 风险提示
4. 操作建议

请用简洁明了的语言回答。
"""
    
    return prompt

