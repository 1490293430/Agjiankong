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
        
        # 获取关键数据（只获取数值，不获取预判）
        ma5 = indicators.get('ma5')
        ma5_prev = indicators.get('ma5_prev')
        ma10 = indicators.get('ma10')
        ma10_prev = indicators.get('ma10_prev')
        ma20 = indicators.get('ma20')
        ma20_prev = indicators.get('ma20_prev')
        ma60 = indicators.get('ma60')
        ma60_prev = indicators.get('ma60_prev')
        
        macd_dif = indicators.get('macd_dif')
        macd_dif_prev = indicators.get('macd_dif_prev')
        macd_dea = indicators.get('macd_dea')
        macd_current = indicators.get('macd')
        macd_prev = indicators.get('macd_prev')
        
        vol_ratio = indicators.get('vol_ratio')
        rsi = indicators.get('rsi')
        recent_low = indicators.get('recent_low')
        current_low = indicators.get('current_low')
        boll_upper = indicators.get('boll_upper')
        boll_middle = indicators.get('boll_middle')
        boll_lower = indicators.get('boll_lower')
        boll_width = indicators.get('boll_width')
        boll_width_prev = indicators.get('boll_width_prev')
        
        kdj_k = indicators.get('kdj_k')
        kdj_d = indicators.get('kdj_d')
        kdj_j = indicators.get('kdj_j')
        
        # CCI顺势指标（只有数值）
        cci = indicators.get('cci')
        cci_prev = indicators.get('cci_prev')
        
        # 斐波那契回撤位（只有数值）
        fib_382 = indicators.get('fib_382')
        fib_500 = indicators.get('fib_500')
        fib_618 = indicators.get('fib_618')
        fib_swing_high = indicators.get('fib_swing_high')
        fib_swing_low = indicators.get('fib_swing_low')
        
        # 历史数据（数值列表）
        recent_5d_pct = indicators.get('recent_5d_pct', [])
        recent_5d_vol = indicators.get('recent_5d_vol', [])
        recent_5d_macd = indicators.get('recent_5d_macd', [])
        vol_change_ratio = indicators.get('vol_change_ratio')
        pct_5d = indicators.get('pct_5d')
        
        prompt = f"""
你是专业的量化交易分析模型，使用"三重过滤趋势波段系统"进行交易决策。
请根据以下数值数据自行判断趋势方向、信号强度等。

【股票基本信息】
股票代码：{stock.get('code', '')}
股票名称：{stock.get('name', '')}
当前价格：{current_price}元
涨跌幅：{stock.get('pct', 0)}%

【大盘参考】
- 上证指数：{indicators.get('sh_index_price', 'N/A')}点
- 大盘涨跌：{indicators.get('sh_index_pct', 'N/A')}%

【日线均线数据】（请自行判断趋势方向：当前值>前值为向上）
- MA5：当前{ma5}，前值{ma5_prev}
- MA10：当前{ma10}，前值{ma10_prev}
- MA20：当前{ma20}，前值{ma20_prev}
- MA60：当前{ma60}，前值{ma60_prev}

【日线MACD数据】（请自行判断金叉死叉、柱状图趋势）
- DIF：当前{macd_dif}，前值{macd_dif_prev}
- DEA：{macd_dea}
- MACD柱：当前{macd_current}，前值{macd_prev}
- 最近5天MACD柱：{recent_5d_macd}

【日线RSI/KDJ/CCI（超买超卖判断，必须在key_factors中说明）】
- RSI：{rsi}（>70超买，<30超卖）
- KDJ：K={kdj_k}，D={kdj_d}，J={kdj_j}（J>80超买，J<20超卖）
- CCI：当前{cci}，前值{cci_prev}（>100超买，<-100超卖）
- 威廉%R：当前{indicators.get('williams_r')}，前值{indicators.get('williams_r_prev')}（>-20超买，<-80超卖）
- 成交量比：{vol_ratio}（>1.5放量，<0.8缩量，必须在key_factors中说明）

【日线ADX趋势强度】
- ADX：当前{indicators.get('adx')}，前值{indicators.get('adx_prev')}（>25趋势明确，<20无趋势）
- +DI：{indicators.get('plus_di')}，-DI：{indicators.get('minus_di')}

【布林带数据】（请自行判断开口/收口：宽度增加为开口）
- 上轨：{boll_upper}
- 中轨：{boll_middle}
- 下轨：{boll_lower}
- 带宽：当前{boll_width}，前值{boll_width_prev}

【斐波那契回撤位】（请自行判断当前价格所处区间）
- 波段高点：{fib_swing_high}
- 波段低点：{fib_swing_low}
- 38.2%位：{fib_382}
- 50%位：{fib_500}
- 61.8%位：{fib_618}

【小时线数据（入场时机判断）】
- 小时MA5：{indicators.get('hourly_ma5')}
- 小时MA20：{indicators.get('hourly_ma20')}
- 小时MACD DIF：{indicators.get('hourly_macd_dif')}
- 小时MACD柱：{indicators.get('hourly_macd')}
- 小时RSI：{indicators.get('hourly_rsi')}（>70超买，<30超卖）
- 小时KDJ：K={indicators.get('hourly_kdj_k')}，D={indicators.get('hourly_kdj_d')}，J={indicators.get('hourly_kdj_j')}
- 小时CCI：{indicators.get('hourly_cci')}
- 小时成交量比：{indicators.get('hourly_vol_ratio')}（⚠️重要：<0.8表示缩量，需在key_factors中说明）
- 小时布林带上轨：{indicators.get('hourly_boll_upper')}

【近5日历史数据】
- 每日涨跌幅：{recent_5d_pct}（从早到近）
- 5日累计涨跌：{pct_5d}%
- 每日成交量：{recent_5d_vol}
- 成交量变化比：{vol_change_ratio}（正数放量，负数缩量）

【止损参考数据】
- 当前K线最低价：{current_low}
- 近5日最低价：{recent_low}
- MA10：{ma10}
- MA20：{ma20}

【风险控制规则】
- 总资金：10000元
- 单笔最大亏损：300元（3%）
- 最小风险回报比：{min_risk_reward}
- RSI上限：{rsi_upper_limit}（超过此值不建议入场）

【分析要求】
请根据以上数值数据，自行判断：
1. 趋势方向（日线MA60与价格的关系，MA是否向上）
2. 多周期共振（日线和小时线趋势是否一致）
3. 入场信号强度（小时线指标是否满足条件）
4. 风险回报比是否合理

【信号类型说明】
- "买入"：满足所有条件，可立即入场，必须返回buy_price/sell_price/stop_loss
- "强烈看多"：多周期共振，信号强烈，但可能不完全满足入场条件，也需返回建议的buy_price/sell_price/stop_loss
- "关注"：趋势向好但入场时机未到
- "观望"：不满足条件
- "回避"：趋势向下

【多周期共振规则（必须明确判断）】
| 日线状态 | 小时线状态 | 信号 | 说明 |
|---------|-----------|------|------|
| 多头（价>MA60且MA60向上） | 多头共振 | 强烈看多 | 最强信号，可立即入场 |
| 多头 | 回调企稳（RSI<40后拐头） | 买入 | 最佳入场时机 |
| 多头 | 下跌中（MACD向下） | 关注 | 等待小时线企稳 |
| 空头（价<MA60或MA60向下） | 任意 | 观望/回避 | 不符合做多条件 |

【指标使用要求（必须在key_factors中体现）】
1. RSI超买超卖：RSI>70必须说明"RSI超买"，RSI<30必须说明"RSI超卖"
2. 成交量：vol_ratio<0.8必须说明"成交量不足"，>1.5说明"放量"
3. 小时线成交量：hourly_vol_ratio<0.8必须说明"小时线缩量"
4. KDJ状态：J>80说明"KDJ超买"，J<20说明"KDJ超卖"
5. CCI状态：CCI>100说明"CCI超买"，CCI<-100说明"CCI超卖"

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

【买卖价格计算规则（必须参考技术位）】

1. 买入价（buy_price）：
   - 优先参考MA20支撑位：{ma20 if ma20 else 'N/A'}元
   - 或斐波那契38.2%位：{fib_382 if fib_382 else 'N/A'}元
   - 或斐波那契50%位：{fib_500 if fib_500 else 'N/A'}元
   - 或当前价格的99%（考虑滑点）

2. 止损价（stop_loss）：
   - 优先参考斐波那契61.8%位：{fib_618 if fib_618 else 'N/A'}元
   - 或近期最低价：{recent_low if recent_low else 'N/A'}元
   - 或设在买入价下方2-3%
   - 单笔最大亏损不超过300元（总资金的3%）

3. 止盈价（sell_price）：
   - 优先参考布林带上轨：{boll_upper if boll_upper else 'N/A'}元
   - 或基于风险回报比1:{min_risk_reward}计算
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

【特别说明：强烈看多信号】
当多周期共振（日线多头+小时线多头）且信号特别强时，即使不满足买入条件，也应返回signal="强烈看多"，并提供建议的交易点位：
- buy_price: 建议买入价（参考MA20或斐波那契回撤位）
- sell_price: 建议止盈价（参考布林带上轨）
- stop_loss: 建议止损价（参考斐波那契61.8%或近期低点）
这样用户可以参考这些建议价格进行决策。

【confidence（置信度）定义】
- 90-100：多周期强烈共振，所有指标一致，信号极强
- 75-89：大部分指标一致，信号较强
- 60-74：部分指标支持，信号中等
- 40-59：指标分歧较大，信号较弱
- 0-39：指标矛盾，信号很弱

请严格返回 JSON 格式，不要输出任何多余文字：
{{
  "code": "{stock.get('code', '')}",
  "name": "{stock.get('name', '')}",
  "trend": "上涨/下跌/震荡/未知",
  "risk": "低/中/高/未知",
  "confidence": 0-100之间的整数（参考上述定义）,
  "score": 一个整数评分（-100到100，越高代表越看多）,
  "signal": "买入/强烈看多/关注/观望/回避",
  "buy_price": 买入价（数字，signal为买入或强烈看多时必填，参考技术位计算，否则为null）,
  "sell_price": 止盈价（数字，signal为买入或强烈看多时必填，参考布林带上轨，否则为null）,
  "stop_loss": 止损价（数字，signal为买入或强烈看多时必填，参考斐波那契61.8%或近期低点，否则为null）,
  "key_factors": ["必须包含具体指标数值，如：日线RSI=65处于正常区间", "小时线成交量比=0.58严重缩量"],
  "advice": "一句话操作建议",
  "summary": "100字以内的综合总结，必须提及关键指标状态",
  "reason": "给出交易点位的理由（30字以内，说明参考了哪个技术位）"
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
    
    # 从第一只股票的indicators中获取大盘数据
    first_indicators = stocks_data[0][1] if stocks_data else {}
    sh_index_price = first_indicators.get('sh_index_price', 'N/A')
    sh_index_pct = first_indicators.get('sh_index_pct', 'N/A')
    
    # 根据大盘涨跌幅判断市场情绪（让AI参考，但不作为预判）
    try:
        sh_pct_val = float(sh_index_pct) if sh_index_pct != 'N/A' else 0
        if sh_pct_val > 1:
            market_sentiment = "强势（涨幅>1%）"
        elif sh_pct_val > 0:
            market_sentiment = "偏强（涨幅0-1%）"
        elif sh_pct_val > -1:
            market_sentiment = "偏弱（跌幅0-1%）"
        else:
            market_sentiment = "弱势（跌幅>1%）"
    except (ValueError, TypeError):
        market_sentiment = "未知"
    
    # 构建股票信息列表（只发送数值，不发送预判）
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

日线均线（请自行判断趋势：当前值>前值为向上）：
- MA60：当前{indicators.get('ma60')}，前值{indicators.get('ma60_prev')}
- MA20：当前{indicators.get('ma20')}，前值{indicators.get('ma20_prev')}
- MA5：当前{indicators.get('ma5')}，前值{indicators.get('ma5_prev')}

日线MACD：
- DIF：当前{indicators.get('macd_dif')}，前值{indicators.get('macd_dif_prev')}
- MACD柱：当前{indicators.get('macd')}，前值{indicators.get('macd_prev')}
- 最近5天MACD柱：{indicators.get('recent_5d_macd')}

日线RSI/KDJ/CCI（超买超卖判断）：
- RSI：{indicators.get('rsi')}（>70超买，<30超卖，需在key_factors中说明）
- KDJ：K={indicators.get('kdj_k')}，D={indicators.get('kdj_d')}，J={indicators.get('kdj_j')}
- CCI：当前{indicators.get('cci')}，前值{indicators.get('cci_prev')}（>100超买，<-100超卖）
- 威廉%R：当前{indicators.get('williams_r')}，前值{indicators.get('williams_r_prev')}（>-20超买，<-80超卖）

日线ADX趋势强度：
- ADX：当前{indicators.get('adx')}，前值{indicators.get('adx_prev')}（>25趋势明确，<20无趋势）
- +DI：{indicators.get('plus_di')}，-DI：{indicators.get('minus_di')}

日线布林带：
- 上轨：{indicators.get('boll_upper')}
- 中轨：{indicators.get('boll_middle')}
- 下轨：{indicators.get('boll_lower')}
- 带宽：当前{indicators.get('boll_width')}，前值{indicators.get('boll_width_prev')}

日线斐波那契回撤位（用于计算止盈止损）：
- 波段高点：{indicators.get('fib_swing_high')}
- 波段低点：{indicators.get('fib_swing_low')}
- 38.2%位：{indicators.get('fib_382')}
- 50%位：{indicators.get('fib_500')}
- 61.8%位：{indicators.get('fib_618')}

日线成交量：
- 成交量比：{indicators.get('vol_ratio')}（>1.5放量，<0.8缩量，需在key_factors中说明）

小时线数据（入场时机判断）：
- 小时MA5：{indicators.get('hourly_ma5')}
- 小时MA20：{indicators.get('hourly_ma20')}
- 小时MACD DIF：{indicators.get('hourly_macd_dif')}
- 小时MACD柱：{indicators.get('hourly_macd')}
- 小时RSI：{indicators.get('hourly_rsi')}（>70超买，<30超卖）
- 小时KDJ：K={indicators.get('hourly_kdj_k')}，D={indicators.get('hourly_kdj_d')}，J={indicators.get('hourly_kdj_j')}
- 小时CCI：{indicators.get('hourly_cci')}
- 小时成交量比：{indicators.get('hourly_vol_ratio')}（⚠️重要：<0.8表示缩量，需在key_factors中说明）
- 小时布林带上轨：{indicators.get('hourly_boll_upper')}

历史数据：
- 近5日涨跌幅：{indicators.get('recent_5d_pct')}
- 5日累计涨跌：{indicators.get('pct_5d')}%
- 成交量变化比：{indicators.get('vol_change_ratio')}

止损参考：
- 当前最低价：{indicators.get('current_low')}元
- 近期最低价：{indicators.get('recent_low')}元
- 斐波那契61.8%位：{indicators.get('fib_618')}元

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

【大盘参考】
- 上证指数：{sh_index_price}点，涨跌：{sh_index_pct}%
- 市场情绪：{market_sentiment}（大盘强势时可适当激进，大盘弱势时需谨慎）

{stocks_info_str}

【分析要求 - 日线定趋势，小时线定进场】
请对每支股票分别进行分析，使用三重过滤系统：

【第一步：趋势过滤器（日线）】
判断条件（必须同时满足）：
- 股价 > MA60
- MA60趋势向上（当前MA60 > 前值MA60）
→ 满足则确认可以做多，进入第二步
→ 不满足则 signal="观望" 或 "回避"

【第二步：入场信号过滤器（小时线）】
需满足{min_conditions}个以上条件：
1. 小时MA5 > 小时MA20（短期均线在长期均线上方）
2. 小时MACD DIF向上（当前DIF > 前值DIF）或MACD柱由负转正
3. 小时成交量比 > {vol_ratio_threshold}（⚠️重要：<0.8为缩量，必须在key_factors中说明）
4. 小时KDJ J值<30后拐头向上 或 小时RSI<40后拐头向上（超卖回升）
5. 小时CCI从<-100回升至>-100（超卖反弹信号）

【第三步：风险控制过滤器】
- 风险回报比 >= {min_risk_reward}
- 单笔最大亏损 <= 300元（总资金10000元的3%）
- RSI < {rsi_upper_limit}（避免超买区入场）

【多周期共振规则（必须明确判断）】
| 日线状态 | 小时线状态 | 信号 | 说明 |
|---------|-----------|------|------|
| 多头（价>MA60且MA60向上） | 多头共振 | 强烈看多 | 最强信号，可立即入场 |
| 多头 | 回调企稳（RSI<40后拐头） | 买入 | 最佳入场时机 |
| 多头 | 下跌中（MACD向下） | 关注 | 等待小时线企稳 |
| 空头（价<MA60或MA60向下） | 任意 | 观望/回避 | 不符合做多条件 |

【指标使用要求（必须在key_factors中体现）】
1. RSI超买超卖：RSI>70必须说明"RSI超买"，RSI<30必须说明"RSI超卖"
2. 成交量：vol_ratio<0.8必须说明"成交量不足"，>1.5说明"放量"
3. 小时线成交量：hourly_vol_ratio<0.8必须说明"小时线缩量"
4. KDJ状态：J>80说明"KDJ超买"，J<20说明"KDJ超卖"
5. CCI状态：CCI>100说明"CCI超买"，CCI<-100说明"CCI超卖"
6. ADX趋势强度：ADX>25说明"趋势明确"，ADX<20说明"无明显趋势"

【买卖价格计算规则（必须参考技术位）】
1. 买入价（buy_price）：
   - 优先参考MA20支撑位或斐波那契38.2%/50%回撤位
   - 或当前价的99%（考虑滑点）
   
2. 止损价（stop_loss）：
   - 优先参考斐波那契61.8%回撤位或近期最低价
   - 或设在买入价下方2-3%
   - 必须确保：单笔亏损 <= 300元
   
3. 止盈价（sell_price）：
   - 优先参考布林带上轨或斐波那契扩展位
   - 或基于风险回报比1:{min_risk_reward}计算
   - 建议收益3-10%

【confidence（置信度）定义】
- 90-100：多周期强烈共振，所有指标一致，信号极强
- 75-89：大部分指标一致，信号较强
- 60-74：部分指标支持，信号中等
- 40-59：指标分歧较大，信号较弱
- 0-39：指标矛盾，信号很弱

【重要规则】
- 总资金10000元，每只股票全仓买入（不限制持仓数量）
- 单笔最大亏损：总资金的3%（300元）
- 只做多，不做空
- key_factors必须包含具体的指标数值和判断依据

请严格返回 JSON 格式，返回一个数组，每支股票一个对象：
[
  {{
    "code": "股票代码",
    "name": "股票名称",
    "trend": "上涨/下跌/震荡/未知",
    "risk": "低/中/高/未知",
    "confidence": 0-100之间的整数（参考上述定义）,
    "score": -100到100的整数评分,
    "signal": "买入/强烈看多/关注/观望/回避",
    "buy_price": 买入价（数字，signal为买入或强烈看多时必填，参考技术位计算，否则为null）,
    "sell_price": 止盈价（数字，signal为买入或强烈看多时必填，参考布林带上轨或斐波那契位，否则为null）,
    "stop_loss": 止损价（数字，signal为买入或强烈看多时必填，参考斐波那契61.8%或近期低点，否则为null）,
    "key_factors": ["必须包含具体指标数值，如：日线RSI=65处于正常区间", "小时线成交量比=0.58严重缩量"],
    "advice": "一句话操作建议",
    "summary": "100字以内的综合总结，必须提及关键指标状态",
    "reason": "给出交易点位的理由（30字以内，说明参考了哪个技术位）"
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

