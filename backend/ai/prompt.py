"""
AI分析提示词构建
"""


def build_stock_analysis_prompt(stock: dict, indicators: dict, news: list = None) -> str:
    """构建股票分析提示词"""
    
    news_summary = ""
    if news:
        news_summary = "\n".join([f"- {n.get('title', '')}" for n in news[:5]])
    
    prompt = f"""
你是一名专业的股票分析师，请对以下股票进行综合分析：

【股票信息】
股票代码：{stock.get('code', '')}
股票名称：{stock.get('name', '')}
当前价格：{stock.get('price', 0)}元
涨跌幅：{stock.get('pct', 0)}%
成交量：{stock.get('volume', 0)}
成交额：{stock.get('amount', 0)}万元

【技术指标】
MA5：{indicators.get('ma5', 'N/A')}
MA10：{indicators.get('ma10', 'N/A')}
MA20：{indicators.get('ma20', 'N/A')}
RSI：{indicators.get('rsi', 'N/A')}
MACD DIF：{indicators.get('macd_dif', 'N/A')}
MACD DEA：{indicators.get('macd_dea', 'N/A')}
成交量比：{indicators.get('vol_ratio', 'N/A')}

【相关资讯】
{news_summary if news_summary else '暂无相关资讯'}

请给出：
1. 趋势判断（短期/中期趋势）
2. 风险评级（低/中/高）
3. 操作建议（买入/持有/观望/卖出）
4. 关键支撑位和压力位（如果有）
5. 简要总结（100字以内）

请用专业但易懂的语言回答。
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

