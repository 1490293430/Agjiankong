"""
资讯分析模块
"""
from typing import List, Dict, Any
import re


def extract_stock_codes(text: str) -> List[str]:
    """从文本中提取股票代码"""
    # A股代码：6位数字
    a_codes = re.findall(r'\b[0][0][0-3]\d{3}\b|\b[6][0]\d{4}\b|\b[3][0]\d{4}\b', text)
    # 港股代码：5位数字
    hk_codes = re.findall(r'\b[0-9]{5}\b', text)
    
    return list(set(a_codes + hk_codes))


def classify_sentiment(text: str) -> str:
    """简单的情感分类（利好/利空/中性）"""
    positive_keywords = ["利好", "上涨", "增长", "盈利", "超预期", "买入", "推荐", "增持"]
    negative_keywords = ["利空", "下跌", "亏损", "风险", "减持", "卖出", "警告"]
    
    text_lower = text.lower()
    positive_count = sum(1 for kw in positive_keywords if kw in text_lower)
    negative_count = sum(1 for kw in negative_keywords if kw in text_lower)
    
    if positive_count > negative_count:
        return "利好"
    elif negative_count > positive_count:
        return "利空"
    else:
        return "中性"


def analyze_news(news: Dict[str, Any]) -> Dict[str, Any]:
    """分析单条资讯"""
    title = str(news.get("title", ""))
    content = str(news.get("content", ""))
    
    text = title + " " + content
    
    # 提取股票代码
    codes = extract_stock_codes(text)
    
    # 情感分析
    sentiment = classify_sentiment(text)
    
    # 影响级别（简单判断）
    impact_level = "低"
    if "重大" in text or "重要" in text or "紧急" in text:
        impact_level = "高"
    elif "公告" in text or "业绩" in text:
        impact_level = "中"
    
    return {
        **news,
        "related_stocks": codes,
        "sentiment": sentiment,
        "impact_level": impact_level
    }

