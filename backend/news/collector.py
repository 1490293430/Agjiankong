"""
财经资讯采集
"""
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from common.redis import set_json
from common.logger import get_logger

logger = get_logger(__name__)


def fetch_news() -> List[Dict[str, Any]]:
    """获取最新财经资讯"""
    try:
        # 获取东方财富新闻
        df = ak.stock_news_em(symbol="全部")
        
        # 标准化字段
        if not df.empty:
            df = df.rename(columns={
                "新闻标题": "title",
                "新闻内容": "content",
                "发布时间": "publish_time",
                "新闻链接": "url"
            })
            
            df["collect_time"] = datetime.now().isoformat()
            df["source"] = "东方财富"
            
            result = df.head(100).to_dict(orient="records")
            
            # 存储到Redis
            set_json("news:latest", result, ex=3600)  # 1小时过期
            
            # 通过SSE广播资讯更新
            try:
                from market.service.sse import broadcast_news_update
                broadcast_news_update(result)
            except Exception as e:
                logger.debug(f"SSE广播资讯更新失败（不影响采集）: {e}")
            
            logger.info(f"资讯采集成功，共{len(result)}条")
            return result
        
        return []
        
    except Exception as e:
        logger.error(f"资讯采集失败: {e}", exc_info=True)
        return []


def fetch_important_news() -> List[Dict[str, Any]]:
    """获取重要资讯（关键词过滤）"""
    try:
        news_list = fetch_news()
        
        # 关键词过滤
        keywords = ["政策", "利好", "利空", "公告", "业绩", "重组", "收购", 
                   "增持", "减持", "停牌", "复牌", "涨停", "跌停"]
        
        important_news = [
            news for news in news_list
            if any(keyword in str(news.get("title", "")) for keyword in keywords)
        ]
        
        # 存储重要资讯
        set_json("news:important", important_news, ex=3600)
        
        logger.info(f"重要资讯筛选完成，共{len(important_news)}条")
        return important_news
        
    except Exception as e:
        logger.error(f"重要资讯采集失败: {e}", exc_info=True)
        return []

