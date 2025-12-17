"""
Telegram通知服务
"""
import requests
from typing import Optional
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)


def send_message(message: str, parse_mode: str = "HTML") -> bool:
    """发送Telegram消息
    
    Args:
        message: 消息内容
        parse_mode: 解析模式（HTML/Markdown）
    
    Returns:
        是否发送成功
    """
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.warning("Telegram配置未设置，跳过发送")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        
        data = {
            "chat_id": settings.telegram_chat_id,
            "text": message,
            "parse_mode": parse_mode
        }
        
        response = requests.post(url, json=data, timeout=10)
        response.raise_for_status()
        
        logger.info("Telegram消息发送成功")
        return True
        
    except Exception as e:
        logger.error(f"Telegram消息发送失败: {e}", exc_info=True)
        return False


def send_stock_alert(code: str, name: str, price: float, message: str) -> bool:
    """发送股票提醒"""
    text = f"""
📈 <b>股票提醒</b>

股票：{name} ({code})
价格：{price} 元

{message}
"""
    return send_message(text)


def send_selection_alert(stocks: list) -> bool:
    """发送选股提醒"""
    if not stocks:
        return False
    
    text = f"📊 <b>自动选股提醒</b>\n\n"
    text += f"共选出 {len(stocks)} 只股票：\n\n"
    
    for i, stock in enumerate(stocks[:10], 1):  # 最多显示10只
        score = stock.get("score", 0)
        text += f"{i}. {stock.get('name', '')} ({stock.get('code', '')}) - 评分：{score}\n"
    
    if len(stocks) > 10:
        text += f"\n... 还有 {len(stocks) - 10} 只股票"
    
    return send_message(text)

