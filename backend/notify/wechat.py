"""
企业微信通知服务
"""
import requests
from typing import Optional
from common.config import settings
from common.runtime_config import get_runtime_config
from common.logger import get_logger

logger = get_logger(__name__)


def send_message(message: str, msg_type: str = "text") -> bool:
    """发送企业微信消息
    
    Args:
        message: 消息内容
        msg_type: 消息类型（text/markdown）
    
    Returns:
        是否发送成功
    """
    # 优先使用运行时配置
    runtime_config = get_runtime_config()
    webhook_url = runtime_config.notify_wechat_webhook_url or settings.wechat_webhook_url
    
    # 检查是否启用
    if not runtime_config.notify_wechat_enabled:
        logger.debug("企业微信通知已禁用，跳过发送")
        return False
    
    if not webhook_url:
        logger.warning("企业微信配置未设置，跳过发送")
        return False
    
    try:
        data = {
            "msgtype": msg_type,
            msg_type: {
                "content": message
            }
        }
        
        response = requests.post(webhook_url, json=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        if result.get("errcode") == 0:
            logger.info("企业微信消息发送成功")
            return True
        else:
            logger.error(f"企业微信消息发送失败: {result.get('errmsg', '')}")
            return False
            
    except Exception as e:
        logger.error(f"企业微信消息发送失败: {e}", exc_info=True)
        return False


def send_stock_alert(code: str, name: str, price: float, message: str) -> bool:
    """发送股票提醒"""
    text = f"📈 股票提醒\n\n股票：{name} ({code})\n价格：{price} 元\n\n{message}"
    return send_message(text)


def send_selection_alert(stocks: list) -> bool:
    """发送选股提醒"""
    if not stocks:
        return False
    
    text = f"📊 自动选股提醒\n\n共选出 {len(stocks)} 只股票：\n\n"
    
    for i, stock in enumerate(stocks[:10], 1):  # 最多显示10只
        score = stock.get("score", 0)
        text += f"{i}. {stock.get('name', '')} ({stock.get('code', '')}) - 评分：{score}\n"
    
    if len(stocks) > 10:
        text += f"\n... 还有 {len(stocks) - 10} 只股票"
    
    return send_message(text)

