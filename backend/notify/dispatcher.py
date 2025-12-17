"""
统一通知调度器
"""
from typing import List, Dict, Any
from notify.telegram import send_message as tg_send, send_selection_alert as tg_alert
from notify.email import send_email, send_selection_alert_email
from notify.wechat import send_message as wx_send, send_selection_alert as wx_alert
from common.logger import get_logger

logger = get_logger(__name__)


def notify(message: str, channels: List[str] = None) -> Dict[str, bool]:
    """统一通知接口
    
    Args:
        message: 消息内容
        channels: 通知渠道列表（telegram/email/wechat），None表示使用所有渠道
    
    Returns:
        各渠道发送结果
    """
    if channels is None:
        channels = ["telegram", "email", "wechat"]
    
    results = {}
    
    if "telegram" in channels:
        results["telegram"] = tg_send(message)
    
    if "email" in channels:
        results["email"] = send_email("股票系统通知", message)
    
    if "wechat" in channels:
        results["wechat"] = wx_send(message)
    
    return results


def notify_selection(stocks: List[Dict[str, Any]], channels: List[str] = None) -> Dict[str, bool]:
    """通知选股结果
    
    Args:
        stocks: 选中的股票列表
        channels: 通知渠道列表
    
    Returns:
        各渠道发送结果
    """
    if channels is None:
        channels = ["telegram", "email", "wechat"]
    
    results = {}
    
    if "telegram" in channels:
        results["telegram"] = tg_alert(stocks)
    
    if "email" in channels:
        results["email"] = send_selection_alert_email(stocks)
    
    if "wechat" in channels:
        results["wechat"] = wx_alert(stocks)
    
    return results

