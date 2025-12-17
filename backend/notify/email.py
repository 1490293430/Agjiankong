"""
邮箱通知服务
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)


def send_email(subject: str, content: str, to_email: Optional[str] = None) -> bool:
    """发送邮件
    
    Args:
        subject: 邮件主题
        content: 邮件内容
        to_email: 收件人邮箱（默认使用配置中的邮箱）
    
    Returns:
        是否发送成功
    """
    if not settings.email_user or not settings.email_password:
        logger.warning("邮箱配置未设置，跳过发送")
        return False
    
    to_email = to_email or settings.email_to
    if not to_email:
        logger.warning("收件人邮箱未设置，跳过发送")
        return False
    
    try:
        # 创建邮件
        msg = MIMEMultipart()
        msg["From"] = settings.email_user
        msg["To"] = to_email
        msg["Subject"] = subject
        
        # 添加正文
        msg.attach(MIMEText(content, "html", "utf-8"))
        
        # 发送邮件
        server = smtplib.SMTP_SSL(settings.email_smtp_host, settings.email_smtp_port)
        server.login(settings.email_user, settings.email_password)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"邮件发送成功：{subject}")
        return True
        
    except Exception as e:
        logger.error(f"邮件发送失败: {e}", exc_info=True)
        return False


def send_stock_alert_email(code: str, name: str, price: float, message: str) -> bool:
    """发送股票提醒邮件"""
    subject = f"股票提醒 - {name} ({code})"
    content = f"""
    <h2>股票提醒</h2>
    <p><strong>股票：</strong>{name} ({code})</p>
    <p><strong>价格：</strong>{price} 元</p>
    <p><strong>提醒内容：</strong></p>
    <p>{message}</p>
    """
    return send_email(subject, content)


def send_selection_alert_email(stocks: list) -> bool:
    """发送选股提醒邮件"""
    if not stocks:
        return False
    
    subject = f"自动选股提醒 - {len(stocks)}只股票"
    content = f"<h2>自动选股提醒</h2><p>共选出 {len(stocks)} 只股票：</p><ul>"
    
    for stock in stocks[:20]:  # 最多显示20只
        score = stock.get("score", 0)
        content += f"<li>{stock.get('name', '')} ({stock.get('code', '')}) - 评分：{score}</li>"
    
    content += "</ul>"
    
    return send_email(subject, content)

