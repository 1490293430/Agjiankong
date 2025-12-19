"""
邮箱通知服务
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from common.config import settings
from common.runtime_config import get_runtime_config
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
    # 优先使用运行时配置
    runtime_config = get_runtime_config()
    
    # 检查是否启用
    if not runtime_config.notify_email_enabled:
        logger.debug("邮箱通知已禁用，跳过发送")
        return False
    
    # 优先使用运行时配置，如果没有则使用环境变量
    smtp_host = runtime_config.notify_email_smtp_host or settings.email_smtp_host
    smtp_port = runtime_config.notify_email_smtp_port or settings.email_smtp_port
    email_user = runtime_config.notify_email_user or settings.email_user
    email_password = runtime_config.notify_email_password or settings.email_password
    to_email = to_email or runtime_config.notify_email_to or settings.email_to
    
    if not smtp_host or not smtp_port or not email_user or not email_password:
        logger.warning("邮箱SMTP配置未设置，跳过发送")
        return False
    
    if not to_email:
        logger.warning("收件人邮箱未设置，跳过发送")
        return False
    
    try:
        # 创建邮件
        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = to_email
        msg["Subject"] = subject
        
        # 添加正文
        msg.attach(MIMEText(content, "html", "utf-8"))
        
        # 发送邮件（根据端口选择SSL或STARTTLS）
        if smtp_port == 465:
            # SSL方式
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            # STARTTLS方式（如587端口）
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
        
        server.login(email_user, email_password)
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

