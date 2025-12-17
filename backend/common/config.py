"""
配置管理模块
"""
import os
import shutil
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv


def _init_env_files() -> None:
    """初始化 .env 配置文件

    优先规则：
    1. 如果主目录或 backend 目录已经存在 .env，则直接使用
    2. 如果主目录不存在 .env 但存在根目录 .env.example，则复制一份到主目录下作为 .env
    """
    current_dir = os.path.dirname(__file__)           # backend/common
    backend_dir = os.path.dirname(current_dir)        # backend
    project_root = os.path.dirname(backend_dir)       # 项目根目录

    root_env = os.path.join(project_root, ".env")
    backend_env = os.path.join(backend_dir, ".env")
    root_example = os.path.join(project_root, ".env.example")

    # 已经有任何 .env 就不再自动复制
    if os.path.exists(root_env) or os.path.exists(backend_env):
        return

    # 仅使用根目录 .env.example 作为模板
    if os.path.exists(root_example):
        try:
            shutil.copyfile(root_example, root_env)
        except Exception:
            # 复制失败不影响程序启动，只是少了默认 .env 文件
            pass


# 初始化并加载环境变量（支持根目录和 backend 目录的 .env）
_init_env_files()

current_dir = os.path.dirname(__file__)
backend_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(backend_dir)

load_dotenv(os.path.join(project_root, ".env"))
load_dotenv(os.path.join(backend_dir, ".env"))


class Settings(BaseSettings):
    """应用配置"""
    
    # Redis配置
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", 6379))
    redis_db: int = int(os.getenv("REDIS_DB", 0))
    redis_password: Optional[str] = os.getenv("REDIS_PASSWORD", None)
    
    # ClickHouse配置
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", 9000))
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB", "stock")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    # 默认给一个弱密码，避免生产环境误用空密码；可在环境变量中覆盖
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "changeme")
    
    # Telegram配置
    telegram_bot_token: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN", None)
    telegram_chat_id: Optional[str] = os.getenv("TELEGRAM_CHAT_ID", None)
    
    # 邮箱配置
    email_smtp_host: str = os.getenv("EMAIL_SMTP_HOST", "smtp.qq.com")
    email_smtp_port: int = int(os.getenv("EMAIL_SMTP_PORT", 465))
    email_user: Optional[str] = os.getenv("EMAIL_USER", None)
    email_password: Optional[str] = os.getenv("EMAIL_PASSWORD", None)
    email_to: Optional[str] = os.getenv("EMAIL_TO", None)
    
    # 企业微信配置
    wechat_webhook_url: Optional[str] = os.getenv("WECHAT_WEBHOOK_URL", None)
    
    # 日志配置
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    # API配置
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", 8000))
    # 允许的前端来源，逗号分隔，例如："http://localhost:8000,https://example.com"
    api_allowed_origins: str = os.getenv("API_ALLOWED_ORIGINS", "*")
    # 可选的简单 API Token，用于保护接口；不设置则不启用鉴权
    api_auth_token: Optional[str] = os.getenv("API_AUTH_TOKEN", None)
    # 超级管理员 Token，用于敏感操作；未设置时默认回退到 api_auth_token
    admin_token: Optional[str] = os.getenv("ADMIN_TOKEN", None)
    # Admin 登录账号（用于前端登录），默认 admin/admin，可在生产环境用环境变量覆盖
    admin_username: str = os.getenv("ADMIN_USERNAME", "admin")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "admin")
    
    class Config:
        case_sensitive = False


settings = Settings()

