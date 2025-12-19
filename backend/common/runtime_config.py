"""
运行时配置管理

通过 Redis 持久化，可在前端动态修改。
"""
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError

from common.redis import get_json, set_json
from common.logger import get_logger

logger = get_logger(__name__)

RUNTIME_CONFIG_KEY = "app:runtime_config"


class RuntimeConfig(BaseModel):
    """系统运行时配置（可通过前端修改）"""

    # 选股相关
    selection_threshold: int = Field(
        default=65, ge=0, le=100, description="选股阈值，0-100"
    )
    selection_max_count: int = Field(
        default=30, ge=1, le=200, description="每次最多选出的股票数量"
    )

    # 行情采集相关
    collector_interval_seconds: int = Field(
        default=60, ge=5, le=3600, description="行情采集间隔（秒），最小5秒"
    )

    # 通知相关
    notify_channels: List[str] = Field(
        default_factory=lambda: ["telegram", "email", "wechat"],
        description="默认通知渠道列表",
    )
    
    # 通知渠道详细配置
    notify_telegram_enabled: bool = Field(default=True, description="Telegram通知是否启用")
    notify_telegram_bot_token: Optional[str] = Field(default=None, description="Telegram Bot Token")
    notify_telegram_chat_id: Optional[str] = Field(default=None, description="Telegram Chat ID")
    
    notify_email_enabled: bool = Field(default=True, description="邮箱通知是否启用")
    notify_email_smtp_host: Optional[str] = Field(default=None, description="SMTP服务器地址")
    notify_email_smtp_port: Optional[int] = Field(default=None, description="SMTP端口")
    notify_email_user: Optional[str] = Field(default=None, description="邮箱账号")
    notify_email_password: Optional[str] = Field(default=None, description="邮箱密码")
    notify_email_to: Optional[str] = Field(default=None, description="接收邮箱地址")
    
    notify_wechat_enabled: bool = Field(default=True, description="企业微信通知是否启用")
    notify_wechat_webhook_url: Optional[str] = Field(default=None, description="企业微信Webhook URL")

    # K线数据相关
    kline_years: float = Field(
        default=1.0, ge=0.5, le=10.0, description="K线数据加载年限（年），范围0.5-10年"
    )
    
    # AI / OpenAI 配置（用于AI分析）
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI 兼容接口的 API Key（只在服务端保存，不在前端明文展示）",
    )
    openai_api_base: str = Field(
        default="https://openai.qiniu.com/v1",
        description="OpenAI 兼容接口的 Base URL，例如：https://openai.qiniu.com/v1",
    )
    openai_model: str = Field(
        default="deepseek/deepseek-v3.2-251201",
        description="用于 AI 分析的模型名称，例如：deepseek/deepseek-v3.2-251201",
    )


class RuntimeConfigUpdate(BaseModel):
    """前端更新配置时使用的部分更新模型"""

    selection_threshold: Optional[int] = Field(default=None, ge=0, le=100)
    selection_max_count: Optional[int] = Field(default=None, ge=1, le=200)
    collector_interval_seconds: Optional[int] = Field(default=None, ge=5, le=3600)
    notify_channels: Optional[List[str]] = None
    kline_years: Optional[float] = Field(default=None, ge=0.5, le=10.0)
    
    # 通知渠道详细配置
    notify_telegram_enabled: Optional[bool] = None
    notify_telegram_bot_token: Optional[str] = None
    notify_telegram_chat_id: Optional[str] = None
    
    notify_email_enabled: Optional[bool] = None
    notify_email_smtp_host: Optional[str] = None
    notify_email_smtp_port: Optional[int] = None
    notify_email_user: Optional[str] = None
    notify_email_password: Optional[str] = None
    notify_email_to: Optional[str] = None
    
    notify_wechat_enabled: Optional[bool] = None
    notify_wechat_webhook_url: Optional[str] = None
    
    # AI / OpenAI 配置
    openai_api_key: Optional[str] = None
    openai_api_base: Optional[str] = None
    openai_model: Optional[str] = None


def get_runtime_config() -> RuntimeConfig:
    """从 Redis 获取运行时配置，失败时返回默认配置"""
    try:
        data = get_json(RUNTIME_CONFIG_KEY)
        if isinstance(data, dict):
            try:
                return RuntimeConfig(**data)
            except ValidationError as e:
                logger.warning(f"运行时配置格式错误，使用默认配置: {e}")
    except Exception as e:
        logger.warning(f"获取运行时配置失败，使用默认配置: {e}")

    return RuntimeConfig()


def save_runtime_config(cfg: RuntimeConfig) -> None:
    """保存完整配置到 Redis"""
    try:
        set_json(RUNTIME_CONFIG_KEY, cfg.model_dump())
    except Exception as e:
        logger.error(f"保存运行时配置失败: {e}")


def update_runtime_config(patch: RuntimeConfigUpdate) -> RuntimeConfig:
    """根据部分更新数据更新配置并持久化"""
    current = get_runtime_config()
    update_data = patch.model_dump(exclude_none=True)

    for field, value in update_data.items():
        # 如果密码或 API Key 为空字符串，则不更新（保持原值）
        if field in ("notify_email_password", "openai_api_key") and value == "":
            continue
        setattr(current, field, value)

    save_runtime_config(current)
    return current



