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
        default=5, ge=1, le=3600, description="行情采集间隔（秒）"
    )

    # 通知相关
    notify_channels: List[str] = Field(
        default_factory=lambda: ["telegram", "email", "wechat"],
        description="默认通知渠道列表",
    )


class RuntimeConfigUpdate(BaseModel):
    """前端更新配置时使用的部分更新模型"""

    selection_threshold: Optional[int] = Field(default=None, ge=0, le=100)
    selection_max_count: Optional[int] = Field(default=None, ge=1, le=200)
    collector_interval_seconds: Optional[int] = Field(default=None, ge=1, le=3600)
    notify_channels: Optional[List[str]] = None


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
        setattr(current, field, value)

    save_runtime_config(current)
    return current



