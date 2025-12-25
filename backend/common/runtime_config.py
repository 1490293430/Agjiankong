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
    selection_max_count: int = Field(
        default=30, ge=1, le=200, description="每次最多选出的股票数量"
    )
    selection_market: str = Field(
        default="A", description="选股市场：A（A股）或HK（港股）"
    )
    
    # 筛选策略配置
    filter_stock_only: bool = Field(
        default=True, description="是否仅筛选股票（排除ETF/指数/基金）"
    )
    filter_market_cap_enable: bool = Field(
        default=False, description="是否启用市值筛选"
    )
    filter_market_cap_min: float = Field(
        default=1, ge=0, description="市值最小值（亿）"
    )
    filter_market_cap_max: float = Field(
        default=100000, ge=0, description="市值最大值（亿）"
    )
    filter_volume_ratio_enable: bool = Field(
        default=True, description="是否启用量比筛选"
    )
    filter_volume_ratio_min: float = Field(
        default=0.8, ge=0, description="量比最小值"
    )
    filter_volume_ratio_max: float = Field(
        default=8.0, ge=0, description="量比最大值"
    )
    filter_rsi_enable: bool = Field(
        default=True, description="是否启用RSI筛选"
    )
    filter_rsi_min: int = Field(
        default=30, ge=0, le=100, description="RSI最小值"
    )
    filter_rsi_max: int = Field(
        default=75, ge=0, le=100, description="RSI最大值"
    )
    filter_ma_enable: bool = Field(
        default=False, description="是否启用MA筛选"
    )
    filter_ma_period: str = Field(
        default="20", description="MA周期"
    )
    filter_ma_condition: str = Field(
        default="above", description="MA条件：above/below"
    )
    filter_ema_enable: bool = Field(
        default=False, description="是否启用EMA筛选"
    )
    filter_ema_period: str = Field(
        default="12", description="EMA周期"
    )
    filter_ema_condition: str = Field(
        default="above", description="EMA条件：above/golden"
    )
    filter_macd_enable: bool = Field(
        default=False, description="是否启用MACD筛选"
    )
    filter_macd_condition: str = Field(
        default="golden", description="MACD条件：golden/dead/above_zero"
    )
    filter_kdj_enable: bool = Field(
        default=False, description="是否启用KDJ筛选"
    )
    filter_kdj_condition: str = Field(
        default="golden", description="KDJ条件：golden/dead/oversold"
    )
    filter_bias_enable: bool = Field(
        default=False, description="是否启用BIAS筛选"
    )
    filter_bias_min: float = Field(
        default=-6, description="BIAS最小值"
    )
    filter_bias_max: float = Field(
        default=6, description="BIAS最大值"
    )
    filter_williams_r_enable: bool = Field(
        default=False, description="是否启用威廉指标筛选"
    )
    filter_break_high_enable: bool = Field(
        default=False, description="是否启用突破高点筛选"
    )
    filter_boll_enable: bool = Field(
        default=False, description="是否启用布林带筛选"
    )
    filter_boll_condition: str = Field(
        default="expanding", description="布林带条件：expanding/above_mid/near_lower"
    )
    filter_adx_enable: bool = Field(
        default=False, description="是否启用ADX筛选"
    )
    filter_adx_min: float = Field(
        default=25, ge=0, le=100, description="ADX最小值"
    )
    filter_ichimoku_enable: bool = Field(
        default=False, description="是否启用一目均衡筛选"
    )
    filter_ichimoku_condition: str = Field(
        default="above_cloud", description="一目均衡条件：above_cloud/below_cloud/tk_cross"
    )

    # 行情采集相关
    collector_interval_seconds: int = Field(
        default=60, ge=5, le=3600, description="行情采集间隔（秒），最小5秒"
    )
    
    # 数据采集配置
    collect_market: str = Field(
        default="ALL", description="数据采集市场：ALL（全部）、A（A股）、HK（港股）"
    )
    collect_period: str = Field(
        default="daily", description="数据采集周期：daily（日线）、1h（小时线）"
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
    kline_data_source: str = Field(
        default="auto",
        description="K线数据源：auto（自动切换）、akshare（仅AKShare）、tushare（仅Tushare）、sina（仅新浪财经）、easyquotation（仅Easyquotation）"
    )
    
    # 实时行情数据源配置（A股）
    spot_data_source: str = Field(
        default="auto",
        description="A股实时行情数据源：auto（自动切换）、easyquotation、eastmoney、sina、akshare"
    )
    
    # 实时行情数据源配置（港股）
    hk_spot_data_source: str = Field(
        default="auto",
        description="港股实时行情数据源：auto（自动切换）、eastmoney、sina、akshare"
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

    # AI 自动分析相关
    ai_auto_analyze_time: Optional[str] = Field(
        default=None,
        description="每日自动分析自选股票的时间，格式为HH:MM（24小时制），例如09:30",
    )
    
    # AI 分析使用的K线数据配置
    ai_data_period: str = Field(
        default="daily",
        description="AI分析使用的K线周期：daily（日线）或 1h（小时线）"
    )
    ai_data_count: int = Field(
        default=500,
        ge=50,
        le=1000,
        description="提交给AI的K线根数，范围50-1000"
    )
    ai_batch_size: int = Field(
        default=5,
        ge=1,
        le=1000,
        description="批量分析时，每批合并分析的股票数量，范围1-1000"
    )

    # AI 分析结果通知渠道开关（独立于全局通知开关）
    ai_notify_telegram: bool = Field(
        default=False,
        description="是否通过 Telegram 发送 AI 分析结果通知",
    )
    ai_notify_email: bool = Field(
        default=False,
        description="是否通过 邮箱 发送 AI 分析结果通知",
    )
    ai_notify_wechat: bool = Field(
        default=False,
        description="是否通过 企业微信 发送 AI 分析结果通知",
    )
    
    # 管理员密码（存储在Redis中，优先于环境变量）
    admin_password: Optional[str] = Field(
        default=None,
        description="管理员登录密码（如果设置，将覆盖环境变量ADMIN_PASSWORD）",
    )
    
    # Tushare 数据源配置
    tushare_token: Optional[str] = Field(
        default=None,
        description="Tushare Pro API Token，用于获取A股数据",
    )


class RuntimeConfigUpdate(BaseModel):
    """前端更新配置时使用的部分更新模型"""

    selection_max_count: Optional[int] = Field(default=None, ge=1, le=200)
    selection_market: Optional[str] = Field(default=None, description="选股市场：A或HK")
    
    # 筛选策略配置
    filter_stock_only: Optional[bool] = None
    filter_market_cap_enable: Optional[bool] = None
    filter_market_cap_min: Optional[float] = Field(default=None, ge=0)
    filter_market_cap_max: Optional[float] = Field(default=None, ge=0)
    filter_volume_ratio_enable: Optional[bool] = None
    filter_volume_ratio_min: Optional[float] = Field(default=None, ge=0)
    filter_volume_ratio_max: Optional[float] = Field(default=None, ge=0)
    filter_rsi_enable: Optional[bool] = None
    filter_rsi_min: Optional[int] = Field(default=None, ge=0, le=100)
    filter_rsi_max: Optional[int] = Field(default=None, ge=0, le=100)
    filter_ma_enable: Optional[bool] = None
    filter_ma_period: Optional[str] = None
    filter_ma_condition: Optional[str] = None
    filter_ema_enable: Optional[bool] = None
    filter_ema_period: Optional[str] = None
    filter_ema_condition: Optional[str] = None
    filter_macd_enable: Optional[bool] = None
    filter_macd_condition: Optional[str] = None
    filter_kdj_enable: Optional[bool] = None
    filter_kdj_condition: Optional[str] = None
    filter_bias_enable: Optional[bool] = None
    filter_bias_min: Optional[float] = None
    filter_bias_max: Optional[float] = None
    filter_williams_r_enable: Optional[bool] = None
    filter_break_high_enable: Optional[bool] = None
    filter_boll_enable: Optional[bool] = None
    filter_boll_condition: Optional[str] = None
    filter_adx_enable: Optional[bool] = None
    filter_adx_min: Optional[float] = Field(default=None, ge=0, le=100)
    filter_ichimoku_enable: Optional[bool] = None
    filter_ichimoku_condition: Optional[str] = None
    collector_interval_seconds: Optional[int] = Field(default=None, ge=5, le=3600)
    collect_market: Optional[str] = Field(default=None, description="数据采集市场：ALL、A、HK")
    collect_period: Optional[str] = Field(default=None, description="数据采集周期：daily、1h")
    notify_channels: Optional[List[str]] = None
    kline_years: Optional[float] = Field(default=None, ge=0.5, le=10.0)
    kline_data_source: Optional[str] = Field(default=None, description="K线数据源：auto、akshare、tushare、sina、easyquotation")
    spot_data_source: Optional[str] = Field(default=None, description="A股实时行情数据源：auto、easyquotation、eastmoney、sina、akshare")
    hk_spot_data_source: Optional[str] = Field(default=None, description="港股实时行情数据源：auto、eastmoney、sina、akshare")
    
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
    ai_auto_analyze_time: Optional[str] = None
    ai_data_period: Optional[str] = Field(default=None, description="AI分析使用的K线周期：daily 或 1h")
    ai_data_count: Optional[int] = Field(default=None, ge=50, le=1000)
    ai_batch_size: Optional[int] = Field(default=None, ge=1, le=1000)
    ai_notify_telegram: Optional[bool] = None
    ai_notify_email: Optional[bool] = None
    ai_notify_wechat: Optional[bool] = None
    
    # 管理员密码
    admin_password: Optional[str] = None
    
    # Tushare 数据源配置
    tushare_token: Optional[str] = None


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
        # 如果密码或 API Key 或 Token 为空字符串，则不更新（保持原值）
        if field in ("notify_email_password", "openai_api_key", "admin_password", "tushare_token") and value == "":
            continue
        setattr(current, field, value)

    save_runtime_config(current)
    return current



