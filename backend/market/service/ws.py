"""
进度管理模块（原WebSocket模块，现仅保留进度数据结构供SSE使用）
"""
from common.logger import get_logger

logger = get_logger(__name__)

# 选股进度管理器（使用字典存储每个任务的进度）
selection_progress: dict = {}

# K线采集进度管理器（使用字典存储每个任务的进度）
kline_collect_progress: dict = {}

# K线采集任务停止标志（存储task_id -> should_stop标志）
kline_collect_stop_flags: dict = {}
