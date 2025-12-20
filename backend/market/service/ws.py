"""
WebSocket实时行情推送（优化版：使用差分数据，有新数据才推送，降低Redis读取频率）
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List, Optional
import json
import asyncio
from common.redis import get_json, get_redis
from common.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


class ConnectionManager:
    """WebSocket连接管理器"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket连接建立，当前连接数: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket连接断开，当前连接数: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"发送消息失败: {e}")
            self.disconnect(websocket)
    
    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"广播消息失败: {e}")
                disconnected.append(connection)
        
        # 移除断开的连接
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()

# 选股进度管理器（使用字典存储每个任务的进度）
selection_progress: dict = {}

# K线采集进度管理器（使用字典存储每个任务的进度）
kline_collect_progress: dict = {}

# K线采集任务停止标志（存储task_id -> should_stop标志）
kline_collect_stop_flags: dict = {}


@router.websocket("/ws/selection/progress")
async def websocket_selection_progress(websocket: WebSocket):
    """选股进度WebSocket"""
    await manager.connect(websocket)
    
    try:
        # 等待客户端发送任务ID
        data = await websocket.receive_json()
        task_id = data.get("task_id")
        
        if not task_id:
            await websocket.close(code=1008, reason="缺少task_id")
            return
        
        logger.info(f"选股进度WebSocket连接：task_id={task_id}")
        
        # 定期推送进度（每0.5秒检查一次）
        last_progress = None
        while True:
            await asyncio.sleep(0.5)  # 每0.5秒检查一次
            
            # 从全局字典获取进度
            progress = selection_progress.get(task_id)
            
            # 如果有新进度，推送
            if progress and progress != last_progress:
                try:
                    await websocket.send_json({
                        "type": "selection_progress",
                        "task_id": task_id,
                        "progress": progress
                    })
                    last_progress = progress
                except Exception as e:
                    logger.error(f"推送选股进度失败: {e}")
                    break
            
            # 如果进度完成或失败，推送最终消息后断开
            if progress and progress.get("status") in ["completed", "failed"]:
                await asyncio.sleep(1)  # 等待1秒确保最终消息送达
                # 清理进度数据
                if task_id in selection_progress:
                    del selection_progress[task_id]
                break
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("选股进度WebSocket客户端断开连接")
    except Exception as e:
        logger.error(f"选股进度WebSocket错误: {e}", exc_info=True)
        manager.disconnect(websocket)


@router.websocket("/ws/kline/collect/progress")
async def websocket_kline_collect_progress(websocket: WebSocket):
    """K线采集进度WebSocket"""
    await manager.connect(websocket)
    
    try:
        # 等待客户端发送任务ID（可选，如果没有则监听所有任务）
        try:
            data = await asyncio.wait_for(websocket.receive_json(), timeout=5.0)
            task_id = data.get("task_id")
        except asyncio.TimeoutError:
            # 如果没有收到task_id，监听最新的任务
            task_id = None
        
        logger.info(f"K线采集进度WebSocket连接：task_id={task_id or 'latest'}")
        
        # 定期推送进度（每1秒检查一次）
        last_progress = None
        while True:
            await asyncio.sleep(1)  # 每1秒检查一次
            
            # 如果没有指定task_id，获取最新的任务
            if not task_id:
                # 获取最新的任务ID（按时间戳排序）
                if kline_collect_progress:
                    task_id = max(kline_collect_progress.keys(), key=lambda k: kline_collect_progress[k].get("start_time", 0))
            
            # 从全局字典获取进度
            progress = kline_collect_progress.get(task_id) if task_id else None
            
            # 如果有新进度，推送
            if progress and progress != last_progress:
                try:
                    await websocket.send_json({
                        "type": "kline_collect_progress",
                        "task_id": task_id,
                        "progress": progress
                    })
                    last_progress = progress.copy() if progress else None
                except Exception as e:
                    logger.error(f"推送K线采集进度失败: {e}")
                    break
            
            # 如果进度完成或失败，推送最终消息后断开
            if progress and progress.get("status") in ["completed", "failed"]:
                await asyncio.sleep(1)  # 等待1秒确保最终消息送达
                # 清理进度数据（可选，保留一段时间以便查看）
                # if task_id and task_id in kline_collect_progress:
                #     del kline_collect_progress[task_id]
                break
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("K线采集进度WebSocket客户端断开连接")
    except Exception as e:
        logger.error(f"K线采集进度WebSocket异常: {e}", exc_info=True)
        manager.disconnect(websocket)


@router.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):
    """实时行情WebSocket（优化版：使用差分数据，有新数据才推送，降低Redis读取频率）"""
    await manager.connect(websocket)
    
    # 记录上次推送的时间戳，用于判断是否有新数据
    last_a_timestamp: Optional[str] = None
    last_hk_timestamp: Optional[str] = None
    
    try:
        # 首次连接时立即推送一次全量数据
        a_stocks = get_json("market:a:spot") or []
        hk_stocks = get_json("market:hk:spot") or []
        
        # 获取时间戳（降低Redis读取频率：只在首次和检测到变化时读取）
        redis = get_redis()
        last_a_timestamp_bytes = redis.get("market:a:time")
        last_hk_timestamp_bytes = redis.get("market:hk:time")
        
        if last_a_timestamp_bytes:
            last_a_timestamp = last_a_timestamp_bytes.decode('utf-8')
        if last_hk_timestamp_bytes:
            last_hk_timestamp = last_hk_timestamp_bytes.decode('utf-8')
        
        initial_data = {
            "type": "market",
            "timestamp": asyncio.get_event_loop().time(),
            "data": {
                "a": a_stocks[:100],
                "hk": hk_stocks[:100]
            }
        }
        await manager.send_personal_message(initial_data, websocket)
        
        # 主循环：检查差分数据，有新数据才推送
        while True:
            # 降低Redis读取频率：每5秒检查一次（而不是每2-3秒）
            await asyncio.sleep(5)
            
            # 检查A股是否有新数据（使用差分数据）
            a_diff = get_json("market:a:spot_diff") or {}
            a_diff_timestamp = a_diff.get("timestamp")
            
            # 检查港股是否有新数据（使用差分数据，类似A股）
            hk_diff = get_json("market:hk:spot_diff") or {}
            hk_diff_timestamp = hk_diff.get("timestamp")
            
            has_new_data = False
            
            # A股：检查差分数据
            if a_diff_timestamp and a_diff_timestamp != last_a_timestamp:
                has_new_data = True
                last_a_timestamp = a_diff_timestamp
                # 获取最新全量数据
                a_stocks = get_json("market:a:spot") or []
            
            # 港股：检查差分数据
            if hk_diff_timestamp and hk_diff_timestamp != last_hk_timestamp:
                has_new_data = True
                last_hk_timestamp = hk_diff_timestamp
                # 获取最新全量数据
                hk_stocks = get_json("market:hk:spot") or []
            
            # 只有有新数据时才推送
            if has_new_data:
                data = {
                    "type": "market",
                    "timestamp": asyncio.get_event_loop().time(),
                    "data": {
                        "a": a_stocks[:100],
                        "hk": hk_stocks[:100]
                    }
                }
                await manager.send_personal_message(data, websocket)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("WebSocket客户端断开连接")
    except Exception as e:
        logger.error(f"WebSocket错误: {e}", exc_info=True)
        manager.disconnect(websocket)


@router.websocket("/ws/stock/{code}")
async def websocket_stock(websocket: WebSocket, code: str):
    """单个股票实时行情WebSocket（优化版：有新数据才推送，降低Redis读取频率）"""
    await manager.connect(websocket)
    
    # 记录上次推送的数据，用于判断是否有变化
    last_stock_data: Optional[dict] = None
    last_timestamp: Optional[str] = None
    
    try:
        # 首次连接时立即推送一次
        a_stocks = get_json("market:a:spot") or []
        hk_stocks = get_json("market:hk:spot") or []
        
        all_stocks = a_stocks + hk_stocks
        stock_data = next(
            (s for s in all_stocks if s.get("code") == code),
            None
        )
        
        if stock_data:
            last_stock_data = stock_data.copy()
            redis = get_redis()
            timestamp_bytes = redis.get("market:a:time") or redis.get("market:hk:time")
            if timestamp_bytes:
                last_timestamp = timestamp_bytes.decode('utf-8')
            
            data = {
                "type": "stock",
                "code": code,
                "timestamp": asyncio.get_event_loop().time(),
                "data": stock_data
            }
            await manager.send_personal_message(data, websocket)
        
        # 主循环：检查是否有新数据
        while True:
            # 降低Redis读取频率：每5秒检查一次
            await asyncio.sleep(5)
            
            # 检查是否有新数据（通过时间戳判断，减少Redis读取）
            redis = get_redis()
            current_a_timestamp_bytes = redis.get("market:a:time")
            current_hk_timestamp_bytes = redis.get("market:hk:time")
            
            current_timestamp = None
            if current_a_timestamp_bytes:
                current_timestamp = current_a_timestamp_bytes.decode('utf-8')
            elif current_hk_timestamp_bytes:
                current_timestamp = current_hk_timestamp_bytes.decode('utf-8')
            
            # 如果时间戳没变化，说明没有新数据，跳过
            if current_timestamp == last_timestamp:
                continue
            
            # 时间戳变化了，获取最新数据
            a_stocks = get_json("market:a:spot") or []
            hk_stocks = get_json("market:hk:spot") or []
            
            all_stocks = a_stocks + hk_stocks
            stock_data = next(
                (s for s in all_stocks if s.get("code") == code),
                None
            )
            
            # 检查股票数据是否有变化
            if stock_data:
                # 比较关键字段是否有变化
                if last_stock_data is None:
                    has_change = True
                else:
                    # 比较价格、涨跌幅等关键字段
                    key_fields = ["price", "pct", "change", "volume", "amount"]
                    has_change = any(
                        stock_data.get(field) != last_stock_data.get(field)
                        for field in key_fields
                    )
                
                if has_change:
                    last_stock_data = stock_data.copy()
                    last_timestamp = current_timestamp
                    
                    data = {
                        "type": "stock",
                        "code": code,
                        "timestamp": asyncio.get_event_loop().time(),
                        "data": stock_data
                    }
                    await manager.send_personal_message(data, websocket)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"股票WebSocket错误: {e}", exc_info=True)
        manager.disconnect(websocket)

