"""
SSE统一推送服务（Server-Sent Events）
替代所有WebSocket，使用单条SSE连接推送所有数据变化
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
import json
import asyncio
from common.redis import get_json, get_redis
from common.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()

# 全局变量：存储所有活跃的SSE连接
sse_connections: Dict[str, asyncio.Queue] = {}


async def create_sse_stream(
    request: Request,
    current_tab: Optional[str] = None,
    task_id: Optional[str] = None,
):
    """
    统一的SSE推送端点
    根据前端当前状态（current_tab）推送相应的数据
    """
    client_id = f"{request.client.host}_{id(request)}"
    logger.info(f"[SSE] 新连接: client_id={client_id}, current_tab={current_tab}, task_id={task_id}")
    
    # 创建消息队列
    message_queue = asyncio.Queue()
    sse_connections[client_id] = message_queue
    
    async def event_generator():
        try:
            # 首次连接时，根据current_tab推送初始数据
            if current_tab == 'market':
                # 推送市场行情数据
                a_stocks = get_json("market:a:spot") or []
                hk_stocks = get_json("market:hk:spot") or []
                yield f"data: {json.dumps({'type': 'market', 'data': {'a': a_stocks[:100], 'hk': hk_stocks[:100]}})}\n\n"
            
            elif current_tab == 'watchlist':
                # 推送自选股列表
                watchlist = get_json("watchlist:default") or []
                yield f"data: {json.dumps({'type': 'watchlist_sync', 'action': 'init', 'data': watchlist})}\n\n"
            
            # 主循环：只等待队列消息，不主动检查数据变化
            # 数据变化时由数据采集器通过broadcast_message自动推送
            while True:
                # 检查是否需要断开连接
                if await request.is_disconnected():
                    logger.info(f"[SSE] 客户端断开: {client_id}")
                    break
                
                try:
                    # 等待队列中的消息（无限等待，直到有消息）
                    message = await message_queue.get()
                    yield f"data: {json.dumps(message)}\n\n"
                    
                except Exception as e:
                    logger.error(f"[SSE] 推送错误: {e}", exc_info=True)
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            logger.info(f"[SSE] 连接被取消: {client_id}")
        finally:
            # 清理连接
            if client_id in sse_connections:
                del sse_connections[client_id]
            logger.info(f"[SSE] 连接清理完成: {client_id}")
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # 禁用nginx缓冲
        }
    )


@router.get("/sse/stream")
async def sse_stream_route(
    request: Request,
    current_tab: Optional[str] = Query(None, description="当前页面：market/watchlist/strategy/ai/news/config"),
    task_id: Optional[str] = Query(None, description="任务ID（用于进度追踪）"),
):
    """统一的SSE推送端点"""
    return await create_sse_stream(request, current_tab, task_id)


def broadcast_message(message: Dict[str, Any], filter_type: Optional[str] = None):
    """
    广播消息给所有SSE连接
    filter_type: 如果指定，只推送给需要该类型数据的连接（目前不使用，所有连接都接收所有消息）
    """
    if not sse_connections:
        return
    
    disconnected = []
    success_count = 0
    
    for client_id, queue in sse_connections.items():
        try:
            # 注意：目前所有连接都接收所有类型的消息（自选股变化对所有页面都重要）
            # 如果未来需要过滤，可以在这里添加逻辑
            
            # 非阻塞方式添加消息
            if queue.qsize() < 100:  # 限制队列大小，避免内存溢出
                queue.put_nowait(message)
                success_count += 1
            else:
                logger.warning(f"[SSE] 队列已满，跳过消息: {client_id}")
        except Exception as e:
            logger.error(f"[SSE] 广播消息失败: {client_id}, {e}")
            disconnected.append(client_id)
    
    # 清理断开的连接
    for client_id in disconnected:
        if client_id in sse_connections:
            del sse_connections[client_id]
    
    if sse_connections:
        logger.info(f"[SSE] 广播消息完成: type={message.get('type')}, 成功={success_count}, 连接数={len(sse_connections)}")


def broadcast_watchlist_update(watchlist: list):
    """广播自选股更新（所有连接都需要接收）"""
    broadcast_message({
        "type": "watchlist_sync",
        "action": "update",
        "data": watchlist
    })


def broadcast_market_update(market_type: str = "both"):
    """
    广播市场行情更新
    market_type: "a" (A股), "hk" (港股), "both" (两者)
    """
    data = {}
    
    if market_type in ["a", "both"]:
        a_stocks = get_json("market:a:spot") or []
        data["a"] = a_stocks[:100]  # 只推送前100只，避免数据过大
    
    if market_type in ["hk", "both"]:
        hk_stocks = get_json("market:hk:spot") or []
        data["hk"] = hk_stocks[:100]  # 只推送前100只，避免数据过大
    
    if data:
        broadcast_message({
            "type": "market",
            "data": data
        })
