"""
SSE统一推送服务（Server-Sent Events）
替代所有WebSocket，使用单条SSE连接推送所有数据变化
"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import StreamingResponse
from typing import Optional, Dict, Any
import json
import asyncio
import time
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
                a_stocks_limited = a_stocks[:100]
                hk_stocks_limited = hk_stocks[:100]
                market_data = {'type': 'market', 'data': {'a': a_stocks_limited, 'hk': hk_stocks_limited}}
                market_json = json.dumps(market_data)
                logger.info(f"[SSE推送] [{client_id}] 推送初始市场行情数据: A股={len(a_stocks_limited)}只, 港股={len(hk_stocks_limited)}只, 数据大小={len(market_json)}字节")
                if a_stocks_limited:
                    logger.debug(f"[SSE推送] [{client_id}] A股示例: {[s.get('code', 'N/A') + ':' + str(s.get('price', 'N/A')) for s in a_stocks_limited[:3]]}")
                if hk_stocks_limited:
                    logger.debug(f"[SSE推送] [{client_id}] 港股示例: {[s.get('code', 'N/A') + ':' + str(s.get('price', 'N/A')) for s in hk_stocks_limited[:3]]}")
                yield f"data: {market_json}\n\n"
            
            elif current_tab == 'watchlist':
                # 推送自选股列表
                watchlist = get_json("watchlist:default") or []
                watchlist_data = {'type': 'watchlist_sync', 'action': 'init', 'data': watchlist}
                watchlist_json = json.dumps(watchlist_data)
                logger.info(f"[SSE推送] [{client_id}] 推送初始自选股列表: 数量={len(watchlist)}只, 数据大小={len(watchlist_json)}字节")
                if watchlist:
                    codes = [s.get('code', 'N/A') for s in watchlist]
                    logger.debug(f"[SSE推送] [{client_id}] 自选股代码: {codes}")
                yield f"data: {watchlist_json}\n\n"
            
            # 无论哪个tab，都推送市场状态（首次连接时）
            try:
                from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time
                is_a_trading = is_a_stock_trading_time()
                is_hk_trading = is_hk_stock_trading_time()
                status_data = {
                    'type': 'market_status',
                    'data': {
                        'a': {'is_trading': is_a_trading, 'status': '交易中' if is_a_trading else '已收盘'},
                        'hk': {'is_trading': is_hk_trading, 'status': '交易中' if is_hk_trading else '已收盘'}
                    }
                }
                status_json = json.dumps(status_data)
                logger.info(f"[SSE推送] [{client_id}] 推送初始市场状态: A股={'交易中' if is_a_trading else '已收盘'}, 港股={'交易中' if is_hk_trading else '已收盘'}, 数据大小={len(status_json)}字节")
                yield f"data: {status_json}\n\n"
            except Exception as e:
                logger.error(f"[SSE推送] [{client_id}] 推送初始市场状态失败: {e}", exc_info=True)
            
            # 主循环：只等待队列消息，不主动检查数据变化
            # 数据变化时由数据采集器通过broadcast_message自动推送
            heartbeat_interval = 30  # 心跳间隔（秒）
            
            while True:
                # 检查是否需要断开连接
                if await request.is_disconnected():
                    logger.info(f"[SSE] 客户端断开: {client_id}")
                    break
                
                try:
                    # 等待队列中的消息（最多等待心跳间隔时间）
                    try:
                        message = await asyncio.wait_for(message_queue.get(), timeout=heartbeat_interval)
                        message_json = json.dumps(message)
                        message_type = message.get('type', 'unknown')
                        message_size = len(message_json)
                        
                        # 根据消息类型记录不同的详细信息
                        if message_type == 'market':
                            data = message.get('data', {})
                            a_count = len(data.get('a', []))
                            hk_count = len(data.get('hk', []))
                            logger.info(f"[SSE推送] [{client_id}] 推送市场行情更新: A股={a_count}只, 港股={hk_count}只, 数据大小={message_size}字节")
                            if a_count > 0:
                                a_samples = [s.get('code', 'N/A') for s in data.get('a', [])[:3]]
                                logger.debug(f"[SSE推送] [{client_id}] A股示例: {a_samples}")
                            if hk_count > 0:
                                hk_samples = [s.get('code', 'N/A') for s in data.get('hk', [])[:3]]
                                logger.debug(f"[SSE推送] [{client_id}] 港股示例: {hk_samples}")
                        elif message_type == 'watchlist_sync':
                            action = message.get('action', 'unknown')
                            watchlist_data = message.get('data', [])
                            watchlist_count = len(watchlist_data) if isinstance(watchlist_data, list) else 0
                            logger.info(f"[SSE推送] [{client_id}] 推送自选股同步: action={action}, 数量={watchlist_count}只, 数据大小={message_size}字节")
                            if watchlist_count > 0:
                                codes = [s.get('code', 'N/A') for s in watchlist_data[:10]]
                                logger.debug(f"[SSE推送] [{client_id}] 自选股代码: {codes}")
                        elif message_type == 'market_status':
                            status_data = message.get('data', {})
                            a_status = status_data.get('a', {}).get('status', 'unknown')
                            hk_status = status_data.get('hk', {}).get('status', 'unknown')
                            logger.info(f"[SSE推送] [{client_id}] 推送市场状态更新: A股={a_status}, 港股={hk_status}, 数据大小={message_size}字节")
                        else:
                            logger.info(f"[SSE推送] [{client_id}] 推送消息: type={message_type}, 数据大小={message_size}字节")
                            logger.debug(f"[SSE推送] [{client_id}] 消息内容: {message}")
                        
                        yield f"data: {message_json}\n\n"
                    except asyncio.TimeoutError:
                        # 超时，发送心跳
                        logger.debug(f"[SSE推送] [{client_id}] 发送心跳消息")
                        yield f": heartbeat\n\n"  # SSE心跳消息（以:开头）
                    
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
    message_type = message.get('type', 'unknown')
    message_json = json.dumps(message)
    message_size = len(message_json)
    
    logger.info(f"[SSE广播] 开始广播消息: type={message_type}, 数据大小={message_size}字节, 当前连接数={len(sse_connections)}")
    
    if not sse_connections:
        logger.warning(f"[SSE广播] 没有活跃连接，消息未发送: type={message_type}")
        return
    
    disconnected = []
    success_count = 0
    skipped_count = 0
    
    for client_id, queue in sse_connections.items():
        try:
            # 注意：目前所有连接都接收所有类型的消息（自选股变化对所有页面都重要）
            # 如果未来需要过滤，可以在这里添加逻辑
            
            # 非阻塞方式添加消息
            queue_size = queue.qsize()
            if queue_size < 100:  # 限制队列大小，避免内存溢出
                queue.put_nowait(message)
                success_count += 1
                logger.debug(f"[SSE广播] [{client_id}] 消息已加入队列 (队列大小: {queue_size + 1})")
            else:
                skipped_count += 1
                logger.warning(f"[SSE广播] [{client_id}] 队列已满 (队列大小: {queue_size}), 跳过消息")
        except Exception as e:
            logger.error(f"[SSE广播] [{client_id}] 广播消息失败: {e}", exc_info=True)
            disconnected.append(client_id)
    
    # 清理断开的连接
    for client_id in disconnected:
        if client_id in sse_connections:
            del sse_connections[client_id]
            logger.info(f"[SSE广播] 清理断开连接: {client_id}")
    
    if sse_connections:
        logger.info(f"[SSE广播] 广播完成: type={message_type}, 成功={success_count}, 跳过={skipped_count}, 断开={len(disconnected)}, 剩余连接数={len(sse_connections)}")
    else:
        logger.warning(f"[SSE广播] 所有连接已断开，消息未发送: type={message_type}")


def broadcast_watchlist_update(watchlist: list):
    """广播自选股更新（所有连接都需要接收）"""
    watchlist_count = len(watchlist) if isinstance(watchlist, list) else 0
    codes = [s.get('code', 'N/A') for s in watchlist[:10]] if watchlist else []
    logger.info(f"[SSE广播] 准备广播自选股更新: 数量={watchlist_count}只, 代码={codes}")
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
        a_count = len(data.get('a', []))
        hk_count = len(data.get('hk', []))
        logger.info(f"[SSE广播] 准备广播市场行情更新: market_type={market_type}, A股={a_count}只, 港股={hk_count}只")
        if a_count > 0:
            a_samples = [s.get('code', 'N/A') + ':' + str(s.get('price', 'N/A')) for s in data.get('a', [])[:3]]
            logger.debug(f"[SSE广播] A股示例: {a_samples}")
        if hk_count > 0:
            hk_samples = [s.get('code', 'N/A') + ':' + str(s.get('price', 'N/A')) for s in data.get('hk', [])[:3]]
            logger.debug(f"[SSE广播] 港股示例: {hk_samples}")
        broadcast_message({
            "type": "market",
            "data": data
        })


def broadcast_market_status_update():
    """广播市场状态更新（A股和港股交易状态）"""
    try:
        from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time
        
        is_a_trading = is_a_stock_trading_time()
        is_hk_trading = is_hk_stock_trading_time()
        
        a_status = "交易中" if is_a_trading else "已收盘"
        hk_status = "交易中" if is_hk_trading else "已收盘"
        logger.info(f"[SSE广播] 准备广播市场状态更新: A股={a_status}, 港股={hk_status}")
        broadcast_message({
            "type": "market_status",
            "data": {
                "a": {
                    "is_trading": is_a_trading,
                    "status": a_status
                },
                "hk": {
                    "is_trading": is_hk_trading,
                    "status": hk_status
                }
            }
        })
    except Exception as e:
        logger.error(f"[SSE] 广播市场状态更新失败: {e}", exc_info=True)
