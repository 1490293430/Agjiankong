"""
WebSocket实时行情推送
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from typing import List
import json
import asyncio
from common.redis import get_json
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


@router.websocket("/ws/market")
async def websocket_market(websocket: WebSocket):
    """实时行情WebSocket"""
    await manager.connect(websocket)
    
    try:
        while True:
            # 获取最新行情
            a_stocks = get_json("market:a:spot") or []
            hk_stocks = get_json("market:hk:spot") or []
            
            # 只推送前100只（减少数据量）
            data = {
                "type": "market",
                "timestamp": asyncio.get_event_loop().time(),
                "data": {
                    "a": a_stocks[:100],
                    "hk": hk_stocks[:100]
                }
            }
            
            await manager.send_personal_message(data, websocket)
            await asyncio.sleep(2)  # 每2秒推送一次
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("WebSocket客户端断开连接")
    except Exception as e:
        logger.error(f"WebSocket错误: {e}", exc_info=True)
        manager.disconnect(websocket)


@router.websocket("/ws/stock/{code}")
async def websocket_stock(websocket: WebSocket, code: str):
    """单个股票实时行情WebSocket"""
    await manager.connect(websocket)
    
    try:
        while True:
            # 获取该股票的实时行情
            a_stocks = get_json("market:a:spot") or []
            hk_stocks = get_json("market:hk:spot") or []
            
            all_stocks = a_stocks + hk_stocks
            stock_data = next(
                (s for s in all_stocks if s.get("code") == code),
                None
            )
            
            if stock_data:
                data = {
                    "type": "stock",
                    "code": code,
                    "timestamp": asyncio.get_event_loop().time(),
                    "data": stock_data
                }
                await manager.send_personal_message(data, websocket)
            
            await asyncio.sleep(1)  # 每1秒推送一次
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"股票WebSocket错误: {e}", exc_info=True)
        manager.disconnect(websocket)

