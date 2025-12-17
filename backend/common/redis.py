"""
Redis连接模块
"""
import redis
import json
from typing import Optional, Any
from common.config import settings
from common.logger import get_logger

logger = get_logger(__name__)

# 全局Redis连接池
_pool: Optional[redis.ConnectionPool] = None
_r: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    """获取Redis连接"""
    global _pool, _r
    
    if _r is None:
        _pool = redis.ConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=True,
            max_connections=50
        )
        _r = redis.Redis(connection_pool=_pool)
        
        # 测试连接
        try:
            _r.ping()
            logger.info(f"Redis连接成功: {settings.redis_host}:{settings.redis_port}")
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            raise
    
    return _r


def set_json(key: str, value: Any, ex: Optional[int] = None) -> bool:
    """存储JSON数据"""
    try:
        r = get_redis()
        json_str = json.dumps(value, ensure_ascii=False, default=str)
        return r.set(key, json_str, ex=ex)
    except Exception as e:
        logger.error(f"Redis存储失败 {key}: {e}")
        return False


def get_json(key: str) -> Optional[Any]:
    """获取JSON数据"""
    try:
        r = get_redis()
        value = r.get(key)
        if value:
            return json.loads(value)
        return None
    except Exception as e:
        logger.error(f"Redis获取失败 {key}: {e}")
        return None


def delete(key: str) -> bool:
    """删除key"""
    try:
        r = get_redis()
        return bool(r.delete(key))
    except Exception as e:
        logger.error(f"Redis删除失败 {key}: {e}")
        return False

