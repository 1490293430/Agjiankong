"""
模拟交易引擎
"""
from typing import Dict, Any, Optional
from common.logger import get_logger
from trading.account import get_account

logger = get_logger(__name__)


def execute_order(
    account_id: str,
    action: str,  # "BUY" or "SELL"
    code: str,
    price: float,
    qty: int
) -> Dict[str, Any]:
    """执行订单
    
    Args:
        account_id: 账户ID
        action: 操作类型（BUY/SELL）
        code: 股票代码
        price: 价格
        qty: 数量（手）
    
    Returns:
        执行结果
    """
    try:
        account = get_account(account_id)
        
        if action.upper() == "BUY":
            return account.buy(code, price, qty)
        elif action.upper() == "SELL":
            return account.sell(code, price, qty)
        else:
            return {
                "success": False,
                "message": f"不支持的操作类型：{action}"
            }
            
    except Exception as e:
        logger.error(f"执行订单失败: {e}", exc_info=True)
        return {
            "success": False,
            "message": str(e)
        }


def get_account_info(account_id: str = "default") -> Dict[str, Any]:
    """获取账户信息"""
    try:
        account = get_account(account_id)
        return {
            "success": True,
            "data": account.get_info()
        }
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}", exc_info=True)
        return {
            "success": False,
            "message": str(e)
        }


def get_positions(
    account_id: str = "default",
    market_prices: Optional[Dict[str, float]] = None
) -> Dict[str, Any]:
    """获取持仓信息"""
    try:
        account = get_account(account_id)
        
        if market_prices is None:
            market_prices = {}
        
        positions = account.get_positions_detail(market_prices)
        total_asset = account.get_total_asset(market_prices)
        
        return {
            "success": True,
            "data": {
                "positions": positions,
                "cash": account.cash,
                "total_asset": total_asset,
                "profit": total_asset - account.initial_cash,
                "profit_rate": (total_asset - account.initial_cash) / account.initial_cash * 100
            }
        }
    except Exception as e:
        logger.error(f"获取持仓信息失败: {e}", exc_info=True)
        return {
            "success": False,
            "message": str(e)
        }

