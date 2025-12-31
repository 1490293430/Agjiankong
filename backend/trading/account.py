"""
模拟交易账户管理
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from common.redis import set_json, get_json
from common.logger import get_logger
import json

logger = get_logger(__name__)


class Account:
    """模拟交易账户"""
    
    def __init__(self, account_id: str = "default", initial_cash: float = 10_000):
        """
        Args:
            account_id: 账户ID
            initial_cash: 初始资金（默认1万）
        """
        self.account_id = account_id
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, int] = {}  # code -> shares
        self.trades: List[Dict[str, Any]] = []
        self._load()
    
    def _load(self):
        """从Redis加载账户数据"""
        try:
            account_data = get_json(f"account:{self.account_id}")
            if account_data:
                self.cash = account_data.get("cash", self.initial_cash)
                self.positions = account_data.get("positions", {})
                self.trades = account_data.get("trades", [])
        except Exception as e:
            logger.error(f"加载账户数据失败: {e}")
    
    def _save(self):
        """保存账户数据到Redis"""
        try:
            account_data = {
                "account_id": self.account_id,
                "cash": self.cash,
                "positions": self.positions,
                "trades": self.trades[-100:],  # 只保留最近100笔交易
                "update_time": datetime.now().isoformat()
            }
            # 账户数据保留30天，如果30天内无操作则自动过期
            set_json(f"account:{self.account_id}", account_data, ex=30 * 24 * 3600)
        except Exception as e:
            logger.error(f"保存账户数据失败: {e}")
    
    def buy(self, code: str, price: float, qty: int) -> Dict[str, Any]:
        """买入股票
        
        Args:
            code: 股票代码
            price: 买入价格
            qty: 买入数量（手，1手=100股）
        
        Returns:
            交易结果
        """
        try:
            shares = qty * 100  # 转换为股数
            cost = price * shares
            
            if self.cash < cost:
                return {
                    "success": False,
                    "message": f"资金不足，当前资金：{self.cash:.2f}，需要：{cost:.2f}"
                }
            
            # 扣除资金
            self.cash -= cost
            
            # 更新持仓
            self.positions[code] = self.positions.get(code, 0) + shares
            
            # 记录交易
            trade = {
                "type": "BUY",
                "code": code,
                "price": price,
                "qty": qty,
                "shares": shares,
                "amount": cost,
                "time": datetime.now().isoformat()
            }
            self.trades.append(trade)
            
            # 保存
            self._save()
            
            logger.info(f"买入成功：{code}，价格：{price}，数量：{qty}手")
            
            return {
                "success": True,
                "message": "买入成功",
                "trade": trade,
                "account": self.get_info()
            }
            
        except Exception as e:
            logger.error(f"买入失败: {e}", exc_info=True)
            return {
                "success": False,
                "message": str(e)
            }
    
    def sell(self, code: str, price: float, qty: int) -> Dict[str, Any]:
        """卖出股票
        
        Args:
            code: 股票代码
            price: 卖出价格
            qty: 卖出数量（手）
        
        Returns:
            交易结果
        """
        try:
            shares = qty * 100
            current_shares = self.positions.get(code, 0)
            
            if current_shares < shares:
                return {
                    "success": False,
                    "message": f"持仓不足，当前持仓：{current_shares}股，需要：{shares}股"
                }
            
            # 计算收入
            income = price * shares
            
            # 增加资金
            self.cash += income
            
            # 更新持仓
            self.positions[code] = current_shares - shares
            if self.positions[code] == 0:
                del self.positions[code]
            
            # 记录交易
            trade = {
                "type": "SELL",
                "code": code,
                "price": price,
                "qty": qty,
                "shares": shares,
                "amount": income,
                "time": datetime.now().isoformat()
            }
            self.trades.append(trade)
            
            # 保存
            self._save()
            
            logger.info(f"卖出成功：{code}，价格：{price}，数量：{qty}手")
            
            return {
                "success": True,
                "message": "卖出成功",
                "trade": trade,
                "account": self.get_info()
            }
            
        except Exception as e:
            logger.error(f"卖出失败: {e}", exc_info=True)
            return {
                "success": False,
                "message": str(e)
            }
    
    def get_info(self) -> Dict[str, Any]:
        """获取账户信息"""
        return {
            "account_id": self.account_id,
            "initial_cash": self.initial_cash,
            "cash": self.cash,
            "positions": self.positions,
            "total_trades": len(self.trades),
            "position_count": len(self.positions)
        }
    
    def get_positions_detail(self, market_prices: Dict[str, float]) -> List[Dict[str, Any]]:
        """获取持仓详情（包含市值）"""
        positions_detail = []
        
        for code, shares in self.positions.items():
            current_price = market_prices.get(code, 0)
            market_value = current_price * shares
            
            positions_detail.append({
                "code": code,
                "shares": shares,
                "qty": shares // 100,  # 转换为手
                "current_price": current_price,
                "market_value": market_value
            })
        
        return positions_detail
    
    def get_total_asset(self, market_prices: Dict[str, float]) -> float:
        """计算总资产（现金+持仓市值）"""
        total = self.cash
        
        for code, shares in self.positions.items():
            current_price = market_prices.get(code, 0)
            total += current_price * shares
        
        return total
    
    def reset(self):
        """重置账户"""
        self.cash = self.initial_cash
        self.positions = {}
        self.trades = []
        self._save()
        logger.info(f"账户已重置：{self.account_id}")


# 全局账户实例
_accounts: Dict[str, Account] = {}


def get_account(account_id: str = "default") -> Account:
    """获取账户实例（单例）"""
    if account_id not in _accounts:
        _accounts[account_id] = Account(account_id)
    return _accounts[account_id]

