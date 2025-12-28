"""
动态参数优化器 - 根据市场状况自动调整过滤条件阈值
"""
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from common.logger import get_logger

logger = get_logger(__name__)


class DynamicParameterOptimizer:
    """根据市场状况动态调整过滤条件阈值"""
    
    def __init__(self):
        self.market_status = "normal"  # normal, volatile, trending
        self.parameter_settings = {
            "normal": {
                "trend_threshold": 0.001,  # 0.1%
                "min_conditions": 3,  # 至少满足3个入场条件
                "min_risk_reward": 1.5,  # 最小风险回报比
                "vol_ratio_threshold": 1.5,  # 成交量比阈值
                "rsi_upper_limit": 80,  # RSI超买上限
            },
            "volatile": {
                "trend_threshold": 0.002,  # 0.2%，提高阈值减少假信号
                "min_conditions": 4,  # 要求更严格，至少4个条件
                "min_risk_reward": 2.0,  # 提高风险回报比要求
                "vol_ratio_threshold": 2.0,  # 提高成交量要求
                "rsi_upper_limit": 75,  # 降低RSI上限，更保守
            },
            "trending": {
                "trend_threshold": 0.0005,  # 0.05%，降低阈值抓住趋势
                "min_conditions": 2,  # 放宽条件，至少2个条件
                "min_risk_reward": 1.2,  # 降低风险回报比要求
                "vol_ratio_threshold": 1.2,  # 降低成交量要求
                "rsi_upper_limit": 85,  # 提高RSI上限，更激进
            }
        }
        self.market_history: list = []  # 市场状态历史记录
    
    def detect_market_status(self, indicators: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> str:
        """检测当前市场状态
        
        Args:
            indicators: 技术指标字典
            df: 价格数据DataFrame（可选，用于计算波动率）
        
        Returns:
            "normal", "volatile", 或 "trending"
        """
        scores = {
            "volatile": 0,
            "trending": 0,
            "normal": 0
        }
        
        # 1. 波动率检测（如果有价格数据）
        if df is not None and len(df) >= 20:
            # 计算20日波动率（标准差）
            returns = df["close"].pct_change().dropna()
            if len(returns) >= 20:
                volatility = returns.tail(20).std()
                avg_volatility = returns.std()  # 历史平均波动率
                
                if volatility > avg_volatility * 1.5:
                    scores["volatile"] += 3
                elif volatility < avg_volatility * 0.7:
                    scores["trending"] += 2
                else:
                    scores["normal"] += 1
        
        # 2. 趋势强度检测（使用当前值和前值比较来判断趋势方向）
        ma5 = indicators.get("ma5")
        ma5_prev = indicators.get("ma5_prev")
        ma20 = indicators.get("ma20")
        ma20_prev = indicators.get("ma20_prev")
        ma60 = indicators.get("ma60")
        ma60_prev = indicators.get("ma60_prev")
        
        # 检查均线是否一致向上（当前值 > 前值）
        up_trend_count = 0
        if ma5 and ma5_prev and ma5 > ma5_prev:
            up_trend_count += 1
        if ma20 and ma20_prev and ma20 > ma20_prev:
            up_trend_count += 1
        if ma60 and ma60_prev and ma60 > ma60_prev:
            up_trend_count += 1
        
        if up_trend_count >= 2:
            scores["trending"] += 2
        elif up_trend_count == 0:
            scores["volatile"] += 1
        
        # 3. RSI检测（超买超卖）
        rsi = indicators.get("rsi")
        if rsi:
            if rsi > 70:
                scores["volatile"] += 1  # 超买区域，波动可能加大
            elif 30 < rsi < 70:
                scores["normal"] += 1
            elif rsi < 30:
                scores["trending"] += 1  # 超卖区域，可能形成趋势
        
        # 4. 成交量检测
        vol_ratio = indicators.get("vol_ratio")
        if vol_ratio:
            if vol_ratio > 2.0:
                scores["volatile"] += 1  # 异常放量
            elif vol_ratio > 1.5:
                scores["trending"] += 1  # 放量启动
            else:
                scores["normal"] += 1
        
        # 5. MACD检测（使用当前值和前值比较来判断趋势方向）
        macd_dif = indicators.get("macd_dif")
        macd_dif_prev = indicators.get("macd_dif_prev")
        
        if macd_dif and macd_dif_prev and macd_dif > macd_dif_prev:
            if macd_dif > 0:
                scores["trending"] += 1
            else:
                scores["normal"] += 1
        
        # 选择得分最高的状态
        max_score = max(scores.values())
        if max_score == 0:
            return "normal"
        
        for status, score in scores.items():
            if score == max_score:
                detected_status = status
                break
        else:
            detected_status = "normal"
        
        # 记录市场状态历史
        self.market_history.append({
            "status": detected_status,
            "scores": scores,
            "timestamp": datetime.now().isoformat()
        })
        
        # 只保留最近100条记录
        if len(self.market_history) > 100:
            self.market_history = self.market_history[-100:]
        
        return detected_status
    
    def get_parameters(self, market_status: Optional[str] = None) -> Dict[str, Any]:
        """获取当前市场状态对应的参数
        
        Args:
            market_status: 市场状态（如果为None，使用当前状态）
        
        Returns:
            参数字典
        """
        status = market_status or self.market_status
        return self.parameter_settings.get(status, self.parameter_settings["normal"])
    
    def update_market_status(self, indicators: Dict[str, Any], df: Optional[pd.DataFrame] = None):
        """更新市场状态
        
        Args:
            indicators: 技术指标字典
            df: 价格数据DataFrame（可选）
        """
        new_status = self.detect_market_status(indicators, df)
        if new_status != self.market_status:
            logger.info(f"市场状态变化: {self.market_status} -> {new_status}")
            self.market_status = new_status
    
    def get_trend_threshold(self) -> float:
        """获取当前趋势判断阈值"""
        return self.get_parameters()["trend_threshold"]
    
    def get_min_conditions(self) -> int:
        """获取最小入场条件数"""
        return self.get_parameters()["min_conditions"]
    
    def get_min_risk_reward(self) -> float:
        """获取最小风险回报比"""
        return self.get_parameters()["min_risk_reward"]
    
    def get_vol_ratio_threshold(self) -> float:
        """获取成交量比阈值"""
        return self.get_parameters()["vol_ratio_threshold"]
    
    def get_rsi_upper_limit(self) -> float:
        """获取RSI超买上限"""
        return self.get_parameters()["rsi_upper_limit"]
    
    def get_status_history(self, limit: int = 10) -> list:
        """获取市场状态历史记录"""
        return self.market_history[-limit:] if self.market_history else []


# 全局优化器实例
_optimizer = DynamicParameterOptimizer()


def get_parameter_optimizer() -> DynamicParameterOptimizer:
    """获取全局参数优化器实例"""
    return _optimizer


def get_dynamic_parameters(indicators: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """获取动态参数（自动检测市场状态）
    
    Args:
        indicators: 技术指标字典
        df: 价格数据DataFrame（可选）
    
    Returns:
        动态参数字典
    """
    optimizer = get_parameter_optimizer()
    optimizer.update_market_status(indicators, df)
    return optimizer.get_parameters()

