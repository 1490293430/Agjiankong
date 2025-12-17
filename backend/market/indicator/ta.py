"""
技术指标计算模块
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional


def ma(df: pd.DataFrame, n: int = 20, column: str = "close") -> pd.Series:
    """计算移动平均线
    
    Args:
        df: 包含价格数据的DataFrame
        n: 周期
        column: 计算列名（默认close）
    
    Returns:
        移动平均线Series
    """
    return df[column].rolling(window=n, min_periods=1).mean()


def ema(df: pd.DataFrame, n: int = 12, column: str = "close") -> pd.Series:
    """计算指数移动平均线"""
    return df[column].ewm(span=n, adjust=False).mean()


def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
    """计算MACD指标
    
    Returns:
        {
            "dif": DIF线,
            "dea": DEA线,
            "macd": MACD柱
        }
    """
    ema_fast = ema(df, fast)
    ema_slow = ema(df, slow)
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_value = 2 * (dif - dea)
    
    return {
        "dif": dif,
        "dea": dea,
        "macd": macd_value
    }


def rsi(df: pd.DataFrame, period: int = 14, column: str = "close") -> pd.Series:
    """计算RSI指标"""
    delta = df[column].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    
    rs = avg_gain / (avg_loss + 1e-10)  # 避免除零
    rsi_value = 100 - (100 / (1 + rs))
    
    return rsi_value


def boll(df: pd.DataFrame, n: int = 20, std: float = 2, column: str = "close") -> Dict[str, pd.Series]:
    """计算布林带
    
    Returns:
        {
            "upper": 上轨,
            "middle": 中轨（MA）,
            "lower": 下轨
        }
    """
    middle = ma(df, n, column)
    std_val = df[column].rolling(window=n, min_periods=1).std()
    upper = middle + std * std_val
    lower = middle - std * std_val
    
    return {
        "upper": upper,
        "middle": middle,
        "lower": lower
    }


def kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> Dict[str, pd.Series]:
    """计算KDJ指标
    
    Returns:
        {
            "k": K值,
            "d": D值,
            "j": J值
        }
    """
    low_list = df["low"].rolling(window=n, min_periods=1).min()
    high_list = df["high"].rolling(window=n, min_periods=1).max()
    
    rsv = (df["close"] - low_list) / (high_list - low_list + 1e-10) * 100
    k = rsv.ewm(com=m1-1, adjust=False).mean()
    d = k.ewm(com=m2-1, adjust=False).mean()
    j = 3 * k - 2 * d
    
    return {
        "k": k,
        "d": d,
        "j": j
    }


def calculate_all_indicators(df: pd.DataFrame) -> Dict[str, Any]:
    """计算所有技术指标
    
    Args:
        df: 包含OHLCV数据的DataFrame
    
    Returns:
        包含所有指标的字典
    """
    if df.empty or len(df) < 2:
        return {}
    
    # 确保必要的列存在
    required_columns = ["open", "high", "low", "close", "volume"]
    for col in required_columns:
        if col not in df.columns:
            return {}
    
    # 转换为数值类型
    for col in required_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 取最后一行作为最新值
    latest = df.iloc[-1]
    
    result = {}
    
    # MA均线
    result["ma5"] = float(ma(df, 5).iloc[-1]) if len(df) >= 5 else None
    result["ma10"] = float(ma(df, 10).iloc[-1]) if len(df) >= 10 else None
    result["ma20"] = float(ma(df, 20).iloc[-1]) if len(df) >= 20 else None
    result["ma60"] = float(ma(df, 60).iloc[-1]) if len(df) >= 60 else None
    
    # MACD
    if len(df) >= 26:
        macd_data = macd(df)
        result["macd_dif"] = float(macd_data["dif"].iloc[-1])
        result["macd_dea"] = float(macd_data["dea"].iloc[-1])
        result["macd"] = float(macd_data["macd"].iloc[-1])
    
    # RSI
    if len(df) >= 14:
        rsi_value = rsi(df)
        result["rsi"] = float(rsi_value.iloc[-1])
    
    # 布林带
    if len(df) >= 20:
        boll_data = boll(df)
        result["boll_upper"] = float(boll_data["upper"].iloc[-1])
        result["boll_middle"] = float(boll_data["middle"].iloc[-1])
        result["boll_lower"] = float(boll_data["lower"].iloc[-1])
    
    # KDJ
    if len(df) >= 9:
        kdj_data = kdj(df)
        result["kdj_k"] = float(kdj_data["k"].iloc[-1])
        result["kdj_d"] = float(kdj_data["d"].iloc[-1])
        result["kdj_j"] = float(kdj_data["j"].iloc[-1])
    
    # 成交量相关
    if "volume" in df.columns and len(df) >= 5:
        avg_volume_5 = df["volume"].tail(5).mean()
        current_volume = latest["volume"]
        result["vol_ratio"] = float(current_volume / (avg_volume_5 + 1e-10))
    
    return result

