"""
技术指标计算模块
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List


def calculate_trend_direction(current_value: float, previous_value: float, threshold: Optional[float] = None) -> str:
    """更稳健的趋势判断，避免微小波动干扰
    
    Args:
        current_value: 当前值
        previous_value: 前一个值
        threshold: 变化阈值（如果为None，尝试从动态参数获取，默认0.1%）
    
    Returns:
        "up" - 向上趋势
        "down" - 向下趋势
        "flat" - 持平
    """
    if previous_value is None or previous_value == 0:
        return "flat"
    
    # 如果未提供阈值，尝试从动态参数获取
    if threshold is None:
        try:
            from ai.parameter_optimizer import get_parameter_optimizer
            optimizer = get_parameter_optimizer()
            threshold = optimizer.get_trend_threshold()
        except Exception:
            threshold = 0.001  # 默认0.1%
    
    change = (current_value - previous_value) / previous_value
    if change > threshold:
        return "up"
    elif change < -threshold:
        return "down"
    else:
        return "flat"


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


def williams_r(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算威廉指标 %R
    
    Args:
        df: 包含high、low、close的DataFrame
        period: 计算周期（默认14）
    
    Returns:
        威廉指标Series，值域为 [-100, 0]
        负值越大（接近-100），表示超卖
        负值越小（接近0），表示超买
    """
    high_high = df["high"].rolling(window=period, min_periods=1).max()
    low_low = df["low"].rolling(window=period, min_periods=1).min()
    
    wr = -100 * (high_high - df["close"]) / (high_high - low_low + 1e-10)
    return wr


def adx(df: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
    """计算ADX平均趋向指数
    
    Args:
        df: 包含high、low、close的DataFrame
        period: 计算周期（默认14）
    
    Returns:
        {
            "adx": ADX值,
            "plus_di": +DI值,
            "minus_di": -DI值
        }
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    # 计算 True Range
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # 计算 +DM 和 -DM
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    
    # 平滑 TR, +DM, -DM
    atr = tr.ewm(span=period, adjust=False).mean()
    plus_dm_smooth = plus_dm.ewm(span=period, adjust=False).mean()
    minus_dm_smooth = minus_dm.ewm(span=period, adjust=False).mean()
    
    # 计算 +DI 和 -DI
    plus_di = 100 * plus_dm_smooth / (atr + 1e-10)
    minus_di = 100 * minus_dm_smooth / (atr + 1e-10)
    
    # 计算 DX 和 ADX
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    adx_value = dx.ewm(span=period, adjust=False).mean()
    
    return {
        "adx": adx_value,
        "plus_di": plus_di,
        "minus_di": minus_di
    }


def cci(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """计算CCI顺势指标（Commodity Channel Index）
    
    CCI = (TP - MA(TP)) / (0.015 * MD)
    其中：TP = (High + Low + Close) / 3
         MD = Mean Deviation（平均绝对偏差）
    
    Args:
        df: 包含high、low、close的DataFrame
        period: 计算周期（默认20）
    
    Returns:
        CCI指标Series
        > +100: 超买区域，可能回调
        < -100: 超卖区域，可能反弹
        -100 ~ +100: 正常区域
    """
    tp = (df["high"] + df["low"] + df["close"]) / 3
    ma_tp = tp.rolling(window=period, min_periods=1).mean()
    
    # 计算平均绝对偏差
    md = tp.rolling(window=period, min_periods=1).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=True
    )
    
    cci_value = (tp - ma_tp) / (0.015 * md + 1e-10)
    return cci_value


def fibonacci_retracement(df: pd.DataFrame, lookback: int = 60) -> Dict[str, Any]:
    """计算斐波那契回撤位
    
    识别近期波段高低点，计算关键回撤位
    
    Args:
        df: 包含high、low、close的DataFrame
        lookback: 回溯周期（默认60根K线）
    
    Returns:
        {
            "swing_high": 波段高点,
            "swing_low": 波段低点,
            "fib_0": 0%回撤位（波段低点）,
            "fib_236": 23.6%回撤位,
            "fib_382": 38.2%回撤位,
            "fib_500": 50%回撤位,
            "fib_618": 61.8%回撤位,
            "fib_786": 78.6%回撤位,
            "fib_100": 100%回撤位（波段高点）,
            "trend": "up"（上涨趋势）或 "down"（下跌趋势）,
            "current_fib_level": 当前价格所处的斐波那契区间
        }
    """
    if len(df) < lookback:
        lookback = len(df)
    
    recent_df = df.tail(lookback)
    
    # 找到波段高点和低点的位置
    high_idx = recent_df["high"].idxmax()
    low_idx = recent_df["low"].idxmin()
    
    swing_high = float(recent_df["high"].max())
    swing_low = float(recent_df["low"].min())
    
    # 判断趋势方向：高点在低点之后为上涨趋势，反之为下跌趋势
    if high_idx > low_idx:
        trend = "up"
        # 上涨趋势：从低点到高点，回撤位从高点往下算
        fib_range = swing_high - swing_low
        fib_0 = swing_low  # 0%（起点）
        fib_236 = swing_high - fib_range * 0.236
        fib_382 = swing_high - fib_range * 0.382
        fib_500 = swing_high - fib_range * 0.500
        fib_618 = swing_high - fib_range * 0.618
        fib_786 = swing_high - fib_range * 0.786
        fib_100 = swing_high  # 100%（终点）
    else:
        trend = "down"
        # 下跌趋势：从高点到低点，回撤位从低点往上算
        fib_range = swing_high - swing_low
        fib_0 = swing_high  # 0%（起点）
        fib_236 = swing_low + fib_range * 0.236
        fib_382 = swing_low + fib_range * 0.382
        fib_500 = swing_low + fib_range * 0.500
        fib_618 = swing_low + fib_range * 0.618
        fib_786 = swing_low + fib_range * 0.786
        fib_100 = swing_low  # 100%（终点）
    
    # 判断当前价格所处的斐波那契区间
    current_price = float(df["close"].iloc[-1])
    
    if trend == "up":
        # 上涨趋势中，价格从高点回撤
        if current_price >= fib_236:
            current_level = "0-23.6%"  # 强势，几乎没回撤
        elif current_price >= fib_382:
            current_level = "23.6-38.2%"  # 浅回撤
        elif current_price >= fib_500:
            current_level = "38.2-50%"  # 正常回撤
        elif current_price >= fib_618:
            current_level = "50-61.8%"  # 深度回撤
        elif current_price >= fib_786:
            current_level = "61.8-78.6%"  # 深度回撤
        else:
            current_level = "78.6-100%"  # 接近完全回撤
    else:
        # 下跌趋势中，价格从低点反弹
        if current_price <= fib_236:
            current_level = "0-23.6%"  # 弱反弹
        elif current_price <= fib_382:
            current_level = "23.6-38.2%"  # 浅反弹
        elif current_price <= fib_500:
            current_level = "38.2-50%"  # 正常反弹
        elif current_price <= fib_618:
            current_level = "50-61.8%"  # 强反弹
        elif current_price <= fib_786:
            current_level = "61.8-78.6%"  # 强反弹
        else:
            current_level = "78.6-100%"  # 接近完全反弹
    
    return {
        "swing_high": swing_high,
        "swing_low": swing_low,
        "fib_0": float(fib_0),
        "fib_236": float(fib_236),
        "fib_382": float(fib_382),
        "fib_500": float(fib_500),
        "fib_618": float(fib_618),
        "fib_786": float(fib_786),
        "fib_100": float(fib_100),
        "trend": trend,
        "current_fib_level": current_level
    }


def ichimoku(df: pd.DataFrame, tenkan: int = 9, kijun: int = 26, senkou_b: int = 52) -> Dict[str, pd.Series]:
    """计算一目均衡表（Ichimoku Cloud）
    
    Args:
        df: 包含high、low、close的DataFrame
        tenkan: 转换线周期（默认9）
        kijun: 基准线周期（默认26）
        senkou_b: 先行带B周期（默认52）
    
    Returns:
        {
            "tenkan_sen": 转换线,
            "kijun_sen": 基准线,
            "senkou_span_a": 先行带A,
            "senkou_span_b": 先行带B,
            "chikou_span": 迟行带
        }
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    # 转换线 = (9日最高 + 9日最低) / 2
    tenkan_high = high.rolling(window=tenkan, min_periods=1).max()
    tenkan_low = low.rolling(window=tenkan, min_periods=1).min()
    tenkan_sen = (tenkan_high + tenkan_low) / 2
    
    # 基准线 = (26日最高 + 26日最低) / 2
    kijun_high = high.rolling(window=kijun, min_periods=1).max()
    kijun_low = low.rolling(window=kijun, min_periods=1).min()
    kijun_sen = (kijun_high + kijun_low) / 2
    
    # 先行带A = (转换线 + 基准线) / 2，向前移动26期
    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(kijun)
    
    # 先行带B = (52日最高 + 52日最低) / 2，向前移动26期
    senkou_b_high = high.rolling(window=senkou_b, min_periods=1).max()
    senkou_b_low = low.rolling(window=senkou_b, min_periods=1).min()
    senkou_span_b = ((senkou_b_high + senkou_b_low) / 2).shift(kijun)
    
    # 迟行带 = 收盘价向后移动26期
    chikou_span = close.shift(-kijun)
    
    return {
        "tenkan_sen": tenkan_sen,
        "kijun_sen": kijun_sen,
        "senkou_span_a": senkou_span_a,
        "senkou_span_b": senkou_span_b,
        "chikou_span": chikou_span
    }


def calculate_ma60_only(df: pd.DataFrame) -> Dict[str, Any] | None:
    """仅计算MA60（用于低成本的第一层筛选）
    
    Args:
        df: 包含OHLCV数据的DataFrame
    
    Returns:
        包含ma60和ma60_trend的字典，如果数据不足返回None
    """
    if df.empty or len(df) < 2:
        return None
    
    required_columns = ["close"]
    if "close" not in df.columns:
        return None
    
    df["close"] = pd.to_numeric(df["close"], errors='coerce')
    latest = df.iloc[-1]
    current_price = float(latest["close"])
    
    # 只计算MA60
    ma60_series = ma(df, 60, "close")
    
    if len(df) < 60:
        return None
    
    result = {
        "ma60": float(ma60_series.iloc[-1]),
        "current_price": current_price,
        "current_close": current_price
    }
    
    # 计算MA60趋势（需要至少61根K线）
    if len(df) >= 61:
        ma60_prev = float(ma60_series.iloc[-2])
        trend = calculate_trend_direction(result["ma60"], ma60_prev)
        result["ma60_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
    else:
        result["ma60_trend"] = "未知"
    
    return result


def calculate_multi_timeframe_indicators(
    daily_df: pd.DataFrame,
    hourly_df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """计算多周期指标（日线 + 小时线）
    
    多周期分析原则：
    - 日线确定大趋势方向
    - 小时线寻找精确入场点
    
    Args:
        daily_df: 日线数据DataFrame
        hourly_df: 小时线数据DataFrame（可选）
    
    Returns:
        包含多周期指标的字典
    """
    result = {}
    
    # 1. 计算日线指标（大周期趋势）
    if daily_df is not None and len(daily_df) >= 20:
        daily_indicators = calculate_all_indicators(daily_df)
        # 添加日线前缀
        for key, value in daily_indicators.items():
            result[f"daily_{key}"] = value
        
        # 日线趋势判断
        result["daily_trend_direction"] = "未知"
        ma60 = daily_indicators.get("ma60")
        ma60_trend = daily_indicators.get("ma60_trend")
        current_close = daily_indicators.get("current_close")
        
        if ma60 and current_close:
            if current_close > ma60 and ma60_trend == "向上":
                result["daily_trend_direction"] = "多头"
            elif current_close < ma60 and ma60_trend == "向下":
                result["daily_trend_direction"] = "空头"
            else:
                result["daily_trend_direction"] = "震荡"
    
    # 2. 计算小时线指标（小周期入场）
    if hourly_df is not None and len(hourly_df) >= 20:
        hourly_indicators = calculate_all_indicators(hourly_df)
        # 添加小时线前缀
        for key, value in hourly_indicators.items():
            result[f"hourly_{key}"] = value
        
        # 小时线趋势判断
        result["hourly_trend_direction"] = "未知"
        ma20 = hourly_indicators.get("ma20")
        ma20_trend = hourly_indicators.get("ma20_trend")
        current_close = hourly_indicators.get("current_close")
        
        if ma20 and current_close:
            if current_close > ma20 and ma20_trend == "向上":
                result["hourly_trend_direction"] = "多头"
            elif current_close < ma20 and ma20_trend == "向下":
                result["hourly_trend_direction"] = "空头"
            else:
                result["hourly_trend_direction"] = "震荡"
    
    # 3. 多周期共振判断
    if "daily_trend_direction" in result and "hourly_trend_direction" in result:
        daily_trend = result["daily_trend_direction"]
        hourly_trend = result["hourly_trend_direction"]
        
        if daily_trend == "多头" and hourly_trend == "多头":
            result["multi_tf_signal"] = "强烈看多"
            result["multi_tf_resonance"] = True
        elif daily_trend == "空头" and hourly_trend == "空头":
            result["multi_tf_signal"] = "强烈看空"
            result["multi_tf_resonance"] = True
        elif daily_trend == "多头" and hourly_trend in ["震荡", "空头"]:
            result["multi_tf_signal"] = "等待回调买入"
            result["multi_tf_resonance"] = False
        elif daily_trend == "空头" and hourly_trend in ["震荡", "多头"]:
            result["multi_tf_signal"] = "反弹做空或观望"
            result["multi_tf_resonance"] = False
        else:
            result["multi_tf_signal"] = "观望"
            result["multi_tf_resonance"] = False
    
    # 4. 入场时机判断（基于小时线）
    if hourly_df is not None and len(hourly_df) >= 20:
        hourly_rsi = result.get("hourly_rsi")
        hourly_kdj_j = result.get("hourly_kdj_j")
        hourly_macd_dif_trend = result.get("hourly_macd_dif_trend")
        daily_trend = result.get("daily_trend_direction")
        
        entry_signals = []
        
        # 日线多头 + 小时线超卖 = 买入时机
        if daily_trend == "多头":
            if hourly_rsi and hourly_rsi < 30:
                entry_signals.append("小时线RSI超卖")
            if hourly_kdj_j and hourly_kdj_j < 20:
                entry_signals.append("小时线KDJ超卖")
            if hourly_macd_dif_trend == "向上":
                entry_signals.append("小时线MACD拐头向上")
        
        result["entry_signals"] = entry_signals
        result["entry_timing"] = "良好" if len(entry_signals) >= 2 else ("一般" if len(entry_signals) == 1 else "等待")
    
    return result


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
    
    # MA均线（当前值）
    ma5_series = ma(df, 5)
    ma10_series = ma(df, 10)
    ma20_series = ma(df, 20)
    ma60_series = ma(df, 60)
    
    result["ma5"] = float(ma5_series.iloc[-1]) if len(df) >= 5 else None
    result["ma10"] = float(ma10_series.iloc[-1]) if len(df) >= 10 else None
    result["ma20"] = float(ma20_series.iloc[-1]) if len(df) >= 20 else None
    result["ma60"] = float(ma60_series.iloc[-1]) if len(df) >= 60 else None
    
    # 均线趋势方向（使用稳健的趋势判断函数）
    if len(df) >= 5:
        ma5_prev = float(ma5_series.iloc[-2]) if len(df) >= 6 else None
        if ma5_prev:
            trend = calculate_trend_direction(result["ma5"], ma5_prev)
            result["ma5_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
        else:
            result["ma5_trend"] = "未知"
    
    if len(df) >= 10:
        ma10_prev = float(ma10_series.iloc[-2]) if len(df) >= 11 else None
        if ma10_prev:
            trend = calculate_trend_direction(result["ma10"], ma10_prev)
            result["ma10_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
        else:
            result["ma10_trend"] = "未知"
    
    if len(df) >= 20:
        ma20_prev = float(ma20_series.iloc[-2]) if len(df) >= 21 else None
        if ma20_prev:
            trend = calculate_trend_direction(result["ma20"], ma20_prev)
            result["ma20_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
        else:
            result["ma20_trend"] = "未知"
    
    if len(df) >= 60:
        ma60_prev = float(ma60_series.iloc[-2]) if len(df) >= 61 else None
        if ma60_prev:
            trend = calculate_trend_direction(result["ma60"], ma60_prev)
            result["ma60_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
        else:
            result["ma60_trend"] = "未知"
    
    # MACD
    if len(df) >= 26:
        macd_data = macd(df)
        result["macd_dif"] = float(macd_data["dif"].iloc[-1])
        result["macd_dea"] = float(macd_data["dea"].iloc[-1])
        result["macd"] = float(macd_data["macd"].iloc[-1])
        
        # MACD趋势方向（使用稳健的趋势判断）
        if len(df) >= 27:
            macd_dif_prev = float(macd_data["dif"].iloc[-2])
            trend = calculate_trend_direction(result["macd_dif"], macd_dif_prev)
            result["macd_dif_trend"] = "向上" if trend == "up" else ("向下" if trend == "down" else "持平")
            result["macd_prev"] = float(macd_data["macd"].iloc[-2])  # 前一天的MACD柱，用于判断绿柱是否缩短
    
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
    
    # 威廉指标
    if len(df) >= 14:
        wr_series = williams_r(df, 14)
        result["williams_r"] = float(wr_series.iloc[-1])
        # 前一天的威廉指标，用于判断是否从超卖区上穿
        if len(df) >= 15:
            result["williams_r_prev"] = float(wr_series.iloc[-2])
    
    # 成交量相关
    if "volume" in df.columns and len(df) >= 5:
        avg_volume_5 = df["volume"].tail(5).mean()
        current_volume = latest["volume"]
        result["vol_ratio"] = float(current_volume / (avg_volume_5 + 1e-10))
    
    # K线数据（用于止损计算）
    if len(df) >= 5:
        # 最近5天的最低点（用于止损参考）
        recent_lows = df["low"].tail(5)
        result["recent_low"] = float(recent_lows.min())
        result["current_low"] = float(latest["low"])
        result["current_high"] = float(latest["high"])
        result["current_open"] = float(latest["open"])
        result["current_close"] = float(latest["close"])
    
    # 20日最高价（保留用于其他计算）
    if len(df) >= 20:
        high_20d = df["high"].tail(20).max()
        result["high_20d"] = float(high_20d)
    
    # 布林带状态判断
    if len(df) >= 20:
        boll_data = boll(df)
        # 计算布林带宽度（上轨-下轨）的变化来判断收口/开口
        boll_width = boll_data["upper"] - boll_data["lower"]
        current_width = float(boll_width.iloc[-1])
        prev_width = float(boll_width.iloc[-2]) if len(df) >= 21 else current_width
        result["boll_width"] = current_width
        result["boll_width_prev"] = prev_width
        result["boll_expanding"] = bool(current_width > prev_width)  # 开口，转换为Python bool
        result["boll_contracting"] = bool(current_width < prev_width)  # 收口，转换为Python bool
    
    # EMA指数移动平均线
    if len(df) >= 12:
        ema12_series = ema(df, 12)
        result["ema12"] = float(ema12_series.iloc[-1])
    if len(df) >= 26:
        ema26_series = ema(df, 26)
        result["ema26"] = float(ema26_series.iloc[-1])
    
    # BIAS乖离率（使用标准的6,12,24周期）
    if len(df) >= 6:
        current_close = float(latest["close"])
        ma6_value = float(ma(df, 6).iloc[-1])
        result["bias6"] = float((current_close - ma6_value) / ma6_value * 100)
    if len(df) >= 12:
        current_close = float(latest["close"])
        ma12_value = float(ma(df, 12).iloc[-1])
        result["bias12"] = float((current_close - ma12_value) / ma12_value * 100)
        result["bias"] = result["bias12"]  # 默认使用12日乖离率
    if len(df) >= 24:
        current_close = float(latest["close"])
        ma24_value = float(ma(df, 24).iloc[-1])
        result["bias24"] = float((current_close - ma24_value) / ma24_value * 100)
    
    # ADX平均趋向指数
    if len(df) >= 28:
        adx_data = adx(df, 14)
        result["adx"] = float(adx_data["adx"].iloc[-1])
        result["plus_di"] = float(adx_data["plus_di"].iloc[-1])
        result["minus_di"] = float(adx_data["minus_di"].iloc[-1])
        # ADX趋势判断
        if len(df) >= 29:
            adx_prev = float(adx_data["adx"].iloc[-2])
            result["adx_prev"] = adx_prev
            result["adx_rising"] = bool(result["adx"] > adx_prev)  # ADX上升表示趋势增强
    
    # CCI顺势指标
    if len(df) >= 20:
        cci_series = cci(df, 20)
        result["cci"] = float(cci_series.iloc[-1])
        # CCI状态判断
        if result["cci"] > 100:
            result["cci_status"] = "超买"
        elif result["cci"] < -100:
            result["cci_status"] = "超卖"
        else:
            result["cci_status"] = "正常"
        # CCI趋势
        if len(df) >= 21:
            cci_prev = float(cci_series.iloc[-2])
            result["cci_prev"] = cci_prev
            result["cci_rising"] = bool(result["cci"] > cci_prev)
    
    # 斐波那契回撤位
    if len(df) >= 20:
        fib_data = fibonacci_retracement(df, min(180, len(df)))
        result["fib_swing_high"] = fib_data["swing_high"]
        result["fib_swing_low"] = fib_data["swing_low"]
        result["fib_236"] = fib_data["fib_236"]
        result["fib_382"] = fib_data["fib_382"]
        result["fib_500"] = fib_data["fib_500"]
        result["fib_618"] = fib_data["fib_618"]
        result["fib_786"] = fib_data["fib_786"]
        result["fib_trend"] = fib_data["trend"]
        result["fib_current_level"] = fib_data["current_fib_level"]
    
    # 一目均衡表（Ichimoku Cloud）
    if len(df) >= 52:
        ichimoku_data = ichimoku(df)
        # 当前值（注意：先行带已经向前移动了26期，所以取当前位置的值）
        result["ichimoku_tenkan"] = float(ichimoku_data["tenkan_sen"].iloc[-1])
        result["ichimoku_kijun"] = float(ichimoku_data["kijun_sen"].iloc[-1])
        
        # 先行带A和B（当前位置的云层）
        senkou_a = ichimoku_data["senkou_span_a"].iloc[-1]
        senkou_b = ichimoku_data["senkou_span_b"].iloc[-1]
        if pd.notna(senkou_a):
            result["ichimoku_senkou_a"] = float(senkou_a)
        if pd.notna(senkou_b):
            result["ichimoku_senkou_b"] = float(senkou_b)
        
        # 判断价格与云层的关系
        current_close = float(latest["close"])
        if pd.notna(senkou_a) and pd.notna(senkou_b):
            cloud_top = max(float(senkou_a), float(senkou_b))
            cloud_bottom = min(float(senkou_a), float(senkou_b))
            result["ichimoku_above_cloud"] = bool(current_close > cloud_top)  # 价格在云上
            result["ichimoku_below_cloud"] = bool(current_close < cloud_bottom)  # 价格在云下
            result["ichimoku_in_cloud"] = bool(cloud_bottom <= current_close <= cloud_top)  # 价格在云中
        
        # 转换线与基准线的交叉判断
        if len(df) >= 53:
            tenkan_prev = float(ichimoku_data["tenkan_sen"].iloc[-2])
            kijun_prev = float(ichimoku_data["kijun_sen"].iloc[-2])
            tenkan_curr = result["ichimoku_tenkan"]
            kijun_curr = result["ichimoku_kijun"]
            # 转换线上穿基准线（金叉）
            result["ichimoku_tk_cross_up"] = bool(tenkan_prev <= kijun_prev and tenkan_curr > kijun_curr)
            # 转换线下穿基准线（死叉）
            result["ichimoku_tk_cross_down"] = bool(tenkan_prev >= kijun_prev and tenkan_curr < kijun_curr)
    
    # 确保所有值都是Python原生类型（防止numpy类型导致序列化错误）
    def convert_numpy_types(value):
        if isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif pd.isna(value):
            return None
        return value
    
    # 递归转换所有值
    for key, value in result.items():
        result[key] = convert_numpy_types(value)
    
    return result

