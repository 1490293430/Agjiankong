"""
A股行情采集
"""
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from common.redis import set_json, get_redis
from common.logger import get_logger

logger = get_logger(__name__)


def fetch_a_stock_spot() -> List[Dict[str, Any]]:
    """获取A股实时行情"""
    try:
        df = ak.stock_zh_a_spot_em()
        
        # 标准化字段名
        df = df.rename(columns={
            "代码": "code",
            "名称": "name",
            "最新价": "price",
            "涨跌幅": "pct",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "最高": "high",
            "最低": "low",
            "今开": "open",
            "昨收": "pre_close",
            "量比": "volume_ratio",
            "换手率": "turnover",
            "市盈率-动态": "pe",
            "总市值": "market_cap",
            "流通市值": "circulating_market_cap"
        })
        
        # 转换数据类型
        numeric_columns = ["price", "pct", "change", "volume", "amount", 
                          "amplitude", "high", "low", "open", "pre_close",
                          "volume_ratio", "turnover", "pe", "market_cap", "circulating_market_cap"]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 添加时间戳
        df["update_time"] = datetime.now().isoformat()
        df["market"] = "A"
        
        # 转换为字典列表
        result = df.to_dict(orient="records")
        
        # 存储到Redis
        set_json("market:a:spot", result, ex=300)  # 5分钟过期
        get_redis().set("market:a:time", datetime.now().isoformat(), ex=300)
        
        logger.info(f"A股行情采集成功，共{len(result)}只股票")
        return result
        
    except Exception as e:
        logger.error(f"A股行情采集失败: {e}", exc_info=True)
        return []


def fetch_a_stock_kline(code: str, period: str = "daily", adjust: str = "") -> List[Dict[str, Any]]:
    """获取A股K线数据
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly）
        adjust: 复权类型（"", "qfq", "hfq"）
    """
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period=period,
            adjust=adjust,
            start_date="",
            end_date=""
        )
        
        # 标准化字段
        if "日期" in df.columns:
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct",
                "涨跌额": "change",
                "换手率": "turnover"
            })
        
        # 转换数据类型
        numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df["code"] = code
        df["market"] = "A"
        
        result = df.to_dict(orient="records")
        logger.info(f"A股K线数据获取成功: {code}, 共{len(result)}条")
        return result
        
    except Exception as e:
        logger.error(f"A股K线数据获取失败 {code}: {e}", exc_info=True)
        return []

