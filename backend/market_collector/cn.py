"""
A股行情采集
"""
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Set
import time
import requests
import urllib3
from common.redis import set_json, get_redis, get_json
from common.logger import get_logger

logger = get_logger(__name__)

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fetch_a_stock_spot(max_retries: int = 5) -> List[Dict[str, Any]]:
    """获取A股实时行情
    
    Args:
        max_retries: 最大重试次数（增加到5次）
    """
    for attempt in range(max_retries):
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
            
            # 转换为字典列表（最新全量快照）
            result: List[Dict[str, Any]] = df.to_dict(orient="records")

            # ---------------- 差分更新逻辑 ----------------
            # 1. 读取旧快照（如果存在），作为基准数据
            old_data: List[Dict[str, Any]] = get_json("market:a:spot") or []
            old_map: Dict[str, Dict[str, Any]] = {
                str(item.get("code")): item for item in old_data if isinstance(item, dict)
            }

            new_map: Dict[str, Dict[str, Any]] = {
                str(item.get("code")): item for item in result if isinstance(item, dict)
            }

            updated: List[Dict[str, Any]] = []
            added: List[Dict[str, Any]] = []

            # 需要比较的关键字段（如果这些字段任一有变化，则认为该股票有更新）
            compare_fields = [
                "price",
                "pct",
                "change",
                "volume",
                "amount",
                "amplitude",
                "high",
                "low",
                "open",
                "pre_close",
                "volume_ratio",
                "turnover",
                "pe",
                "market_cap",
                "circulating_market_cap",
            ]

            for code, new_item in new_map.items():
                old_item = old_map.get(code)
                if not old_item:
                    # 新增股票
                    added.append(new_item)
                    continue

                # 对比关键字段是否有变化
                changed = False
                for field in compare_fields:
                    if old_item.get(field) != new_item.get(field):
                        changed = True
                        break

                if changed:
                    updated.append(new_item)

            # 计算被删除的股票代码（通常较少发生）
            old_codes: Set[str] = set(old_map.keys())
            new_codes: Set[str] = set(new_map.keys())
            removed_codes: List[str] = sorted(list(old_codes - new_codes))

            # 如果完全没有变化，则保留原有快照，不更新 Redis，避免无意义推送
            if not updated and not added and not removed_codes:
                logger.info("A股行情采集成功，本次数据与上次完全一致，跳过Redis更新")
                return result

            # 将旧快照备份一份，方便需要时回溯
            if old_data:
                set_json("market:a:spot_prev", old_data, ex=3600)

            # 2. 写入新的全量快照（前端HTTP/WS读取的主数据）
            set_json("market:a:spot", result, ex=1800)  # 30分钟过期
            get_redis().set("market:a:time", datetime.now().isoformat(), ex=1800)

            # 3. 同时写入一份差分数据，供前端或WebSocket按需使用
            diff_payload = {
                "timestamp": datetime.now().isoformat(),
                "added": added,
                "updated": updated,
                "removed_codes": removed_codes,
            }
            set_json("market:a:spot_diff", diff_payload, ex=300)

            logger.info(
                f"A股行情采集成功，全量{len(result)}只股票，其中新增{len(added)}只，更新{len(updated)}只，删除{len(removed_codes)}只"
            )
            return result
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3  # 递增等待时间：3s, 6s, 9s, 12s
                error_msg = str(e)
                # 只记录关键错误信息，避免日志过长
                if "SSL" in error_msg or "SSLError" in error_msg:
                    logger.warning(f"A股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: SSL连接错误")
                else:
                    logger.warning(f"A股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: {error_msg[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"A股行情采集失败（已重试{max_retries}次）: {str(e)[:200]}", exc_info=True)
                # 即使失败也返回空列表，避免影响其他采集任务
                return []


def _standardize_kline_data(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """标准化K线数据格式"""
    if df.empty:
        return []
    
    # 标准化字段名（支持多种可能的字段名）
    rename_map = {}
    if "日期" in df.columns:
        rename_map["日期"] = "date"
    elif "date" in df.columns:
        pass  # 已经是标准格式
    elif "time" in df.columns:
        rename_map["time"] = "date"
    
    if "开盘" in df.columns:
        rename_map["开盘"] = "open"
    if "收盘" in df.columns:
        rename_map["收盘"] = "close"
    if "最高" in df.columns:
        rename_map["最高"] = "high"
    if "最低" in df.columns:
        rename_map["最低"] = "low"
    if "成交量" in df.columns:
        rename_map["成交量"] = "volume"
    if "成交额" in df.columns:
        rename_map["成交额"] = "amount"
    if "振幅" in df.columns:
        rename_map["振幅"] = "amplitude"
    if "涨跌幅" in df.columns:
        rename_map["涨跌幅"] = "pct"
    if "涨跌额" in df.columns:
        rename_map["涨跌额"] = "change"
    if "换手率" in df.columns:
        rename_map["换手率"] = "turnover"
    
    if rename_map:
        df = df.rename(columns=rename_map)
    
    # 转换数据类型
    numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 确保日期格式统一
    if "date" in df.columns:
        # 将日期转换为字符串格式 YYYY-MM-DD
        df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.strftime('%Y-%m-%d')
        # 同时添加time字段用于图表显示
        df["time"] = df["date"]
    
    df["code"] = code
    df["market"] = "A"
    
    return df.to_dict(orient="records")


def _fetch_kline_source1(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源1: stock_zh_a_hist (主数据源)"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period=period,
            adjust=adjust,
            start_date=start_date,
            end_date=end_date
        )
        if df.empty:
            return []
        return _standardize_kline_data(df, code)
    except Exception as e:
        logger.debug(f"数据源1失败 {code}: {str(e)[:100]}")
        return []


def _fetch_kline_source2(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源2: 尝试不同的参数组合 (备用数据源1)"""
    try:
        # 尝试不指定复权类型
        df = ak.stock_zh_a_hist(
            symbol=code,
            period=period,
            adjust="",  # 强制不复权
            start_date=start_date,
            end_date=end_date
        )
        if df.empty:
            return []
        return _standardize_kline_data(df, code)
    except Exception as e:
        logger.debug(f"数据源2失败 {code}: {str(e)[:100]}")
        return []


def _fetch_kline_source3(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源3: 尝试使用更短的日期范围 (备用数据源2)"""
    try:
        # 如果日期范围太大可能导致失败，尝试只获取最近的数据
        from datetime import datetime, timedelta
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        # 只获取最近3年的数据
        start_dt = end_dt - timedelta(days=3*365)
        short_start = start_dt.strftime("%Y%m%d")
        
        df = ak.stock_zh_a_hist(
            symbol=code,
            period=period,
            adjust=adjust,
            start_date=short_start,
            end_date=end_date
        )
        if df.empty:
            return []
        return _standardize_kline_data(df, code)
    except Exception as e:
        logger.debug(f"数据源3失败 {code}: {str(e)[:100]}")
        return []


def _fetch_kline_source4(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源4: 尝试使用通用历史数据接口 (备用数据源3)"""
    try:
        # 尝试使用tool_trade_date_hist_sina或其他通用接口
        # 如果akshare有更新，可以在这里添加新的接口
        # 暂时返回空，等待后续扩展
        return []
    except Exception as e:
        logger.debug(f"数据源4失败 {code}: {str(e)[:100]}")
        return []


def fetch_a_stock_kline(
    code: str,
    period: str = "daily",
    adjust: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
) -> List[Dict[str, Any]]:
    """获取A股K线数据（支持多数据源备用）
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly）
        adjust: 复权类型（"", "qfq", "hfq"）
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    """
    from datetime import datetime
    
    # 设置默认日期
    default_start = start_date or "20000101"
    default_end = end_date or datetime.now().strftime("%Y%m%d")
    
    # 统一日期格式为 YYYYMMDD
    if default_start and "-" in default_start:
        default_start = default_start.replace("-", "")
    if default_end and "-" in default_end:
        default_end = default_end.replace("-", "")
    
    # 定义数据源列表（按优先级排序）
    data_sources = [
        ("数据源1(主)", _fetch_kline_source1),
        ("数据源2(备用1)", _fetch_kline_source2),
        ("数据源3(备用2)", _fetch_kline_source3),
        # ("数据源4(备用3)", _fetch_kline_source4),  # 暂时禁用，等待扩展
    ]
    
    # 依次尝试各个数据源
    for source_name, fetch_func in data_sources:
        try:
            logger.debug(f"尝试使用{source_name}获取K线数据: {code}")
            result = fetch_func(code, period, adjust, default_start, default_end)
            
            if result and len(result) > 0:
                logger.info(f"A股K线数据获取成功({source_name}): {code}, 共{len(result)}条")
                return result
            else:
                logger.debug(f"{source_name}返回空数据: {code}")
        except Exception as e:
            logger.warning(f"{source_name}获取K线数据失败 {code}: {str(e)[:150]}")
            continue
    
    # 所有数据源都失败
    logger.error(f"所有数据源均失败，无法获取K线数据: {code}")
    return []

