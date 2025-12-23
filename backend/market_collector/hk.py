"""
港股行情采集
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
import time
from common.redis import set_json, get_redis, get_json
from common.logger import get_logger

logger = get_logger(__name__)


def _classify_security_hk(code: str, name: str) -> str:
    """港股启发式分类，返回 'stock'|'index'|'etf'|'fund'。仅在采集时使用，不做定时任务。"""
    try:
        name_str = (name or "").upper()
        code_str = str(code or "").strip()
    except Exception:
        return "stock"

    if "指数" in name or "指数" in name_str:
        return "index"
    if "ETF" in name_str:
        return "etf"
    if "基金" in name or "基金" in name_str or "REIT" in name_str:
        return "fund"
    return "stock"

# yfinance导入（可选，如果安装失败不影响其他功能）
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("yfinance未安装，yahoo数据源不可用")


def _convert_hk_code_to_yahoo(code: str) -> str:
    """将港股代码转换为yahoo格式
    例如: 00700 -> 00700.HK, 03690 -> 03690.HK, 3690 -> 03690.HK, 0700 -> 00700.HK
    注意：Yahoo Finance要求港股代码保持5位数字格式（保留前导0）
    """
    code_str = str(code).strip()
    # 去掉前导0，获取纯数字部分
    code_num = code_str.lstrip('0')
    if not code_num:
        code_num = '0'
    # 统一补到5位数字（Yahoo Finance要求5位格式）
    # 使用zfill方法确保是5位数字，不足的用0补齐
    code_num = code_num.zfill(5)
    return f"{code_num}.HK"


def _try_yahoo_formats(code: str) -> List[str]:
    """尝试多种Yahoo Finance代码格式
    返回可能的格式列表，按优先级排序
    """
    code_str = str(code).strip()
    formats = []
    
    # 去掉前导0获取纯数字
    code_num = code_str.lstrip('0')
    if not code_num:
        code_num = '0'
    
    # 格式1: 5位数字（03690.HK）
    formats.append(f"{code_num.zfill(5)}.HK")
    
    # 格式2: 4位数字（3690.HK）- 某些股票可能需要
    if len(code_num) <= 4:
        formats.append(f"{code_num.zfill(4)}.HK")
    
    # 格式3: 保持原始格式（如果有前导0，如00700.HK）
    if code_str.startswith('0') and len(code_str) >= 4:
        formats.append(f"{code_str}.HK")
    
    # 去重并保持顺序
    seen = set()
    unique_formats = []
    for fmt in formats:
        if fmt not in seen:
            seen.add(fmt)
            unique_formats.append(fmt)
    
    return unique_formats


def fetch_hk_stock_spot(max_retries: int = 1) -> List[Dict[str, Any]]:
    """获取港股实时行情
    
    数据源顺序：新浪财经 → AKShare(东方财富)
    
    Args:
        max_retries: 最大重试次数（每个源只尝试1次）
    """
    # 数据源列表：新浪 → AKShare
    sources = [
        ("新浪财经", _fetch_hk_spot_sina),
        ("AKShare(东方财富)", _fetch_hk_spot_akshare),
    ]
    
    for source_name, fetch_func in sources:
        try:
            logger.info(f"[港股实时行情] 尝试使用 {source_name} 数据源...")
            result = fetch_func()
            if result and len(result) > 0:
                logger.info(f"[港股实时行情] {source_name} 获取成功，共 {len(result)} 只股票")
                return result, source_name
            else:
                logger.warning(f"[港股实时行情] {source_name} 返回空数据，尝试下一个源")
        except Exception as e:
            logger.warning(f"[港股实时行情] {source_name} 失败: {e}，尝试下一个源")
    
    logger.error("[港股实时行情] 所有数据源都失败")
    return [], ""


def _fetch_hk_spot_sina() -> List[Dict[str, Any]]:
    """使用新浪财经获取港股实时行情"""
    from market_collector.sina_source import fetch_sina_hk_stock_spot
    result = fetch_sina_hk_stock_spot(max_retries=1)
    if not result:
        raise Exception("新浪港股数据为空")
    
    # 保存到Redis并处理差分更新
    _save_hk_spot_to_redis(result)
    return result


def _fetch_hk_spot_akshare() -> List[Dict[str, Any]]:
    """使用AKShare(东方财富)获取港股实时行情"""
    # 使用线程池包装，增加总体超时时间（180秒）
    # 港股采集需要分46批请求，每批约2秒，总共需要约1.5分钟
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(ak.stock_hk_spot_em)
        try:
            df = future.result(timeout=180)  # 180秒超时
        except concurrent.futures.TimeoutError:
            raise TimeoutError("akshare API调用超时（180秒）")
    
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
        "市盈率": "pe",
        "总市值": "market_cap"
    })
    
    # 转换数据类型
    numeric_columns = ["price", "pct", "change", "volume", "amount",
                      "amplitude", "high", "low", "open", "pre_close",
                      "volume_ratio", "turnover", "pe", "market_cap"]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 添加时间戳
    df["update_time"] = datetime.now().isoformat()
    df["market"] = "HK"
    
    # 转换为字典列表
    result: List[Dict[str, Any]] = df.to_dict(orient="records")
    
    if not result:
        raise Exception("AKShare港股数据为空")
    
    # 保存到Redis并处理差分更新
    _save_hk_spot_to_redis(result)
    return result


def _save_hk_spot_to_redis(result: List[Dict[str, Any]]) -> None:
    """保存港股实时行情到Redis，并处理差分更新"""
    # 读取旧快照（如果存在），作为基准数据
    old_data: List[Dict[str, Any]] = get_json("market:hk:spot") or []
    old_map: Dict[str, Dict[str, Any]] = {
        str(item.get("code")): item for item in old_data if isinstance(item, dict)
    }
    
    new_map: Dict[str, Dict[str, Any]] = {
        str(item.get("code")): item for item in result if isinstance(item, dict)
    }
    
    updated: List[Dict[str, Any]] = []
    added: List[Dict[str, Any]] = []
    
    # 需要比较的关键字段
    compare_fields = [
        "price", "pct", "change", "volume", "amount",
        "amplitude", "high", "low", "open", "pre_close",
        "volume_ratio", "turnover", "pe", "market_cap",
    ]
    
    for code, new_item in new_map.items():
        old_item = old_map.get(code)
        if not old_item:
            added.append(new_item)
            continue
        
        changed = False
        for field in compare_fields:
            if old_item.get(field) != new_item.get(field):
                changed = True
                break
        
        if changed:
            updated.append(new_item)
    
    # 计算被删除的股票代码
    old_codes: Set[str] = set(old_map.keys())
    new_codes: Set[str] = set(new_map.keys())
    removed_codes: List[str] = sorted(list(old_codes - new_codes))
    
    # 如果完全没有变化，跳过Redis更新
    if not updated and not added and not removed_codes:
        logger.info("港股行情采集成功，本次数据与上次完全一致，跳过Redis更新")
        return
    
    # 备份旧快照
    if old_data:
        set_json("market:hk:spot_prev", old_data, ex=30 * 24 * 3600)
    
    # 在写入新的全量快照前，为每条记录注入启发式的 sec_type（仅在采集时运行）
    for item in result:
        try:
            code = item.get('code')
            name = item.get('name')
            item['sec_type'] = _classify_security_hk(code, name)
        except Exception:
            item['sec_type'] = 'stock'

    # 写入新的全量快照
    set_json("market:hk:spot", result, ex=30 * 24 * 3600)
    get_redis().set("market:hk:time", datetime.now().isoformat(), ex=30 * 24 * 3600)
    
    # 写入差分数据
    diff_payload = {
        "timestamp": datetime.now().isoformat(),
        "added": added,
        "updated": updated,
        "removed_codes": removed_codes,
    }
    set_json("market:hk:spot_diff", diff_payload, ex=300)
    
    # SSE广播
    try:
        from market.service.sse import broadcast_market_update
        broadcast_market_update("hk")
    except Exception as e:
        logger.debug(f"SSE广播港股数据失败（不影响数据采集）: {e}")
    
    logger.info(
        f"港股行情采集成功，全量{len(result)}只股票，其中新增{len(added)}只，更新{len(updated)}只，删除{len(removed_codes)}只"
    )


def _standardize_hk_kline_data_minute(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """标准化港股分钟/小时级别K线数据格式
    
    港股小时K线数据格式：
    - 列名：['时间', '开盘', '收盘', '最高', '最低', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '换手率']
    - 时间格式：'YYYY-MM-DD HH:MM:SS'
    """
    if df.empty:
        logger.warning(f"港股小时K线DataFrame为空 {code}")
        return []
    
    logger.info(f"开始标准化港股小时K线数据 {code}: DataFrame shape={df.shape}, columns={list(df.columns)}")
    
    # 标准化字段名
    rename_map = {}
    
    # 查找时间列（港股返回的是'时间'列）
    time_col = None
    for col in ["时间", "time", "datetime", "date", "day"]:
        if col in df.columns:
            time_col = col
            logger.info(f"找到时间列 {code}: {col}")
            break
    
    if time_col and time_col != "time":
        rename_map[time_col] = "time"
    
    # 查找价格和成交量列（港股返回的是中文列名）
    price_volume_map = {
        # 中文列名（港股实际返回的）
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        # 英文列名（兼容）
        "open": "open",
        "close": "close",
        "high": "high",
        "low": "low",
        "volume": "volume",
        "amount": "amount"
    }
    
    for old_name, new_name in price_volume_map.items():
        if old_name in df.columns and old_name not in rename_map:
            rename_map[old_name] = new_name
    
    if rename_map:
        logger.info(f"重命名列 {code}: {rename_map}")
        df = df.rename(columns=rename_map)
    
    # 转换数据类型
    numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 处理时间字段
    if "time" in df.columns:
        # 将时间转换为datetime对象
        df["time"] = pd.to_datetime(df["time"], errors='coerce')
        # 格式化为 YYYY-MM-DD HH:MM:SS 格式
        df["time"] = df["time"].dt.strftime('%Y-%m-%d %H:%M:%S')
        # date字段使用日期部分（YYYY-MM-DD）
        df["date"] = df["time"].apply(lambda x: x[:10] if x and isinstance(x, str) and len(x) >= 10 else "")
    else:
        logger.warning(f"港股小时K线数据缺少时间列 {code}")
        return []
    
    # 添加必要字段
    df["code"] = code
    df["market"] = "HK"
    
    # 确保有必要的列
    required_cols = ["open", "close"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"港股小时K线数据缺少必要列 {code}: {missing_cols}")
        return []
    
    # 过滤掉time为空的记录
    if "time" in df.columns:
        df = df[df["time"].notna() & (df["time"] != "")]
    
    # 确保high、low、volume有默认值（如果有缺失）
    if "high" not in df.columns:
        df["high"] = df[["open", "close"]].max(axis=1)
    if "low" not in df.columns:
        df["low"] = df[["open", "close"]].min(axis=1)
    if "volume" not in df.columns:
        df["volume"] = 0
    if "amount" not in df.columns:
        df["amount"] = 0
    
    result = df.to_dict(orient="records")
    logger.info(f"港股小时K线数据标准化完成 {code}: {len(result)}条")
    return result


def _fetch_hk_kline_yahoo(
    code: str,
    period: str = "daily",
    start_date: str | None = None,
    end_date: str | None = None,
) -> List[Dict[str, Any]]:
    """使用Yahoo Finance获取港股K线数据
    
    Args:
        code: 股票代码（如：00700）
        period: 周期（daily, weekly, monthly, 1h/hourly）
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    
    Returns:
        K线数据列表
    """
    if not YFINANCE_AVAILABLE:
        logger.debug(f"yfinance不可用，跳过yahoo数据源: {code}")
        return []
    
    try:
        # 转换周期格式（yfinance使用1d, 1wk, 1mo, 1h等）
        yfinance_interval_map = {
            "daily": "1d",
            "weekly": "1wk",
            "monthly": "1mo",
            "1h": "1h",
            "hourly": "1h",
            "60": "1h",
        }
        interval = yfinance_interval_map.get(period, "1d")
        
        # 转换日期格式
        start_dt = None
        end_dt = None
        if start_date:
            try:
                start_str = start_date.replace("-", "") if "-" in start_date else start_date
                start_dt = datetime.strptime(start_str, "%Y%m%d")
            except Exception as e:
                logger.warning(f"解析start_date失败 {code}: {e}")
        
        if end_date:
            try:
                end_str = end_date.replace("-", "") if "-" in end_date else end_date
                end_dt = datetime.strptime(end_str, "%Y%m%d")
            except Exception as e:
                logger.warning(f"解析end_date失败 {code}: {e}")
        else:
            end_dt = datetime.now()
        
        # 尝试多种Yahoo Finance代码格式
        yahoo_formats = _try_yahoo_formats(code)
        df = None
        last_error = None
        
        for yahoo_code in yahoo_formats:
            try:
                # 获取数据
                ticker = yf.Ticker(yahoo_code)
                
                # yfinance的history方法使用start和end参数
                df = ticker.history(
                    start=start_dt,
                    end=end_dt,
                    interval=interval,
                    auto_adjust=False,  # 不自动调整价格
                    prepost=False,  # 不包括盘前盘后数据
                )
                
                # 如果成功获取数据（不为空），跳出循环
                if df is not None and not df.empty:
                    logger.debug(f"Yahoo Finance使用格式 {yahoo_code} 成功获取数据: {code}")
                    break
                    
            except Exception as e:
                last_error = e
                logger.debug(f"Yahoo Finance格式 {yahoo_code} 失败 {code}: {e}")
                continue
        
        # 如果所有格式都失败，抛出最后一个错误
        if df is None or df.empty:
            if last_error:
                raise last_error
            logger.debug(f"Yahoo Finance所有格式都失败或无数据: {code}")
            return []
        
        if df.empty:
            logger.debug(f"Yahoo Finance返回空数据: {code}")
            return []
        
        # yfinance返回的列名：Open, High, Low, Close, Volume
        # 标准化字段名
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        
        # 转换数据类型
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 添加成交额（如果yfinance没有提供，设为0）
        if "amount" not in df.columns:
            # 成交额 = 收盘价 * 成交量
            df["amount"] = df["close"] * df["volume"]
        
        # 处理日期索引（yfinance返回的DataFrame索引是日期）
        is_hourly = interval in ['1h']
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "Date" in df.columns:
                if is_hourly:
                    # 小时K线使用time字段（YYYY-MM-DD HH:MM:SS格式）
                    df["time"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                    # date字段使用日期部分（YYYY-MM-DD）
                    df["date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
                else:
                    # 日线/周线/月线使用date字段（YYYY-MM-DD格式）
                    df["date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
                df = df.drop(columns=["Date"])
        elif "Date" in df.columns:
            if is_hourly:
                df["time"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                df["date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                df["date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.drop(columns=["Date"])
        
        # 添加股票代码和市场标识
        df["code"] = code
        df["market"] = "HK"
        
        # 过滤掉日期为空的数据
        date_col = "time" if is_hourly else "date"
        if date_col in df.columns:
            df = df[df[date_col].notna() & (df[date_col] != "")]
        
        result = df.to_dict(orient="records")
        logger.info(f"Yahoo Finance港股K线数据获取成功: {code}, {len(result)}条")
        return result
        
    except Exception as e:
        logger.warning(f"Yahoo Finance港股K线数据获取失败 {code}: {e}")
        return []


def fetch_hk_stock_kline(
    code: str,
    period: str = "daily",
    adjust: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    force_full_refresh: bool = False,
    skip_db: bool = False,  # 新增参数：是否跳过数据库操作
) -> List[Dict[str, Any]]:
    """获取港股K线数据（使用Yahoo Finance，国外VPS稳定）
    
    完全使用Yahoo Finance数据源，不再使用AKShare
    
    Args:
        code: 股票代码（如：00700, 03690）
        period: 周期（daily, weekly, monthly, 1h/hourly - 1小时K线）
        adjust: 复权类型（Yahoo Finance忽略此参数）
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（默认今天）
        force_full_refresh: 是否强制全量刷新
        skip_db: 是否跳过数据库操作
    
    Returns:
        K线数据列表
    """
    if not YFINANCE_AVAILABLE:
        logger.error("yfinance不可用，无法获取港股K线数据")
        return []
    
    from common.db import get_kline_latest_date, save_kline_data, get_kline_from_db
    
    # 小时级别数据使用专门的处理逻辑
    is_hourly = period in ['1h', 'hourly', '60']
    if is_hourly:
        logger.info(f"检测到小时K线请求 {code}, period={period}，使用小时数据处理逻辑")
        return _fetch_hk_stock_kline_hourly(code, start_date, end_date, force_full_refresh)
    
    # 转换代码格式：确保是5位数字格式（如3690 -> 03690）
    code_str = str(code).strip()
    code_num = code_str.lstrip('0')
    if not code_num:
        code_num = '0'
    code_5digit = code_num.zfill(5)
    symbol = f"{code_5digit}.HK"
    
    try:
        ticker = yf.Ticker(symbol)
        
        # 获取数据（Yahoo Finance获取所有可用历史数据）
        df = ticker.history(
            interval="1d",
            start="1970-01-01",  # 获取所有历史数据
            auto_adjust=False
        )
        
        if df.empty:
            logger.warning(f"Yahoo Finance港股无数据: {code} (symbol: {symbol})")
            return []
        
        # 重置索引，将Date列提取出来
        df = df.reset_index()
        
        # 重命名列
        df.rename(columns={
            "Date": "trade_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)
        
        # 转换日期格式为YYYY-MM-DD
        if "trade_date" in df.columns:
            df["date"] = pd.to_datetime(df["trade_date"], errors='coerce').dt.strftime('%Y-%m-%d')
            df["time"] = pd.to_datetime(df["trade_date"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 转换数据类型
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 计算成交额（成交额 = 收盘价 * 成交量）
        if "amount" not in df.columns:
            df["amount"] = df["close"] * df["volume"]
        
        # 添加股票代码和市场标识
        df["code"] = code  # 保持原始代码格式
        df["market"] = "HK"
        
        # 过滤掉日期为空的数据
        df = df[df["date"].notna() & (df["date"] != "")]
        
        # 如果指定了日期范围，进行过滤
        if start_date:
            start_str = start_date.replace("-", "") if "-" in start_date else start_date
            try:
                start_dt = datetime.strptime(start_str, "%Y%m%d")
                df = df[pd.to_datetime(df["date"]) >= start_dt]
            except Exception:
                pass
        
        if end_date:
            end_str = end_date.replace("-", "") if "-" in end_date else end_date
            try:
                end_dt = datetime.strptime(end_str, "%Y%m%d")
                df = df[pd.to_datetime(df["date"]) <= end_dt]
            except Exception:
                pass
        
        # 转换为字典列表
        result = df.to_dict("records")
        
        logger.info(f"Yahoo Finance港股K线数据获取成功: {code}, {len(result)}条")
        
        # 如果 skip_db=False，保存到数据库
        if not skip_db:
            try:
                save_kline_data(result, period)
                logger.info(f"港股K线数据已保存到数据库: {code}, {len(result)}条")
            except Exception as e:
                logger.warning(f"保存港股K线数据到数据库失败 {code}: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"Yahoo Finance港股K线数据获取失败 {code}: {e}", exc_info=True)
        return []


def _fetch_hk_stock_kline_hourly(
    code: str,
    start_date: str | None = None,
    end_date: str | None = None,
    force_full_refresh: bool = False,
) -> List[Dict[str, Any]]:
    """获取港股小时K线数据
    
    使用 ak.stock_hk_hist_min_em 接口获取小时级别数据
    小时数据通常只能获取最近1-3个月的数据，所以不使用增量策略
    
    Args:
        code: 股票代码（如：00700）
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（默认今天）
        force_full_refresh: 是否强制全量刷新
    
    Returns:
        小时K线数据列表
    """
    from common.db import save_kline_data, get_kline_from_db
    
    logger.info(f"开始获取港股小时K线数据 {code}")
    
    try:
        # 设置时间范围（小时数据通常只能获取最近的数据，默认获取最近3个月）
        end_dt = datetime.now()
        if end_date:
            try:
                end_str_clean = end_date.replace("-", "") if "-" in end_date else end_date
                end_dt = datetime.strptime(end_str_clean, "%Y%m%d")
            except Exception as e:
                logger.warning(f"解析end_date失败 {code}: {e}，使用当前时间")
                end_dt = datetime.now()
        
        # 如果没有指定start_date，默认获取最近3个月
        if start_date:
            try:
                start_str_clean = start_date.replace("-", "") if "-" in start_date else start_date
                start_dt = datetime.strptime(start_str_clean, "%Y%m%d")
            except Exception as e:
                logger.warning(f"解析start_date失败 {code}: {e}，使用默认3个月前")
                start_dt = end_dt - timedelta(days=90)
        else:
            start_dt = end_dt - timedelta(days=90)  # 默认3个月
        
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        start_str_ymd = start_dt.strftime("%Y%m%d")
        end_str_ymd = end_dt.strftime("%Y%m%d")
        
        logger.info(f"港股小时K线数据时间范围 {code}: {start_str} 到 {end_str}")
        
        # 优先尝试Yahoo Finance数据源
        new_kline_data = []
        try:
            result = _fetch_hk_kline_yahoo(code, "1h", start_str_ymd, end_str_ymd)
            if result and len(result) > 0:
                new_kline_data = result
                logger.info(f"Yahoo Finance港股小时K线数据获取成功: {code}, {len(new_kline_data)}条")
        except Exception as e:
            logger.warning(f"Yahoo Finance获取港股小时K线数据失败 {code}: {e}，尝试备用数据源")
        
        # 如果Yahoo Finance失败，尝试AKShare数据源
        if not new_kline_data:
            try:
                df = ak.stock_hk_hist_min_em(
                    symbol=code,
                    period='60',  # 60分钟 = 1小时
                    adjust='',
                    start_date=start_str,
                    end_date=end_str
                )
            except AttributeError:
                logger.error(f"ak.stock_hk_hist_min_em 接口不存在，可能akshare版本不支持 {code}")
                # 尝试从数据库返回已有数据
                existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), "1h")
                if existing_data:
                    logger.info(f"接口不可用，返回数据库已有数据: {code}, {len(existing_data)}条")
                    return existing_data
                return []
            except Exception as e:
                logger.error(f"获取港股小时K线数据失败 {code}: {e}", exc_info=True)
                # 尝试从数据库返回已有数据
                existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), "1h")
                if existing_data:
                    logger.info(f"获取失败，返回数据库已有数据: {code}, {len(existing_data)}条")
                    return existing_data
                return []
            
            # 如果AKShare数据源成功，处理数据
            if df.empty:
                logger.warning(f"港股小时K线数据获取为空(AKShare) {code}")
                # 尝试从数据库返回已有数据
                existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), "1h")
                if existing_data:
                    logger.info(f"数据源返回空，返回数据库已有数据: {code}, {len(existing_data)}条")
                    return existing_data
                return []
            
            # 标准化数据格式（AKShare数据源）
            new_kline_data = _standardize_hk_kline_data_minute(df, code)
        
        if not new_kline_data:
            logger.warning(f"港股小时K线数据标准化后为空 {code}")
            # 尝试从数据库返回已有数据
            existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), "1h")
            if existing_data:
                return existing_data
            return []
        
        logger.info(f"港股小时K线数据获取成功 {code}: {len(new_kline_data)}条")
        
        # 保存到数据库（period='1h'）
        save_kline_data(new_kline_data, "1h")
        logger.info(f"港股小时K线数据已保存到数据库: {code}, {len(new_kline_data)}条")
        
        # 从数据库查询完整数据返回（如果有start_date/end_date过滤）
        query_start = start_date.replace("-", "") if start_date and "-" in start_date else (start_date or None)
        query_end = end_date.replace("-", "") if end_date and "-" in end_date else (end_date or datetime.now().strftime("%Y%m%d"))
        full_data = get_kline_from_db(code, query_start, query_end, "1h")
        
        if full_data:
            logger.info(f"港股小时K线数据查询完成: {code}, 共{len(full_data)}条")
            return full_data
        else:
            # 如果数据库查询失败，返回新获取的数据
            logger.warning(f"从数据库查询失败，返回新获取的数据: {code}")
            return new_kline_data
        
    except Exception as e:
        logger.error(f"港股小时K线数据获取失败 {code}: {e}", exc_info=True)
        # 尝试从数据库返回已有数据
        from common.db import get_kline_from_db
        existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), "1h")
        if existing_data:
            logger.info(f"异常发生，返回数据库已有数据: {code}, {len(existing_data)}条")
            return existing_data
        return []

