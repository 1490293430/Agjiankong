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

# yfinance导入（可选，如果安装失败不影响其他功能）
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("yfinance未安装，yahoo数据源不可用")


def _convert_hk_code_to_yahoo(code: str) -> str:
    """将港股代码转换为yahoo格式
    例如: 00700 -> 0700.HK, 00001 -> 0001.HK
    """
    code_str = str(code).strip()
    # 去掉前导0，但保留至少4位数字
    code_num = code_str.lstrip('0')
    if not code_num:
        code_num = '0'
    # 确保至少4位数字
    while len(code_num) < 4:
        code_num = '0' + code_num
    return f"{code_num}.HK"


def fetch_hk_stock_spot(max_retries: int = 3) -> List[Dict[str, Any]]:
    """获取港股实时行情
    
    Args:
        max_retries: 最大重试次数
    """
    for attempt in range(max_retries):
        try:
            # 使用线程池包装，增加总体超时时间（5分钟），给网络更多时间
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ak.stock_hk_spot_em)
                try:
                    df = future.result(timeout=300)  # 5分钟超时，给网络更多时间
                except concurrent.futures.TimeoutError:
                    raise TimeoutError("akshare API调用超时（5分钟）")
            
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
            
            # ---------------- 差分更新逻辑（类似A股） ----------------
            # 1. 读取旧快照（如果存在），作为基准数据
            old_data: List[Dict[str, Any]] = get_json("market:hk:spot") or []
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
                logger.info("港股行情采集成功，本次数据与上次完全一致，跳过Redis更新")
                return result
            
            # 将旧快照备份一份，方便需要时回溯（保留 30 天）
            if old_data:
                set_json("market:hk:spot_prev", old_data, ex=30 * 24 * 3600)
            
            # 2. 写入新的全量快照（前端HTTP/WS读取的主数据，保留 30 天）
            set_json("market:hk:spot", result, ex=30 * 24 * 3600)
            get_redis().set("market:hk:time", datetime.now().isoformat(), ex=30 * 24 * 3600)
            
            # 3. 同时写入一份差分数据，供前端或WebSocket按需使用
            diff_payload = {
                "timestamp": datetime.now().isoformat(),
                "added": added,
                "updated": updated,
                "removed_codes": removed_codes,
            }
            set_json("market:hk:spot_diff", diff_payload, ex=300)
            
            logger.info(
                f"港股行情采集成功，全量{len(result)}只股票，其中新增{len(added)}只，更新{len(updated)}只，删除{len(removed_codes)}只"
            )
            return result
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 递增等待时间：2s, 4s, 6s
                logger.warning(f"港股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: {e}")
                time.sleep(wait_time)
            else:
                logger.error(f"港股行情采集失败（已重试{max_retries}次）: {e}", exc_info=True)
                return []


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
        # 转换代码格式
        yahoo_code = _convert_hk_code_to_yahoo(code)
        
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
    """获取港股K线数据（增量获取策略）
    
    策略说明：
    1. 首次获取：从数据库查询最新日期，如果没有则全量获取并存储
    2. 增量更新：只获取数据库最新日期之后的数据并追加
    3. 返回数据：从数据库查询完整数据（包含历史+增量）
    4. 小时数据：不使用增量策略，每次都全量获取（因为小时数据通常只能获取最近的数据）
    
    Args:
        code: 股票代码（如：00700）
        period: 周期（daily, weekly, monthly, 1h/hourly - 1小时K线）
        adjust: 复权类型
        start_date: 开始日期 YYYYMMDD（用于查询时的过滤，不影响增量逻辑）
        end_date: 结束日期 YYYYMMDD（默认今天）
        force_full_refresh: 是否强制全量刷新（用于初始化或修复数据）
    
    Returns:
        K线数据列表
    """
    from common.db import get_kline_latest_date, save_kline_data, get_kline_from_db
    
    # 小时级别（1h, hourly）数据通常只能获取最近的数据，不使用增量策略
    is_hourly = period in ['1h', 'hourly', '60']
    
    # 如果请求小时数据，使用专门的处理逻辑
    if is_hourly:
        logger.info(f"检测到小时K线请求 {code}, period={period}，使用小时数据处理逻辑")
        return _fetch_hk_stock_kline_hourly(code, start_date, end_date, force_full_refresh)
    
    # 设置默认结束日期
    default_end = end_date or datetime.now().strftime("%Y%m%d")
    if default_end and "-" in default_end:
        default_end = default_end.replace("-", "")
    
    # 查询数据库中该股票的最新日期（如果 skip_db=True，跳过数据库查询）
    db_latest_date = None
    if skip_db:
        logger.debug(f"跳过数据库查询 {code}，直接从数据源获取")
    elif not force_full_refresh:
        db_latest_date = get_kline_latest_date(code, period)
    
    # 确定需要从数据源获取的日期范围
    if db_latest_date:
        # 数据库中有数据，只获取增量（从最新日期的下一天开始）
        try:
            latest_dt = datetime.strptime(db_latest_date, "%Y%m%d")
            increment_start_dt = latest_dt + timedelta(days=1)
            increment_start = increment_start_dt.strftime("%Y%m%d")
            
            # 如果增量开始日期已经超过结束日期，说明数据已是最新，直接返回数据库数据
            if increment_start > default_end:
                logger.debug(f"港股K线数据已是最新 {code}，直接从数据库返回")
                return get_kline_from_db(code, start_date, default_end, period)
        except Exception as e:
            logger.warning(f"解析数据库最新日期失败 {code}: {e}，将全量获取")
            db_latest_date = None
    
    # 确定获取开始日期
    if db_latest_date:
        # 增量获取：从数据库最新日期的下一天开始
        fetch_start = increment_start
        fetch_end = default_end
        fetch_mode = "增量"
    else:
        # 全量获取：如果没有指定start_date，默认获取最近2年数据（约500根K线，已完全足够）
        if start_date:
            fetch_start = start_date.replace("-", "") if "-" in start_date else start_date
        else:
            # 默认获取最近2年（约500根K线）
            two_years_ago = datetime.now() - timedelta(days=2*365)
            fetch_start = two_years_ago.strftime("%Y%m%d")
        fetch_end = default_end
        fetch_mode = "全量"
    
    logger.info(f"开始{fetch_mode}获取港股K线数据 {code}: {fetch_start} 到 {fetch_end}")
    
    try:
        # 定义数据源列表（按优先级排序，yahoo优先）
        data_sources = [
            ("Yahoo Finance(优先)", _fetch_hk_kline_yahoo),
            ("AKShare(备用)", None),  # 将在下面单独处理
        ]
        
        # 优先尝试Yahoo Finance数据源
        new_kline_data = []
        for source_name, fetch_func in data_sources:
            if fetch_func is None:
                # AKShare数据源（备用）
                continue
            
            try:
                result = fetch_func(code, period, fetch_start, fetch_end)
                if result and len(result) > 0:
                    new_kline_data = result
                    logger.info(f"港股K线数据获取成功({source_name}): {code}, {len(new_kline_data)}条")
                    break
            except Exception as e:
                logger.warning(f"{source_name}获取港股K线数据失败 {code}: {e}，尝试备用数据源")
                continue
        
        # 如果Yahoo Finance失败，尝试AKShare数据源
        if not new_kline_data:
            try:
                # AKShare港股历史数据接口
                # 注意：ak.stock_hk_hist可能不支持start_date和end_date参数
                # 如果不支持，先全量获取再过滤
                try:
                    df = ak.stock_hk_hist(
                        symbol=code,
                        period=period,
                        start_date=fetch_start if fetch_start else "",
                        end_date=fetch_end if fetch_end else "",
                        adjust=adjust if adjust else ""
                    )
                except TypeError:
                    # 如果参数不支持，尝试不使用start_date和end_date，全量获取后过滤
                    logger.debug(f"stock_hk_hist不支持start_date/end_date参数，全量获取后过滤: {code}")
                df = ak.stock_hk_hist(
                    symbol=code,
                    period=period,
                    adjust=adjust if adjust else ""
                )
                # 如果全量获取成功，需要根据日期过滤
                if not df.empty and "日期" in df.columns:
                    from datetime import datetime
                    if fetch_start:
                        start_dt = datetime.strptime(fetch_start, "%Y%m%d")
                        df = df[pd.to_datetime(df["日期"]) >= start_dt]
                    if fetch_end:
                        end_dt = datetime.strptime(fetch_end, "%Y%m%d")
                        df = df[pd.to_datetime(df["日期"]) <= end_dt]
            
                if df.empty:
                    # 如果增量获取返回空，可能是数据已是最新，尝试从数据库返回
                    if db_latest_date:
                        logger.debug(f"增量获取返回空数据，从数据库返回: {code}")
                        return get_kline_from_db(code, start_date, default_end, period)
                    logger.warning(f"港股K线数据获取为空: {code}")
                    return []
                
                # 标准化字段
                if "日期" in df.columns:
                    df = df.rename(columns={
                        "日期": "date",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                        "成交额": "amount"
                    })
                
                # 转换数据类型
                numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df["code"] = code
                df["market"] = "HK"
                
                # 转换日期格式为YYYY-MM-DD
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.strftime('%Y-%m-%d')
                
                new_kline_data = df.to_dict(orient="records")
                logger.info(f"港股K线数据获取成功(AKShare备用): {code}, {len(new_kline_data)}条")
            except Exception as e:
                logger.error(f"AKShare港股K线数据获取失败 {code}: {e}", exc_info=True)
                # 如果增量获取返回空，可能是数据已是最新，尝试从数据库返回
                if db_latest_date:
                    logger.debug(f"所有数据源获取失败，从数据库返回: {code}")
                    return get_kline_from_db(code, start_date, default_end, period)
                return []
        
        # 过滤掉日期为空的数据
        new_kline_data = [item for item in new_kline_data if item.get("date")]
        
        if new_kline_data:
            logger.info(f"港股K线数据获取成功({fetch_mode}): {code}, 新增{len(new_kline_data)}条")
            # 如果 skip_db=False，保存到数据库
            if not skip_db:
                try:
                    save_kline_data(new_kline_data, period)
                    logger.info(f"港股K线增量数据已保存到数据库: {code}, {len(new_kline_data)}条")
                except Exception as e:
                    logger.warning(f"保存港股K线数据到数据库失败 {code}: {e}")
        
        # 如果 skip_db=True，直接返回新获取的数据
        if skip_db:
            return new_kline_data if new_kline_data else []
        
        # 从数据库查询完整数据返回
        query_start = start_date.replace("-", "") if start_date and "-" in start_date else (start_date or None)
        full_data = get_kline_from_db(code, query_start, default_end, period)
        
        if full_data:
            logger.info(f"港股K线数据查询完成: {code}, 共{len(full_data)}条（含历史数据）")
            return full_data
        elif new_kline_data:
            logger.warning(f"从数据库查询失败，返回新获取的数据: {code}")
            return new_kline_data
        else:
            return []
        
    except Exception as e:
        logger.error(f"港股K线数据获取失败 {code}: {e}", exc_info=True)
        # 尝试从数据库返回已有数据
        if not force_full_refresh:
            existing_data = get_kline_from_db(code, start_date, default_end, period)
            if existing_data:
                logger.info(f"数据源获取失败，返回数据库已有数据: {code}, {len(existing_data)}条")
                return existing_data
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

