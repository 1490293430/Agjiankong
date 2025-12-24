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


def _get_tushare_stock_codes() -> Set[str]:
    """尝试从 market_collector.tushare_source 获取 A股代码列表，返回代码集合。
    仅在采集时调用一次作为参考（不做定时任务）。
    如果无法获取或 Tushare 未配置，则返回空集合。
    """
    try:
        from market_collector.tushare_source import fetch_stock_list_tushare, get_tushare_api
        api = get_tushare_api()
        if not api:
            return set()
        stocks = fetch_stock_list_tushare()
        return {str(s.get('code')).strip() for s in stocks if s.get('code')}
    except Exception:
        return set()


def _classify_security(code: str, name: str) -> str:
    """启发式分类：'stock'|'index'|'etf'|'fund'"""
    try:
        name_upper = (name or "").upper()
        code_str = str(code or "").strip()
    except Exception:
        return "stock"

    # ETF 优先判断（名称含 ETF）
    if "ETF" in name_upper:
        return "etf"
    
    # LOF 和基金
    if "LOF" in name_upper or "基金" in name:
        return "fund"
    
    # 债券
    if "债" in name or "可转债" in name:
        return "fund"
    
    # 代码规则判断
    if code_str.startswith("399"):
        return "index"
    if code_str.startswith("51") or code_str.startswith("159"):
        return "etf"
    if code_str.startswith("501") or code_str.startswith("502"):
        return "fund"
    
    # 688 开头是科创板股票
    if code_str.startswith("688"):
        return "stock"
    
    # 指数关键词判断（放在公司特征词之前！）
    index_keywords = [
        # 交易所/市场前缀
        "上证", "深证", "沪深", "中证", "沪", "深", "创业板", "科创",
        # 指数类型
        "指数", "综指", "成指", "等权", "全指", "红利", "价值", "成长",
        "基本", "波动", "稳定", "动态", "治理", "高贝", "低贝", "分层",
        "优选", "领先", "百强", "央视", "腾讯", "济安", "丝路",
        # 特殊指数
        "股通", "互联", "龙头", "央企", "国企", "民企", "地企", "沪企",
        "海外", "周期", "非周期", "上游", "中游", "下游", "投资品",
        "中小", "大盘", "小盘", "中盘", "超大盘", "流通",
        # 行业指数后缀
        "TMT", "ESG", "碳中和", "新丝路", "一带一路", "持续产业",
        "中国造", "高端装备", "内地资源", "A股资源"
    ]
    
    for kw in index_keywords:
        if kw in name:
            return "index"
    
    # 000 开头的特殊处理：更多行业指数关键词（只对000开头生效，避免误判普通股票）
    if code_str.startswith("000"):
        index_000_keywords = [
            "有色", "资源", "消费", "医药", "优势", "百发", "细分", "主题",
            "HK", "CS", "农业", "精准", "金融", "材料", "能源", "信息",
            "电信", "可选", "必需", "公用", "工业", "地产"
        ]
        for kw in index_000_keywords:
            if kw in name:
                return "index"
        # 数字开头的名称 = 指数
        import re
        if re.match(r'^[\d]+', name):
            return "index"
    
    # 名称以 A/B 结尾的通常是股票
    if name.endswith("Ａ") or name.endswith("Ｂ") or name.endswith("A") or name.endswith("B"):
        return "stock"
    
    # 公司名称特征词
    company_patterns = ["股份", "集团", "控股", "实业", "科技", "电子", 
                        "生物", "新材", "智能", "网络", "软件",
                        "环境", "建设", "工程", "制造", "机械", "设备",
                        "汽车", "电气", "服饰", "家居", "文化", "教育", 
                        "物流", "运输", "船舶", "置业", "租赁", "信托"]
    
    for pattern in company_patterns:
        if pattern in name:
            return "stock"
    
    return "stock"

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置requests的默认超时，帮助akshare处理网络慢的情况
# 注意：这会影响所有使用requests的地方，但akshare内部也使用requests
try:
    # 设置默认连接和读取超时
    requests.adapters.DEFAULT_TIMEOUT = (10, 120)  # (连接超时, 读取超时)
except Exception:
    # 如果设置失败，忽略（某些版本的requests可能不支持）
    pass


def fetch_a_stock_spot(max_retries: int = 3) -> List[Dict[str, Any]]:
    """获取A股实时行情
    
    Args:
        max_retries: 最大重试次数
    """
    # 配置requests的默认超时，避免akshare内部15秒超时不够用
    import requests
    original_timeout = requests.Session().timeout if hasattr(requests.Session(), 'timeout') else None
    
    for attempt in range(max_retries):
        try:
            # 使用线程池包装，增加总体超时时间（60秒）
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ak.stock_zh_a_spot_em)
                try:
                    df = future.result(timeout=60)  # 60秒超时
                except concurrent.futures.TimeoutError:
                    raise TimeoutError("akshare API调用超时（60秒）")
            
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
            # 使用启发式方法分类每条记录
            df["sec_type"] = df.apply(lambda row: _classify_security(row.get('code', ''), row.get('name', '')), axis=1)
            
            # 转换为字典列表（股票数据）
            stock_list: List[Dict[str, Any]] = df.to_dict(orient="records")
            
            # ============ 额外获取 ETF 和指数数据 ============
            etf_list: List[Dict[str, Any]] = []
            index_list: List[Dict[str, Any]] = []
            
            # 获取 ETF 实时行情
            try:
                etf_df = ak.fund_etf_spot_em()
                if not etf_df.empty:
                    etf_df = etf_df.rename(columns={
                        "代码": "code", "名称": "name", "最新价": "price",
                        "涨跌幅": "pct", "涨跌额": "change", "成交量": "volume",
                        "成交额": "amount", "最高": "high", "最低": "low",
                        "今开": "open", "昨收": "pre_close", "换手率": "turnover"
                    })
                    for col in ["price", "pct", "change", "volume", "amount", "high", "low", "open", "pre_close", "turnover"]:
                        if col in etf_df.columns:
                            etf_df[col] = pd.to_numeric(etf_df[col], errors='coerce')
                    etf_df["update_time"] = datetime.now().isoformat()
                    etf_df["market"] = "A"
                    etf_df["sec_type"] = "etf"
                    etf_list = etf_df.to_dict(orient="records")
                    logger.info(f"额外获取 ETF 数据: {len(etf_list)} 只")
            except Exception as e:
                logger.warning(f"获取 ETF 数据失败（不影响主数据）: {e}")
            
            # 获取指数实时行情
            try:
                index_df = ak.stock_zh_index_spot_em()
                if not index_df.empty:
                    index_df = index_df.rename(columns={
                        "代码": "code", "名称": "name", "最新价": "price",
                        "涨跌幅": "pct", "涨跌额": "change", "成交量": "volume",
                        "成交额": "amount", "最高": "high", "最低": "low",
                        "今开": "open", "昨收": "pre_close"
                    })
                    for col in ["price", "pct", "change", "volume", "amount", "high", "low", "open", "pre_close"]:
                        if col in index_df.columns:
                            index_df[col] = pd.to_numeric(index_df[col], errors='coerce')
                    index_df["update_time"] = datetime.now().isoformat()
                    index_df["market"] = "A"
                    index_df["sec_type"] = "index"
                    index_list = index_df.to_dict(orient="records")
                    logger.info(f"额外获取指数数据: {len(index_list)} 只")
            except Exception as e:
                logger.warning(f"获取指数数据失败（不影响主数据）: {e}")
            
            # ============ 合并数据，去重时优先保留 ETF/指数 ============
            # 收集 ETF 和指数的代码
            etf_codes = {str(item.get('code', '')) for item in etf_list}
            index_codes = {str(item.get('code', '')) for item in index_list}
            non_stock_codes = etf_codes | index_codes
            
            # 过滤掉股票列表中与 ETF/指数代码重复的
            filtered_stock_list = [
                item for item in stock_list 
                if str(item.get('code', '')) not in non_stock_codes
            ]
            logger.info(f"股票数据去重: 原{len(stock_list)}只，去除与ETF/指数重复后{len(filtered_stock_list)}只")
            
            # 合并：ETF + 指数 + 过滤后的股票
            result: List[Dict[str, Any]] = etf_list + index_list + filtered_stock_list

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

            # 将旧快照备份一份，方便需要时回溯（保留 30 天）
            if old_data:
                set_json("market:a:spot_prev", old_data, ex=30 * 24 * 3600)

            # 在写入 Redis 前，优先尝试使用权威列表（Tushare）区分股票与其它品种
            try:
                tushare_codes = _get_tushare_stock_codes()
            except Exception:
                tushare_codes = set()

            # 为每条记录注入 sec_type：
            # - 如果已经有 sec_type（ETF/指数数据），保留原值
            # - 否则优先用 tushare 列表判定（存在即为股票），否则使用启发式回退
            for item in result:
                try:
                    # 如果已经有 sec_type，跳过（保留 ETF/指数的原始标记）
                    if item.get('sec_type'):
                        continue
                    
                    code = str(item.get('code') or '').strip()
                    name = item.get('name') or ''
                    if tushare_codes and code in tushare_codes:
                        item['sec_type'] = 'stock'
                    else:
                        item['sec_type'] = _classify_security(code, name)
                except Exception:
                    item['sec_type'] = 'stock'

            # 2. 写入新的全量快照（前端HTTP/WS读取的主数据，保留 30 天）
            set_json("market:a:spot", result, ex=30 * 24 * 3600)
            get_redis().set("market:a:time", datetime.now().isoformat(), ex=30 * 24 * 3600)

            # 3. 同时写入一份差分数据，供前端或WebSocket按需使用
            diff_payload = {
                "timestamp": datetime.now().isoformat(),
                "added": added,
                "updated": updated,
                "removed_codes": removed_codes,
            }
            set_json("market:a:spot_diff", diff_payload, ex=300)

            # 4. 通过SSE广播市场数据更新
            try:
                from market.service.sse import broadcast_market_update
                broadcast_market_update("a")
            except Exception as e:
                logger.debug(f"SSE广播A股数据失败（不影响数据采集）: {e}")

            logger.info(
                f"A股行情采集成功，全量{len(result)}只股票，其中新增{len(added)}只，更新{len(updated)}只，删除{len(removed_codes)}只"
            )
            return result
            
        except Exception as e:
            if attempt < max_retries - 1:
                # 增加重试间隔：5s, 10s, 15s, 20s...，给网络更多恢复时间
                wait_time = (attempt + 1) * 5  # 递增等待时间：5s, 10s, 15s, 20s, 25s...
                error_msg = str(e)
                err_type = type(e).__name__
                # 只记录关键错误信息，避免日志过长
                if "SSL" in error_msg or "SSLError" in error_msg or "handshake" in error_msg.lower():
                    logger.warning(f"A股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: {err_type} SSL连接错误")
                elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                    logger.warning(f"A股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: {err_type} 网络超时")
                else:
                    logger.warning(f"A股行情采集失败（第{attempt + 1}次尝试），{wait_time}秒后重试: {err_type} {error_msg[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"A股行情采集失败（已重试{max_retries}次）: {type(e).__name__} {str(e)[:200]}", exc_info=True)
                # 尝试使用 Tushare 作为备用数据源
                try:
                    from market_collector.tushare_source import fetch_stock_list_tushare, get_tushare_api
                    api = get_tushare_api()
                    if api:
                        logger.info("akshare 失败，尝试使用 Tushare 获取股票列表...")
                        tushare_stocks = fetch_stock_list_tushare()
                        if tushare_stocks and len(tushare_stocks) > 0:
                            # Tushare 返回的是股票列表，不是实时行情，但可以作为基础数据
                            # 添加必要的字段
                            for stock in tushare_stocks:
                                stock['update_time'] = datetime.now().isoformat()
                                stock['market'] = 'A'
                                # 设置默认值（Tushare 股票列表没有实时价格）
                                stock.setdefault('price', 0)
                                stock.setdefault('pct', 0)
                                stock.setdefault('volume', 0)
                            logger.info(f"Tushare 获取股票列表成功: {len(tushare_stocks)}只")
                            # 注意：这只是股票列表，不是实时行情，但可以避免完全没有数据
                            return tushare_stocks
                except Exception as ts_error:
                    logger.warning(f"Tushare 备用数据源也失败: {ts_error}")
                # 即使失败也返回空列表，避免影响其他采集任务
                return []


def _standardize_kline_data(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """标准化K线数据格式（日线、周线、月线）"""
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
    
    # 转换为字典，并将numpy类型转换为Python原生类型
    result = df.to_dict(orient="records")
    # 确保所有数值都是Python原生类型，避免序列化错误
    import numpy as np
    for item in result:
        for key, value in item.items():
            if isinstance(value, np.integer):
                item[key] = int(value)
            elif isinstance(value, np.floating):
                item[key] = float(value)
            elif isinstance(value, np.bool_):
                item[key] = bool(value)
            elif pd.isna(value):
                item[key] = None
    
    return result


def _standardize_kline_data_minute(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """标准化分钟/小时级别K线数据格式"""
    import numpy as np
    if df.empty:
        logger.warning(f"小时K线DataFrame为空 {code}")
        return []
    
    logger.info(f"开始标准化小时K线数据 {code}: DataFrame shape={df.shape}, columns={list(df.columns)}")
    
    # 标准化字段名（分钟级别的字段名可能不同）
    rename_map = {}
    
    # 查找时间列（支持中英文：时间/day/datetime/date/time/trade_time）
    time_col = None
    for col in ["day", "时间", "datetime", "date", "time", "trade_time"]:
        if col in df.columns:
            time_col = col
            logger.info(f"找到时间列 {code}: {col}")
            break
    
    # 如果没找到时间列，检查是否有索引是时间类型
    if not time_col:
        if isinstance(df.index, pd.DatetimeIndex):
            # 索引是时间，将索引转为列
            df = df.reset_index()
            if len(df.columns) > 0:
                time_col = df.columns[0]
                logger.info(f"使用索引作为时间列 {code}: {time_col}")
        elif len(df.columns) > 0:
            # 尝试第一列是否是时间
            first_col = df.columns[0]
            try:
                # 检查第一列是否看起来像时间
                sample_val = df[first_col].iloc[0] if len(df) > 0 else None
                if isinstance(sample_val, str) and ('-' in sample_val or ':' in sample_val):
                    time_col = first_col
                    logger.info(f"使用第一列作为时间列 {code}: {first_col}")
            except Exception:
                pass
    
    if time_col and time_col != "time":
        rename_map[time_col] = "time"
    
    # 查找价格和成交量列（支持中英文列名，优先英文，因为akshare返回的是英文列名）
    price_volume_map = {
        # 英文列名（akshare实际返回的）
        "open": "open",
        "close": "close",
        "high": "high",
        "low": "low",
        "volume": "volume",
        "amount": "amount",
        # 中文列名（兼容）
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount"
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
    
    # 处理时间字段（分钟级别通常包含日期和时间）
    # 注意：如果day列被重命名为time，这里应该检查time列
    if "time" in df.columns:
        # 将时间转换为datetime对象
        df["time"] = pd.to_datetime(df["time"], errors='coerce')
        # 格式化为 YYYY-MM-DD HH:MM:SS 格式（如果有效）
        df["time"] = df["time"].dt.strftime('%Y-%m-%d %H:%M:%S')
        # date字段使用日期部分（YYYY-MM-DD）
        df["date"] = df["time"].apply(lambda x: x[:10] if x and isinstance(x, str) and len(x) >= 10 else "")
    elif "datetime" in df.columns:
        # 如果字段名是datetime而不是time
        df["time"] = pd.to_datetime(df["datetime"], errors='coerce')
        df["time"] = df["time"].dt.strftime('%Y-%m-%d %H:%M:%S')
        df["date"] = df["time"].apply(lambda x: x[:10] if x and isinstance(x, str) and len(x) >= 10 else "")
    else:
        # 如果没有时间字段，尝试使用索引
        if isinstance(df.index, pd.DatetimeIndex):
            df["time"] = df.index.strftime('%Y-%m-%d %H:%M:%S')
            df["date"] = df.index.strftime('%Y-%m-%d')
        elif len(df) > 0:
            # 尝试从第一列获取时间（某些情况下时间在第一列）
            first_col = df.columns[0]
            try:
                df["time"] = pd.to_datetime(df[first_col], errors='coerce')
                df["time"] = df["time"].dt.strftime('%Y-%m-%d %H:%M:%S')
                df["date"] = df["time"].apply(lambda x: x[:10] if x and isinstance(x, str) and len(x) >= 10 else "")
            except Exception:
                df["time"] = df.index.astype(str)
                df["date"] = ""
        else:
            df["time"] = ""
            df["date"] = ""
    
    df["code"] = code
    df["market"] = "A"
    
    # 检查是否有必要的数据列
    required_cols = ["open", "close"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.warning(f"小时K线数据标准化失败 {code}: 缺少必要列 {missing_cols}")
        return []
    
    # 过滤掉无效数据（时间或价格为空）
    initial_count = len(df)
    df = df.dropna(subset=["open", "close"], how="any")
    
    # 如果没有time列但有date列，可以保留；如果都没有，至少要有价格数据
    if "time" not in df.columns and "date" not in df.columns:
        logger.warning(f"小时K线数据缺少时间字段 {code}，尝试使用索引")
        if len(df) > 0:
            # 使用索引作为时间（假设索引是时间）
            df["time"] = df.index.astype(str)
            df["date"] = df.index.astype(str).str[:10] if hasattr(df.index, 'str') else ""
    
    # 过滤掉time为空的记录（如果有time列）
    if "time" in df.columns:
        df = df[df["time"].notna() & (df["time"] != "")]
    
    final_count = len(df)
    if final_count < initial_count:
        logger.debug(f"小时K线数据过滤: {code}，从{initial_count}条过滤到{final_count}条")
    
    if final_count == 0:
        logger.warning(f"小时K线数据标准化后无有效数据 {code}")
        return []
    
    # 转换为字典，并将numpy类型转换为Python原生类型
    result = df.to_dict(orient="records")
    # 确保所有数值都是Python原生类型，避免序列化错误
    for item in result:
        for key, value in item.items():
            if isinstance(value, np.integer):
                item[key] = int(value)
            elif isinstance(value, np.floating):
                item[key] = float(value)
            elif isinstance(value, np.bool_):
                item[key] = bool(value)
            elif pd.isna(value):
                item[key] = None
    
    return result


def _fetch_kline_source1(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源1: stock_zh_a_hist (主数据源) 或 stock_zh_a_minute (分钟/小时级别)"""
    try:
        # 如果是小时级别（1h, hourly），使用stock_zh_a_minute接口
        if period in ['1h', 'hourly', '60']:
            # 确定股票代码的市场前缀
            # 深市：000、001、002、003开头（主板/中小板），300开头（创业板）
            # 沪市：600、601、603、605开头（主板），688开头（科创板）
            code_str = str(code).strip()
            symbol_variants = []
            
            # 根据代码前缀判断市场
            if code_str.startswith(('000', '001', '002', '003', '300')):
                # 深市：sz前缀
                symbol_variants = [f"sz{code_str}", code_str]
            elif code_str.startswith(('600', '601', '603', '605', '688')):
                # 沪市：sh前缀
                symbol_variants = [f"sh{code_str}", code_str]
            else:
                # 未知，尝试所有可能
                symbol_variants = [f"sz{code_str}", f"sh{code_str}", code_str]
            
            # 依次尝试不同的symbol格式和复权类型
            last_error = None
            # 先尝试不复权，如果失败再尝试其他复权类型
            adjust_variants = [adjust] if adjust else ['', 'qfq', 'hfq']
            
            for symbol_variant in symbol_variants:
                for adjust_variant in adjust_variants:
                    try:
                        logger.info(f"尝试获取小时K线数据 {code} (symbol={symbol_variant}, period=60, adjust={adjust_variant})")
                        # 按照akshare官方用法：symbol需要市场前缀，period="60"（字符串），adjust参数也需要传递
                        df = ak.stock_zh_a_minute(symbol=symbol_variant, period="60", adjust=adjust_variant)
                    
                        if df.empty:
                            logger.info(f"小时K线数据为空 {code} (symbol={symbol_variant}, adjust={adjust_variant})")
                            continue
                        
                        # 检查DataFrame是否有必要的列（支持中英文列名）
                        # akshare返回的列名是英文：day, open, high, low, close, volume
                        has_chinese_cols = all(col in df.columns for col in ['时间', '开盘', '收盘', '最高', '最低'])
                        has_english_cols = all(col in df.columns for col in ['open', 'close', 'high', 'low'])
                        
                        if not has_chinese_cols and not has_english_cols:
                            logger.warning(f"小时K线数据缺少必要列 {code} (symbol={symbol_variant}, adjust={adjust_variant}): 实际列={list(df.columns)}")
                            continue
                        
                        if has_english_cols:
                            logger.info(f"小时K线数据使用英文列名 {code} (symbol={symbol_variant}, adjust={adjust_variant})")
                        else:
                            logger.info(f"小时K线数据使用中文列名 {code} (symbol={symbol_variant}, adjust={adjust_variant})")
                        
                        # 小时级别的数据处理
                        logger.info(f"DataFrame信息 {code}: shape={df.shape}, columns={list(df.columns)}")
                        result = _standardize_kline_data_minute(df, code)
                        if result:
                            logger.info(f"✅ 成功获取小时K线数据 {code} (symbol={symbol_variant}, adjust={adjust_variant}): {len(result)}条")
                            return result
                        else:
                            logger.warning(f"小时K线数据标准化后为空 {code} (symbol={symbol_variant}, adjust={adjust_variant})")
                            continue
                            
                    except Exception as e:
                        error_msg = str(e)
                        last_error = error_msg
                        logger.warning(f"获取小时K线数据异常 {code} (symbol={symbol_variant}, adjust={adjust_variant}): {error_msg}")
                        continue
            
            # 所有尝试都失败
            logger.warning(f"获取小时K线数据失败 {code}: 所有symbol和adjust组合都失败，最后错误: {last_error}")
            return []
        else:
            # 日线、周线、月线使用原有接口
            # 添加超时控制，避免akshare API调用阻塞
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    ak.stock_zh_a_hist,
                    symbol=code,
                    period=period,
                    adjust=adjust,
                    start_date=start_date,
                    end_date=end_date
                )
                try:
                    df = future.result(timeout=25)  # 增加到25秒超时，给网络更多时间
                    if df is None:
                        logger.debug(f"akshare返回None {code}")
                        return []
                    if df.empty:
                        logger.debug(f"akshare返回空DataFrame {code}")
                        return []
                    result = _standardize_kline_data(df, code)
                    if not result:
                        logger.debug(f"标准化后数据为空 {code}")
                    return result
                except concurrent.futures.TimeoutError:
                    logger.warning(f"akshare API调用超时 {code}（25秒），尝试返回数据库已有数据")
                    # 超时后尝试返回数据库已有数据
                    try:
                        from common.db import get_kline_from_db
                        from datetime import datetime
                        # 尝试从数据库获取已有数据
                        existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), period)
                        if existing_data and len(existing_data) > 0:
                            logger.info(f"超时后返回数据库已有数据: {code}, {len(existing_data)}条")
                            return existing_data
                        else:
                            logger.warning(f"数据库无数据，返回空: {code}")
                            return []
                    except Exception as db_error:
                        logger.warning(f"从数据库获取数据失败 {code}: {db_error}")
                        return []
                except Exception as e:
                    logger.warning(f"akshare API调用失败 {code}: {e}，尝试返回数据库已有数据")
                    # 失败后也尝试返回数据库已有数据
                    try:
                        from common.db import get_kline_from_db
                        from datetime import datetime
                        existing_data = get_kline_from_db(code, start_date, end_date or datetime.now().strftime("%Y%m%d"), period)
                        if existing_data and len(existing_data) > 0:
                            logger.info(f"API失败后返回数据库已有数据: {code}, {len(existing_data)}条")
                            return existing_data
                        else:
                            return []
                    except Exception as db_error:
                        logger.warning(f"从数据库获取数据失败 {code}: {db_error}")
                        return []
    except Exception as e:
        logger.warning(f"数据源1失败 {code}: {str(e)[:200]}")
        return []


def _fetch_kline_source2(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源2: 尝试不同的参数组合 (备用数据源1)"""
    try:
        # 如果是小时级别，直接返回空（数据源1已处理）
        if period in ['1h', 'hourly', '60']:
            return []
        
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
        # 如果是小时级别，直接返回空（数据源1已处理）
        if period in ['1h', 'hourly', '60']:
            return []
        
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
    """数据源4: Tushare 备用数据源（当akshare失败时自动切换）"""
    try:
        # 小时级别数据 Tushare 不支持
        if period in ['1h', 'hourly', '60']:
            return []
        
        # 只支持日线数据
        if period not in ['daily', 'day', '']:
            logger.debug(f"Tushare 数据源暂不支持 {period} 周期")
            return []
        
        # 尝试导入 Tushare 模块
        try:
            from market_collector.tushare_source import fetch_daily_kline_tushare, get_tushare_api
        except ImportError:
            logger.debug("Tushare 模块未安装")
            return []
        
        # 检查 Tushare API 是否可用
        api = get_tushare_api()
        if not api:
            logger.debug("Tushare API 未配置或初始化失败")
            return []
        
        # 调用 Tushare 获取日线数据
        logger.info(f"尝试使用 Tushare 获取K线数据: {code}")
        result = fetch_daily_kline_tushare(code, start_date, end_date, limit=1000)
        
        if result and len(result) > 0:
            # 转换为标准格式（Tushare 返回的格式与标准格式略有不同）
            standardized = []
            for item in result:
                standardized.append({
                    'date': item.get('date', ''),
                    'time': item.get('date', ''),  # 日线数据 time 等于 date
                    'open': item.get('open', 0),
                    'high': item.get('high', 0),
                    'low': item.get('low', 0),
                    'close': item.get('close', 0),
                    'volume': item.get('volume', 0),
                    'amount': item.get('amount', 0),
                    'code': code,
                    'market': 'A'
                })
            logger.info(f"Tushare 获取K线数据成功: {code}, {len(standardized)}条")
            return standardized
        
        return []
    except Exception as e:
        logger.debug(f"数据源4(Tushare)失败 {code}: {str(e)[:100]}")
        return []


def _fetch_kline_source5(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源5: 新浪财经 K线数据源"""
    try:
        from market_collector.sina_source import fetch_sina_kline
        
        logger.info(f"尝试使用 新浪财经 获取K线数据: {code}")
        result = fetch_sina_kline(code, period, adjust, start_date, end_date, limit=1000)
        
        if result and len(result) > 0:
            logger.info(f"新浪财经 获取K线数据成功: {code}, {len(result)}条")
            return result
        
        return []
    except ImportError:
        logger.debug("新浪财经模块导入失败")
        return []
    except Exception as e:
        logger.debug(f"数据源5(新浪财经)失败 {code}: {str(e)[:100]}")
        return []


def _fetch_kline_source6(code: str, period: str, adjust: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """数据源6: Easyquotation K线数据源（基于腾讯数据）"""
    try:
        from market_collector.easyquotation_source import fetch_easyquotation_kline
        
        logger.info(f"尝试使用 Easyquotation 获取K线数据: {code}")
        result = fetch_easyquotation_kline(code, period, adjust, start_date, end_date, limit=1000)
        
        if result and len(result) > 0:
            logger.info(f"Easyquotation 获取K线数据成功: {code}, {len(result)}条")
            return result
        
        return []
    except ImportError:
        logger.debug("Easyquotation模块导入失败")
        return []
    except Exception as e:
        logger.debug(f"数据源6(Easyquotation)失败 {code}: {str(e)[:100]}")
        return []


def fetch_a_stock_kline(
    code: str,
    period: str = "daily",
    adjust: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    force_full_refresh: bool = False,
    skip_db: bool = False,  # 新增参数：是否跳过数据库操作
    return_source: bool = False,  # 新增参数：是否返回数据源名称
) -> List[Dict[str, Any]] | tuple:
    """获取A股K线数据（增量获取策略）
    
    策略说明：
    1. 首次获取：从数据库查询最新日期，如果没有则全量获取并存储
    2. 增量更新：只获取数据库最新日期之后的数据并追加
    3. 返回数据：从数据库查询完整数据（包含历史+增量）
    
    Args:
        code: 股票代码（如：600519）
        period: 周期（daily, weekly, monthly, 1h/hourly - 1小时K线）
        adjust: 复权类型（"", "qfq", "hfq"）
        start_date: 开始日期 YYYYMMDD（用于查询时的过滤，不影响增量逻辑）
        end_date: 结束日期 YYYYMMDD（默认今天）
        force_full_refresh: 是否强制全量刷新（用于初始化或修复数据）
    
    Returns:
        K线数据列表
    """
    from datetime import datetime, timedelta
    from common.db import get_kline_latest_date, get_kline_earliest_date, save_kline_data, get_kline_from_db
    from common.logger import get_logger
    
    logger = get_logger(__name__)
    
    # 设置默认结束日期
    default_end = end_date or datetime.now().strftime("%Y%m%d")
    if default_end and "-" in default_end:
        default_end = default_end.replace("-", "")
    
    # 小时级别（1h, hourly）数据通常只能获取最近的数据，不使用增量策略
    is_hourly = period in ['1h', 'hourly', '60']
    
    # 查询数据库中该股票的最新日期和最早日期（小时级别不使用增量策略）
    # 添加超时和错误处理，避免ClickHouse连接失败导致阻塞
    # 如果 skip_db=True，完全跳过数据库操作
    db_latest_date = None
    db_earliest_date = None
    if skip_db:
        logger.debug(f"跳过数据库查询 {code}，直接从数据源获取")
    elif not (force_full_refresh or is_hourly):
        # 在采集模式下，如果数据库为空，直接全量获取，避免查询冲突
        try:
            # 快速检查数据库是否有数据（使用独立连接，避免冲突）
            from common.db import get_clickhouse
            from common.config import settings
            from clickhouse_driver import Client
            temp_client = Client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_db,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                connect_timeout=5,  # 5秒连接超时
                send_receive_timeout=10  # 10秒查询超时（高并发时需要更长时间）
            )
            # 设置线程限制，降低CPU占用
            try:
                temp_client.execute("SET max_threads = 4")
                temp_client.execute("SET max_final_threads = 2")
                temp_client.execute("SET max_parsing_threads = 2")
            except Exception:
                pass  # 忽略设置失败，不影响查询
            try:
                result = temp_client.execute("SELECT COUNT(*) FROM kline WHERE code = %(code)s AND period = %(period)s", {'code': code, 'period': period if period != 'daily' else 'daily'})
                has_data = result[0][0] > 0 if result else False
                if has_data:
                    db_latest_date = get_kline_latest_date(code, period)
                    db_earliest_date = get_kline_earliest_date(code, period)
            finally:
                temp_client.disconnect()
        except Exception as e:
            logger.debug(f"查询K线日期失败 {code}: {e}，将全量获取")
    
    # 确定需要从数据源获取的日期范围
    need_fetch_earlier = False
    if db_latest_date and not is_hourly:
        # 数据库中有数据，检查是否需要获取更早的数据
        # 如果前端请求的start_date早于数据库中的最早日期，需要获取更早的数据
        if start_date and db_earliest_date:
            try:
                request_start = start_date.replace("-", "") if "-" in start_date else start_date
                if len(request_start) == 8:
                    request_start_dt = datetime.strptime(request_start, "%Y%m%d")
                    db_earliest_dt = datetime.strptime(db_earliest_date, "%Y%m%d")
                    if request_start_dt < db_earliest_dt:
                        # 需要获取更早的数据
                        need_fetch_earlier = True
                        logger.info(f"请求的起始日期({request_start})早于数据库最早日期({db_earliest_date})，需要获取更早数据: {code}")
            except Exception as e:
                logger.warning(f"比较日期失败 {code}: {e}")
        
        # 获取增量（从最新日期的下一天开始）
        try:
            latest_dt = datetime.strptime(db_latest_date, "%Y%m%d")
            increment_start_dt = latest_dt + timedelta(days=1)
            increment_start = increment_start_dt.strftime("%Y%m%d")
            
            # 如果增量开始日期已经超过结束日期，说明数据已是最新
            if increment_start > default_end and not need_fetch_earlier:
                logger.debug(f"K线数据已是最新 {code}，尝试从数据库返回")
                try:
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(get_kline_from_db, code, start_date, default_end, period)
                        try:
                            db_data = future.result(timeout=8)  # 8秒超时（高并发时需要更长时间）
                            if db_data and len(db_data) > 0:
                                # 数据已是最新，返回数据库缓存（不返回数据源名称，让调用方知道是缓存）
                                return (db_data, None) if return_source else db_data
                        except concurrent.futures.TimeoutError:
                            logger.debug(f"从数据库获取K线数据超时 {code}，将从数据源获取")
                        except Exception as e:
                            logger.debug(f"从数据库获取K线数据失败 {code}: {e}，将从数据源获取")
                except Exception as e:
                    logger.debug(f"从数据库获取K线数据异常 {code}: {e}，将从数据源获取")
                # 如果数据库获取失败，继续从数据源获取
        except Exception as e:
            logger.warning(f"解析数据库最新日期失败 {code}: {e}，将全量获取")
            db_latest_date = None
    
    # 确定获取开始日期
    # 如果 skip_db=True，直接全量获取，不依赖数据库
    if skip_db:
        fetch_start = start_date or "20200101"  # 默认从2020年开始
        fetch_mode = "全量（跳过数据库）"
    elif db_latest_date and not is_hourly and not need_fetch_earlier:
        # 增量获取：从数据库最新日期的下一天开始（小时级别不使用增量）
        fetch_start = increment_start
        fetch_mode = "增量"
    else:
        # 全量获取
        if is_hourly:
            # 小时级别数据：不使用start_date和end_date（stock_zh_a_minute接口不支持这些参数）
            fetch_start = ""
            fetch_mode = "全量（小时级别）"
        elif start_date:
            # 使用前端传递的start_date（可能是10年）
            fetch_start = start_date.replace("-", "") if "-" in start_date else start_date
            fetch_mode = "全量（按请求日期）"
        else:
            # 默认获取最近2年（约500根K线）
            two_years_ago = datetime.now() - timedelta(days=2*365)
            fetch_start = two_years_ago.strftime("%Y%m%d")
            fetch_mode = "全量"
    
    logger.info(f"开始{fetch_mode}获取K线数据 {code}: {fetch_start} 到 {default_end}")
    
    # 获取配置的数据源选择
    from common.runtime_config import get_runtime_config
    config = get_runtime_config()
    preferred_source = config.kline_data_source or "auto"
    
    # 定义数据源列表（按优先级排序）
    if preferred_source == "akshare":
        # 仅使用 AKShare
        data_sources = [
            ("AKShare数据源1", _fetch_kline_source1),
            ("AKShare数据源2", _fetch_kline_source2),
            ("AKShare数据源3", _fetch_kline_source3),
        ]
    elif preferred_source == "tushare":
        # 仅使用 Tushare
        data_sources = [
            ("Tushare数据源", _fetch_kline_source4),
        ]
    elif preferred_source == "sina":
        # 仅使用 新浪财经
        data_sources = [
            ("新浪财经数据源", _fetch_kline_source5),
        ]
    elif preferred_source == "easyquotation":
        # 仅使用 Easyquotation（腾讯数据）
        data_sources = [
            ("Easyquotation数据源", _fetch_kline_source6),
        ]
    else:
        # 自动切换（默认）：优先 AKShare，失败时依次切换到其他数据源
        data_sources = [
            ("AKShare数据源1", _fetch_kline_source1),
            ("AKShare数据源2", _fetch_kline_source2),
            ("AKShare数据源3", _fetch_kline_source3),
            ("Tushare数据源", _fetch_kline_source4),
            ("新浪财经数据源", _fetch_kline_source5),
            ("Easyquotation数据源", _fetch_kline_source6),
        ]
    
    # 依次尝试各个数据源获取增量数据
    new_kline_data = []
    used_source = None  # 记录实际使用的数据源
    last_error = None  # 记录最后一个错误
    for source_name, fetch_func in data_sources:
        try:
            logger.debug(f"尝试使用{source_name}{fetch_mode}获取K线数据: {code}")
            # 小时级别不使用start_date和end_date参数
            if is_hourly:
                result = fetch_func(code, period, adjust, "", "")
            else:
                result = fetch_func(code, period, adjust, fetch_start, default_end)
            
            if result and len(result) > 0:
                new_kline_data = result
                used_source = source_name  # 记录成功使用的数据源
                logger.info(f"A股K线数据获取成功({source_name}, {fetch_mode}): {code}, 新增{len(result)}条")
                break
            else:
                last_error = f"{source_name}返回空数据"
                logger.debug(f"{source_name}返回空数据: {code}")
        except Exception as e:
            last_error = str(e)[:150]
            logger.warning(f"{source_name}获取K线数据失败 {code}: {last_error}")
            continue
    
    # 如果所有数据源都失败，记录警告（只记录前100个，避免日志过多）
    import random
    if not new_kline_data and not db_latest_date:
        if random.random() < 0.02:  # 2%的概率记录，避免日志过多
            logger.warning(f"所有数据源获取K线数据失败 {code}，最后错误: {last_error}")
    
    # 如果获取到了新数据，保存到数据库
    # 优化：同步保存，确保数据不丢失（独立连接已优化，性能影响较小）
    # 如果 skip_db=True，跳过保存
    if new_kline_data and not skip_db:
        try:
            # 直接同步保存（使用独立连接，不会阻塞其他线程）
            save_kline_data(new_kline_data, period)
            logger.debug(f"K线数据保存成功: {code}, {len(new_kline_data)}条")
        except Exception as e:
            logger.warning(f"保存K线数据失败 {code}: {e}，继续执行")
    
    # 从数据库查询完整数据返回（包含历史+新增）
    # 如果指定了start_date，按start_date过滤；否则返回所有数据
    # 如果 skip_db=True，直接返回新获取的数据，不查询数据库
    if skip_db:
        if new_kline_data:
            logger.info(f"K线数据获取完成（跳过数据库）: {code}, 共{len(new_kline_data)}条")
            return (new_kline_data, used_source) if return_source else new_kline_data
        else:
            logger.warning(f"K线数据获取失败（跳过数据库）: {code}")
            return ([], None) if return_source else []
    
    query_start = start_date.replace("-", "") if start_date and "-" in start_date else (start_date or None)
    full_data = None
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(get_kline_from_db, code, query_start, default_end, period)
            try:
                full_data = future.result(timeout=8)  # 8秒超时（增加超时时间避免高并发时失败）
            except concurrent.futures.TimeoutError:
                logger.debug(f"从数据库查询完整K线数据超时 {code}，返回新获取的数据")
            except Exception as e:
                logger.debug(f"从数据库查询完整K线数据失败 {code}: {e}，返回新获取的数据")
    except Exception as e:
        logger.debug(f"从数据库查询完整K线数据异常 {code}: {e}，返回新获取的数据")
    
    if full_data and len(full_data) > 0:
        logger.info(f"K线数据查询完成: {code}, 共{len(full_data)}条（含历史数据）")
        return (full_data, used_source) if return_source else full_data
    elif new_kline_data:
        # 如果数据库查询失败但新数据存在，返回新数据
        logger.warning(f"从数据库查询失败，返回新获取的数据: {code}")
        return (new_kline_data, used_source) if return_source else new_kline_data
    else:
        # 所有数据源都失败，尝试从数据库返回已有数据（即使不完整）
        if not force_full_refresh:
            try:
                existing_data = get_kline_from_db(code, query_start, default_end, period)
                if existing_data and len(existing_data) > 0:
                    logger.info(f"数据源获取失败，返回数据库已有数据（可能不完整）: {code}, {len(existing_data)}条（{period}）")
                    return (existing_data, None) if return_source else existing_data
            except Exception as e:
                logger.debug(f"从数据库获取已有数据失败 {code}: {e}")
        
        # 对于小时数据，提供更详细的错误信息
        if is_hourly:
            logger.error(f"获取小时K线数据失败 {code}: 所有数据源均失败且数据库无数据。可能原因：1) 该股票代码不支持小时数据；2) 数据源接口暂时不可用；3) 股票可能停牌或退市")
        else:
            logger.error(f"所有数据源均失败且数据库无数据: {code}（{period}）")
        return ([], None) if return_source else []



def fetch_a_stock_spot_with_source(source: str = "auto", max_retries: int = 2) -> tuple:
    """
    根据配置的数据源获取A股实时行情
    
    Args:
        source: 数据源选择
            - "auto": 自动切换，按顺序尝试 Easyquotation → 新浪 → AKShare
            - "akshare": 仅使用AKShare
            - "sina": 仅使用新浪财经
            - "easyquotation": 仅使用Easyquotation
        max_retries: 每个数据源的最大重试次数（单源模式下使用）
    
    Returns:
        (result, source_name): 行情数据列表和实际使用的数据源名称
    """
    sources_order = []
    
    if source == "auto":
        # 优先级：Easyquotation > 新浪 > AKShare（按速度排序）
        sources_order = ["easyquotation", "sina", "akshare"]
    elif source == "akshare":
        sources_order = ["akshare"]
    elif source == "sina":
        sources_order = ["sina"]
    elif source == "easyquotation":
        sources_order = ["easyquotation"]
    else:
        sources_order = ["easyquotation", "sina", "akshare"]
    
    last_error = None
    
    # auto模式不重试，失败直接切换下一个源
    for src in sources_order:
        try:
            if src == "akshare":
                logger.info("[实时行情] 尝试使用 AKShare(东方财富) 数据源...")
                result = fetch_a_stock_spot(max_retries=1)
                if result and len(result) > 0:
                    logger.info(f"[实时行情] AKShare 获取成功: {len(result)}只股票")
                    return result, "AKShare(东方财富)"
                    
            elif src == "sina":
                logger.info("[实时行情] 尝试使用 新浪财经 数据源...")
                from market_collector.sina_source import fetch_sina_stock_spot
                result = fetch_sina_stock_spot(max_retries=1)
                if result and len(result) > 0:
                    # 保存到Redis
                    _save_spot_to_redis(result, "A")
                    logger.info(f"[实时行情] 新浪财经 获取成功: {len(result)}只股票")
                    return result, "新浪财经"
                    
            elif src == "easyquotation":
                logger.info("[实时行情] 尝试使用 Easyquotation 数据源...")
                from market_collector.easyquotation_source import fetch_easyquotation_stock_spot
                result = fetch_easyquotation_stock_spot(max_retries=1)
                if result and len(result) > 0:
                    # 保存到Redis
                    _save_spot_to_redis(result, "A")
                    logger.info(f"[实时行情] Easyquotation 获取成功: {len(result)}只股票")
                    return result, "Easyquotation"
                    
        except Exception as e:
            last_error = e
            logger.warning(f"[实时行情] {src} 数据源失败: {e}")
            continue
    
    # 所有数据源都失败
    logger.error(f"[实时行情] 所有数据源均失败: {last_error}")
    return [], None


def _save_spot_to_redis(result: List[Dict[str, Any]], market: str):
    """保存实时行情到Redis"""
    key = f"market:{market.lower()}:spot"
    set_json(key, result, ex=30 * 24 * 3600)
    get_redis().set(f"market:{market.lower()}:time", datetime.now().isoformat(), ex=30 * 24 * 3600)
    
    # 通过SSE广播市场数据更新
    try:
        from market.service.sse import broadcast_market_update
        broadcast_market_update(market.lower())
    except Exception as e:
        logger.debug(f"SSE广播{market}股数据失败（不影响数据采集）: {e}")
