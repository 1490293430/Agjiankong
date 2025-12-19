"""
港股行情采集
"""
import akshare as ak
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Set
import time
from common.redis import set_json, get_redis, get_json
from common.logger import get_logger

logger = get_logger(__name__)


def fetch_hk_stock_spot(max_retries: int = 3) -> List[Dict[str, Any]]:
    """获取港股实时行情
    
    Args:
        max_retries: 最大重试次数
    """
    for attempt in range(max_retries):
        try:
            df = ak.stock_hk_spot_em()
            
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
            
            # 将旧快照备份一份，方便需要时回溯
            if old_data:
                set_json("market:hk:spot_prev", old_data, ex=3600)
            
            # 2. 写入新的全量快照（前端HTTP/WS读取的主数据）
            set_json("market:hk:spot", result, ex=1800)  # 30分钟过期
            get_redis().set("market:hk:time", datetime.now().isoformat(), ex=1800)
            
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


def fetch_hk_stock_kline(code: str, period: str = "daily", adjust: str = "") -> List[Dict[str, Any]]:
    """获取港股K线数据
    
    Args:
        code: 股票代码（如：00700）
        period: 周期（daily, weekly, monthly）
        adjust: 复权类型
    """
    try:
        # AKShare港股历史数据接口
        df = ak.stock_hk_hist(
            symbol=code,
            period=period,
            start_date="",
            end_date="",
            adjust=adjust if adjust else ""
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
                "成交额": "amount"
            })
        
        # 转换数据类型
        numeric_columns = ["open", "high", "low", "close", "volume", "amount"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df["code"] = code
        df["market"] = "HK"
        
        result = df.to_dict(orient="records")
        logger.info(f"港股K线数据获取成功: {code}, 共{len(result)}条")
        return result
        
    except Exception as e:
        logger.error(f"港股K线数据获取失败 {code}: {e}", exc_info=True)
        return []

