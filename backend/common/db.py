"""
数据库连接模块（ClickHouse）
"""
from clickhouse_driver import Client
from datetime import datetime, timedelta
from common.config import settings
from common.logger import get_logger
from common.runtime_config import get_runtime_config
from typing import List, Dict, Any

logger = get_logger(__name__)

_client: Client = None


def _create_clickhouse_client() -> Client:
    """创建独立的ClickHouse连接（用于多线程环境，避免连接冲突）
    
    Returns:
        ClickHouse Client实例
    """
    client = Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=5,
        send_receive_timeout=10
    )
    
    # 设置线程限制，降低CPU占用
    try:
        client.execute("SET max_threads = 4")
        client.execute("SET max_final_threads = 2")
        client.execute("SET max_parsing_threads = 2")
    except Exception:
        pass  # 忽略设置失败，不影响使用
    
    return client


def get_clickhouse() -> Client:
    """获取ClickHouse连接"""
    global _client
    
    if _client is None:
        try:
            _client = Client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_db,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                connect_timeout=5,  # 5秒连接超时
                send_receive_timeout=10  # 10秒发送接收超时
            )
            # 测试连接
            _client.execute("SELECT 1")
            
            # 设置线程限制，降低CPU占用
            try:
                _client.execute("SET max_threads = 4")
                _client.execute("SET max_final_threads = 2")
                _client.execute("SET max_parsing_threads = 2")
                logger.info("ClickHouse线程限制已设置: max_threads=4, max_final_threads=2, max_parsing_threads=2")
            except Exception as e:
                logger.warning(f"设置ClickHouse线程限制失败（不影响使用）: {e}")
            
            logger.info(f"ClickHouse连接成功: {settings.clickhouse_host}:{settings.clickhouse_port}")
        except Exception as e:
            logger.error(f"ClickHouse连接失败: {e}")
            _client = None  # 重置连接，下次重试
            raise
    
    # 如果连接已存在，测试是否仍然有效
    try:
        _client.execute("SELECT 1")
    except Exception as e:
        logger.warning(f"ClickHouse连接已断开，重新连接: {e}")
        _client = None
        return get_clickhouse()  # 递归重试
    
    return _client


def init_tables():
    """初始化数据表"""
    client = None
    try:
        client = _create_clickhouse_client()
        
        # K线表（使用ReplacingMergeTree自动去重，避免频繁DELETE导致mutation堆积）
        client.execute("""
        CREATE TABLE IF NOT EXISTS kline
        (
            code String,
            period String,
            date Date,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            amount Float64,
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, period, date)
        """)
        
        # 检查表结构，确保使用ReplacingMergeTree引擎
        try:
            # 检查表是否存在以及引擎类型
            engine_info = client.execute("""
                SELECT engine, engine_full 
                FROM system.tables 
                WHERE database = %(db)s AND name = 'kline'
            """, {'db': settings.clickhouse_db})
            
            if engine_info and len(engine_info) > 0:
                current_engine = engine_info[0][0]
                if current_engine != 'ReplacingMergeTree':
                    logger.warning(f"⚠️ kline表当前引擎为 {current_engine}，建议改为 ReplacingMergeTree 以避免 mutation 堆积")
                    logger.warning("⚠️ 如需迁移，请执行以下步骤：")
                    logger.warning("   1. 停止数据采集")
                    logger.warning("   2. 执行: ALTER TABLE kline MODIFY ENGINE = ReplacingMergeTree(update_time)")
                    logger.warning("   3. 或重建表（需要先添加 update_time 字段）")
                else:
                    logger.info("✓ kline表已使用ReplacingMergeTree引擎")
            
            # 检查是否有update_time字段
            columns = client.execute("DESCRIBE kline")
            column_names = [col[0] for col in columns]
            
            if "update_time" not in column_names:
                logger.info("检测到kline表缺少update_time字段，正在添加...")
                try:
                    client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS update_time DateTime DEFAULT now()")
                    logger.info("✓ update_time字段添加成功")
                except Exception as e:
                    logger.warning(f"添加update_time字段失败: {e}")
            
            if "period" not in column_names:
                logger.info("检测到kline表缺少period字段，正在添加...")
                try:
                    client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS period String DEFAULT 'daily'")
                    logger.info("✓ period字段添加成功")
                except Exception as e:
                    logger.warning(f"添加period字段失败: {e}")
        except Exception as e:
            logger.debug(f"表结构检查可能失败（表可能不存在）: {e}")
        
        # 技术指标表（存储预计算的指标，每日更新）
        client.execute("""
        CREATE TABLE IF NOT EXISTS indicators
        (
            code String,
            market String,
            date Date,
            -- 均线
            ma5 Float64,
            ma10 Float64,
            ma20 Float64,
            ma60 Float64,
            ma5_trend String,
            ma10_trend String,
            ma20_trend String,
            ma60_trend String,
            -- MACD
            macd_dif Float64,
            macd_dea Float64,
            macd Float64,
            macd_dif_trend String,
            -- RSI
            rsi Float64,
            -- 布林带
            boll_upper Float64,
            boll_middle Float64,
            boll_lower Float64,
            boll_expanding UInt8,
            boll_contracting UInt8,
            -- KDJ
            kdj_k Float64,
            kdj_d Float64,
            kdj_j Float64,
            -- 威廉指标
            williams_r Float64,
            williams_r_prev Float64,
            -- 成交量
            vol_ratio Float64,
            -- 价格突破
            high_20d Float64,
            break_high_20d UInt8,
            -- 当前价格数据
            current_price Float64,
            current_open Float64,
            current_high Float64,
            current_low Float64,
            current_close Float64,
            -- 更新时间
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, market, date)
        """)
        
        # 交易计划表（使用ReplacingMergeTree支持状态更新）
        client.execute("""
        CREATE TABLE IF NOT EXISTS trade_plan
        (
            id UInt64,
            code String,
            name String,
            buy_price Float64,
            sell_price Float64,
            stop_loss Float64,
            confidence Float64,
            reason String,
            created_at DateTime DEFAULT now(),
            status String DEFAULT 'waiting_buy',
            buy_date Nullable(Date),
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (id, code)
        PRIMARY KEY (id)
        """)
        
        # 尝试添加buy_date字段（如果表已存在且没有该字段）
        try:
            client.execute("ALTER TABLE trade_plan ADD COLUMN IF NOT EXISTS buy_date Nullable(Date)")
            client.execute("ALTER TABLE trade_plan ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()")
            # 将旧的status='pending'改为'waiting_buy'
            client.execute("ALTER TABLE trade_plan UPDATE status = 'waiting_buy' WHERE status = 'pending'")
        except Exception as e:
            logger.debug(f"表结构更新可能已存在或失败: {e}")
        
        # 交易结果表
        client.execute("""
        CREATE TABLE IF NOT EXISTS trade_result
        (
            id UInt64,
            plan_id UInt64,
            code String,
            outcome String,
            entry_date Date,
            exit_date Date,
            entry_price Float64,
            exit_price Float64,
            profit_pct Float64,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (code, created_at)
        """)
        
        logger.info("数据表初始化完成")
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_kline_latest_date(code: str, period: str = "daily") -> str | None:
    """查询K线表中某只股票的最新日期
    
    Args:
        code: 股票代码
        period: 周期（daily, weekly, monthly, 1h），用于精确查询指定周期的数据
    
    Returns:
        最新日期的字符串（YYYYMMDD格式），如果不存在则返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 标准化period字段
        period_normalized = period
        if period in ['1h', 'hourly', '60']:
            period_normalized = '1h'
        elif period in ['daily', 'd', 'day']:
            period_normalized = 'daily'
        elif period in ['weekly', 'w', 'week']:
            period_normalized = 'weekly'
        elif period in ['monthly', 'm', 'month']:
            period_normalized = 'monthly'
        
        # 查询该股票指定周期的最大日期
        # 兼容旧表结构（没有period字段）
        # 使用FINAL确保ReplacingMergeTree去重后的结果
        try:
            result = client.execute(
                "SELECT max(date) as max_date FROM kline FINAL WHERE code = %(code)s AND period = %(period)s",
                {'code': code, 'period': period_normalized}
            )
        except Exception:
            # 如果period字段不存在，使用旧查询方式（兼容）
            logger.debug(f"使用兼容模式查询（可能表结构未更新）: {code}")
            result = client.execute(
                "SELECT max(date) as max_date FROM kline FINAL WHERE code = %(code)s",
                {'code': code}
            )
        
        if result and len(result) > 0 and result[0][0]:
            max_date = result[0][0]
            # 转换为YYYYMMDD格式
            if isinstance(max_date, str):
                return max_date.replace("-", "")
            else:
                # Date对象转换为字符串
                return max_date.strftime("%Y%m%d")
        return None
    except Exception as e:
        logger.debug(f"查询K线最新日期失败 {code}: {e}")
        return None
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_kline_earliest_date(code: str, period: str = "daily") -> str | None:
    """查询K线表中某只股票的最早日期
    
    Args:
        code: 股票代码
        period: 周期（daily, weekly, monthly, 1h），用于精确查询指定周期的数据
    
    Returns:
        最早日期的字符串（YYYYMMDD格式），如果不存在则返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 标准化period字段
        period_normalized = period
        if period in ['1h', 'hourly', '60']:
            period_normalized = '1h'
        elif period in ['daily', 'd', 'day']:
            period_normalized = 'daily'
        elif period in ['weekly', 'w', 'week']:
            period_normalized = 'weekly'
        elif period in ['monthly', 'm', 'month']:
            period_normalized = 'monthly'
        
        # 查询该股票指定周期的最小日期
        # 兼容旧表结构（没有period字段）
        # 使用FINAL确保ReplacingMergeTree去重后的结果
        try:
            result = client.execute(
                "SELECT min(date) as min_date FROM kline FINAL WHERE code = %(code)s AND period = %(period)s",
                {'code': code, 'period': period_normalized}
            )
        except Exception:
            # 如果period字段不存在，使用旧查询方式（兼容）
            logger.debug(f"使用兼容模式查询（可能表结构未更新）: {code}")
            result = client.execute(
                "SELECT min(date) as min_date FROM kline FINAL WHERE code = %(code)s",
                {'code': code}
            )
        
        if result and len(result) > 0 and result[0][0]:
            min_date = result[0][0]
            # 转换为YYYYMMDD格式
            if isinstance(min_date, str):
                return min_date.replace("-", "")
            else:
                # Date对象转换为字符串
                return min_date.strftime("%Y%m%d")
        return None
    except Exception as e:
        logger.debug(f"查询K线最早日期失败 {code}: {e}")
        return None
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def save_kline_data(kline_data: List[Dict[str, Any]], period: str = "daily") -> bool:
    """将K线数据保存到ClickHouse数据库
    
    Args:
        kline_data: K线数据列表，每个元素包含 code, date, open, high, low, close, volume, amount
        period: 周期（daily, weekly, monthly, 1h/hourly），用于判断数据保留期限和区分数据类型
    
    Returns:
        是否保存成功
    """
    if not kline_data:
        return True
    
    try:
        # 创建新连接，避免多线程并发冲突
        # 不使用全局连接，因为ClickHouse driver不支持单连接并发查询
        # 优化：减少超时时间，提高响应速度
        client = Client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            connect_timeout=3,  # 减少到3秒
            send_receive_timeout=8  # 减少到8秒
        )
        
        # 设置线程限制，降低CPU占用
        try:
            client.execute("SET max_threads = 4")
            client.execute("SET max_final_threads = 2")
            client.execute("SET max_parsing_threads = 2")
        except Exception:
            pass  # 忽略设置失败，不影响数据保存
        
        # 标准化period字段（统一格式）
        period_normalized = period
        if period in ['1h', 'hourly', '60']:
            period_normalized = '1h'
        elif period in ['daily', 'd', 'day']:
            period_normalized = 'daily'
        elif period in ['weekly', 'w', 'week']:
            period_normalized = 'weekly'
        elif period in ['monthly', 'm', 'month']:
            period_normalized = 'monthly'
        
        # 准备批量插入的数据
        data_to_insert = []
        codes = set()  # 记录所有涉及的股票代码
        for item in kline_data:
            # 确保日期格式正确
            date_str = item.get("date", "")
            if isinstance(date_str, str):
                # 支持YYYY-MM-DD和YYYYMMDD格式
                if "-" in date_str:
                    date_value = date_str
                else:
                    # YYYYMMDD转YYYY-MM-DD
                    if len(date_str) == 8:
                        date_value = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    else:
                        continue
            else:
                continue
            
            code = str(item.get("code", ""))
            codes.add(code)
            
            # 将日期字符串转换为date对象（ClickHouse driver需要date对象）
            try:
                if isinstance(date_value, str):
                    from datetime import datetime as dt
                    date_obj = dt.strptime(date_value, "%Y-%m-%d").date()
                else:
                    date_obj = date_value
            except Exception:
                continue  # 日期格式错误，跳过这条数据
            
            data_to_insert.append((
                code,
                period_normalized,  # 添加period字段
                date_obj,  # 使用date对象而不是字符串
                float(item.get("open", 0) or 0),
                float(item.get("high", 0) or 0),
                float(item.get("low", 0) or 0),
                float(item.get("close", 0) or 0),
                float(item.get("volume", 0) or 0),
                float(item.get("amount", 0) or 0),
            ))
        
        if not data_to_insert:
            return True
        
        # 直接使用INSERT批量插入，ReplacingMergeTree会自动去重
        # 不再使用ALTER TABLE DELETE，避免产生大量mutation导致"Too many unfinished mutations"错误
        # ClickHouse driver的execute方法支持直接传入数据列表
        try:
            client.execute(
                "INSERT INTO kline (code, period, date, open, high, low, close, volume, amount) VALUES",
                data_to_insert
            )
            logger.info(f"K线数据保存成功: {len(data_to_insert)}条（周期: {period_normalized}），涉及{len(codes)}只股票")
        except Exception as insert_error:
            # 如果表还没有period字段（旧表），尝试兼容插入
            error_msg = str(insert_error)
            logger.error(f"保存K线数据失败: {error_msg}", exc_info=True)
            if "period" in error_msg.lower() or "column" in error_msg.lower():
                logger.warning(f"表结构可能未更新，尝试兼容模式插入: {error_msg}")
                # 尝试使用旧格式插入（如果period字段不存在，会自动使用默认值）
                # 但这样不安全，应该先迁移表结构
                raise
            else:
                raise
        
        # 注意：不再在每次保存时清理旧数据，避免产生大量mutation
        # 如需清理旧数据，建议使用定期任务批量处理，或使用TTL自动清理
        # 临时禁用自动清理，避免mutation堆积
        # for code in codes:
        #     cleanup_old_kline_data(code, period_normalized)
        
        # 关闭临时连接
        client.disconnect()
        return True
    except Exception as e:
        logger.error(f"保存K线数据失败: {e}", exc_info=True)
        return False


def cleanup_old_kline_data(code: str, period: str = "daily") -> None:
    """清理超过保留期限的K线数据（按period精确清理）
    
    ⚠️ 警告：此函数会触发 ALTER TABLE DELETE，产生 mutation
    在数据采集频繁时可能导致 "Too many unfinished mutations" 错误
    建议改为定期批量清理或使用 TTL 自动清理
    
    Args:
        code: 股票代码
        period: 周期（daily, weekly, monthly, 1h/hourly）
                - 小时数据（1h/hourly/60）：最多保留1年
                - 其他周期：根据配置的kline_years保留
    
    现在支持按period精确清理，不会误删其他周期的数据。
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 标准化period字段
        period_normalized = period
        if period in ['1h', 'hourly', '60']:
            period_normalized = '1h'
        elif period in ['daily', 'd', 'day']:
            period_normalized = 'daily'
        elif period in ['weekly', 'w', 'week']:
            period_normalized = 'weekly'
        elif period in ['monthly', 'm', 'month']:
            period_normalized = 'monthly'
        
        # 判断是否是小时数据
        is_hourly = period_normalized == '1h'
        
        # 计算保留期限
        if is_hourly:
            # 小时数据最多保留1年
            retention_years = 1.0
            logger.info(f"小时K线数据保留期限: {retention_years}年")
        else:
            # 其他周期根据配置保留
            config = get_runtime_config()
            retention_years = config.kline_years
            logger.info(f"K线数据保留期限（{period_normalized}）: {retention_years}年（来自配置）")
        
        # 计算截止日期（保留N年的数据）
        cutoff_date = datetime.now() - timedelta(days=int(retention_years * 365))
        cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
        
        # 删除超过期限的数据（按period精确删除）
        # 注意：ClickHouse的DELETE操作在MergeTree引擎中需要通过ALTER TABLE实现
        # 使用ALTER TABLE DELETE语法（ClickHouse 1.1.54+支持）
        try:
            # 尝试使用period字段精确删除
            try:
                delete_query = "ALTER TABLE kline DELETE WHERE code = %(code)s AND period = %(period)s AND date < %(date)s"
                client.execute(delete_query, {'code': code, 'period': period_normalized, 'date': cutoff_date_str})
                logger.info(f"清理K线数据: {code}（{period_normalized}），删除{cutoff_date_str}之前的数据，保留{retention_years}年")
            except Exception:
                # 如果period字段不存在，使用兼容删除（会删除该股票所有周期的数据）
                logger.warning(f"表结构未更新（无period字段），使用兼容模式删除（可能误删其他周期数据）: {code}")
                delete_query = "ALTER TABLE kline DELETE WHERE code = %(code)s AND date < %(date)s"
                client.execute(delete_query, {'code': code, 'date': cutoff_date_str})
                logger.info(f"清理K线数据（兼容模式）: {code}，删除{cutoff_date_str}之前的数据，保留{retention_years}年")
        except Exception as delete_error:
            # ClickHouse的DELETE是异步的，可能需要OPTIMIZE TABLE来立即生效
            # 如果ALTER DELETE语法不支持，记录警告日志
            error_msg = str(delete_error)
            if "DELETE" in error_msg.upper() or "syntax" in error_msg.lower():
                logger.warning(f"ClickHouse版本可能不支持ALTER TABLE DELETE，跳过自动清理: {code}")
            else:
                logger.warning(f"自动清理K线数据失败: {code}, 错误: {error_msg}")
            # 注意：ClickHouse的DELETE操作是异步的，数据不会立即删除
            # 可以在后台定期运行 OPTIMIZE TABLE kline FINAL 来立即清理
        
    except Exception as e:
        logger.warning(f"清理K线旧数据失败 {code}: {e}", exc_info=True)
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_kline_from_db(code: str, start_date: str | None = None, end_date: str | None = None, period: str = "daily") -> List[Dict[str, Any]]:
    """从ClickHouse数据库查询K线数据
    
    Args:
        code: 股票代码
        start_date: 开始日期 YYYYMMDD格式
        end_date: 结束日期 YYYYMMDD格式
        period: 周期（daily, weekly, monthly, 1h），用于精确查询指定周期的数据
    
    Returns:
        K线数据列表
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 标准化period字段
        period_normalized = period
        if period in ['1h', 'hourly', '60']:
            period_normalized = '1h'
        elif period in ['daily', 'd', 'day']:
            period_normalized = 'daily'
        elif period in ['weekly', 'w', 'week']:
            period_normalized = 'weekly'
        elif period in ['monthly', 'm', 'month']:
            period_normalized = 'monthly'
        
        # 构建查询条件（使用字典格式参数）
        where_conditions = ["code = %(code)s"]
        params = {'code': code}
        
        # 添加period条件（如果表有period字段）
        try:
            # 先尝试查询包含period字段
            where_conditions.append("period = %(period)s")
            params['period'] = period_normalized
            use_period = True
        except Exception:
            use_period = False
        
        if start_date:
            # 转换为YYYY-MM-DD格式
            if len(start_date) == 8 and "-" not in start_date:
                start_date_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            else:
                start_date_str = start_date.replace("-", "")
                if len(start_date_str) == 8:
                    start_date_str = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
                else:
                    start_date_str = start_date
            where_conditions.append("date >= %(start_date)s")
            params['start_date'] = start_date_str
        
        if end_date:
            # 转换为YYYY-MM-DD格式
            if len(end_date) == 8 and "-" not in end_date:
                end_date_str = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
            else:
                end_date_str = end_date.replace("-", "")
                if len(end_date_str) == 8:
                    end_date_str = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
                else:
                    end_date_str = end_date
            where_conditions.append("date <= %(end_date)s")
            params['end_date'] = end_date_str
        
        # 尝试查询（包含period字段）
        # 使用FINAL确保ReplacingMergeTree去重后的结果（注意：FINAL有性能开销，但能保证数据一致性）
        try:
            query = f"""
                SELECT code, period, date, open, high, low, close, volume, amount
                FROM kline FINAL
                WHERE {' AND '.join(where_conditions)}
                ORDER BY date ASC
            """
            result = client.execute(query, params)
            has_period = True
        except Exception:
            # 如果period字段不存在，使用兼容查询
            logger.debug(f"使用兼容模式查询（可能表结构未更新）: {code}")
            if use_period:
                # 移除period条件
                where_conditions = [c for c in where_conditions if 'period' not in c]
                params.pop('period', None)
            query = f"""
                SELECT code, date, open, high, low, close, volume, amount
                FROM kline FINAL
                WHERE {' AND '.join(where_conditions)}
                ORDER BY date ASC
            """
            result = client.execute(query, params)
            has_period = False
        
        # 转换为字典列表
        kline_data = []
        for row in result:
            if has_period:
                # 新格式：包含period字段
                date_str = row[2].strftime("%Y-%m-%d") if hasattr(row[2], 'strftime') else str(row[2])
                kline_dict = {
                    "code": row[0],
                    "period": row[1],
                    "date": date_str,
                    "open": float(row[3]),
                    "high": float(row[4]),
                    "low": float(row[5]),
                    "close": float(row[6]),
                    "volume": float(row[7]),
                    "amount": float(row[8]),
                }
            else:
                # 旧格式：不包含period字段
                date_str = row[1].strftime("%Y-%m-%d") if hasattr(row[1], 'strftime') else str(row[1])
                kline_dict = {
                    "code": row[0],
                    "period": period_normalized,  # 默认使用查询时的period
                    "date": date_str,
                    "open": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "close": float(row[5]),
                    "volume": float(row[6]),
                    "amount": float(row[7]),
                }
            
            # 根据code判断市场类型（港股代码通常以0开头且长度为5位，A股代码为6位数字）
            code_str = str(kline_dict["code"])
            if len(code_str) == 5 and code_str.startswith("0"):
                kline_dict["market"] = "HK"
            else:
                kline_dict["market"] = "A"
            kline_data.append(kline_dict)
        
        return kline_data
    except Exception as e:
        logger.warning(f"从数据库查询K线数据失败 {code}: {e}")
        return []
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def save_indicator(code: str, market: str, date: str, indicators: Dict[str, Any]) -> bool:
    """保存技术指标到数据库
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        date: 日期 YYYY-MM-DD格式
        indicators: 指标字典
    
    Returns:
        是否成功
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 转换为日期格式
        if len(date) == 8 and "-" not in date:
            date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        else:
            date_str = date
        
        # 将日期字符串转换为date对象（ClickHouse driver需要date对象）
        from datetime import datetime as dt
        try:
            date_obj = dt.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            logger.error(f"日期格式错误: {date_str}")
            return False
        
        # 辅助函数：将值转换为float，None转为0.0
        def to_float(value):
            if value is None:
                return 0.0
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
        
        # 构建插入数据（确保所有Float64字段都是float类型，None转为0.0）
        insert_data = {
            "code": code,
            "market": market.upper(),
            "date": date_obj,  # 使用date对象而不是字符串
            "ma5": to_float(indicators.get("ma5")),
            "ma10": to_float(indicators.get("ma10")),
            "ma20": to_float(indicators.get("ma20")),
            "ma60": to_float(indicators.get("ma60")),
            "ma5_trend": indicators.get("ma5_trend", ""),
            "ma10_trend": indicators.get("ma10_trend", ""),
            "ma20_trend": indicators.get("ma20_trend", ""),
            "ma60_trend": indicators.get("ma60_trend", ""),
            "macd_dif": to_float(indicators.get("macd_dif")),
            "macd_dea": to_float(indicators.get("macd_dea")),
            "macd": to_float(indicators.get("macd")),
            "macd_dif_trend": indicators.get("macd_dif_trend", ""),
            "rsi": to_float(indicators.get("rsi")),
            "boll_upper": to_float(indicators.get("boll_upper")),
            "boll_middle": to_float(indicators.get("boll_middle")),
            "boll_lower": to_float(indicators.get("boll_lower")),
            "boll_expanding": 1 if indicators.get("boll_expanding") else 0,
            "boll_contracting": 1 if indicators.get("boll_contracting") else 0,
            "kdj_k": to_float(indicators.get("kdj_k")),
            "kdj_d": to_float(indicators.get("kdj_d")),
            "kdj_j": to_float(indicators.get("kdj_j")),
            "williams_r": to_float(indicators.get("williams_r")),
            "williams_r_prev": to_float(indicators.get("williams_r_prev")),
            "vol_ratio": to_float(indicators.get("vol_ratio")),
            "high_20d": to_float(indicators.get("high_20d")),
            "break_high_20d": 1 if indicators.get("break_high_20d") else 0,
            "current_price": to_float(indicators.get("current_close")),  # 使用收盘价作为当前价格
            "current_open": to_float(indicators.get("current_open")),
            "current_high": to_float(indicators.get("current_high")),
            "current_low": to_float(indicators.get("current_low")),
            "current_close": to_float(indicators.get("current_close")),
        }
        
        # 执行插入（使用ReplacingMergeTree自动去重）
        client.execute(
            """
            INSERT INTO indicators 
            (code, market, date, ma5, ma10, ma20, ma60, ma5_trend, ma10_trend, ma20_trend, ma60_trend,
             macd_dif, macd_dea, macd, macd_dif_trend, rsi,
             boll_upper, boll_middle, boll_lower, boll_expanding, boll_contracting,
             kdj_k, kdj_d, kdj_j, williams_r, williams_r_prev, vol_ratio,
             high_20d, break_high_20d, current_price, current_open, current_high, current_low, current_close)
            VALUES
            """,
            [[
                insert_data["code"], insert_data["market"], insert_data["date"],
                insert_data["ma5"], insert_data["ma10"], insert_data["ma20"], insert_data["ma60"],
                insert_data["ma5_trend"], insert_data["ma10_trend"], insert_data["ma20_trend"], insert_data["ma60_trend"],
                insert_data["macd_dif"], insert_data["macd_dea"], insert_data["macd"], insert_data["macd_dif_trend"],
                insert_data["rsi"],
                insert_data["boll_upper"], insert_data["boll_middle"], insert_data["boll_lower"],
                insert_data["boll_expanding"], insert_data["boll_contracting"],
                insert_data["kdj_k"], insert_data["kdj_d"], insert_data["kdj_j"],
                insert_data["williams_r"], insert_data["williams_r_prev"], insert_data["vol_ratio"],
                insert_data["high_20d"], insert_data["break_high_20d"],
                insert_data["current_price"], insert_data["current_open"], insert_data["current_high"],
                insert_data["current_low"], insert_data["current_close"]
            ]]
        )
        
        return True
    except Exception as e:
        logger.error(f"保存指标失败 {code}: {e}", exc_info=True)
        return False
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_indicator_date(code: str, market: str) -> str | None:
    """获取指标表中某只股票的最新日期
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
    
    Returns:
        最新日期的字符串（YYYY-MM-DD格式），如果不存在则返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        query = """
            SELECT max(date) as max_date FROM indicators
            WHERE code = %(code)s AND market = %(market)s
        """
        result = client.execute(query, {'code': code, 'market': market.upper()})
        
        if result and len(result) > 0 and result[0][0]:
            max_date = result[0][0]
            if isinstance(max_date, str):
                return max_date
            else:
                return max_date.strftime("%Y-%m-%d")
        return None
    except Exception as e:
        logger.debug(f"查询指标最新日期失败 {code}: {e}")
        return None
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_indicator(code: str, market: str, date: str | None = None) -> Dict[str, Any] | None:
    """从数据库获取技术指标（获取最新日期的指标）
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        date: 日期 YYYY-MM-DD格式，如果为None则获取最新的
    
    Returns:
        指标字典，如果不存在返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        if date:
            # 转换为日期格式
            if len(date) == 8 and "-" not in date:
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = date
            query = """
                SELECT * FROM indicators
                WHERE code = %(code)s AND market = %(market)s AND date = %(date)s
                ORDER BY update_time DESC
                LIMIT 1
            """
            result = client.execute(query, {'code': code, 'market': market.upper(), 'date': date_str})
        else:
            # 获取最新的指标
            query = """
                SELECT * FROM indicators
                WHERE code = %(code)s AND market = %(market)s
                ORDER BY date DESC, update_time DESC
                LIMIT 1
            """
            result = client.execute(query, {'code': code, 'market': market.upper()})
        
        if not result:
            return None
        
        row = result[0]
        # 转换为字典（根据表结构）
        columns = [
            "code", "market", "date", "ma5", "ma10", "ma20", "ma60",
            "ma5_trend", "ma10_trend", "ma20_trend", "ma60_trend",
            "macd_dif", "macd_dea", "macd", "macd_dif_trend", "rsi",
            "boll_upper", "boll_middle", "boll_lower", "boll_expanding", "boll_contracting",
            "kdj_k", "kdj_d", "kdj_j", "williams_r", "williams_r_prev", "vol_ratio",
            "high_20d", "break_high_20d", "current_price", "current_open", "current_high",
            "current_low", "current_close", "update_time"
        ]
        
        indicator_dict = {}
        for i, col in enumerate(columns):
            if i < len(row):
                value = row[i]
                # 转换布尔类型
                if col in ["boll_expanding", "boll_contracting", "break_high_20d"]:
                    value = bool(value) if value is not None else False
                indicator_dict[col] = value
        
        return indicator_dict
    except Exception as e:
        logger.debug(f"获取指标失败 {code}: {e}")
        return None
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def batch_get_indicators(codes: List[str], market: str, date: str | None = None) -> Dict[str, Dict[str, Any]]:
    """批量获取指标（用于选股时的快速查询）
    
    Args:
        codes: 股票代码列表
        market: 市场类型（A或HK）
        date: 日期，如果为None则获取最新的
    
    Returns:
        {code: indicators} 字典
    """
    if not codes:
        return {}
    
    client = None
    try:
        client = _create_clickhouse_client()
        
        if date:
            if len(date) == 8 and "-" not in date:
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = date
            # 对于IN子句，直接拼接代码列表（已转义），使用字典格式参数
            codes_str = ','.join([f"'{c}'" for c in codes])
            query = f"""
                SELECT * FROM indicators FINAL
                WHERE code IN ({codes_str}) AND market = %(market)s AND date = %(date)s
                ORDER BY code
            """
            params = {'market': market.upper(), 'date': date_str}
        else:
            # 获取每个股票的最新指标（使用FINAL和分组）
            # ClickHouse的ReplacingMergeTree配合FINAL可以自动去重获取最新数据
            # 对于IN子句，直接拼接代码列表（已转义）
            codes_str = ','.join([f"'{c}'" for c in codes])
            query = f"""
                SELECT code, market, 
                    argMax(date, update_time) as date,
                    argMax(ma5, update_time) as ma5,
                    argMax(ma10, update_time) as ma10,
                    argMax(ma20, update_time) as ma20,
                    argMax(ma60, update_time) as ma60,
                    argMax(ma5_trend, update_time) as ma5_trend,
                    argMax(ma10_trend, update_time) as ma10_trend,
                    argMax(ma20_trend, update_time) as ma20_trend,
                    argMax(ma60_trend, update_time) as ma60_trend,
                    argMax(macd_dif, update_time) as macd_dif,
                    argMax(macd_dea, update_time) as macd_dea,
                    argMax(macd, update_time) as macd,
                    argMax(macd_dif_trend, update_time) as macd_dif_trend,
                    argMax(rsi, update_time) as rsi,
                    argMax(boll_upper, update_time) as boll_upper,
                    argMax(boll_middle, update_time) as boll_middle,
                    argMax(boll_lower, update_time) as boll_lower,
                    argMax(boll_expanding, update_time) as boll_expanding,
                    argMax(boll_contracting, update_time) as boll_contracting,
                    argMax(kdj_k, update_time) as kdj_k,
                    argMax(kdj_d, update_time) as kdj_d,
                    argMax(kdj_j, update_time) as kdj_j,
                    argMax(williams_r, update_time) as williams_r,
                    argMax(williams_r_prev, update_time) as williams_r_prev,
                    argMax(vol_ratio, update_time) as vol_ratio,
                    argMax(high_20d, update_time) as high_20d,
                    argMax(break_high_20d, update_time) as break_high_20d,
                    argMax(current_price, update_time) as current_price,
                    argMax(current_open, update_time) as current_open,
                    argMax(current_high, update_time) as current_high,
                    argMax(current_low, update_time) as current_low,
                    argMax(current_close, update_time) as current_close,
                    max(update_time) as update_time
                FROM indicators
                WHERE code IN ({codes_str}) AND market = %(market)s
                GROUP BY code, market
            """
            params = {'market': market.upper()}
        
        result = client.execute(query, params)
        
        if date:
            # 有日期时，使用标准列顺序
            columns = [
                "code", "market", "date", "ma5", "ma10", "ma20", "ma60",
                "ma5_trend", "ma10_trend", "ma20_trend", "ma60_trend",
                "macd_dif", "macd_dea", "macd", "macd_dif_trend", "rsi",
                "boll_upper", "boll_middle", "boll_lower", "boll_expanding", "boll_contracting",
                "kdj_k", "kdj_d", "kdj_j", "williams_r", "williams_r_prev", "vol_ratio",
                "high_20d", "break_high_20d", "current_price", "current_open", "current_high",
                "current_low", "current_close", "update_time"
            ]
        else:
            # 使用argMax时，列顺序就是SELECT中的顺序
            columns = [
                "code", "market", "date", "ma5", "ma10", "ma20", "ma60",
                "ma5_trend", "ma10_trend", "ma20_trend", "ma60_trend",
                "macd_dif", "macd_dea", "macd", "macd_dif_trend", "rsi",
                "boll_upper", "boll_middle", "boll_lower", "boll_expanding", "boll_contracting",
                "kdj_k", "kdj_d", "kdj_j", "williams_r", "williams_r_prev", "vol_ratio",
                "high_20d", "break_high_20d", "current_price", "current_open", "current_high",
                "current_low", "current_close", "update_time"
            ]
        
        indicators_map = {}
        for row in result:
            indicator_dict = {}
            code = None
            for i, col in enumerate(columns):
                if i < len(row):
                    value = row[i]
                    if col == "code":
                        code = value
                    # 转换布尔类型
                    if col in ["boll_expanding", "boll_contracting", "break_high_20d"]:
                        value = bool(value) if value is not None else False
                    indicator_dict[col] = value
            
            if code:
                indicators_map[code] = indicator_dict
        
        return indicators_map
    except Exception as e:
        logger.error(f"批量获取指标失败: {e}", exc_info=True)
        return {}
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_stock_list_from_db(market: str = "A") -> List[Dict[str, Any]]:
    """从ClickHouse获取股票列表（从kline表获取所有股票的最新价格等信息）
    
    Args:
        market: 市场类型（A或HK）
    
    Returns:
        股票列表，每个股票包含：code, name, price, pct, volume, amount等字段
    """
    client = None
    try:
        client = _create_clickhouse_client()
        # 先检查表中是否有数据
        count_result = client.execute("SELECT COUNT(*) FROM kline")
        total_count = count_result[0][0] if count_result and len(count_result) > 0 else 0
        
        if total_count == 0:
            logger.warning(f"ClickHouse的kline表中没有数据，无法获取股票列表。请先运行数据采集程序。")
            return []
        
        # 从kline表获取所有不重复的股票代码，并获取每只股票的最新价格等信息
        # 使用子查询获取每只股票的最新日期，然后关联获取该日期的K线数据
        # 注意：如果表没有period字段，需要兼容处理
        try:
            # 先尝试查询是否有period字段
            columns = client.execute("DESCRIBE kline")
            column_names = [col[0] for col in columns]
            has_period = "period" in column_names
        except Exception:
            has_period = False
        
        # 获取最新一条及上一条K线，用于计算pct，限制线程/内存/结果数防止超限
        query_settings = {
            "max_memory_usage": 1_200_000_000,  # 1.2GB
            "max_threads": 2,
            "max_final_threads": 2,
            "max_parsing_threads": 2,
            "max_block_size": 4096,
        }
        
        if has_period:
            query = """
                SELECT
                    code,
                    latest.date,
                    latest.price,
                    latest.volume,
                    latest.amount,
                    IF(prev.price != 0, (latest.price - prev.price) / prev.price, 0) AS pct
                FROM (
                    SELECT
                        code,
                        arr[1].1 AS date,
                        arr[1].2 AS price,
                        arr[1].3 AS volume,
                        arr[1].4 AS amount,
                        length(arr) AS arr_len
                    FROM (
                        SELECT
                            code,
                            arraySlice(arrayReverseSort(groupArray((date, close, volume, amount))), 1, 2) AS arr
                        FROM kline FINAL
                        WHERE period = 'daily'
                        GROUP BY code
                    )
                ) AS latest
                LEFT JOIN (
                    SELECT
                        code,
                        arr[2].2 AS price
                    FROM (
                        SELECT
                            code,
                            arraySlice(arrayReverseSort(groupArray((date, close))), 1, 2) AS arr
                        FROM kline FINAL
                        WHERE period = 'daily'
                        GROUP BY code
                    )
                ) AS prev ON latest.code = prev.code
                ORDER BY latest.amount DESC
                LIMIT 20000
            """
        else:
            # 兼容旧表结构（没有period字段）
            query = """
                SELECT
                    code,
                    latest.date,
                    latest.price,
                    latest.volume,
                    latest.amount,
                    IF(prev.price != 0, (latest.price - prev.price) / prev.price, 0) AS pct
                FROM (
                    SELECT
                        code,
                        arr[1].1 AS date,
                        arr[1].2 AS price,
                        arr[1].3 AS volume,
                        arr[1].4 AS amount,
                        length(arr) AS arr_len
                    FROM (
                        SELECT
                            code,
                            arraySlice(arrayReverseSort(groupArray((date, close, volume, amount))), 1, 2) AS arr
                        FROM kline FINAL
                        GROUP BY code
                    )
                ) AS latest
                LEFT JOIN (
                    SELECT
                        code,
                        arr[2].2 AS price
                    FROM (
                        SELECT
                            code,
                            arraySlice(arrayReverseSort(groupArray((date, close))), 1, 2) AS arr
                        FROM kline FINAL
                        GROUP BY code
                    )
                ) AS prev ON latest.code = prev.code
                ORDER BY latest.amount DESC
                LIMIT 20000
            """
        
        rows = client.execute(query, settings=query_settings)
        
        stocks = []
        for row in rows:
            code = str(row[0])
            date = row[1]
            price = float(row[2]) if row[2] is not None else 0.0
            volume = float(row[3]) if row[3] is not None else 0.0
            amount = float(row[4]) if row[4] is not None else 0.0
            pct = float(row[5]) if row[5] is not None else 0.0
            
            # 根据市场类型过滤股票代码
            if market.upper() == "A":
                # A股：6开头（上海），0/3开头（深圳）
                if not (code.startswith("6") or code.startswith("0") or code.startswith("3")):
                    continue
            elif market.upper() == "HK":
                # 港股：通常以数字开头，或者需要其他判断逻辑
                # 这里假设港股代码都是纯数字或特定格式
                if code.startswith("6") or code.startswith("0") or code.startswith("3"):
                    continue  # 排除A股代码
            
            stocks.append({
                "code": code,
                "name": code,  # 名称需要从其他地方获取，这里先用代码
                "price": price,
                "pct": pct,
                "volume": volume,
                "amount": amount,
                "date": date.strftime("%Y-%m-%d") if hasattr(date, 'strftime') else str(date)
            })
        
        logger.info(f"从ClickHouse获取股票列表：市场={market}，共{len(stocks)}只股票（总数据{total_count}条）")
        return stocks
        
    except Exception as e:
        logger.error(f"从ClickHouse获取股票列表失败: {e}", exc_info=True)
        return []
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass

