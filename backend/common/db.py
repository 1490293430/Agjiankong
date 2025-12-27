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
        connect_timeout=10,
        send_receive_timeout=30  # 增加超时时间，避免高并发时超时
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
                connect_timeout=10,  # 10秒连接超时
                send_receive_timeout=30  # 30秒发送接收超时（高并发时需要更长时间）
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


def _get_stock_codes_from_cache() -> set:
    """从Redis缓存获取股票代码列表（仅sec_type='stock'的）
    
    Returns:
        股票代码集合
    """
    try:
        from common.redis import get_json
        
        stock_codes = set()
        
        # 获取A股股票代码
        a_spot = get_json("market:a:spot") or []
        for item in a_spot:
            if item.get('sec_type') == 'stock':
                code = str(item.get('code', '')).strip()
                if code:
                    stock_codes.add(code)
        
        # 获取港股股票代码
        hk_spot = get_json("market:hk:spot") or []
        for item in hk_spot:
            if item.get('sec_type') == 'stock':
                code = str(item.get('code', '')).strip()
                if code:
                    stock_codes.add(code)
        
        return stock_codes
    except Exception as e:
        logger.warning(f"获取股票代码缓存失败: {e}")
        return set()


def init_tables():
    """初始化数据表"""
    client = None
    try:
        client = _create_clickhouse_client()
        
        # K线表（使用ReplacingMergeTree自动去重，避免频繁DELETE导致mutation堆积）
        # 注意：time字段用于存储完整时间戳，对于小时线数据尤为重要
        # ORDER BY 包含 time 字段，确保同一天的多条小时线数据不会被去重
        client.execute("""
        CREATE TABLE IF NOT EXISTS kline
        (
            code String,
            period String,
            date Date,
            time DateTime DEFAULT toDateTime(date),
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            amount Float64,
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, period, date, time)
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
            
            # 检查是否有time字段（用于存储完整时间戳，支持小时线数据）
            if "time" not in column_names:
                logger.info("检测到kline表缺少time字段，正在添加...")
                logger.warning("⚠️ 重要：添加time字段后，需要重建表的ORDER BY才能正确支持小时线数据去重")
                logger.warning("⚠️ 建议执行迁移脚本: python -m scripts.migrate_kline_add_time")
                try:
                    client.execute("ALTER TABLE kline ADD COLUMN IF NOT EXISTS time DateTime DEFAULT toDateTime(date)")
                    logger.info("✓ time字段添加成功")
                    logger.warning("⚠️ 注意：仅添加字段不会改变ORDER BY，小时线数据仍可能被去重")
                    logger.warning("⚠️ 如需完整支持小时线，请执行迁移脚本重建表结构")
                except Exception as e:
                    logger.warning(f"添加time字段失败: {e}")
        except Exception as e:
            logger.debug(f"表结构检查可能失败（表可能不存在）: {e}")
        
        # 技术指标表（存储预计算的指标，每日更新）
        client.execute("""
        CREATE TABLE IF NOT EXISTS indicators
        (
            code String,
            market String,
            date Date,
            period String DEFAULT 'daily',  -- K线周期：daily（日线）、1h（小时线）
            -- 均线
            ma5 Float64,
            ma10 Float64,
            ma20 Float64,
            ma60 Float64,
            ma5_trend String,
            ma10_trend String,
            ma20_trend String,
            ma60_trend String,
            -- EMA指数移动平均线
            ema12 Float64,
            ema26 Float64,
            -- MACD
            macd_dif Float64,
            macd_dea Float64,
            macd Float64,
            macd_dif_trend String,
            macd_prev Float64,
            -- RSI
            rsi Float64,
            -- BIAS乖离率
            bias6 Float64,
            bias12 Float64,
            bias24 Float64,
            -- 布林带
            boll_upper Float64,
            boll_middle Float64,
            boll_lower Float64,
            boll_expanding UInt8,
            boll_contracting UInt8,
            boll_width Float64,
            boll_width_prev Float64,
            -- KDJ
            kdj_k Float64,
            kdj_d Float64,
            kdj_j Float64,
            -- 威廉指标
            williams_r Float64,
            williams_r_prev Float64,
            -- ADX平均趋向指数
            adx Float64,
            plus_di Float64,
            minus_di Float64,
            adx_prev Float64,
            adx_rising UInt8,
            -- CCI顺势指标
            cci Float64,
            cci_prev Float64,
            cci_rising UInt8,
            cci_status String,
            -- 一目均衡表
            ichimoku_tenkan Float64,
            ichimoku_kijun Float64,
            ichimoku_senkou_a Float64,
            ichimoku_senkou_b Float64,
            ichimoku_above_cloud UInt8,
            ichimoku_below_cloud UInt8,
            ichimoku_in_cloud UInt8,
            ichimoku_tk_cross_up UInt8,
            ichimoku_tk_cross_down UInt8,
            -- 斐波那契
            fib_swing_high Float64,
            fib_swing_low Float64,
            fib_236 Float64,
            fib_382 Float64,
            fib_500 Float64,
            fib_618 Float64,
            fib_786 Float64,
            fib_trend String,
            fib_current_level String,
            -- 成交量
            vol_ratio Float64,
            -- 价格数据
            high_20d Float64,
            recent_low Float64,
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
        ORDER BY (code, market, date, period)
        """)
        
        # 股票基本信息表
        client.execute("""
        CREATE TABLE IF NOT EXISTS stock_info
        (
            code String,
            name String,
            market String,
            industry String DEFAULT '',
            pe Float64 DEFAULT 0,
            market_cap Float64 DEFAULT 0,
            circulating_market_cap Float64 DEFAULT 0,
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, market)
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
        
        # 为indicators表添加高级指标字段（如果表已存在但缺少这些字段）
        try:
            # EMA指数移动平均线
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ema12 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ema26 Float64 DEFAULT 0")
            # MACD辅助字段
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS macd_prev Float64 DEFAULT 0")
            # BIAS乖离率
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS bias6 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS bias12 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS bias24 Float64 DEFAULT 0")
            # 布林带辅助字段
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS boll_width Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS boll_width_prev Float64 DEFAULT 0")
            # ADX平均趋向指数
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS adx Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS plus_di Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS minus_di Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS adx_prev Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS adx_rising UInt8 DEFAULT 0")
            # CCI顺势指标
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS cci Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS cci_prev Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS cci_rising UInt8 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS cci_status String DEFAULT ''")
            # 一目均衡表
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_tenkan Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_kijun Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_senkou_a Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_senkou_b Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_above_cloud UInt8 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_below_cloud UInt8 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_in_cloud UInt8 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_tk_cross_up UInt8 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ichimoku_tk_cross_down UInt8 DEFAULT 0")
            # 斐波那契
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_swing_high Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_swing_low Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_236 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_382 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_500 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_618 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_786 Float64 DEFAULT 0")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_trend String DEFAULT ''")
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS fib_current_level String DEFAULT ''")
            # 近期低点
            client.execute("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS recent_low Float64 DEFAULT 0")
            logger.info("✓ indicators表高级指标字段已添加/确认存在")
        except Exception as e:
            logger.debug(f"indicators表高级指标字段添加可能已存在或失败: {e}")
        
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
        
        # 实时快照表（保存每只股票最新快照数据，每只股票只保留一条）
        client.execute("""
        CREATE TABLE IF NOT EXISTS snapshot
        (
            code String,
            name String,
            market String,
            price Float64,
            pct Float64,
            change Float64,
            volume Float64,
            amount Float64,
            open Float64,
            high Float64,
            low Float64,
            pre_close Float64,
            volume_ratio Float64,
            turnover Float64,
            pe Float64,
            market_cap Float64,
            circulating_market_cap Float64,
            amplitude Float64,
            sec_type String DEFAULT 'stock',
            update_time DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (code, market)
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
    
    # 根据配置决定是否过滤非股票数据
    config = get_runtime_config()
    if config.collect_stock_only:
        # 获取股票代码列表（从Redis缓存）
        stock_codes = _get_stock_codes_from_cache()
        if stock_codes:
            before_count = len(kline_data)
            kline_data = [item for item in kline_data if str(item.get('code', '')) in stock_codes]
            filtered_count = before_count - len(kline_data)
            if filtered_count > 0:
                logger.info(f"K线数据过滤非股票: {filtered_count}条，保留: {len(kline_data)}条")
    
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
            
            # 数据校验：过滤异常数据
            try:
                open_price = float(item.get("open", 0) or 0)
                high_price = float(item.get("high", 0) or 0)
                low_price = float(item.get("low", 0) or 0)
                close_price = float(item.get("close", 0) or 0)
                volume = float(item.get("volume", 0) or 0)
                amount = float(item.get("amount", 0) or 0)
                
                # 校验1：价格必须为正数
                if close_price <= 0 or open_price <= 0:
                    logger.debug(f"跳过异常数据 {code} {date_value}: 价格<=0 (close={close_price}, open={open_price})")
                    continue
                
                # 校验2：A股价格不应超过3000元（茅台历史最高约2600，留余量）
                if len(code) == 6 and (code.startswith('0') or code.startswith('3') or code.startswith('6')):
                    if close_price > 3000 or open_price > 3000:
                        logger.warning(f"跳过异常数据 {code} {date_value}: A股价格异常高 (close={close_price}, open={open_price})")
                        continue
                
                # 校验3：high >= low
                if high_price < low_price:
                    logger.debug(f"跳过异常数据 {code} {date_value}: high < low")
                    continue
                    
            except (ValueError, TypeError) as e:
                logger.debug(f"跳过异常数据 {code} {date_value}: 数值转换失败 {e}")
                continue
            
            # 将日期字符串转换为date对象（ClickHouse driver需要date对象）
            # 注意：date_value 可能包含时间部分（如 "2025-12-26 11:30"），需要先提取日期部分
            try:
                if isinstance(date_value, str):
                    from datetime import datetime as dt
                    # 提取日期部分（去掉时间）
                    date_only = date_value.split(" ")[0]
                    date_obj = dt.strptime(date_only, "%Y-%m-%d").date()
                else:
                    date_obj = date_value
            except Exception:
                continue  # 日期格式错误，跳过这条数据
            
            # 处理时间字段（对于小时线数据，需要完整的时间戳）
            # 从原始数据中获取时间信息
            time_obj = None
            original_date = item.get("date", "")
            original_time = item.get("time", "")  # 可能有单独的time字段
            
            if period_normalized == '1h':
                # 小时线数据需要完整时间戳
                from datetime import datetime as dt
                
                # 尝试从原始date字段解析时间（格式可能是 "2024-01-01 09:30" 或 "2024-01-01 09:30:00"）
                if isinstance(original_date, str) and " " in original_date:
                    try:
                        # 尝试解析带时间的日期字符串
                        if len(original_date.split(" ")[1]) == 5:  # HH:MM
                            time_obj = dt.strptime(original_date, "%Y-%m-%d %H:%M")
                        else:  # HH:MM:SS
                            time_obj = dt.strptime(original_date, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
                
                # 如果有单独的time字段
                if time_obj is None and original_time:
                    try:
                        if isinstance(original_time, str):
                            # 尝试解析时间字符串
                            if len(original_time) == 5:  # HH:MM
                                time_obj = dt.combine(date_obj, dt.strptime(original_time, "%H:%M").time())
                            elif len(original_time) == 8:  # HH:MM:SS
                                time_obj = dt.combine(date_obj, dt.strptime(original_time, "%H:%M:%S").time())
                        elif hasattr(original_time, 'hour'):  # datetime.time 对象
                            time_obj = dt.combine(date_obj, original_time)
                    except Exception:
                        pass
                
                # 如果还是没有时间，使用日期的00:00:00（但这会导致去重问题）
                if time_obj is None:
                    # 尝试从item中获取hour字段
                    hour = item.get("hour", 0)
                    if hour:
                        try:
                            time_obj = dt.combine(date_obj, dt.strptime(f"{hour:02d}:00", "%H:%M").time())
                        except Exception:
                            time_obj = dt.combine(date_obj, dt.min.time())
                    else:
                        time_obj = dt.combine(date_obj, dt.min.time())
            else:
                # 日线/周线/月线数据，时间设为当天00:00:00
                from datetime import datetime as dt
                time_obj = dt.combine(date_obj, dt.min.time())
            
            data_to_insert.append((
                code,
                period_normalized,  # 添加period字段
                date_obj,  # 使用date对象而不是字符串
                time_obj,  # 添加time字段（DateTime类型）
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                amount,
            ))
        
        if not data_to_insert:
            return True
        
        # 直接使用INSERT批量插入，ReplacingMergeTree会自动去重
        # 不再使用ALTER TABLE DELETE，避免产生大量mutation导致"Too many unfinished mutations"错误
        # ClickHouse driver的execute方法支持直接传入数据列表
        try:
            client.execute(
                "INSERT INTO kline (code, period, date, time, open, high, low, close, volume, amount) VALUES",
                data_to_insert
            )
            logger.info(f"K线数据保存成功: {len(data_to_insert)}条（周期: {period_normalized}），涉及{len(codes)}只股票")
        except Exception as insert_error:
            # 如果表还没有time字段（旧表），尝试兼容插入
            error_msg = str(insert_error)
            if "time" in error_msg.lower() and "column" in error_msg.lower():
                logger.warning(f"表结构可能未更新（缺少time字段），尝试兼容模式插入")
                # 移除time字段，使用旧格式插入
                data_without_time = [(d[0], d[1], d[2], d[4], d[5], d[6], d[7], d[8], d[9]) for d in data_to_insert]
                try:
                    client.execute(
                        "INSERT INTO kline (code, period, date, open, high, low, close, volume, amount) VALUES",
                        data_without_time
                    )
                    logger.info(f"K线数据保存成功（兼容模式）: {len(data_without_time)}条（周期: {period_normalized}）")
                    logger.warning("⚠️ 小时线数据可能因缺少time字段而被去重，建议执行迁移脚本")
                except Exception as compat_error:
                    logger.error(f"兼容模式插入也失败: {compat_error}", exc_info=True)
                    raise
            elif "period" in error_msg.lower() or "column" in error_msg.lower():
                logger.warning(f"表结构可能未更新，尝试兼容模式插入: {error_msg}")
                raise
            else:
                logger.error(f"保存K线数据失败: {error_msg}", exc_info=True)
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
        
        # 尝试查询（包含period和time字段）
        # 使用FINAL确保ReplacingMergeTree去重后的结果（注意：FINAL有性能开销，但能保证数据一致性）
        try:
            # 先尝试查询包含time字段的新表结构
            query = f"""
                SELECT code, period, date, time, open, high, low, close, volume, amount
                FROM kline FINAL
                WHERE {' AND '.join(where_conditions)}
                ORDER BY date ASC, time ASC
            """
            result = client.execute(query, params)
            has_period = True
            has_time = True
        except Exception as e:
            # 如果time字段不存在，尝试不带time的查询
            try:
                query = f"""
                    SELECT code, period, date, open, high, low, close, volume, amount
                    FROM kline FINAL
                    WHERE {' AND '.join(where_conditions)}
                    ORDER BY date ASC
                """
                result = client.execute(query, params)
                has_period = True
                has_time = False
            except Exception:
                # 如果period字段也不存在，使用兼容查询
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
                has_time = False
        
        # 转换为字典列表
        kline_data = []
        for row in result:
            if has_period and has_time:
                # 新格式：包含period和time字段
                date_str = row[2].strftime("%Y-%m-%d") if hasattr(row[2], 'strftime') else str(row[2])
                time_str = row[3].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[3], 'strftime') else str(row[3])
                kline_dict = {
                    "code": row[0],
                    "period": row[1],
                    "date": date_str,
                    "time": time_str,  # 完整时间戳
                    "open": float(row[4]),
                    "high": float(row[5]),
                    "low": float(row[6]),
                    "close": float(row[7]),
                    "volume": float(row[8]),
                    "amount": float(row[9]),
                }
            elif has_period:
                # 中间格式：包含period但不包含time字段
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


def save_indicator(code: str, market: str, date: str, indicators: Dict[str, Any], period: str = "daily") -> bool:
    """保存技术指标到数据库
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        date: 日期 YYYY-MM-DD格式
        indicators: 指标字典
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    
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
        # 包含所有基础指标和高级指标
        insert_data = {
            "code": code,
            "market": market.upper(),
            "date": date_obj,  # 使用date对象而不是字符串
            "period": period,  # K线周期
            # 均线
            "ma5": to_float(indicators.get("ma5")),
            "ma10": to_float(indicators.get("ma10")),
            "ma20": to_float(indicators.get("ma20")),
            "ma60": to_float(indicators.get("ma60")),
            "ma5_trend": indicators.get("ma5_trend", ""),
            "ma10_trend": indicators.get("ma10_trend", ""),
            "ma20_trend": indicators.get("ma20_trend", ""),
            "ma60_trend": indicators.get("ma60_trend", ""),
            # EMA指数移动平均线
            "ema12": to_float(indicators.get("ema12")),
            "ema26": to_float(indicators.get("ema26")),
            # MACD
            "macd_dif": to_float(indicators.get("macd_dif")),
            "macd_dea": to_float(indicators.get("macd_dea")),
            "macd": to_float(indicators.get("macd")),
            "macd_dif_trend": indicators.get("macd_dif_trend", ""),
            "macd_prev": to_float(indicators.get("macd_prev")),
            # RSI
            "rsi": to_float(indicators.get("rsi")),
            # BIAS乖离率
            "bias6": to_float(indicators.get("bias6")),
            "bias12": to_float(indicators.get("bias12", indicators.get("bias"))),  # bias12也叫bias
            "bias24": to_float(indicators.get("bias24")),
            # 布林带
            "boll_upper": to_float(indicators.get("boll_upper")),
            "boll_middle": to_float(indicators.get("boll_middle")),
            "boll_lower": to_float(indicators.get("boll_lower")),
            "boll_expanding": 1 if indicators.get("boll_expanding") else 0,
            "boll_contracting": 1 if indicators.get("boll_contracting") else 0,
            "boll_width": to_float(indicators.get("boll_width")),
            "boll_width_prev": to_float(indicators.get("boll_width_prev")),
            # KDJ
            "kdj_k": to_float(indicators.get("kdj_k")),
            "kdj_d": to_float(indicators.get("kdj_d")),
            "kdj_j": to_float(indicators.get("kdj_j")),
            # 威廉指标
            "williams_r": to_float(indicators.get("williams_r")),
            "williams_r_prev": to_float(indicators.get("williams_r_prev")),
            # ADX平均趋向指数
            "adx": to_float(indicators.get("adx")),
            "plus_di": to_float(indicators.get("plus_di")),
            "minus_di": to_float(indicators.get("minus_di")),
            "adx_prev": to_float(indicators.get("adx_prev")),
            "adx_rising": 1 if indicators.get("adx_rising") else 0,
            # CCI顺势指标
            "cci": to_float(indicators.get("cci")),
            "cci_prev": to_float(indicators.get("cci_prev")),
            "cci_rising": 1 if indicators.get("cci_rising") else 0,
            "cci_status": indicators.get("cci_status", ""),
            # 一目均衡表
            "ichimoku_tenkan": to_float(indicators.get("ichimoku_tenkan")),
            "ichimoku_kijun": to_float(indicators.get("ichimoku_kijun")),
            "ichimoku_senkou_a": to_float(indicators.get("ichimoku_senkou_a")),
            "ichimoku_senkou_b": to_float(indicators.get("ichimoku_senkou_b")),
            "ichimoku_above_cloud": 1 if indicators.get("ichimoku_above_cloud") else 0,
            "ichimoku_below_cloud": 1 if indicators.get("ichimoku_below_cloud") else 0,
            "ichimoku_in_cloud": 1 if indicators.get("ichimoku_in_cloud") else 0,
            "ichimoku_tk_cross_up": 1 if indicators.get("ichimoku_tk_cross_up") else 0,
            "ichimoku_tk_cross_down": 1 if indicators.get("ichimoku_tk_cross_down") else 0,
            # 斐波那契
            "fib_swing_high": to_float(indicators.get("fib_swing_high")),
            "fib_swing_low": to_float(indicators.get("fib_swing_low")),
            "fib_236": to_float(indicators.get("fib_236")),
            "fib_382": to_float(indicators.get("fib_382")),
            "fib_500": to_float(indicators.get("fib_500")),
            "fib_618": to_float(indicators.get("fib_618")),
            "fib_786": to_float(indicators.get("fib_786")),
            "fib_trend": indicators.get("fib_trend", ""),
            "fib_current_level": indicators.get("fib_current_level", ""),
            # 成交量
            "vol_ratio": to_float(indicators.get("vol_ratio")),
            # 价格数据
            "high_20d": to_float(indicators.get("high_20d")),
            "recent_low": to_float(indicators.get("recent_low")),
            # 当前价格数据
            "current_price": to_float(indicators.get("current_close")),  # 使用收盘价作为当前价格
            "current_open": to_float(indicators.get("current_open")),
            "current_high": to_float(indicators.get("current_high")),
            "current_low": to_float(indicators.get("current_low")),
            "current_close": to_float(indicators.get("current_close")),
        }
        
        # 执行插入（使用ReplacingMergeTree自动去重）
        # 包含所有基础指标和高级指标字段
        client.execute(
            """
            INSERT INTO indicators 
            (code, market, date, period,
             ma5, ma10, ma20, ma60, ma5_trend, ma10_trend, ma20_trend, ma60_trend,
             ema12, ema26,
             macd_dif, macd_dea, macd, macd_dif_trend, macd_prev, rsi,
             bias6, bias12, bias24,
             boll_upper, boll_middle, boll_lower, boll_expanding, boll_contracting, boll_width, boll_width_prev,
             kdj_k, kdj_d, kdj_j, williams_r, williams_r_prev,
             adx, plus_di, minus_di, adx_prev, adx_rising,
             cci, cci_prev, cci_rising, cci_status,
             ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, ichimoku_senkou_b, 
             ichimoku_above_cloud, ichimoku_below_cloud, ichimoku_in_cloud, ichimoku_tk_cross_up, ichimoku_tk_cross_down,
             fib_swing_high, fib_swing_low, fib_236, fib_382, fib_500, fib_618, fib_786, fib_trend, fib_current_level,
             vol_ratio, high_20d, recent_low,
             current_price, current_open, current_high, current_low, current_close)
            VALUES
            """,
            [[
                insert_data["code"], insert_data["market"], insert_data["date"], insert_data["period"],
                insert_data["ma5"], insert_data["ma10"], insert_data["ma20"], insert_data["ma60"],
                insert_data["ma5_trend"], insert_data["ma10_trend"], insert_data["ma20_trend"], insert_data["ma60_trend"],
                insert_data["ema12"], insert_data["ema26"],
                insert_data["macd_dif"], insert_data["macd_dea"], insert_data["macd"], insert_data["macd_dif_trend"],
                insert_data["macd_prev"], insert_data["rsi"],
                insert_data["bias6"], insert_data["bias12"], insert_data["bias24"],
                insert_data["boll_upper"], insert_data["boll_middle"], insert_data["boll_lower"],
                insert_data["boll_expanding"], insert_data["boll_contracting"], insert_data["boll_width"], insert_data["boll_width_prev"],
                insert_data["kdj_k"], insert_data["kdj_d"], insert_data["kdj_j"],
                insert_data["williams_r"], insert_data["williams_r_prev"],
                insert_data["adx"], insert_data["plus_di"], insert_data["minus_di"], insert_data["adx_prev"], insert_data["adx_rising"],
                insert_data["cci"], insert_data["cci_prev"], insert_data["cci_rising"], insert_data["cci_status"],
                insert_data["ichimoku_tenkan"], insert_data["ichimoku_kijun"], 
                insert_data["ichimoku_senkou_a"], insert_data["ichimoku_senkou_b"],
                insert_data["ichimoku_above_cloud"], insert_data["ichimoku_below_cloud"], 
                insert_data["ichimoku_in_cloud"], insert_data["ichimoku_tk_cross_up"], insert_data["ichimoku_tk_cross_down"],
                insert_data["fib_swing_high"], insert_data["fib_swing_low"], 
                insert_data["fib_236"], insert_data["fib_382"], insert_data["fib_500"], insert_data["fib_618"], insert_data["fib_786"],
                insert_data["fib_trend"], insert_data["fib_current_level"],
                insert_data["vol_ratio"], insert_data["high_20d"], insert_data["recent_low"],
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


def get_indicator_date(code: str, market: str, period: str = "daily") -> str | None:
    """获取指标表中某只股票的最新日期
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    
    Returns:
        最新日期的字符串（YYYY-MM-DD格式），如果不存在则返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        query = """
            SELECT max(date) as max_date FROM indicators
            WHERE code = %(code)s AND market = %(market)s AND period = %(period)s
        """
        result = client.execute(query, {'code': code, 'market': market.upper(), 'period': period})
        
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


def get_indicator(code: str, market: str, date: str | None = None, period: str = "daily") -> Dict[str, Any] | None:
    """从数据库获取技术指标（获取最新日期的指标）
    
    Args:
        code: 股票代码
        market: 市场类型（A或HK）
        date: 日期 YYYY-MM-DD格式，如果为None则获取最新的
        period: K线周期，daily（日线）或 1h（小时线），默认 daily
    
    Returns:
        指标字典，如果不存在返回None
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 显式指定列名，避免 SELECT * 导致的列顺序问题
        columns_sql = """code, market, date, period, ma5, ma10, ma20, ma60,
            ma5_trend, ma10_trend, ma20_trend, ma60_trend,
            ema12, ema26,
            macd_dif, macd_dea, macd, macd_dif_trend, macd_prev, rsi,
            bias6, bias12, bias24,
            boll_upper, boll_middle, boll_lower, boll_expanding, boll_contracting, boll_width, boll_width_prev,
            kdj_k, kdj_d, kdj_j, williams_r, williams_r_prev,
            adx, plus_di, minus_di, adx_prev, adx_rising,
            cci, cci_prev, cci_rising, cci_status,
            ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, ichimoku_senkou_b,
            ichimoku_above_cloud, ichimoku_below_cloud, ichimoku_in_cloud, ichimoku_tk_cross_up, ichimoku_tk_cross_down,
            fib_swing_high, fib_swing_low, fib_236, fib_382, fib_500, fib_618, fib_786, fib_trend, fib_current_level,
            vol_ratio, high_20d, recent_low,
            current_price, current_open, current_high, current_low, current_close,
            update_time"""
        
        if date:
            # 转换为日期格式
            if len(date) == 8 and "-" not in date:
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = date
            query = f"""
                SELECT {columns_sql} FROM indicators
                WHERE code = %(code)s AND market = %(market)s AND date = %(date)s AND period = %(period)s
                ORDER BY update_time DESC
                LIMIT 1
            """
            result = client.execute(query, {'code': code, 'market': market.upper(), 'date': date_str, 'period': period})
        else:
            # 获取最新的指标
            query = f"""
                SELECT {columns_sql} FROM indicators
                WHERE code = %(code)s AND market = %(market)s AND period = %(period)s
                ORDER BY date DESC, update_time DESC
                LIMIT 1
            """
            result = client.execute(query, {'code': code, 'market': market.upper(), 'period': period})
        
        if not result:
            return None
        
        row = result[0]
        # 转换为字典（根据表结构，包含高级指标）
        columns = [
            "code", "market", "date", "period", "ma5", "ma10", "ma20", "ma60",
            "ma5_trend", "ma10_trend", "ma20_trend", "ma60_trend",
            "ema12", "ema26",
            "macd_dif", "macd_dea", "macd", "macd_dif_trend", "macd_prev", "rsi",
            "bias6", "bias12", "bias24",
            "boll_upper", "boll_middle", "boll_lower", "boll_expanding", "boll_contracting", "boll_width", "boll_width_prev",
            "kdj_k", "kdj_d", "kdj_j", "williams_r", "williams_r_prev",
            "adx", "plus_di", "minus_di", "adx_prev", "adx_rising",
            "cci", "cci_prev", "cci_rising", "cci_status",
            "ichimoku_tenkan", "ichimoku_kijun", "ichimoku_senkou_a", "ichimoku_senkou_b",
            "ichimoku_above_cloud", "ichimoku_below_cloud", "ichimoku_in_cloud", "ichimoku_tk_cross_up", "ichimoku_tk_cross_down",
            "fib_swing_high", "fib_swing_low", "fib_236", "fib_382", "fib_500", "fib_618", "fib_786", "fib_trend", "fib_current_level",
            "vol_ratio", "high_20d", "recent_low",
            "current_price", "current_open", "current_high", "current_low", "current_close", 
            "update_time"
        ]
        
        indicator_dict = {}
        for i, col in enumerate(columns):
            if i < len(row):
                value = row[i]
                # 转换布尔类型
                if col in ["boll_expanding", "boll_contracting", 
                           "ichimoku_above_cloud", "ichimoku_below_cloud", "ichimoku_in_cloud",
                           "ichimoku_tk_cross_up", "ichimoku_tk_cross_down", "adx_rising", "cci_rising"]:
                    value = bool(value) if value is not None else False
                indicator_dict[col] = value
        
        # 添加bias别名（bias12也叫bias）
        if "bias12" in indicator_dict:
            indicator_dict["bias"] = indicator_dict["bias12"]
        
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
        
        # 包含所有基础指标和高级指标的字段列表
        all_columns = """code, market, date, ma5, ma10, ma20, ma60,
                    ma5_trend, ma10_trend, ma20_trend, ma60_trend,
                    ema12, ema26,
                    macd_dif, macd_dea, macd, macd_dif_trend, macd_prev, rsi,
                    bias6, bias12, bias24,
                    boll_upper, boll_middle, boll_lower, boll_expanding, boll_contracting, boll_width, boll_width_prev,
                    kdj_k, kdj_d, kdj_j, williams_r, williams_r_prev,
                    adx, plus_di, minus_di, adx_prev, adx_rising,
                    cci, cci_prev, cci_rising, cci_status,
                    ichimoku_tenkan, ichimoku_kijun, ichimoku_senkou_a, ichimoku_senkou_b,
                    ichimoku_above_cloud, ichimoku_below_cloud, ichimoku_in_cloud, ichimoku_tk_cross_up, ichimoku_tk_cross_down,
                    fib_swing_high, fib_swing_low, fib_236, fib_382, fib_500, fib_618, fib_786, fib_trend, fib_current_level,
                    vol_ratio, high_20d, recent_low,
                    current_price, current_open, current_high, current_low, current_close, 
                    update_time"""
        
        if date:
            if len(date) == 8 and "-" not in date:
                date_str = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
            else:
                date_str = date
            # 对于IN子句，直接拼接代码列表（已转义），使用字典格式参数
            codes_str = ','.join([f"'{c}'" for c in codes])
            query = f"""
                SELECT {all_columns} FROM indicators FINAL
                WHERE code IN ({codes_str}) AND market = %(market)s AND date = %(date)s
                ORDER BY code
            """
            params = {'market': market.upper(), 'date': date_str}
        else:
            # 获取每个股票的最新指标（使用子查询获取最新记录）
            # 对于IN子句，直接拼接代码列表（已转义）
            codes_str = ','.join([f"'{c}'" for c in codes])
            query = f"""
                SELECT {all_columns}
                FROM indicators FINAL
                WHERE code IN ({codes_str}) AND market = %(market)s
                ORDER BY code
            """
            params = {'market': market.upper()}
        
        result = client.execute(query, params)
        
        # 列顺序（包含所有基础指标和高级指标）
        columns = [
            "code", "market", "date", "ma5", "ma10", "ma20", "ma60",
            "ma5_trend", "ma10_trend", "ma20_trend", "ma60_trend",
            "ema12", "ema26",
            "macd_dif", "macd_dea", "macd", "macd_dif_trend", "macd_prev", "rsi",
            "bias6", "bias12", "bias24",
            "boll_upper", "boll_middle", "boll_lower", "boll_expanding", "boll_contracting", "boll_width", "boll_width_prev",
            "kdj_k", "kdj_d", "kdj_j", "williams_r", "williams_r_prev",
            "adx", "plus_di", "minus_di", "adx_prev", "adx_rising",
            "cci", "cci_prev", "cci_rising", "cci_status",
            "ichimoku_tenkan", "ichimoku_kijun", "ichimoku_senkou_a", "ichimoku_senkou_b",
            "ichimoku_above_cloud", "ichimoku_below_cloud", "ichimoku_in_cloud", "ichimoku_tk_cross_up", "ichimoku_tk_cross_down",
            "fib_swing_high", "fib_swing_low", "fib_236", "fib_382", "fib_500", "fib_618", "fib_786", "fib_trend", "fib_current_level",
            "vol_ratio", "high_20d", "recent_low",
            "current_price", "current_open", "current_high", "current_low", "current_close", 
            "update_time"
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
                    if col in ["boll_expanding", "boll_contracting", 
                               "ichimoku_above_cloud", "ichimoku_below_cloud", "ichimoku_in_cloud",
                               "ichimoku_tk_cross_up", "ichimoku_tk_cross_down", "adx_rising", "cci_rising"]:
                        value = bool(value) if value is not None else False
                    indicator_dict[col] = value
            
            if code:
                # 添加bias别名（bias12也叫bias）
                if "bias12" in indicator_dict:
                    indicator_dict["bias"] = indicator_dict["bias12"]
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


def get_indicator_stats(market: str = "A") -> Dict[str, Any]:
    """获取指标计算统计信息
    
    Args:
        market: 市场类型（A、HK或ALL）
    
    Returns:
        统计信息字典，包含：
        - latest_date: 最新指标日期
        - latest_update_time: 最新更新时间
        - total_stocks: 总股票数
        - computed_stocks: 已计算指标的股票数
        - coverage_rate: 覆盖率
        - missing_indicators: 缺失指标的字段列表
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 构建市场条件
        if market.upper() == "ALL":
            market_condition = "1=1"
            params = {}
        else:
            market_condition = "market = %(market)s"
            params = {'market': market.upper()}
        
        # 1. 获取最新指标日期和更新时间
        query = f"""
            SELECT 
                max(date) as latest_date,
                max(update_time) as latest_update_time
            FROM indicators
            WHERE {market_condition}
        """
        result = client.execute(query, params)
        
        latest_date = None
        latest_update_time = None
        if result and len(result) > 0:
            latest_date = result[0][0]
            latest_update_time = result[0][1]
            if latest_date:
                latest_date = latest_date.strftime("%Y-%m-%d") if hasattr(latest_date, 'strftime') else str(latest_date)
            if latest_update_time:
                latest_update_time = latest_update_time.strftime("%Y-%m-%d %H:%M:%S") if hasattr(latest_update_time, 'strftime') else str(latest_update_time)
        
        # 2. 获取已计算指标的股票数（按最新日期）
        computed_stocks = 0
        if latest_date:
            query = f"""
                SELECT COUNT(DISTINCT code) 
                FROM indicators FINAL
                WHERE {market_condition} AND date = %(date)s
            """
            params_with_date = {**params, 'date': latest_date}
            result = client.execute(query, params_with_date)
            if result and len(result) > 0:
                computed_stocks = result[0][0]
        
        # 3. 获取K线表中的总股票数（作为基准）
        if market.upper() == "ALL":
            kline_market_condition = "1=1"
        else:
            # A股代码6位，港股代码5位
            if market.upper() == "A":
                kline_market_condition = "length(code) = 6"
            else:
                kline_market_condition = "length(code) = 5"
        
        query = f"""
            SELECT COUNT(DISTINCT code) 
            FROM kline FINAL
            WHERE {kline_market_condition} AND period = 'daily'
        """
        result = client.execute(query)
        total_stocks = result[0][0] if result and len(result) > 0 else 0
        
        # 4. 计算覆盖率
        coverage_rate = round(computed_stocks / total_stocks * 100, 1) if total_stocks > 0 else 0
        
        # 5. 检查缺失的指标字段（抽样检查最新日期的数据）
        missing_indicators = []
        if latest_date and computed_stocks > 0:
            # 定义需要检查的关键指标字段
            indicator_fields = [
                ("ma60", "MA60"),
                ("macd", "MACD"),
                ("rsi", "RSI"),
                ("kdj_k", "KDJ"),
                ("boll_upper", "布林带"),
                ("adx", "ADX"),
                ("cci", "CCI"),
                ("ichimoku_tenkan", "一目均衡"),
                ("fib_236", "斐波那契"),
                ("ema12", "EMA"),
                ("bias12", "BIAS"),
            ]
            
            for field, name in indicator_fields:
                # 检查该字段有多少股票有有效值（非0）
                query = f"""
                    SELECT COUNT(*) 
                    FROM indicators FINAL
                    WHERE {market_condition} AND date = %(date)s AND {field} != 0
                """
                params_with_date = {**params, 'date': latest_date}
                result = client.execute(query, params_with_date)
                field_count = result[0][0] if result and len(result) > 0 else 0
                
                # 如果有效值少于已计算股票数的50%，认为该指标缺失
                if field_count < computed_stocks * 0.5:
                    missing_indicators.append(name)
        
        return {
            "latest_date": latest_date,
            "latest_update_time": latest_update_time,
            "total_stocks": total_stocks,
            "computed_stocks": computed_stocks,
            "coverage_rate": coverage_rate,
            "missing_indicators": missing_indicators,
            "market": market.upper()
        }
    except Exception as e:
        logger.error(f"获取指标统计失败: {e}", exc_info=True)
        return {
            "latest_date": None,
            "latest_update_time": None,
            "total_stocks": 0,
            "computed_stocks": 0,
            "coverage_rate": 0,
            "missing_indicators": [],
            "market": market.upper(),
            "error": str(e)
        }
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
                # A股：6位代码，且以6/0/3开头（排除ETF 15开头、B股 90/20开头等）
                if len(code) != 6:
                    continue
                if not (code.startswith("60") or code.startswith("00") or code.startswith("30") or code.startswith("68")):
                    continue
            elif market.upper() == "HK":
                # 港股：5位代码
                if len(code) != 5:
                    continue
            
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


def save_stock_info_batch(stocks: List[Dict[str, Any]], market: str = "A") -> int:
    """批量保存股票基本信息
    
    Args:
        stocks: 股票列表，每个股票包含code, name等字段
        market: 市场类型（A或HK）
    
    Returns:
        保存成功的数量
    """
    if not stocks:
        return 0
    
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 准备数据
        data = []
        for stock in stocks:
            code = str(stock.get("code", "")).strip()
            name = str(stock.get("name", "")).strip()
            if not code or not name:
                continue
            
            data.append({
                "code": code,
                "name": name,
                "market": market,
                "industry": str(stock.get("industry", "")).strip(),
                "pe": float(stock.get("pe", 0) or 0),
                "market_cap": float(stock.get("market_cap", 0) or 0),
                "circulating_market_cap": float(stock.get("circulating_market_cap", 0) or 0),
            })
        
        if not data:
            return 0
        
        # 批量插入
        client.execute(
            """
            INSERT INTO stock_info (code, name, market, industry, pe, market_cap, circulating_market_cap)
            VALUES
            """,
            data
        )
        
        logger.info(f"保存股票基本信息成功: {market}股 {len(data)} 只")
        return len(data)
    except Exception as e:
        logger.error(f"保存股票基本信息失败: {e}")
        return 0
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_stock_name_map(market: str = "A") -> Dict[str, str]:
    """获取股票代码到名称的映射
    
    Args:
        market: 市场类型（A或HK）
    
    Returns:
        {code: name} 字典
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        result = client.execute(
            """
            SELECT code, name FROM stock_info FINAL
            WHERE market = %(market)s
            """,
            {"market": market}
        )
        
        return {row[0]: row[1] for row in result}
    except Exception as e:
        logger.error(f"获取股票名称映射失败: {e}")
        return {}
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def save_snapshot_data(snapshot_data: List[Dict[str, Any]], market: str = "A") -> bool:
    """将实时快照数据保存到ClickHouse数据库（每只股票只保留最新一条）
    
    Args:
        snapshot_data: 快照数据列表，每个元素包含 code, name, price, pct, volume 等字段
        market: 市场类型（A或HK）
    
    Returns:
        是否保存成功
    """
    if not snapshot_data:
        return True
    
    # 根据配置决定是否过滤非股票数据
    config = get_runtime_config()
    if config.collect_stock_only:
        before_count = len(snapshot_data)
        snapshot_data = [item for item in snapshot_data if item.get('sec_type') == 'stock']
        filtered_count = before_count - len(snapshot_data)
        if filtered_count > 0:
            logger.info(f"[{market}] 快照数据过滤非股票: {filtered_count}条，保留: {len(snapshot_data)}条")
    
    if not snapshot_data:
        return True
    
    client = None
    try:
        client = _create_clickhouse_client()
        
        # 准备批量插入的数据
        data_to_insert = []
        for item in snapshot_data:
            code = str(item.get("code", ""))
            if not code:
                continue
            
            # 跳过价格为0或无效的数据
            price = item.get("price")
            if price is None or price == 0:
                continue
            
            data_to_insert.append((
                code,
                str(item.get("name", ""))[:50],  # 限制名称长度
                market.upper(),
                float(item.get("price", 0) or 0),
                float(item.get("pct", 0) or 0),
                float(item.get("change", 0) or 0),
                float(item.get("volume", 0) or 0),
                float(item.get("amount", 0) or 0),
                float(item.get("open", 0) or 0),
                float(item.get("high", 0) or 0),
                float(item.get("low", 0) or 0),
                float(item.get("pre_close", 0) or 0),
                float(item.get("volume_ratio", 0) or 0),
                float(item.get("turnover", 0) or 0),
                float(item.get("pe", 0) or 0),
                float(item.get("market_cap", 0) or 0),
                float(item.get("circulating_market_cap", 0) or 0),
                float(item.get("amplitude", 0) or 0),
                str(item.get("sec_type", "stock") or "stock"),
            ))
        
        if not data_to_insert:
            return True
        
        # 批量插入（ReplacingMergeTree会按code+market自动去重，只保留最新）
        client.execute(
            """INSERT INTO snapshot 
            (code, name, market, price, pct, change, volume, amount, 
             open, high, low, pre_close, volume_ratio, turnover, pe, 
             market_cap, circulating_market_cap, amplitude, sec_type) 
            VALUES""",
            data_to_insert
        )
        
        logger.info(f"快照数据保存成功: {market}股 {len(data_to_insert)}条")
        return True
        
    except Exception as e:
        logger.error(f"保存快照数据失败: {e}", exc_info=True)
        return False
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass


def get_snapshot_from_db(code: str = None, market: str = "A") -> List[Dict[str, Any]]:
    """从数据库获取快照数据
    
    Args:
        code: 股票代码（可选，不传则获取全部）
        market: 市场类型（A或HK）
    
    Returns:
        快照数据列表
    """
    client = None
    try:
        client = _create_clickhouse_client()
        
        if code:
            result = client.execute(
                """
                SELECT code, name, market, price, pct, change, volume, amount,
                       open, high, low, pre_close, volume_ratio, turnover, pe,
                       market_cap, circulating_market_cap, amplitude, sec_type, update_time
                FROM snapshot FINAL
                WHERE code = %(code)s AND market = %(market)s
                """,
                {"code": code, "market": market}
            )
        else:
            result = client.execute(
                """
                SELECT code, name, market, price, pct, change, volume, amount,
                       open, high, low, pre_close, volume_ratio, turnover, pe,
                       market_cap, circulating_market_cap, amplitude, sec_type, update_time
                FROM snapshot FINAL
                WHERE market = %(market)s
                """,
                {"market": market}
            )
        
        columns = ["code", "name", "market", "price", "pct", "change", 
                   "volume", "amount", "open", "high", "low", "pre_close",
                   "volume_ratio", "turnover", "pe", "market_cap", 
                   "circulating_market_cap", "amplitude", "sec_type", "update_time"]
        
        return [dict(zip(columns, row)) for row in result]
        
    except Exception as e:
        logger.error(f"获取快照数据失败: {e}")
        return []
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass
