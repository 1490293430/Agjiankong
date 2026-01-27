"""
行情采集调度器（优化版：交易时间内才采集，非交易时间延长等待）
"""
import time
from datetime import datetime, timedelta

from market_collector.cn import fetch_a_stock_spot
from market_collector.hk import fetch_hk_stock_spot
from common.logger import get_logger
from common.runtime_config import get_runtime_config
from common.trading_hours import is_a_stock_trading_time, is_hk_stock_trading_time, get_next_trading_start_time

logger = get_logger(__name__)

# 记录上次批量计算指标的日期，避免同一天重复计算
_last_batch_compute_date = None
_last_batch_compute_market = set()  # 存储格式："{market}_{date}"，例如 "A_2025-12-20"

# 记录上次资讯采集时间
_last_news_collect_time = None

# Redis key 用于记录小时K线采集状态
HOURLY_KLINE_COLLECT_KEY_A = "hourly_kline:collected:a"
HOURLY_KLINE_COLLECT_KEY_HK = "hourly_kline:collected:hk"

# 记录上次数据清理日期（避免同一天重复清理）
_last_cleanup_date = None

# 记录上次AI自动分析日期（避免同一天重复分析）
_last_ai_analysis_date = None


def _broadcast_spot_result(a_count: int, hk_count: int, a_time: str, hk_time: str, a_source: str, hk_source: str, a_success: bool, hk_success: bool):
    """广播采集结果到前端顶部状态栏
    
    每次推送时，从Redis读取A股和港股各自的最新采集时间，
    这样即使一个市场采集失败，也能显示另一个市场的最新状态。
    
    Args:
        a_count: A股采集数量
        hk_count: 港股采集数量
        a_time: A股采集时间（格式：MM-DD HH:MM），如果为空则从Redis读取
        hk_time: 港股采集时间（格式：MM-DD HH:MM），如果为空则从Redis读取
        a_source: A股数据源
        hk_source: 港股数据源
        a_success: A股采集是否成功
        hk_success: 港股采集是否成功
    """
    try:
        from market.service.sse import broadcast_message
        from common.redis import set_json, get_json
        
        # 从Redis读取上次保存的采集结果
        last_result = get_json("spot:collect:result") or {}
        
        # 如果本次有新的采集时间，使用新的；否则使用上次保存的
        final_a_time = a_time if a_time else last_result.get("a_time", "")
        final_hk_time = hk_time if hk_time else last_result.get("hk_time", "")
        final_a_count = a_count if a_success else last_result.get("a_count", 0)
        final_hk_count = hk_count if hk_success else last_result.get("hk_count", 0)
        final_a_source = a_source if a_source else last_result.get("a_source", "")
        final_hk_source = hk_source if hk_source else last_result.get("hk_source", "")
        final_a_success = a_success if a_time else last_result.get("a_success", False)
        final_hk_success = hk_success if hk_time else last_result.get("hk_success", False)
        
        spot_result_data = {
            "success": final_a_success or final_hk_success,
            "time": datetime.now().strftime("%m-%d %H:%M"),
            "a_count": final_a_count,
            "hk_count": final_hk_count,
            "a_time": final_a_time,
            "hk_time": final_hk_time,
            "a_source": final_a_source,
            "hk_source": final_hk_source,
            "a_success": final_a_success,
            "hk_success": final_hk_success,
            "source": final_a_source  # 兼容旧版本
        }
        # 保存到Redis持久化
        set_json("spot:collect:result", spot_result_data)
        broadcast_message({
            "type": "spot_collect_result",
            "data": spot_result_data
        })
    except Exception as e:
        logger.debug(f"广播采集结果失败: {e}")


def collect_job():
    """采集任务（单次采集，不做交易时间判断）

    说明：
    - 是否在交易时间内的判断由调度循环控制；
    - 这里始终尝试采集一次 A 股和港股，方便在启动时或手动触发时获取最新快照，
      避免非交易时间页面完全没有数据。
    - A股和港股分开采集，各自完成后立即推送采集结果（包含独立的时间戳）
    - 每次推送会从Redis读取另一个市场的最新状态，确保即使一个市场采集失败也不影响另一个
    - 采集完成后自动检查交易计划
    - 采集结果会通过SSE广播到前端顶部状态栏
    - 只在交易时间内采集对应市场的数据
    """
    try:
        # 检查各市场是否在交易时间
        is_a_trading = is_a_stock_trading_time()
        is_hk_trading = is_hk_stock_trading_time()
        
        if not is_a_trading and not is_hk_trading:
            logger.debug("A股和港股都不在交易时间，跳过采集")
            return
        
        logger.info(f"开始采集行情数据... (A股交易中={is_a_trading}, 港股交易中={is_hk_trading})")
        
        # ========== 采集A股（仅在A股交易时间）==========
        a_count = 0
        a_source = ""
        a_success = False
        a_time = ""
        
        if is_a_trading:
            try:
                from market_collector.cn import fetch_a_stock_spot_with_source
                from common.runtime_config import get_runtime_config
                
                config = get_runtime_config()
                a_spot_source = config.spot_data_source or "auto"
                
                a_result, a_source = fetch_a_stock_spot_with_source(a_spot_source, 2)
                a_count = len(a_result) if a_result else 0
                a_success = a_count > 0
                if a_success:
                    a_time = datetime.now().strftime("%m-%d %H:%M")
                logger.info(f"A股采集完成: {a_count}只，数据源: {a_source}（配置: {a_spot_source}），时间: {a_time}")
                
            except Exception as e:
                logger.error(f"A股采集失败: {e}", exc_info=True)
                # 回退到原始方法
                try:
                    a_result = fetch_a_stock_spot()
                    a_count = len(a_result) if a_result else 0
                    a_source = "AKShare"
                    a_success = a_count > 0
                    if a_success:
                        a_time = datetime.now().strftime("%m-%d %H:%M")
                except Exception as e2:
                    logger.error(f"A股采集回退也失败: {e2}")
            
            # A股采集完成后立即推送结果（港股数据从Redis读取上次的）
            _broadcast_spot_result(a_count, 0, a_time, "", a_source, "", a_success, False)
        else:
            logger.debug("A股不在交易时间，跳过采集")
        
        # ========== 采集港股（仅在港股交易时间）==========
        hk_count = 0
        hk_source = ""
        hk_success = False
        hk_time = ""
        
        if is_hk_trading:
            try:
                # 港股使用独立的数据源配置
                from common.runtime_config import get_runtime_config
                config = get_runtime_config()
                hk_spot_source = config.hk_spot_data_source or "auto"
                
                result_tuple = fetch_hk_stock_spot(source=hk_spot_source)
                # 新的返回格式：(result, source_name)
                if isinstance(result_tuple, tuple) and len(result_tuple) == 2:
                    hk_result, hk_source = result_tuple
                else:
                    hk_result = result_tuple
                    hk_source = "AKShare(东方财富)"
                hk_count = len(hk_result) if hk_result else 0
                hk_success = hk_count > 0
                if hk_success:
                    hk_time = datetime.now().strftime("%m-%d %H:%M")
                logger.info(f"港股采集完成: {hk_count}只 [{hk_source}]（配置: {hk_spot_source}），时间: {hk_time}")
                
            except Exception as e:
                logger.error(f"港股采集失败: {e}", exc_info=True)
            
            # 港股采集完成后推送结果（A股数据从Redis读取上次的）
            _broadcast_spot_result(0, hk_count, "", hk_time, "", hk_source, False, hk_success)
        else:
            logger.debug("港股不在交易时间，跳过采集")
        
        if is_a_trading or is_hk_trading:
            logger.info("行情数据采集完成")
        
        # 采集完成后，自动检查交易计划（只在有采集时检查）
        if a_success or hk_success:
            try:
                from trading.plan import check_trade_plans_by_spot_price
                result = check_trade_plans_by_spot_price()
                if result.get("checked", 0) > 0:
                    logger.info(f"交易计划检查完成：检查{result.get('checked')}个，买入{result.get('bought')}个，盈利{result.get('win')}个，亏损{result.get('loss')}个")
            except Exception as e:
                logger.warning(f"自动检查交易计划失败: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"采集任务执行失败: {e}", exc_info=True)


def news_collect_job():
    """资讯采集任务（每30分钟执行一次）"""
    global _last_news_collect_time
    
    try:
        now = datetime.now()
        
        # 检查是否需要采集（每30分钟一次）
        if _last_news_collect_time:
            elapsed = (now - _last_news_collect_time).total_seconds()
            if elapsed < 1800:  # 30分钟 = 1800秒
                return
        
        logger.info("开始采集财经资讯...")
        from news.collector import fetch_news
        
        result = fetch_news()
        news_count = len(result) if result else 0
        
        if news_count > 0:
            _last_news_collect_time = now
            logger.info(f"资讯采集完成: {news_count}条")
        else:
            logger.warning("资讯采集返回空数据")
            
    except Exception as e:
        logger.error(f"资讯采集失败: {e}", exc_info=True)


def hourly_kline_collect_job():
    """小时K线采集任务
    
    采集时间：
    - A股：17:00后采集全天的小时K线
    - 港股：17:50后采集全天的小时K线
    - 使用Redis记录状态，避免重复采集和程序重启后状态丢失
    """
    from common.trading_hours import TZ_SHANGHAI, TZ_HONGKONG, is_trading_day
    from common.redis import get_redis
    
    now_sh = datetime.now(TZ_SHANGHAI)
    now_hk = datetime.now(TZ_HONGKONG)
    today_sh = now_sh.strftime("%Y-%m-%d")
    today_hk = now_hk.strftime("%Y-%m-%d")
    
    redis_client = get_redis()
    
    # 定义采集时间点：A股17:00，港股17:50
    collect_hour_a, collect_minute_a = 17, 0
    collect_hour_hk, collect_minute_hk = 17, 50
    
    # 判断是否应该采集
    should_collect_a = False
    should_collect_hk = False
    
    # 获取A股和港股的采集状态
    last_collected_a_raw = redis_client.get(HOURLY_KLINE_COLLECT_KEY_A)
    last_collected_hk_raw = redis_client.get(HOURLY_KLINE_COLLECT_KEY_HK)
    
    last_collected_a = None
    if last_collected_a_raw:
        last_collected_a = last_collected_a_raw.decode('utf-8') if isinstance(last_collected_a_raw, bytes) else last_collected_a_raw
    
    last_collected_hk = None
    if last_collected_hk_raw:
        last_collected_hk = last_collected_hk_raw.decode('utf-8') if isinstance(last_collected_hk_raw, bytes) else last_collected_hk_raw
    
    # A股采集判断（17:00后，10分钟内都可以触发）
    collect_key_a = f"{today_sh}_collected"
    is_after_collect_time_a = now_sh.hour > collect_hour_a or (now_sh.hour == collect_hour_a and now_sh.minute >= collect_minute_a)
    is_before_deadline_a = (now_sh.hour == collect_hour_a and now_sh.minute < collect_minute_a + 10) or (now_sh.hour == collect_hour_a and collect_minute_a >= 50)  # 10分钟窗口
    
    if is_after_collect_time_a and (now_sh.hour == collect_hour_a or is_before_deadline_a) and is_trading_day("A", now_sh.date()):
        if not last_collected_a or last_collected_a != collect_key_a:
            should_collect_a = True
    
    # 港股采集判断（17:50后，10分钟内都可以触发）
    collect_key_hk = f"{today_hk}_collected"
    is_after_collect_time_hk = now_hk.hour > collect_hour_hk or (now_hk.hour == collect_hour_hk and now_hk.minute >= collect_minute_hk)
    is_before_deadline_hk = (now_hk.hour == collect_hour_hk and now_hk.minute < collect_minute_hk + 10) or (now_hk.hour == collect_hour_hk and collect_minute_hk >= 50)  # 10分钟窗口
    
    if is_after_collect_time_hk and (now_hk.hour == collect_hour_hk or is_before_deadline_hk) and is_trading_day("HK", now_hk.date()):
        if not last_collected_hk or last_collected_hk != collect_key_hk:
            should_collect_hk = True
    
    # 采集A股小时K线
    if should_collect_a:
        try:
            logger.info(f"[A股] 开始采集全天小时K线数据 (时间: {now_sh.strftime('%H:%M')})")
            _collect_hourly_kline_for_market("A")
            redis_client.set(HOURLY_KLINE_COLLECT_KEY_A, collect_key_a, ex=86400 * 2)  # 保留2天
            logger.info(f"[A股] 小时K线采集完成并已记录状态")
        except Exception as e:
            logger.error(f"[A股] 小时K线采集失败: {e}", exc_info=True)
    
    # 采集港股小时K线
    if should_collect_hk:
        try:
            logger.info(f"[港股] 开始采集全天小时K线数据 (时间: {now_hk.strftime('%H:%M')})")
            _collect_hourly_kline_for_market("HK")
            redis_client.set(HOURLY_KLINE_COLLECT_KEY_HK, collect_key_hk, ex=86400 * 2)  # 保留2天
            logger.info(f"[港股] 小时K线采集完成并已记录状态")
        except Exception as e:
            logger.error(f"[港股] 小时K线采集失败: {e}", exc_info=True)


def _collect_hourly_kline_for_market(market: str):
    """采集指定市场的小时K线数据（优化版：降低并发+添加延迟）
    
    Args:
        market: "A" 或 "HK"
    """
    import concurrent.futures
    import random
    from common.db import save_kline_data, get_clickhouse
    from common.runtime_config import get_runtime_config
    from market_collector.eastmoney_source import fetch_eastmoney_a_kline, fetch_eastmoney_hk_kline
    
    # 从数据库获取股票列表
    try:
        client = get_clickhouse()
        # 查询数据库中有日K线数据的股票代码
        query = """
            SELECT DISTINCT code 
            FROM kline 
            WHERE market = %(market)s AND period = 'daily'
        """
        result = client.execute(query, {"market": market})
        codes = [row[0] for row in result if row[0]]
    except Exception as e:
        logger.error(f"[{market}] 从数据库获取股票列表失败: {e}")
        return
    
    if not codes:
        logger.warning(f"[{market}] 数据库中无股票数据，跳过小时K线采集")
        return
    
    logger.info(f"[{market}] 准备采集 {len(codes)} 只股票的小时K线（降低频率模式）")
    
    start_time = time.time()
    
    # 定义单个股票的采集函数（添加随机延迟）
    def fetch_single(code):
        try:
            # 添加随机延迟 0.3-0.8秒，避免请求过于密集
            time.sleep(random.uniform(0.3, 0.8))
            
            if market == "A":
                return fetch_eastmoney_a_kline(code, period="1h", limit=5)
            else:
                return fetch_eastmoney_hk_kline(code, period="1h", limit=5)
        except Exception as e:
            logger.debug(f"[{market}] {code} 小时K线获取失败: {e}")
            return None
    
    # 并发采集（降低到3个线程，避免触发反爬虫）
    all_kline_data = []
    success_count = 0
    fail_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_code = {executor.submit(fetch_single, code): code for code in codes}
        
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            try:
                kline_data = future.result()
                if kline_data:
                    # market字段已由数据源函数设置，无需覆盖
                    all_kline_data.extend(kline_data)
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                logger.debug(f"[{market}] {code} 采集异常: {e}")
    
    elapsed = time.time() - start_time
    
    # 批量保存到数据库
    if all_kline_data:
        try:
            save_kline_data(all_kline_data, period="1h")
            logger.info(f"[{market}] 小时K线采集完成: 成功={success_count}, 失败={fail_count}, 总数据={len(all_kline_data)}条, 耗时={elapsed:.1f}秒")
        except Exception as e:
            logger.error(f"[{market}] 小时K线保存失败: {e}", exc_info=True)
    else:
        logger.warning(f"[{market}] 小时K线采集无数据")
    
    # A股市场额外采集上证指数小时K线
    if market == "A":
        try:
            from market_collector.eastmoney_source import fetch_eastmoney_index_kline
            sh_index_klines = fetch_eastmoney_index_kline("1.000001", period="1h", limit=5)
            if sh_index_klines:
                for k in sh_index_klines:
                    k["code"] = "1A0001"
                    k["market"] = "A"
                save_kline_data(sh_index_klines, period="1h")
                logger.info(f"[A股] 额外采集上证指数小时K线: {len(sh_index_klines)}条")
        except Exception as e:
            logger.warning(f"[A股] 采集上证指数小时K线失败（不影响股票数据）: {e}")


def batch_compute_indicators_job(market: str = "A"):
    """批量计算指标任务（收盘后自动执行，计算所有股票的日线和小时线指标）
    
    Args:
        market: 市场类型（A或HK）
    """
    global _last_batch_compute_date, _last_batch_compute_market
    
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        market_key = f"{market}_{today}"
        
        # 清理过期的记录（只保留最近3天的）
        if _last_batch_compute_date and _last_batch_compute_date != today:
            # 清理旧记录
            _last_batch_compute_market = {
                key for key in _last_batch_compute_market
                if key.endswith(f"_{today}") or key.endswith(f"_{_last_batch_compute_date}")
            }
        
        # 检查今天是否已经计算过
        if market_key in _last_batch_compute_market:
            logger.debug(f"{market}股指标今天已批量计算过，跳过")
            return
        
        from strategy.indicator_batch import batch_compute_indicators
        
        # 计算日线指标
        logger.info(f"开始收盘后批量计算{market}股日线技术指标（增量更新模式，计算所有股票）...")
        result_daily = batch_compute_indicators(market, max_count=None, incremental=True, period="daily")
        logger.info(f"{market}股日线指标计算完成：成功={result_daily.get('success')}，跳过={result_daily.get('skipped')}，失败={result_daily.get('failed')}")
        
        # 计算小时线指标
        logger.info(f"开始收盘后批量计算{market}股小时线技术指标（增量更新模式，计算所有股票）...")
        result_hourly = batch_compute_indicators(market, max_count=None, incremental=True, period="1h")
        logger.info(f"{market}股小时线指标计算完成：成功={result_hourly.get('success')}，跳过={result_hourly.get('skipped')}，失败={result_hourly.get('failed')}")
        
        # 只要有一个周期计算成功，就标记为已完成
        total_success = result_daily.get("success", 0) + result_hourly.get("success", 0)
        total_skipped = result_daily.get("skipped", 0) + result_hourly.get("skipped", 0)
        
        if total_success > 0 or total_skipped > 0:
            _last_batch_compute_market.add(market_key)
            _last_batch_compute_date = today
            logger.info(f"{market}股指标批量计算全部完成（日线+小时线）")
        else:
            logger.warning(f"{market}股指标批量计算失败或无数据")
            
    except Exception as e:
        logger.error(f"批量计算{market}股指标失败: {e}", exc_info=True)


def snapshot_to_kline_job():
    """快照转K线任务（收盘后自动执行）
    
    将实时快照数据转换为当日K线并入库
    - A股：15:30 后执行
    - 港股：16:30 后执行
    """
    try:
        from market_collector.snapshot_to_kline import auto_convert_snapshot_to_kline
        results = auto_convert_snapshot_to_kline()
        
        for market, result in results.items():
            if result.get("count", 0) > 0:
                logger.info(f"[{market}] 快照转K线完成: {result.get('message')}")
    except Exception as e:
        logger.error(f"快照转K线任务失败: {e}", exc_info=True)


def cleanup_old_data_job():
    """定期清理旧数据任务（每天17:30后执行一次）
    
    ⚠️ 警告：此函数会产生大量mutation，导致内存占用持续增长
    已禁用自动执行，改为手动执行：python backend/scripts/cleanup_and_optimize.py
    
    清理内容：
    - 日线K线数据：保留8年（根据配置）
    - 小时线K线数据：保留1年
    - 交易结果数据：保留1年
    - 优化表释放空间（每7天执行一次）
    """
    global _last_cleanup_date
    
    # 禁用自动清理，避免产生大量mutation导致内存泄漏
    logger.debug("自动清理任务已禁用，请手动执行: python backend/scripts/cleanup_and_optimize.py")
    return
    
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 检查今天是否已经清理过
        if _last_cleanup_date == today:
            return
        
        from common.db import get_clickhouse
        from datetime import timedelta
        from common.runtime_config import get_runtime_config
        
        client = get_clickhouse()
        
        logger.info("开始定期清理旧数据...")
        cleanup_count = 0
        
        # 1. 清理日线K线数据（保留8年，根据配置）
        try:
            config = get_runtime_config()
            retention_years = config.kline_years  # 默认8年
            cutoff_date = datetime.now() - timedelta(days=int(retention_years * 365))
            cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")
            
            # 统计要删除的数据量
            count_query = "SELECT count() FROM kline WHERE period = 'daily' AND date < %(date)s"
            result = client.execute(count_query, {'date': cutoff_date_str})
            count = result[0][0] if result else 0
            
            if count > 0:
                logger.info(f"清理日线K线数据：删除{count:,}条（{cutoff_date_str}之前的数据，保留{retention_years}年）")
                delete_query = "ALTER TABLE kline DELETE WHERE period = 'daily' AND date < %(date)s"
                client.execute(delete_query, {'date': cutoff_date_str})
                cleanup_count += 1
            else:
                logger.info("日线K线数据无需清理")
        except Exception as e:
            logger.error(f"清理日线K线数据失败: {e}", exc_info=True)
        
        # 2. 清理小时线K线数据（保留1年）
        try:
            hourly_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            count_query = "SELECT count() FROM kline WHERE period = '1h' AND date < %(date)s"
            result = client.execute(count_query, {'date': hourly_cutoff})
            count = result[0][0] if result else 0
            
            if count > 0:
                logger.info(f"清理小时线K线数据：删除{count:,}条（{hourly_cutoff}之前的数据，保留1年）")
                delete_query = "ALTER TABLE kline DELETE WHERE period = '1h' AND date < %(date)s"
                client.execute(delete_query, {'date': hourly_cutoff})
                cleanup_count += 1
            else:
                logger.info("小时线K线数据无需清理")
        except Exception as e:
            logger.error(f"清理小时线K线数据失败: {e}", exc_info=True)
        
        # 3. 清理交易结果数据（保留1年）
        try:
            trade_cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            count_query = "SELECT count() FROM trade_result WHERE exit_date < %(date)s"
            result = client.execute(count_query, {'date': trade_cutoff})
            count = result[0][0] if result else 0
            
            if count > 0:
                logger.info(f"清理交易结果数据：删除{count:,}条（{trade_cutoff}之前的数据，保留1年）")
                delete_query = "ALTER TABLE trade_result DELETE WHERE exit_date < %(date)s"
                client.execute(delete_query, {'date': trade_cutoff})
                cleanup_count += 1
            else:
                logger.info("交易结果数据无需清理")
        except Exception as e:
            logger.error(f"清理交易结果数据失败: {e}", exc_info=True)
        
        if cleanup_count > 0:
            logger.info(f"数据清理完成，共清理{cleanup_count}类数据（删除操作异步执行）")
            logger.info("提示：删除操作是异步的，需要执行OPTIMIZE TABLE才能释放空间")
            _last_cleanup_date = today
        else:
            logger.info("所有数据都无需清理")
            _last_cleanup_date = today
        
        # 4. 优化表释放空间（每7天执行一次）
        try:
            from datetime import date
            today_date = date.today()
            last_optimize_date = getattr(cleanup_old_data_job, '_last_optimize_date', None)
            
            if last_optimize_date is None or (today_date - last_optimize_date).days >= 7:
                logger.info("执行表优化（释放已删除数据的空间）...")
                try:
                    client.execute("OPTIMIZE TABLE kline FINAL")
                    logger.info("✓ kline表优化完成")
                except Exception as e:
                    logger.warning(f"kline表优化失败: {e}")
                
                try:
                    client.execute("OPTIMIZE TABLE indicators FINAL")
                    logger.info("✓ indicators表优化完成")
                except Exception as e:
                    logger.warning(f"indicators表优化失败: {e}")
                
                try:
                    client.execute("OPTIMIZE TABLE snapshot FINAL")
                    logger.info("✓ snapshot表优化完成")
                except Exception as e:
                    logger.warning(f"snapshot表优化失败: {e}")
                
                try:
                    client.execute("OPTIMIZE TABLE trade_result FINAL")
                    logger.info("✓ trade_result表优化完成")
                except Exception as e:
                    logger.warning(f"trade_result表优化失败: {e}")
                
                cleanup_old_data_job._last_optimize_date = today_date
                logger.info("所有表优化完成")
            else:
                logger.debug(f"距离上次优化只有{(today_date - last_optimize_date).days}天，跳过优化（每7天执行一次）")
        except Exception as e:
            logger.error(f"表优化失败: {e}", exc_info=True)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass
        
    except Exception as e:
        logger.error(f"定期清理旧数据失败: {e}", exc_info=True)


def ai_auto_analysis_job():
    """AI自动分析任务（根据配置的时间每天执行一次）
    
    从配置中读取自动分析时间（格式：HH:MM），在指定时间自动分析自选股
    """
    global _last_ai_analysis_date
    
    try:
        from common.runtime_config import get_runtime_config
        from common.redis import get_json
        
        config = get_runtime_config()
        auto_time = config.ai_auto_analyze_time
        
        # 如果未配置自动分析时间，跳过
        if not auto_time:
            return
        
        # 解析配置的时间（格式：HH:MM）
        try:
            hour, minute = map(int, auto_time.split(':'))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                logger.warning(f"AI自动分析时间配置无效: {auto_time}，应为HH:MM格式")
                return
        except Exception as e:
            logger.warning(f"AI自动分析时间配置解析失败: {auto_time}，错误: {e}")
            return
        
        # 检查当前时间是否匹配
        from common.trading_hours import TZ_SHANGHAI
        now = datetime.now(TZ_SHANGHAI)
        today = now.strftime("%Y-%m-%d")
        
        # 检查今天是否已经分析过
        if _last_ai_analysis_date == today:
            return
        
        # 检查当前时间是否已到达或超过配置的时间（使用时间范围判断，避免错过）
        target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        # 如果当前时间在目标时间之前，或者已经超过目标时间10分钟，则跳过
        if now < target_time or (now - target_time).total_seconds() > 600:
            return
        
        logger.info(f"开始执行AI自动分析任务（配置时间: {auto_time}）...")
        
        # 从Redis获取自选股列表
        watchlist = get_json("watchlist") or []
        
        if not watchlist:
            logger.info("自选股列表为空，跳过AI自动分析")
            _last_ai_analysis_date = today
            return
        
        # 提取股票代码
        codes = []
        for item in watchlist:
            if isinstance(item, dict):
                code = item.get("code")
            else:
                code = item
            if code:
                codes.append(str(code).strip())
        
        if not codes:
            logger.info("自选股列表中无有效代码，跳过AI自动分析")
            _last_ai_analysis_date = today
            return
        
        logger.info(f"准备分析 {len(codes)} 只自选股: {', '.join(codes[:5])}{'...' if len(codes) > 5 else ''}")
        
        # 调用AI分析接口
        try:
            from ai.analyzer import analyze_stocks_batch
            
            results = analyze_stocks_batch(codes)
            
            if results:
                success_count = sum(1 for r in results if r.get("success"))
                logger.info(f"AI自动分析完成: 成功={success_count}/{len(codes)}")
                
                # 发送通知（如果配置了通知渠道）
                try:
                    from notify.dispatcher import send_ai_analysis_notification
                    
                    # 筛选出买入信号的股票
                    buy_signals = [r for r in results if r.get("success") and r.get("data", {}).get("signal") == "买入"]
                    
                    if buy_signals:
                        send_ai_analysis_notification(buy_signals, auto_triggered=True)
                        logger.info(f"已发送AI分析通知，包含 {len(buy_signals)} 个买入信号")
                except Exception as e:
                    logger.warning(f"发送AI分析通知失败: {e}")
                
                _last_ai_analysis_date = today
            else:
                logger.warning("AI自动分析返回空结果")
                
        except Exception as e:
            logger.error(f"AI自动分析执行失败: {e}", exc_info=True)
            
    except Exception as e:
        logger.error(f"AI自动分析任务失败: {e}", exc_info=True)


def main():
    """主函数"""
    logger.info("行情采集调度器启动（实时判断交易时间）...")

    # 简单循环调度，根据运行时配置动态调整采集间隔
    while True:
        try:
            cfg = get_runtime_config()
            interval = max(30, int(cfg.collector_interval_seconds))  # 最小30秒，降低CPU占用
        except Exception as e:
            logger.warning(f"获取采集间隔配置失败，使用默认60秒: {e}")
            interval = 60

        # 实时检查是否在交易时间内
        is_a_trading, is_hk_trading = is_a_stock_trading_time(), is_hk_stock_trading_time()
        from common.trading_hours import TZ_SHANGHAI, TZ_HONGKONG
        now_sh = datetime.now(TZ_SHANGHAI)
        now_hk = datetime.now(TZ_HONGKONG)
        current_hour_a = now_sh.hour
        current_minute_a = now_sh.minute
        current_hour_hk = now_hk.hour
        current_minute_hk = now_hk.minute
        
        if is_a_trading or is_hk_trading:
            # 在交易时间内，正常采集
            start_ts = time.time()
            collect_job()
            elapsed = time.time() - start_ts
            if elapsed > 10:  # 只有采集时间超过10秒才记录
                logger.info(f"本次采集耗时: {elapsed:.2f}秒")
            
            # 采集资讯（每30分钟一次）
            news_collect_job()
            
            # 采集小时K线（在特定时间点采集，A股和港股分开）
            hourly_kline_collect_job()
            
            # 检查AI自动分析任务（每分钟检查一次）
            ai_auto_analysis_job()
            
            # 采集完成后，广播市场状态更新（通过SSE）
            try:
                from market.service.sse import broadcast_market_status_update
                broadcast_market_status_update()
            except Exception as e:
                logger.debug(f"SSE广播市场状态失败（不影响采集）: {e}")
            
            # 交易时间内使用正常间隔
            time.sleep(interval)
        else:
            # 不在交易时间内，也要采集资讯（每30分钟一次）
            news_collect_job()
            
            # 不在交易时间内，也检查小时K线采集（处理收盘后的采集时间点）
            hourly_kline_collect_job()
            
            # 检查AI自动分析任务（每分钟检查一次）
            ai_auto_analysis_job()
            
            # 不在交易时间内，检查是否需要执行收盘后任务
            
            # A股收盘后立即转换（15:12-15:22，10分钟窗口）
            if current_hour_a == 15 and 12 <= current_minute_a < 22:
                try:
                    from market_collector.snapshot_to_kline import should_convert_snapshot, convert_snapshot_to_kline
                    if should_convert_snapshot("A"):
                        logger.info("[A股] 收盘后执行快照转K线（日K线采集）")
                        result = convert_snapshot_to_kline("A")
                        logger.info(f"[A股] 快照转K线完成: {result}")
                except Exception as e:
                    logger.error(f"[A股] 快照转K线失败: {e}")
            
            # 港股收盘后立即转换（16:30-16:40，10分钟窗口）
            if current_hour_hk == 16 and 30 <= current_minute_hk < 40:
                try:
                    from market_collector.snapshot_to_kline import should_convert_snapshot, convert_snapshot_to_kline
                    if should_convert_snapshot("HK"):
                        logger.info("[港股] 收盘后执行快照转K线（日K线采集）")
                        result = convert_snapshot_to_kline("HK")
                        logger.info(f"[港股] 快照转K线完成: {result}")
                except Exception as e:
                    logger.error(f"[港股] 快照转K线失败: {e}")
            
            # 18:30 执行批量计算指标（使用上海时区）
            if current_hour_a == 18 and 30 <= current_minute_a < 40:
                
                # 判断A股今天是否有交易
                a_has_traded_today = False
                try:
                    from common.redis import get_redis
                    from common.trading_hours import TZ_SHANGHAI
                    
                    redis_client = get_redis()
                    a_time_key = "market:a:time"
                    a_update_time_str = redis_client.get(a_time_key)
                    
                    if a_update_time_str:
                        if isinstance(a_update_time_str, bytes):
                            a_update_time_str = a_update_time_str.decode('utf-8')
                        
                        if isinstance(a_update_time_str, str):
                            if 'T' in a_update_time_str:
                                a_update_time = datetime.fromisoformat(a_update_time_str.replace('Z', '+00:00'))
                            else:
                                a_update_time = datetime.fromisoformat(a_update_time_str)
                        else:
                            a_update_time = a_update_time_str
                        
                        if a_update_time.tzinfo is None:
                            a_update_time = TZ_SHANGHAI.localize(a_update_time)
                        else:
                            a_update_time = a_update_time.astimezone(TZ_SHANGHAI)
                        
                        now_sh = datetime.now(TZ_SHANGHAI)
                        today_start = now_sh.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        if a_update_time >= today_start:
                            a_has_traded_today = True
                except Exception as e:
                    logger.debug(f"检查A股今日交易状态失败: {e}")
                
                # 判断港股今天是否有交易
                hk_has_traded_today = False
                try:
                    from common.trading_hours import TZ_HONGKONG
                    
                    hk_time_key = "market:hk:time"
                    update_time_str = redis_client.get(hk_time_key)
                    
                    if update_time_str:
                        if isinstance(update_time_str, bytes):
                            update_time_str = update_time_str.decode('utf-8')
                        
                        if isinstance(update_time_str, str):
                            if 'T' in update_time_str:
                                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
                            else:
                                update_time = datetime.fromisoformat(update_time_str)
                        else:
                            update_time = update_time_str
                        
                        if update_time.tzinfo is None:
                            update_time = TZ_HONGKONG.localize(update_time)
                        else:
                            update_time = update_time.astimezone(TZ_HONGKONG)
                        
                        now_hk = datetime.now(TZ_HONGKONG)
                        today_start = now_hk.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        if update_time >= today_start:
                            hk_has_traded_today = True
                except Exception as e:
                    logger.debug(f"检查港股今日交易状态失败: {e}")
                
                # 计算指标（batch_compute_indicators_job 内部会检查是否已计算过，防止重复）
                if a_has_traded_today:
                    batch_compute_indicators_job("A")
                
                if hk_has_traded_today:
                    batch_compute_indicators_job("HK")
            
            # 不在交易时间内，不采集数据，延长等待时间
            # 但如果还没到22:00，需要短间隔检查以确保收盘后任务能触发
            if current_hour_a < 22:
                # 22:00之前，每5分钟检查一次，确保收盘后任务能触发
                logger.info(f"当前时间 {current_hour_a}:{current_minute_a:02d}，22:00前保持短间隔检查（5分钟）")
                time.sleep(300)
            else:
                # 22:00之后，执行定期清理任务（每天执行一次）
                if current_hour_a == 22 and current_minute_a < 10:
                    cleanup_old_data_job()
                
                # 22:00之后，可以长时间睡眠到下一个交易日
                next_a_start = get_next_trading_start_time("A")
                next_hk_start = get_next_trading_start_time("HK")
                next_start = min(next_a_start, next_hk_start)
                
                if next_start.tzinfo:
                    if now_sh.tzinfo is None:
                        now_tz = TZ_SHANGHAI.localize(now_sh)
                    else:
                        now_tz = now_sh.astimezone(TZ_SHANGHAI)
                else:
                    now_tz = now_sh
                
                wait_seconds = max(300, int((next_start - now_tz).total_seconds()))
                
                logger.info(f"当前不在交易时间内且已过22:00，等待 {wait_seconds // 60} 分钟后重新检查（估算下次交易时间: {next_start.strftime('%Y-%m-%d %H:%M:%S')}）")
                time.sleep(min(wait_seconds, 3600))  # 最多等待1小时，然后重新检查


if __name__ == "__main__":
    main()

