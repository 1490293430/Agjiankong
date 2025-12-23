"""
自动选股引擎
"""
from typing import List, Dict, Any, Optional
from strategy.scorer import score_stock
from common.logger import get_logger
from common.redis import get_json, set_json

logger = get_logger(__name__)

# 选股结果存储键
SELECTED_STOCKS_KEY = "app:selected_stocks"


def select_stocks(
    stocks: List[Dict[str, Any]],
    indicators_map: Dict[str, Dict[str, Any]],
    threshold: int = 65,
    max_count: int = 30,
    exclude_st: bool = True,
    min_volume: float = 1000000  # 最小成交额（万元）
) -> List[Dict[str, Any]]:
    """自动选股
    
    Args:
        stocks: 股票列表
        indicators_map: 技术指标字典 {code: indicators}
        threshold: 分数阈值（默认65）
        max_count: 最大选择数量（默认30）
        exclude_st: 是否排除ST股票
        min_volume: 最小成交额（万元）
    
    Returns:
        选中的股票列表（按分数降序）
    """
    result = []
    
    try:
        for stock in stocks:
            code = str(stock.get("code", ""))
            name = str(stock.get("name", ""))
            
            # 过滤ST股票
            if exclude_st and ("ST" in name or "*" in name):
                continue
            
            # 过滤成交额过小的股票
            amount = stock.get("amount", 0)
            if isinstance(amount, (int, float)) and amount < min_volume:
                continue
            
            # 过滤停牌股票（涨跌幅为0且成交量为0）
            pct = stock.get("pct", 0)
            volume = stock.get("volume", 0)
            if abs(pct) < 0.01 and volume == 0:
                continue
            
            # 获取技术指标
            indicators = indicators_map.get(code)
            if not indicators:
                continue
            
            # 打分
            score = score_stock(stock, indicators)
            
            # 达到阈值则加入
            if score >= threshold:
                stock_copy = stock.copy()
                stock_copy["score"] = score
                result.append(stock_copy)
        
        # 按分数降序排序
        result.sort(key=lambda x: x.get("score", 0), reverse=True)
        
        # 限制数量
        result = result[:max_count]
        
        logger.info(f"选股完成：阈值{threshold}，选中{len(result)}只股票")
        
    except Exception as e:
        logger.error(f"选股失败: {e}", exc_info=True)
    
    return result


def filter_by_sector(
    stocks: List[Dict[str, Any]],
    sectors: List[str]
) -> List[Dict[str, Any]]:
    """按板块筛选股票
    
    Args:
        stocks: 股票列表
        sectors: 板块列表（需要股票数据中包含sector字段）
    
    Returns:
        筛选后的股票列表
    """
    if not sectors:
        return stocks
    
    return [
        stock for stock in stocks
        if stock.get("sector") in sectors
    ]


def check_market_environment(all_stocks: List[Dict[str, Any]]) -> bool:
    """检查市场环境：上涨家数比率 > 50%
    
    Args:
        all_stocks: 全市场股票列表
    
    Returns:
        True表示市场环境良好（上涨家数>50%）
    """
    if not all_stocks:
        return False
    
    # 过滤掉无效数据（pct为NaN、None或0的股票）
    import math
    valid_stocks = []
    for stock in all_stocks:
        pct = stock.get("pct")
        # 检查pct是否为有效数值（不是NaN、None，且是数字）
        if pct is not None and not (isinstance(pct, float) and math.isnan(pct)):
            try:
                pct_val = float(pct)
                if not math.isnan(pct_val):
                    valid_stocks.append(stock)
            except (ValueError, TypeError):
                continue
    
    if not valid_stocks:
        logger.warning(f"市场环境检查：没有有效的股票数据（所有股票的pct都是NaN或无效）")
        return False
    
    total = len(valid_stocks)
    rising_count = sum(1 for stock in valid_stocks if stock.get("pct", 0) > 0)
    ratio = rising_count / total if total > 0 else 0
    
    logger.info(f"市场环境检查：有效股票数={total}，上涨股票数={rising_count}，比率={ratio:.2%}")
    # 放宽条件：如果有效股票数太少（可能是非交易时间），直接返回True允许选股
    if total < 100:
        logger.warning(f"有效股票数过少（{total}），可能是非交易时间，放宽市场环境检查")
        return True
    # 放宽条件：从50%降低到30%，避免过于严格
    return ratio > 0.3


def filter_stocks_by_criteria(
    stocks: List[Dict[str, Any]],
    indicators_map: Dict[str, Dict[str, Any]],
    config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """基于多维度筛选股票（支持前端配置的所有指标）
    
    Args:
        stocks: 股票列表
        indicators_map: 技术指标字典 {code: indicators}
        config: 筛选配置（从前端传入）
    
    Returns:
        筛选后的股票列表
    """
    result = []
    
    # 解析筛选配置（不使用默认值，只用勾选的指标）
    volume_ratio_enable = config.get("volume_ratio_enable", False)
    volume_ratio_min = config.get("volume_ratio_min", 0.8)
    volume_ratio_max = config.get("volume_ratio_max", 8.0)
    
    rsi_enable = config.get("rsi_enable", False)
    rsi_min = config.get("rsi_min", 30)
    rsi_max = config.get("rsi_max", 75)
    
    ma_enable = config.get("ma_enable", False)
    ma_period = config.get("ma_period", "20")
    ma_condition = config.get("ma_condition", "above")
    
    ema_enable = config.get("ema_enable", False)
    ema_period = config.get("ema_period", "12")
    ema_condition = config.get("ema_condition", "above")
    
    macd_enable = config.get("macd_enable", False)
    macd_condition = config.get("macd_condition", "golden")
    
    kdj_enable = config.get("kdj_enable", False)
    kdj_condition = config.get("kdj_condition", "golden")
    
    bias_enable = config.get("bias_enable", False)
    bias_min = config.get("bias_min", -6)
    bias_max = config.get("bias_max", 6)
    
    williams_r_enable = config.get("williams_r_enable", False)
    break_high_enable = config.get("break_high_enable", False)
    
    boll_enable = config.get("boll_enable", False)
    boll_condition = config.get("boll_condition", "expanding")
    
    adx_enable = config.get("adx_enable", False)
    adx_min = config.get("adx_min", 25)
    
    ichimoku_enable = config.get("ichimoku_enable", False)
    ichimoku_condition = config.get("ichimoku_condition", "above_cloud")
    
    # 如果没有启用任何筛选条件，使用默认的基础筛选
    any_filter_enabled = any([
        volume_ratio_enable, rsi_enable, ma_enable, ema_enable, macd_enable,
        kdj_enable, bias_enable, williams_r_enable, break_high_enable,
        boll_enable, adx_enable, ichimoku_enable
    ])
    
    if not any_filter_enabled:
        # 没有启用任何筛选条件，返回全部股票
        logger.info("没有启用任何筛选条件，返回全部股票")
        return stocks
    
    for stock in stocks:
        code = str(stock.get("code", ""))
        indicators = indicators_map.get(code)
        
        if not indicators:
            continue
        
        current_price = stock.get("price", 0)
        if current_price <= 0:
            continue
        
        passed = True
        
        # 量比筛选
        if volume_ratio_enable and passed:
            vol_ratio = indicators.get("vol_ratio", 0)
            if vol_ratio < volume_ratio_min or vol_ratio > volume_ratio_max:
                passed = False
        
        # RSI筛选
        if rsi_enable and passed:
            rsi = indicators.get("rsi")
            if rsi is None or rsi < rsi_min or rsi > rsi_max:
                passed = False
        
        # MA均线筛选
        if ma_enable and passed:
            ma_key = f"ma{ma_period}"
            ma_value = indicators.get(ma_key)
            ma_trend = indicators.get(f"{ma_key}_trend", "")
            
            if ma_value:
                if ma_condition == "above":
                    # 价格上穿MA
                    if current_price <= ma_value:
                        passed = False
                elif ma_condition == "below":
                    # 价格下穿MA
                    if current_price >= ma_value:
                        passed = False
                elif ma_condition == "up":
                    # MA向上
                    if ma_trend not in ["向上", "up"]:
                        passed = False
            else:
                passed = False
        
        # EMA均线筛选
        if ema_enable and passed:
            ema_key = f"ema{ema_period}"
            ema_value = indicators.get(ema_key)
            
            if ema_value:
                if ema_condition == "above":
                    # 价格上穿EMA
                    if current_price <= ema_value:
                        passed = False
                elif ema_condition == "golden":
                    # EMA金叉（EMA12 > EMA26）
                    ema12 = indicators.get("ema12")
                    ema26 = indicators.get("ema26")
                    if ema12 and ema26:
                        if ema12 <= ema26:
                            passed = False
                    else:
                        passed = False
            else:
                passed = False
        
        # MACD筛选
        if macd_enable and passed:
            macd_dif = indicators.get("macd_dif")
            macd_dea = indicators.get("macd_dea")
            macd_dif_prev = indicators.get("macd_dif_prev")
            macd_dea_prev = indicators.get("macd_dea_prev")
            
            if macd_dif is not None and macd_dea is not None:
                if macd_condition == "golden":
                    # MACD金叉：DIF上穿DEA
                    if macd_dif_prev is not None and macd_dea_prev is not None:
                        if not (macd_dif_prev <= macd_dea_prev and macd_dif > macd_dea):
                            passed = False
                    else:
                        if macd_dif <= macd_dea:
                            passed = False
                elif macd_condition == "dead":
                    # MACD死叉：DIF下穿DEA
                    if macd_dif >= macd_dea:
                        passed = False
                elif macd_condition == "above_zero":
                    # DIF在零轴上方
                    if macd_dif <= 0:
                        passed = False
                elif macd_condition == "below_zero":
                    # DIF在零轴下方
                    if macd_dif >= 0:
                        passed = False
            else:
                passed = False
        
        # KDJ筛选
        if kdj_enable and passed:
            kdj_k = indicators.get("kdj_k")
            kdj_d = indicators.get("kdj_d")
            kdj_k_prev = indicators.get("kdj_k_prev")
            kdj_d_prev = indicators.get("kdj_d_prev")
            
            if kdj_k is not None and kdj_d is not None:
                if kdj_condition == "golden":
                    # KDJ金叉：K上穿D
                    if kdj_k_prev is not None and kdj_d_prev is not None:
                        if not (kdj_k_prev <= kdj_d_prev and kdj_k > kdj_d):
                            passed = False
                    else:
                        if kdj_k <= kdj_d:
                            passed = False
                elif kdj_condition == "dead":
                    # KDJ死叉：K下穿D
                    if kdj_k >= kdj_d:
                        passed = False
                elif kdj_condition == "oversold":
                    # 超卖区：K < 20
                    if kdj_k >= 20:
                        passed = False
                elif kdj_condition == "overbought":
                    # 超买区：K > 80
                    if kdj_k <= 80:
                        passed = False
            else:
                passed = False
        
        # BIAS乖离率筛选
        if bias_enable and passed:
            bias = indicators.get("bias")
            if bias is not None:
                if bias < bias_min or bias > bias_max:
                    passed = False
            else:
                passed = False
        
        # 威廉指标筛选
        if williams_r_enable and passed:
            williams_r = indicators.get("williams_r")
            williams_r_prev = indicators.get("williams_r_prev")
            
            if williams_r is not None:
                # 从超卖区上穿
                if williams_r_prev is not None:
                    if not (williams_r_prev < -80 and williams_r > -50):
                        passed = False
                else:
                    if williams_r < -80:
                        passed = False
            else:
                passed = False
        
        # 突破高点筛选
        if break_high_enable and passed:
            break_high = indicators.get("break_high_20d", False)
            if not break_high:
                passed = False
        
        # 布林带筛选
        if boll_enable and passed:
            boll_middle = indicators.get("boll_middle")
            boll_upper = indicators.get("boll_upper")
            boll_lower = indicators.get("boll_lower")
            boll_expanding = indicators.get("boll_expanding", False)
            
            if boll_middle:
                if boll_condition == "expanding":
                    # 布林带开口扩张
                    if not boll_expanding:
                        passed = False
                elif boll_condition == "above_mid":
                    # 价格上穿中轨
                    if current_price <= boll_middle:
                        passed = False
                elif boll_condition == "near_lower":
                    # 接近下轨（价格在下轨附近10%范围内）
                    if boll_lower and boll_middle:
                        band_width = boll_middle - boll_lower
                        if current_price > boll_lower + band_width * 0.1:
                            passed = False
            else:
                passed = False
        
        # ADX趋势筛选
        if adx_enable and passed:
            adx = indicators.get("adx")
            if adx is not None:
                if adx < adx_min:
                    passed = False
            else:
                passed = False
        
        # 一目均衡表筛选
        if ichimoku_enable and passed:
            senkou_span_a = indicators.get("ichimoku_senkou_span_a")
            senkou_span_b = indicators.get("ichimoku_senkou_span_b")
            tenkan_sen = indicators.get("ichimoku_tenkan_sen")
            kijun_sen = indicators.get("ichimoku_kijun_sen")
            
            if senkou_span_a is not None and senkou_span_b is not None:
                cloud_top = max(senkou_span_a, senkou_span_b)
                cloud_bottom = min(senkou_span_a, senkou_span_b)
                
                if ichimoku_condition == "above_cloud":
                    # 价格在云上
                    if current_price <= cloud_top:
                        passed = False
                elif ichimoku_condition == "below_cloud":
                    # 价格在云下
                    if current_price >= cloud_bottom:
                        passed = False
                elif ichimoku_condition == "tk_cross":
                    # 转换线上穿基准线
                    if tenkan_sen and kijun_sen:
                        if tenkan_sen <= kijun_sen:
                            passed = False
                    else:
                        passed = False
            else:
                passed = False
        
        # 通过所有筛选，添加到结果
        if passed:
            stock_copy = stock.copy()
            stock_copy["indicators"] = {
                "ma5": indicators.get("ma5"),
                "ma10": indicators.get("ma10"),
                "ma20": indicators.get("ma20"),
                "ma60": indicators.get("ma60"),
                "ma60_trend": indicators.get("ma60_trend"),
                "vol_ratio": indicators.get("vol_ratio"),
                "rsi": indicators.get("rsi"),
                "williams_r": indicators.get("williams_r"),
                "macd_dif": indicators.get("macd_dif"),
                "macd_dea": indicators.get("macd_dea"),
                "macd": indicators.get("macd"),
                "kdj_k": indicators.get("kdj_k"),
                "kdj_d": indicators.get("kdj_d"),
                "kdj_j": indicators.get("kdj_j"),
                "bias": indicators.get("bias"),
                "boll_upper": indicators.get("boll_upper"),
                "boll_middle": indicators.get("boll_middle"),
                "boll_lower": indicators.get("boll_lower"),
                "boll_expanding": indicators.get("boll_expanding"),
                "break_high_20d": indicators.get("break_high_20d"),
                "adx": indicators.get("adx"),
                "ema12": indicators.get("ema12"),
                "ema26": indicators.get("ema26"),
            }
            result.append(stock_copy)
    
    logger.info(f"筛选完成：从{len(stocks)}只股票中筛选出{len(result)}只")
    return result


def save_selected_stocks(stocks: List[Dict[str, Any]], market: str = "A") -> None:
    """保存选股结果到Redis
    
    Args:
        stocks: 选中的股票列表
        market: 市场类型（A或HK）
    """
    try:
        data = {
            "market": market,
            "stocks": stocks,
            "count": len(stocks),
            "timestamp": __import__("datetime").datetime.now().isoformat()
        }
        set_json(SELECTED_STOCKS_KEY, data)
        logger.info(f"选股结果已保存：{market}股市场，{len(stocks)}只股票")
    except Exception as e:
        logger.error(f"保存选股结果失败: {e}")


def get_selected_stocks() -> Dict[str, Any]:
    """获取上次选股结果
    
    Returns:
        包含market、stocks、count、timestamp的字典
    """
    try:
        data = get_json(SELECTED_STOCKS_KEY)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.debug(f"获取选股结果失败: {e}")
    
    return {"market": "A", "stocks": [], "count": 0, "timestamp": None}

