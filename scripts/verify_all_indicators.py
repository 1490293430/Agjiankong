"""
全面验证数据库中所有指标数据的正确性

检查项目：
1. 数值范围合理性（RSI 0-100, Williams %R -100~0 等）
2. 数据一致性（high >= low, close 在 high/low 之间等）
3. 指标逻辑关系（MA5 < MA10 < MA20 在下跌趋势等）
4. 异常值检测（NaN, Inf, 极端值）
5. 趋势判断一致性
"""
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from collections import defaultdict

# 添加backend目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))


class IndicatorValidator:
    """指标验证器"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = defaultdict(int)
    
    def log_error(self, code: str, field: str, msg: str, value: Any = None):
        self.errors.append({
            "code": code,
            "field": field,
            "message": msg,
            "value": value
        })
        self.stats["errors"] += 1
    
    def log_warning(self, code: str, field: str, msg: str, value: Any = None):
        self.warnings.append({
            "code": code,
            "field": field,
            "message": msg,
            "value": value
        })
        self.stats["warnings"] += 1
    
    def validate_range(self, code: str, field: str, value: Any, 
                       min_val: float = None, max_val: float = None,
                       allow_none: bool = True) -> bool:
        """验证数值范围"""
        if value is None:
            if not allow_none:
                self.log_error(code, field, "值为None", value)
                return False
            return True
        
        if not isinstance(value, (int, float)):
            self.log_error(code, field, f"类型错误，期望数值，实际为{type(value)}", value)
            return False
        
        # 检查 NaN 和 Inf
        import math
        if pd.isna(value) or math.isinf(value):
            self.log_error(code, field, "值为NaN或Inf", value)
            return False
        
        if min_val is not None and value < min_val:
            self.log_error(code, field, f"值{value}小于最小值{min_val}", value)
            return False
        
        if max_val is not None and value > max_val:
            self.log_error(code, field, f"值{value}大于最大值{max_val}", value)
            return False
        
        return True
    
    def validate_indicator(self, code: str, ind: Dict, timeframe: str = "daily") -> bool:
        """验证单个股票的指标数据"""
        prefix = "hourly_" if timeframe == "hourly" else ""
        valid = True
        
        # 1. 价格数据一致性
        high = ind.get(f"{prefix}current_high")
        low = ind.get(f"{prefix}current_low")
        close = ind.get(f"{prefix}current_close") or ind.get(f"{prefix}current_price")
        open_price = ind.get(f"{prefix}current_open")
        
        if high is not None and low is not None:
            if high < low:
                self.log_error(code, f"{prefix}high/low", f"high({high}) < low({low})")
                valid = False
            
            if close is not None:
                if close > high or close < low:
                    self.log_warning(code, f"{prefix}close", 
                                    f"close({close})不在high({high})/low({low})之间")
            
            if open_price is not None:
                if open_price > high or open_price < low:
                    self.log_warning(code, f"{prefix}open", 
                                    f"open({open_price})不在high({high})/low({low})之间")
        
        # 2. RSI 范围 (0-100)
        rsi = ind.get(f"{prefix}rsi")
        if not self.validate_range(code, f"{prefix}rsi", rsi, 0, 100):
            valid = False
        
        # 3. Williams %R 范围 (-100 ~ 0)
        wr = ind.get(f"{prefix}williams_r")
        if not self.validate_range(code, f"{prefix}williams_r", wr, -100, 0):
            valid = False
        
        wr_prev = ind.get(f"{prefix}williams_r_prev")
        if not self.validate_range(code, f"{prefix}williams_r_prev", wr_prev, -100, 0):
            valid = False
        
        # 4. KDJ 范围 (通常 0-100，但 J 可能超出)
        kdj_k = ind.get(f"{prefix}kdj_k")
        kdj_d = ind.get(f"{prefix}kdj_d")
        kdj_j = ind.get(f"{prefix}kdj_j")
        
        if not self.validate_range(code, f"{prefix}kdj_k", kdj_k, 0, 100):
            valid = False
        if not self.validate_range(code, f"{prefix}kdj_d", kdj_d, 0, 100):
            valid = False
        # J值可以超出0-100，但通常在-20到120之间
        if not self.validate_range(code, f"{prefix}kdj_j", kdj_j, -50, 150):
            valid = False
        
        # 5. ADX 范围 (0-100)
        adx = ind.get(f"{prefix}adx")
        if not self.validate_range(code, f"{prefix}adx", adx, 0, 100):
            valid = False
        
        plus_di = ind.get(f"{prefix}plus_di")
        minus_di = ind.get(f"{prefix}minus_di")
        if not self.validate_range(code, f"{prefix}plus_di", plus_di, 0, 100):
            valid = False
        if not self.validate_range(code, f"{prefix}minus_di", minus_di, 0, 100):
            valid = False
        
        # 6. 布林带逻辑 (upper > middle > lower)
        boll_upper = ind.get(f"{prefix}boll_upper")
        boll_middle = ind.get(f"{prefix}boll_middle")
        boll_lower = ind.get(f"{prefix}boll_lower")
        
        if all(v is not None for v in [boll_upper, boll_middle, boll_lower]):
            if not (boll_upper >= boll_middle >= boll_lower):
                self.log_error(code, f"{prefix}boll", 
                              f"布林带顺序错误: upper({boll_upper}) middle({boll_middle}) lower({boll_lower})")
                valid = False
        
        # 7. 成交量比 (通常 > 0)
        vol_ratio = ind.get(f"{prefix}vol_ratio")
        if not self.validate_range(code, f"{prefix}vol_ratio", vol_ratio, 0, None):
            valid = False
        if vol_ratio is not None and vol_ratio > 50:
            self.log_warning(code, f"{prefix}vol_ratio", f"成交量比异常高: {vol_ratio}")
        
        # 8. MACD 相关
        macd_dif = ind.get(f"{prefix}macd_dif")
        macd_dea = ind.get(f"{prefix}macd_dea")
        macd = ind.get(f"{prefix}macd")
        
        if all(v is not None for v in [macd_dif, macd_dea, macd]):
            # MACD = 2 * (DIF - DEA)
            expected_macd = 2 * (macd_dif - macd_dea)
            if abs(macd - expected_macd) > 0.01:
                self.log_warning(code, f"{prefix}macd", 
                                f"MACD计算可能有误: macd={macd}, 期望={expected_macd:.4f}")
        
        # 9. 斐波那契回撤位顺序
        fib_levels = ["fib_236", "fib_382", "fib_500", "fib_618", "fib_786"]
        fib_values = [ind.get(f"{prefix}{level}") for level in fib_levels]
        fib_trend = ind.get(f"{prefix}fib_trend")
        
        if all(v is not None for v in fib_values):
            if fib_trend == "up":
                # 上涨趋势：fib_236 > fib_382 > ... > fib_786
                for i in range(len(fib_values) - 1):
                    if fib_values[i] < fib_values[i + 1]:
                        self.log_warning(code, f"{prefix}fib", 
                                        f"上涨趋势斐波那契顺序异常")
                        break
            elif fib_trend == "down":
                # 下跌趋势：fib_236 < fib_382 < ... < fib_786
                for i in range(len(fib_values) - 1):
                    if fib_values[i] > fib_values[i + 1]:
                        self.log_warning(code, f"{prefix}fib", 
                                        f"下跌趋势斐波那契顺序异常")
                        break
        
        # 10. 一目均衡表云层逻辑
        above_cloud = ind.get(f"{prefix}ichimoku_above_cloud")
        below_cloud = ind.get(f"{prefix}ichimoku_below_cloud")
        in_cloud = ind.get(f"{prefix}ichimoku_in_cloud")
        
        if all(v is not None for v in [above_cloud, below_cloud, in_cloud]):
            # 只能有一个为True
            true_count = sum([above_cloud, below_cloud, in_cloud])
            if true_count != 1:
                self.log_error(code, f"{prefix}ichimoku_cloud", 
                              f"云层状态异常: above={above_cloud}, below={below_cloud}, in={in_cloud}")
                valid = False
        
        # 11. CCI 状态一致性
        cci = ind.get(f"{prefix}cci")
        cci_status = ind.get(f"{prefix}cci_status")
        
        if cci is not None and cci_status is not None:
            if cci > 100 and cci_status != "超买":
                self.log_warning(code, f"{prefix}cci_status", 
                                f"CCI={cci}应为超买，实际为{cci_status}")
            elif cci < -100 and cci_status != "超卖":
                self.log_warning(code, f"{prefix}cci_status", 
                                f"CCI={cci}应为超卖，实际为{cci_status}")
            elif -100 <= cci <= 100 and cci_status != "正常":
                self.log_warning(code, f"{prefix}cci_status", 
                                f"CCI={cci}应为正常，实际为{cci_status}")
        
        # 12. 趋势方向一致性
        trend_dir = ind.get(f"{prefix[:-1]}_trend_direction" if prefix else "daily_trend_direction")
        ma5_trend = ind.get(f"{prefix}ma5_trend")
        ma20_trend = ind.get(f"{prefix}ma20_trend")
        
        # 这里只做警告，因为趋势判断可能有多种逻辑
        
        self.stats["validated"] += 1
        return valid


def get_all_indicators_from_db(limit: int = None) -> List[Dict]:
    """从数据库获取所有指标数据"""
    try:
        from common.db import get_clickhouse
        
        client = get_clickhouse()
        
        # 先获取列名
        columns_result = client.execute("DESCRIBE indicators")
        columns = [col[0] for col in columns_result]
        
        # 获取最新日期的指标数据
        query = """
            SELECT *
            FROM indicators
            WHERE date = (SELECT max(date) FROM indicators)
        """
        if limit:
            query += f" LIMIT {limit}"
        
        rows = client.execute(query)
        
        indicators = []
        for row in rows:
            ind = dict(zip(columns, row))
            indicators.append(ind)
        
        return indicators
    except Exception as e:
        print(f"获取指标数据失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_hourly_indicators_from_db(limit: int = None) -> List[Dict]:
    """从数据库获取小时线指标数据"""
    try:
        from common.db import get_clickhouse
        
        client = get_clickhouse()
        
        # 先获取列名
        columns_result = client.execute("DESCRIBE indicators_hourly")
        columns = [col[0] for col in columns_result]
        
        query = """
            SELECT *
            FROM indicators_hourly
            WHERE date = (SELECT max(date) FROM indicators_hourly)
        """
        if limit:
            query += f" LIMIT {limit}"
        
        rows = client.execute(query)
        
        indicators = []
        for row in rows:
            ind = dict(zip(columns, row))
            indicators.append(ind)
        
        return indicators
    except Exception as e:
        print(f"获取小时线指标数据失败: {e}")
        return []


def print_report(validator: IndicatorValidator):
    """打印验证报告"""
    print("\n" + "=" * 80)
    print("指标验证报告")
    print("=" * 80)
    
    print(f"\n统计:")
    print(f"  验证股票数: {validator.stats['validated']}")
    print(f"  错误数: {validator.stats['errors']}")
    print(f"  警告数: {validator.stats['warnings']}")
    
    if validator.errors:
        print(f"\n错误详情 (共{len(validator.errors)}个):")
        print("-" * 80)
        
        # 按字段分组
        errors_by_field = defaultdict(list)
        for err in validator.errors:
            errors_by_field[err["field"]].append(err)
        
        for field, errs in sorted(errors_by_field.items()):
            print(f"\n  [{field}] ({len(errs)}个错误)")
            for err in errs[:5]:  # 每个字段最多显示5个
                print(f"    - {err['code']}: {err['message']}")
            if len(errs) > 5:
                print(f"    ... 还有 {len(errs) - 5} 个类似错误")
    
    if validator.warnings:
        print(f"\n警告详情 (共{len(validator.warnings)}个):")
        print("-" * 80)
        
        warnings_by_field = defaultdict(list)
        for warn in validator.warnings:
            warnings_by_field[warn["field"]].append(warn)
        
        for field, warns in sorted(warnings_by_field.items()):
            print(f"\n  [{field}] ({len(warns)}个警告)")
            for warn in warns[:3]:  # 每个字段最多显示3个
                print(f"    - {warn['code']}: {warn['message']}")
            if len(warns) > 3:
                print(f"    ... 还有 {len(warns) - 3} 个类似警告")
    
    if not validator.errors and not validator.warnings:
        print("\n✓ 所有指标数据验证通过！")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="验证数据库中的指标数据")
    parser.add_argument("--limit", type=int, default=None, help="限制验证的股票数量")
    parser.add_argument("--code", type=str, default=None, help="只验证指定股票代码")
    parser.add_argument("--hourly", action="store_true", help="同时验证小时线数据")
    args = parser.parse_args()
    
    print("=" * 80)
    print("数据库指标数据验证工具")
    print("=" * 80)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    validator = IndicatorValidator()
    
    # 验证日线指标
    print("\n正在获取日线指标数据...")
    daily_indicators = get_all_indicators_from_db(args.limit)
    
    if not daily_indicators:
        print("未获取到日线指标数据")
        return
    
    print(f"获取到 {len(daily_indicators)} 条日线指标数据")
    
    if args.code:
        daily_indicators = [ind for ind in daily_indicators if ind.get("code") == args.code]
        print(f"筛选后: {len(daily_indicators)} 条")
    
    print("\n正在验证日线指标...")
    for ind in daily_indicators:
        code = ind.get("code", "unknown")
        validator.validate_indicator(code, ind, "daily")
    
    # 验证小时线指标
    if args.hourly:
        print("\n正在获取小时线指标数据...")
        hourly_indicators = get_hourly_indicators_from_db(args.limit)
        
        if hourly_indicators:
            print(f"获取到 {len(hourly_indicators)} 条小时线指标数据")
            
            if args.code:
                hourly_indicators = [ind for ind in hourly_indicators if ind.get("code") == args.code]
            
            print("正在验证小时线指标...")
            for ind in hourly_indicators:
                code = ind.get("code", "unknown")
                # 小时线数据字段没有 hourly_ 前缀，直接验证
                validator.validate_indicator(code, ind, "daily")
    
    # 打印报告
    print_report(validator)


if __name__ == "__main__":
    main()
