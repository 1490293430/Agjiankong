"""
Tushare 数据源采集模块

使用 Tushare Pro API 获取 A 股数据
"""
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from common.logger import get_logger
from common.runtime_config import get_runtime_config

logger = get_logger(__name__)

# Tushare 实例（延迟初始化）
_ts_api = None


def get_tushare_api():
    """获取 Tushare API 实例"""
    global _ts_api
    
    if _ts_api is not None:
        return _ts_api
    
    config = get_runtime_config()
    token = config.tushare_token
    
    if not token:
        logger.warning("Tushare Token 未配置，请在配置页设置")
        return None
    
    try:
        import tushare as ts
        ts.set_token(token)
        _ts_api = ts.pro_api()
        logger.info("Tushare API 初始化成功")
        return _ts_api
    except ImportError:
        logger.error("tushare 库未安装，请运行: pip install tushare")
        return None
    except Exception as e:
        logger.error(f"Tushare API 初始化失败: {e}")
        return None


def reset_tushare_api():
    """重置 Tushare API（Token 更新后调用）"""
    global _ts_api
    _ts_api = None


def fetch_stock_list_tushare() -> List[Dict[str, Any]]:
    """从 Tushare 获取 A 股股票列表（只返回股票，不包含ETF/基金/指数）"""
    api = get_tushare_api()
    if not api:
        return []
    
    try:
        # 获取股票基本信息
        # stock_basic接口只返回股票，不包含ETF、基金、指数等
        df = api.stock_basic(
            exchange='',
            list_status='L',  # 上市状态：L=上市，D=退市，P=暂停上市
            fields='ts_code,symbol,name,area,industry,market,list_date'
        )
        
        if df is None or df.empty:
            logger.warning("Tushare 返回空数据")
            return []
        
        stocks = []
        for _, row in df.iterrows():
            # ts_code 格式: 000001.SZ -> 转换为 000001
            code = row['symbol']
            name = row['name']
            
            # 额外过滤：排除退市、ST等特殊股票（可选）
            # 如果名称包含"退"、"PT"等，跳过
            if '退' in name or name.startswith('PT'):
                continue
            
            stocks.append({
                'code': code,
                'name': name,
                'industry': row.get('industry', ''),
                'area': row.get('area', ''),
                'market': row.get('market', ''),
                'list_date': row.get('list_date', ''),
            })
        
        logger.info(f"从 Tushare 获取到 {len(stocks)} 只 A 股（已过滤退市股票）")
        return stocks
    except Exception as e:
        logger.error(f"Tushare 获取股票列表失败: {e}")
        return []


def fetch_daily_kline_tushare(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 500
) -> List[Dict[str, Any]]:
    """从 Tushare 获取日线 K 线数据
    
    Args:
        code: 股票代码（如 000001）
        start_date: 开始日期（YYYYMMDD 格式）
        end_date: 结束日期（YYYYMMDD 格式）
        limit: 最大返回条数
    
    Returns:
        K 线数据列表
    """
    api = get_tushare_api()
    if not api:
        return []
    
    try:
        # 转换代码格式：000001 -> 000001.SZ 或 600000 -> 600000.SH
        if code.startswith('6'):
            ts_code = f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            ts_code = f"{code}.SZ"
        elif code.startswith('8') or code.startswith('4'):
            ts_code = f"{code}.BJ"  # 北交所
        else:
            ts_code = f"{code}.SZ"
        
        # 默认获取最近一年数据
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        # 获取日线数据
        df = api.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        
        if df is None or df.empty:
            logger.debug(f"Tushare 返回空数据: {code}")
            return []
        
        # 按日期升序排列
        df = df.sort_values('trade_date', ascending=True)
        
        # 限制返回条数
        if len(df) > limit:
            df = df.tail(limit)
        
        klines = []
        for _, row in df.iterrows():
            # 转换日期格式: 20231225 -> 2023-12-25
            date_str = row['trade_date']
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            klines.append({
                'date': formatted_date,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['vol'] * 100),  # Tushare 单位是手，转换为股
                'amount': float(row['amount'] * 1000),  # Tushare 单位是千元，转换为元
                'pct_chg': float(row.get('pct_chg', 0)),
            })
        
        logger.debug(f"从 Tushare 获取 {code} 日线数据 {len(klines)} 条")
        return klines
    except Exception as e:
        logger.error(f"Tushare 获取 {code} 日线数据失败: {e}")
        return []


def fetch_realtime_quotes_tushare(codes: List[str]) -> List[Dict[str, Any]]:
    """从 Tushare 获取实时行情（注意：Tushare 实时行情需要较高积分）
    
    Args:
        codes: 股票代码列表
    
    Returns:
        实时行情数据列表
    """
    api = get_tushare_api()
    if not api:
        return []
    
    try:
        # 转换代码格式
        ts_codes = []
        for code in codes:
            if code.startswith('6'):
                ts_codes.append(f"{code}.SH")
            elif code.startswith('0') or code.startswith('3'):
                ts_codes.append(f"{code}.SZ")
            elif code.startswith('8') or code.startswith('4'):
                ts_codes.append(f"{code}.BJ")
            else:
                ts_codes.append(f"{code}.SZ")
        
        # 获取当日行情
        today = datetime.now().strftime('%Y%m%d')
        df = api.daily(
            ts_code=','.join(ts_codes),
            trade_date=today
        )
        
        if df is None or df.empty:
            # 如果当日没有数据，尝试获取最近交易日
            df = api.daily(
                ts_code=','.join(ts_codes),
                end_date=today,
                limit=len(codes)
            )
        
        if df is None or df.empty:
            return []
        
        quotes = []
        for _, row in df.iterrows():
            # 提取代码
            ts_code = row['ts_code']
            code = ts_code.split('.')[0]
            
            quotes.append({
                'code': code,
                'price': float(row['close']),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'pre_close': float(row.get('pre_close', row['close'])),
                'volume': int(row['vol'] * 100),
                'amount': float(row['amount'] * 1000),
                'pct': float(row.get('pct_chg', 0)),
            })
        
        return quotes
    except Exception as e:
        logger.error(f"Tushare 获取实时行情失败: {e}")
        return []


def check_tushare_connection() -> Dict[str, Any]:
    """检查 Tushare 连接状态"""
    config = get_runtime_config()
    token = config.tushare_token
    
    if not token:
        return {
            'connected': False,
            'message': 'Tushare Token 未配置',
            'token_configured': False
        }
    
    api = get_tushare_api()
    if not api:
        return {
            'connected': False,
            'message': 'Tushare API 初始化失败',
            'token_configured': True
        }
    
    try:
        # 尝试获取交易日历来测试连接
        df = api.trade_cal(exchange='SSE', is_open='1', limit=1)
        if df is not None and not df.empty:
            return {
                'connected': True,
                'message': 'Tushare 连接正常',
                'token_configured': True
            }
        else:
            return {
                'connected': False,
                'message': 'Tushare 返回空数据',
                'token_configured': True
            }
    except Exception as e:
        return {
            'connected': False,
            'message': f'Tushare 连接失败: {str(e)}',
            'token_configured': True
        }
