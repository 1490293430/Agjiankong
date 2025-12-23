"""
新浪财经实时行情数据源
"""
import requests
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from common.logger import get_logger

logger = get_logger(__name__)

# 新浪财经行情接口
SINA_API_URL = "http://hq.sinajs.cn/list="


def fetch_sina_stock_spot(codes: List[str] = None, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    从新浪财经获取A股实时行情
    
    Args:
        codes: 股票代码列表，如果为None则获取全部A股
        max_retries: 最大重试次数
    
    Returns:
        股票行情列表
    """
    for attempt in range(max_retries):
        try:
            # 如果没有指定代码，先获取股票列表
            if not codes:
                codes = _get_all_a_stock_codes()
                if not codes:
                    raise Exception("无法获取A股代码列表")
            
            logger.info(f"[新浪] 开始获取 {len(codes)} 只股票行情...")
            
            # 分批获取（每批最多800只）
            batch_size = 800
            all_results = []
            
            for i in range(0, len(codes), batch_size):
                batch_codes = codes[i:i + batch_size]
                batch_results = _fetch_batch_sina(batch_codes)
                all_results.extend(batch_results)
                
            logger.info(f"[新浪] 获取完成，共 {len(all_results)} 只股票")
            return all_results
            
        except Exception as e:
            logger.warning(f"[新浪] 第{attempt + 1}次尝试失败: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)
            else:
                logger.error(f"[新浪] 获取A股行情失败，已重试{max_retries}次")
                raise
    
    return []


def _get_all_a_stock_codes() -> List[str]:
    """获取所有A股代码列表"""
    try:
        # 使用akshare获取股票列表
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        codes = df['代码'].tolist()
        return codes
    except Exception as e:
        logger.error(f"[新浪] 获取A股代码列表失败: {e}")
        return []


def _fetch_batch_sina(codes: List[str]) -> List[Dict[str, Any]]:
    """批量获取新浪行情数据"""
    results = []
    
    # 转换代码格式：600519 -> sh600519, 000001 -> sz000001
    sina_codes = []
    for code in codes:
        code = str(code).zfill(6)
        if code.startswith(('6', '9')):
            sina_codes.append(f"sh{code}")
        elif code.startswith(('0', '3', '2')):
            sina_codes.append(f"sz{code}")
        elif code.startswith(('4', '8')):
            sina_codes.append(f"bj{code}")  # 北交所
    
    if not sina_codes:
        return results
    
    # 请求新浪接口
    url = SINA_API_URL + ",".join(sina_codes)
    headers = {
        "Referer": "http://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'gbk'
        
        # 解析返回数据
        # 格式: var hq_str_sh600519="贵州茅台,1800.00,1795.00,1810.50,...";
        pattern = r'var hq_str_(\w+)="([^"]+)";'
        matches = re.findall(pattern, response.text)
        
        for sina_code, data_str in matches:
            if not data_str:
                continue
            
            try:
                fields = data_str.split(',')
                if len(fields) < 32:
                    continue
                
                # 提取代码
                code = sina_code[2:]  # 去掉sh/sz/bj前缀
                
                # 解析数据
                result = {
                    "code": code,
                    "name": fields[0],
                    "open": _safe_float(fields[1]),
                    "pre_close": _safe_float(fields[2]),
                    "price": _safe_float(fields[3]),
                    "high": _safe_float(fields[4]),
                    "low": _safe_float(fields[5]),
                    "volume": _safe_float(fields[8]) / 100,  # 转换为手
                    "amount": _safe_float(fields[9]),
                    "update_time": datetime.now().isoformat(),
                    "market": "A"
                }
                
                # 计算涨跌幅
                if result["pre_close"] and result["pre_close"] > 0:
                    result["pct"] = round((result["price"] - result["pre_close"]) / result["pre_close"] * 100, 2)
                    result["change"] = round(result["price"] - result["pre_close"], 2)
                else:
                    result["pct"] = 0
                    result["change"] = 0
                
                results.append(result)
                
            except Exception as e:
                logger.debug(f"[新浪] 解析股票数据失败: {sina_code}, {e}")
                continue
                
    except Exception as e:
        logger.error(f"[新浪] 请求失败: {e}")
        raise
    
    return results


def _safe_float(value: str) -> float:
    """安全转换为浮点数"""
    try:
        return float(value) if value else 0.0
    except (ValueError, TypeError):
        return 0.0
