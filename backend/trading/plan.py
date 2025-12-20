"""
交易计划和结果跟踪模块
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from common.db import get_clickhouse
from common.logger import get_logger
import uuid

logger = get_logger(__name__)


def create_trade_plan(code: str, name: str, buy_price: float, sell_price: float, 
                     stop_loss: float, confidence: float, reason: str) -> Dict[str, Any]:
    """创建交易计划"""
    try:
        client = get_clickhouse()
        plan_id = int(uuid.uuid4().int % (10 ** 10))  # 生成一个10位数字ID
        now = datetime.now()
        
        client.execute(
            "INSERT INTO trade_plan (id, code, name, buy_price, sell_price, stop_loss, confidence, reason, status, created_at, buy_date, updated_at) VALUES",
            [(plan_id, code, name, buy_price, sell_price, stop_loss, confidence, reason, 'waiting_buy', now, None, now)]
        )
        
        return {
            "id": plan_id,
            "code": code,
            "name": name,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "stop_loss": stop_loss,
            "confidence": confidence,
            "reason": reason,
            "status": "waiting_buy",
            "created_at": now.isoformat(),
            "buy_date": None
        }
    except Exception as e:
        logger.error(f"创建交易计划失败: {e}", exc_info=True)
        raise


def get_active_plans(code: Optional[str] = None) -> List[Dict[str, Any]]:
    """获取活跃的交易计划（等待买入或已买入，未关闭未丢弃）
    
    使用FINAL关键字获取ReplacingMergeTree的最新状态
    """
    try:
        client = get_clickhouse()
        
        # status可以是：'waiting_buy'（等待买入）、'bought'（已买入）
        # 排除：'closed'（已关闭）、'discarded'（已丢弃）
        if code:
            rows = client.execute(
                "SELECT id, code, name, buy_price, sell_price, stop_loss, confidence, reason, status, created_at, buy_date FROM trade_plan FINAL WHERE code = %(code)s AND status IN ('waiting_buy', 'bought') ORDER BY created_at DESC",
                {"code": code}
            )
        else:
            rows = client.execute(
                "SELECT id, code, name, buy_price, sell_price, stop_loss, confidence, reason, status, created_at, buy_date FROM trade_plan FINAL WHERE status IN ('waiting_buy', 'bought') ORDER BY created_at DESC"
            )
        
        plans = []
        for row in rows:
            buy_date = row[10]
            plans.append({
                "id": row[0],
                "code": row[1],
                "name": row[2],
                "buy_price": row[3],
                "sell_price": row[4],
                "stop_loss": row[5],
                "confidence": row[6],
                "reason": row[7],
                "status": row[8],
                "created_at": row[9].isoformat() if isinstance(row[9], datetime) else str(row[9]),
                "buy_date": buy_date.isoformat() if buy_date and hasattr(buy_date, 'isoformat') else (str(buy_date) if buy_date else None)
            })
        
        return plans
    except Exception as e:
        logger.error(f"获取交易计划失败: {e}", exc_info=True)
        return []


def get_pending_plans(code: Optional[str] = None) -> List[Dict[str, Any]]:
    """获取待执行的交易计划（兼容旧接口）"""
    return get_active_plans(code)


def update_plan_status(plan_id: int, status: str, buy_date: Optional[date] = None):
    """更新交易计划状态（通过插入新记录实现）"""
    try:
        client = get_clickhouse()
        
        # 获取计划信息
        plan_rows = client.execute(
            "SELECT id, code, name, buy_price, sell_price, stop_loss, confidence, reason, created_at, buy_date FROM trade_plan FINAL WHERE id = %(plan_id)s",
            {"plan_id": plan_id}
        )
        
        if not plan_rows:
            raise ValueError(f"交易计划 {plan_id} 不存在")
        
        row = plan_rows[0]
        now = datetime.now()
        
        # 使用ReplacingMergeTree，插入新记录来更新状态
        client.execute(
            "INSERT INTO trade_plan (id, code, name, buy_price, sell_price, stop_loss, confidence, reason, created_at, status, buy_date, updated_at) VALUES",
            [(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], status, buy_date or row[9], now)]
        )
        
        logger.info(f"交易计划 {plan_id} 状态更新为: {status}")
    except Exception as e:
        logger.error(f"更新交易计划状态失败: {e}", exc_info=True)
        raise


def close_trade_plan(plan_id: int, outcome: str, entry_date: date, exit_date: date,
                    entry_price: float, exit_price: float, profit_pct: float):
    """关闭交易计划并记录结果"""
    try:
        client = get_clickhouse()
        
        # 获取计划信息
        plan_rows = client.execute(
            "SELECT code FROM trade_plan FINAL WHERE id = %(plan_id)s AND status IN ('waiting_buy', 'bought')",
            {"plan_id": plan_id}
        )
        
        if not plan_rows:
            raise ValueError(f"交易计划 {plan_id} 不存在或已关闭")
        
        code = plan_rows[0][0]
        result_id = int(uuid.uuid4().int % (10 ** 10))
        
        # 插入交易结果
        client.execute(
            "INSERT INTO trade_result (id, plan_id, code, outcome, entry_date, exit_date, entry_price, exit_price, profit_pct, created_at) VALUES",
            [(result_id, plan_id, code, outcome, entry_date, exit_date, entry_price, exit_price, profit_pct, datetime.now())]
        )
        
        # 更新计划状态为已关闭
        update_plan_status(plan_id, 'closed')
        
        return {
            "id": result_id,
            "plan_id": plan_id,
            "code": code,
            "outcome": outcome,
            "entry_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "profit_pct": profit_pct
        }
    except Exception as e:
        logger.error(f"关闭交易计划失败: {e}", exc_info=True)
        raise


def get_trade_statistics(code: Optional[str] = None) -> Dict[str, Any]:
    """获取交易统计（胜率、平均收益等）"""
    try:
        client = get_clickhouse()
        
        if code:
            stats_rows = client.execute(
                "SELECT COUNT(*) as total, SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as win_count, SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as loss_count, AVG(CASE WHEN outcome = 'win' THEN profit_pct ELSE NULL END) as avg_profit, AVG(CASE WHEN outcome = 'loss' THEN profit_pct ELSE NULL END) as avg_loss, MAX(profit_pct) as max_profit, MIN(profit_pct) as max_loss FROM trade_result WHERE code = %(code)s",
                {"code": code}
            )
        else:
            stats_rows = client.execute(
                "SELECT COUNT(*) as total, SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as win_count, SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as loss_count, AVG(CASE WHEN outcome = 'win' THEN profit_pct ELSE NULL END) as avg_profit, AVG(CASE WHEN outcome = 'loss' THEN profit_pct ELSE NULL END) as avg_loss, MAX(profit_pct) as max_profit, MIN(profit_pct) as max_loss FROM trade_result"
            )
        
        if not stats_rows or not stats_rows[0]:
            return {
                "total": 0,
                "win_count": 0,
                "loss_count": 0,
                "win_rate": 0.0,
                "avg_profit": 0.0,
                "avg_loss": 0.0,
                "max_profit": 0.0,
                "max_loss": 0.0
            }
        
        row = stats_rows[0]
        total = row[0] or 0
        win_count = row[1] or 0
        loss_count = row[2] or 0
        avg_profit = row[3] or 0.0
        avg_loss = row[4] or 0.0
        max_profit = row[5] or 0.0
        max_loss = row[6] or 0.0
        
        win_rate = (win_count / total * 100) if total > 0 else 0.0
        
        return {
            "total": total,
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": round(win_rate, 2),
            "avg_profit": round(avg_profit, 2),
            "avg_loss": round(avg_loss, 2),
            "max_profit": round(max_profit, 2),
            "max_loss": round(max_loss, 2)
        }
    except Exception as e:
        logger.error(f"获取交易统计失败: {e}", exc_info=True)
        return {
            "total": 0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0.0,
            "avg_profit": 0.0,
            "avg_loss": 0.0,
            "max_profit": 0.0,
            "max_loss": 0.0
        }


def get_stock_statistics() -> List[Dict[str, Any]]:
    """获取每只股票的统计信息"""
    try:
        client = get_clickhouse()
        
        # 获取交易结果统计
        result_rows = client.execute(
            "SELECT code, COUNT(*) as total, SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as win_count, SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as loss_count, ROUND(SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_rate, AVG(CASE WHEN outcome = 'win' THEN profit_pct ELSE NULL END) as avg_profit, AVG(CASE WHEN outcome = 'loss' THEN profit_pct ELSE NULL END) as avg_loss, MAX(profit_pct) as max_profit, MIN(profit_pct) as max_loss FROM trade_result GROUP BY code"
        )
        
        # 获取每只股票最近一次买入时间（从trade_plan表获取）
        plan_rows = client.execute(
            "SELECT code, MAX(created_at) as latest_time FROM trade_plan GROUP BY code"
        )
        
        # 构建时间映射
        time_map = {row[0]: row[1] for row in plan_rows}
        
        stats = []
        for row in result_rows:
            total = row[1] or 0
            win_count = row[2] or 0
            loss_count = row[3] or 0
            win_rate = row[4] or 0.0
            code = row[0]
            
            # 获取最近一次买入时间
            latest_time = time_map.get(code)
            latest_time_str = latest_time.isoformat() if latest_time and isinstance(latest_time, datetime) else (str(latest_time) if latest_time else None)
            
            stats.append({
                "code": code,
                "total": total,
                "win_count": win_count,
                "loss_count": loss_count,
                "win_rate": win_rate,
                "avg_profit": round(row[5] or 0.0, 2),
                "avg_loss": round(row[6] or 0.0, 2),
                "max_profit": round(row[7] or 0.0, 2),
                "max_loss": round(row[8] or 0.0, 2),
                "latest_buy_time": latest_time_str
            })
        
        # 默认按胜率从高到低排序
        stats.sort(key=lambda x: (x["win_rate"], x["total"]), reverse=True)
        
        return stats
    except Exception as e:
        logger.error(f"获取股票统计失败: {e}", exc_info=True)
        return []


def check_trade_plans_by_spot_price() -> Dict[str, Any]:
    """根据实时行情价格检查交易计划
    
    在每次采集行情数据完成后调用，自动判断：
    - 等待买入的计划：检查是否达到买入价
    - 已买入的计划：检查是否达到卖出价或止损价
    - 5天未达到买入价：标记为丢弃
    - 买入后5天未达到止盈/止损：按收盘价计算盈亏并关闭
    """
    try:
        from common.redis import get_json
        from datetime import timedelta
        
        # 获取实时行情数据
        a_stocks = get_json("market:a:spot") or []
        stock_map = {str(s.get("code", "")): s for s in a_stocks if s.get("code")}
        
        # 获取所有活跃的交易计划
        plans = get_active_plans()
        
        if not plans:
            return {
                "total": 0,
                "checked": 0,
                "bought": 0,
                "win": 0,
                "loss": 0,
                "discarded": 0,
                "timeout_closed": 0
            }
        
        checked_count = 0
        bought_count = 0
        win_count = 0
        loss_count = 0
        discarded_count = 0
        timeout_closed_count = 0
        today = date.today()
        
        for plan in plans:
            code = plan["code"]
            plan_id = plan["id"]
            status = plan["status"]
            buy_price = plan["buy_price"]
            sell_price = plan["sell_price"]
            stop_loss = plan["stop_loss"]
            created_at_str = plan.get("created_at")
            buy_date_str = plan.get("buy_date")
            
            # 解析日期
            try:
                if isinstance(created_at_str, str):
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                else:
                    created_at = created_at_str
                created_date = created_at.date() if hasattr(created_at, 'date') else created_at
            except:
                created_date = today
            
            buy_date = None
            if buy_date_str:
                try:
                    if isinstance(buy_date_str, str):
                        buy_date = date.fromisoformat(buy_date_str)
                    else:
                        buy_date = buy_date_str
                except:
                    pass
            
            # 获取股票当前价格
            stock = stock_map.get(code)
            if not stock:
                continue
            
            current_price = float(stock.get("price", 0))
            if current_price == 0:
                continue
            
            checked_count += 1
            
            # 计算天数
            days_since_created = (today - created_date).days
            days_since_bought = (today - buy_date).days if buy_date else None
            
            # 状态1：等待买入（waiting_buy）
            if status == 'waiting_buy':
                # 检查是否达到买入价
                if current_price <= buy_price:
                    # 达到买入价，更新状态为已买入
                    update_plan_status(plan_id, 'bought', today)
                    bought_count += 1
                    logger.info(f"交易计划 {plan_id} ({code}) 已买入，价格: {current_price}")
                # 检查是否超过5天未达到买入价
                elif days_since_created >= 5:
                    # 标记为丢弃
                    update_plan_status(plan_id, 'discarded')
                    discarded_count += 1
                    logger.info(f"交易计划 {plan_id} ({code}) 5天未达到买入价，已丢弃")
            
            # 状态2：已买入（bought）
            elif status == 'bought':
                if not buy_date:
                    # 如果没有买入日期，使用创建日期
                    buy_date = created_date
                    update_plan_status(plan_id, 'bought', buy_date)
                    days_since_bought = (today - buy_date).days
                
                # 检查是否达到止损价（优先判断）
                if current_price <= stop_loss:
                    # 触及止损
                    profit_pct = ((stop_loss - buy_price) / buy_price) * 100
                    close_trade_plan(
                        plan_id=plan_id,
                        outcome="loss",
                        entry_date=buy_date,
                        exit_date=today,
                        entry_price=buy_price,
                        exit_price=stop_loss,
                        profit_pct=profit_pct
                    )
                    loss_count += 1
                    logger.info(f"交易计划 {plan_id} ({code}) 触及止损，收益率={profit_pct:.2f}%")
                # 检查是否达到卖出价
                elif current_price >= sell_price:
                    # 触及卖出价
                    profit_pct = ((sell_price - buy_price) / buy_price) * 100
                    close_trade_plan(
                        plan_id=plan_id,
                        outcome="win",
                        entry_date=buy_date,
                        exit_date=today,
                        entry_price=buy_price,
                        exit_price=sell_price,
                        profit_pct=profit_pct
                    )
                    win_count += 1
                    logger.info(f"交易计划 {plan_id} ({code}) 触及卖出价，收益率={profit_pct:.2f}%")
                # 检查是否买入后5天未达到止盈/止损
                elif days_since_bought and days_since_bought >= 5:
                    # 按当前价格（收盘价）计算盈亏并关闭
                    profit_pct = ((current_price - buy_price) / buy_price) * 100
                    outcome = "win" if profit_pct > 0 else "loss"
                    close_trade_plan(
                        plan_id=plan_id,
                        outcome=outcome,
                        entry_date=buy_date,
                        exit_date=today,
                        entry_price=buy_price,
                        exit_price=current_price,
                        profit_pct=profit_pct
                    )
                    timeout_closed_count += 1
                    logger.info(f"交易计划 {plan_id} ({code}) 买入后5天未达到止盈/止损，按收盘价关闭，收益率={profit_pct:.2f}%")
        
        result = {
            "total": len(plans),
            "checked": checked_count,
            "bought": bought_count,
            "win": win_count,
            "loss": loss_count,
            "discarded": discarded_count,
            "timeout_closed": timeout_closed_count,
            "date": today.isoformat()
        }
        
        logger.info(f"交易计划检查完成：{result}")
        return result
        
    except Exception as e:
        logger.error(f"检查交易计划失败: {e}", exc_info=True)
        return {
            "total": 0,
            "checked": 0,
            "bought": 0,
            "win": 0,
            "loss": 0,
            "discarded": 0,
            "timeout_closed": 0,
            "error": str(e)
        }



