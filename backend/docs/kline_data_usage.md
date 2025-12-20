# K线历史数据使用情况汇总

## 数据获取方式

### 1. `fetch_a_stock_kline()` / `fetch_hk_stock_kline()`
**位置**: `backend/market_collector/cn.py` / `backend/market_collector/hk.py`

**策略**: 增量获取 + 自动保存
- 先从ClickHouse查询最新日期
- 如果数据库有数据：只获取增量（从最新日期之后）
- 如果数据库没有数据：全量获取并保存
- 返回完整数据（历史+增量）

**参数**: `skip_db=False` (默认会保存到数据库)

### 2. `get_kline_from_db()`
**位置**: `backend/common/db.py`

**策略**: 只从ClickHouse读取，不获取新数据
- 直接从数据库查询
- 不调用外部数据源
- 不保存新数据

---

## 使用历史数据的功能模块

### 1. 选股功能 (`/strategy/select`)
**文件**: `backend/gateway/app.py:223`

**数据获取**:
- 股票列表: `get_stock_list_from_db()` - 从ClickHouse kline表获取
- MA60计算: `fetch_a_stock_kline(code, "daily", "", None, None, False, False)` - 增量获取并保存
- 完整指标: `fetch_a_stock_kline(code, "daily", "", None, None, False, False)` - 增量获取并保存

**需要的数据量**: 至少60根K线（用于计算MA60）

---

### 2. AI单只股票分析 (`/ai/analyze/{code}`)
**文件**: `backend/gateway/app.py:818`

**数据获取**:
```python
kline_data = fetch_a_stock_kline(code, period=ai_period)
```
- 使用配置的周期（daily或1h）
- 增量获取并保存
- 限制数量为配置的根数（默认500根）

**需要的数据量**: 至少20根K线

---

### 3. AI批量分析 (`/ai/analyze/batch`)
**文件**: `backend/gateway/app.py:990`

**数据获取**:
```python
kline_data = fetch_a_stock_kline(code, period=ai_period)
```
- 批量分析多只股票
- 增量获取并保存
- 限制数量为配置的根数

**需要的数据量**: 至少20根K线

---

### 4. 指标批量计算 (`batch_compute_indicators`)
**文件**: `backend/strategy/indicator_batch.py:16`

**数据获取**:
```python
# 先尝试从数据库读取
kline_data = get_kline_from_db(code, None, None, "daily")

# 如果数据库没有或不足，从数据源获取
if not kline_data or len(kline_data) < 60:
    kline_data = fetch_kline_func(code, period="daily")
```

**策略**: 
- 优先从数据库读取（快速）
- 如果数据库没有或不足60根，从数据源获取（会保存）

**需要的数据量**: 至少60根K线

---

### 5. 市场服务API (`/market/kline`)
**文件**: `backend/market/service/api.py:193`

**数据获取**:
```python
kline_data = fetch_a_stock_kline(code, period, adjust, start_date, end_date)
```
- 用于前端图表显示
- 增量获取并保存
- 支持自定义日期范围

---

### 6. 前端图表显示
**文件**: `frontend/app.js:618`

**数据获取**: 通过 `/market/kline` API
- 调用 `fetch_a_stock_kline()` 获取数据
- 增量获取并保存

---

## 数据流向总结

```
外部数据源 (akshare等)
    ↓
fetch_a_stock_kline() / fetch_hk_stock_kline()
    ↓ (增量获取策略)
ClickHouse kline表 (保存历史数据)
    ↓
get_kline_from_db() (读取历史数据)
    ↓
计算技术指标 (MA60, RSI, MACD等)
    ↓
选股/AI分析/图表显示
```

---

## 关键发现

1. **所有功能都使用增量策略**: `fetch_a_stock_kline()` 会自动检查数据库，只获取新数据
2. **数据会自动保存**: 除了 `get_kline_from_db()` 只读，其他都会保存到ClickHouse
3. **优先使用数据库**: 指标批量计算会先尝试从数据库读取，没有才获取
4. **数据量要求**:
   - 选股: 至少60根（MA60需要）
   - AI分析: 至少20根
   - 指标计算: 至少60根

---

## 建议

所有使用历史数据的地方都已经正确配置：
- ✅ 使用 `fetch_a_stock_kline()` 会自动增量获取并保存
- ✅ 使用 `get_kline_from_db()` 会优先从数据库读取
- ✅ 数据会自动保存到ClickHouse，无需手动采集

**唯一需要手动操作**: 首次批量采集K线数据（点击"采集K线数据"按钮）

