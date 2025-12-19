const { createChart, ColorType } = window.LightweightCharts || {};

const API_BASE = window.location.origin;
let apiToken = null;
let adminToken = null;
let chart = null;
let candleSeries = null;
let volumeSeries = null;
let ws = null;

// 初始化
document.addEventListener('DOMContentLoaded', async () => {
    await initAuth();
});

function startApp() {
    initTabs();
    initMarket();
    initWatchlist();
    initKlineModal();
    initStrategy();
    initAI();
    initNews();
    initConfig();
}

// 统一封装带 Token 的请求
async function apiFetch(url, options = {}) {
    const headers = options.headers ? { ...options.headers } : {};
    if (apiToken) {
        headers['X-API-Token'] = apiToken;
    }
    if (adminToken) {
        headers['X-Admin-Token'] = adminToken;
    }
    return fetch(url, { ...options, headers });
}

// 标签切换
function initTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    const contents = document.querySelectorAll('.tab-content');
    
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetTab = tab.dataset.tab;
            
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));
            
            tab.classList.add('active');
            document.getElementById(`${targetTab}-tab`).classList.add('active');
            
            // 切换到自选页时，刷新自选股列表和行情
            if (targetTab === 'watchlist') {
                loadWatchlist();
            }
        });
    });
}

// 行情模块
let currentPage = 1;
const pageSize = 30;
let isLoading = false;
let hasMore = true;
let currentMarket = 'a';

let marketRefreshInterval = null;

async function initMarket() {
    const marketSelect = document.getElementById('market-select');
    const searchInput = document.getElementById('search-input');
    const refreshBtn = document.getElementById('refresh-btn');
    const container = document.querySelector('.stock-list-container');
    
    refreshBtn.addEventListener('click', () => resetAndLoadMarket());
    marketSelect.addEventListener('change', () => resetAndLoadMarket());
    searchInput.addEventListener('input', handleSearch);
    
    // 监听滚动事件实现无限加载
    if (container) {
        container.addEventListener('scroll', () => {
            const scrollTop = container.scrollTop;
            const scrollHeight = container.scrollHeight;
            const clientHeight = container.clientHeight;
            
            // 距离底部100px时加载下一页
            if (scrollTop + clientHeight >= scrollHeight - 100 && !isLoading && hasMore) {
                loadMarket();
            }
        });
    }
    
    await loadMarket();
    
    // 无感自动刷新：每30秒静默刷新当前页数据（不重置分页）
    marketRefreshInterval = setInterval(() => {
        if (!isLoading && currentPage === 1) {
            silentRefreshMarket();
        }
    }, 30000); // 30秒刷新一次
}

// 静默刷新（不显示加载提示，不重置分页）
async function silentRefreshMarket() {
    if (isLoading) return;
    
    isLoading = true;
    const market = document.getElementById('market-select').value;
    
    try {
        // 添加超时控制，避免长时间等待
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000); // 5秒超时
        
        const response = await apiFetch(`${API_BASE}/api/market/${market}/spot?page=1&page_size=${pageSize}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        const result = await response.json();
        
        if (result.code === 0 && result.data && result.data.length > 0) {
            // 只更新第一页数据，保持滚动位置
            const tbody = document.getElementById('stock-list');
            if (!tbody) {
                isLoading = false;
                return;
            }
            
            const firstPageRows = Math.min(pageSize, result.data.length);
            const existingRows = tbody.querySelectorAll('tr');
            
            // 只更新前30条数据，避免DOM操作过多
            const updateCount = Math.min(firstPageRows, existingRows.length);
            for (let index = 0; index < updateCount; index++) {
                if (existingRows[index] && result.data[index]) {
                    const stock = result.data[index];
                    const watchlist = getWatchlist();
                    const isInWatchlist = watchlist.some(s => s.code === stock.code);
                    const row = existingRows[index];
                    row.setAttribute('data-stock', JSON.stringify(stock));
                    row.style.cursor = 'pointer';
                    row.innerHTML = `
                        <td>${stock.code}</td>
                        <td>${stock.name}</td>
                        <td>${stock.price?.toFixed(2) || '-'}</td>
                        <td class="${stock.pct >= 0 ? 'up' : 'down'}">
                            ${stock.pct?.toFixed(2) || '-'}%
                        </td>
                        <td>${formatVolume(stock.volume)}</td>
                        <td>
                            <button class="add-watchlist-btn" data-code="${stock.code}" data-name="${stock.name}" style="padding: 4px 8px; background: ${isInWatchlist ? '#94a3b8' : '#10b981'}; color: white; border: none; border-radius: 4px; cursor: pointer; ${isInWatchlist ? 'opacity: 0.6;' : ''}" onclick="event.stopPropagation();">${isInWatchlist ? '已添加' : '加入自选'}</button>
                        </td>
                    `;
                    
                    // 重新绑定单击事件
                    row.addEventListener('click', function(e) {
                        // 如果点击的是按钮，不触发
                        if (e.target.tagName === 'BUTTON' || e.target.closest('button')) {
                            return;
                        }
                        e.preventDefault();
                        const stockData = JSON.parse(this.getAttribute('data-stock'));
                        openKlineModal(stockData.code, stockData.name, stockData);
                    });
                }
            }
            
            // 重新绑定按钮事件（只绑定新更新的按钮）
            existingRows.forEach((row, index) => {
                if (index < updateCount) {
                    const watchlistBtn = row.querySelector('.add-watchlist-btn');
                    if (watchlistBtn) {
                        const code = watchlistBtn.getAttribute('data-code');
                        const name = watchlistBtn.getAttribute('data-name');
                        watchlistBtn.onclick = function(e) {
                            e.preventDefault();
                            e.stopPropagation();
                            addToWatchlist(code, name);
                        };
                    }
                }
            });
        }
    } catch (error) {
        if (error.name !== 'AbortError') {
            console.error('静默刷新失败:', error);
        }
    } finally {
        isLoading = false;
    }
}

function resetAndLoadMarket() {
    currentPage = 1;
    hasMore = true;
    document.getElementById('stock-list').innerHTML = '';
    loadMarket();
}

// 初始化时更新按钮状态
function updateWatchlistButtonStates() {
    const watchlist = getWatchlist();
    document.querySelectorAll('.add-watchlist-btn').forEach(btn => {
        const code = btn.getAttribute('data-code');
        if (watchlist.some(s => s.code === code)) {
            btn.textContent = '已添加';
            btn.style.background = '#94a3b8';
            btn.disabled = true;
        } else {
            btn.textContent = '加入自选';
            btn.style.background = '#10b981';
            btn.disabled = false;
        }
    });
}

async function loadMarket() {
    if (isLoading) return;
    
    isLoading = true;
    const market = document.getElementById('market-select').value;
    currentMarket = market;
    const tbody = document.getElementById('stock-list');
    
    // 如果是第一页，显示加载提示
    if (currentPage === 1) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">加载中...</td></tr>';
    } else {
        // 追加加载提示
        const loadingRow = document.createElement('tr');
        loadingRow.id = 'loading-indicator';
        loadingRow.innerHTML = '<td colspan="6" class="loading">加载更多...</td>';
        tbody.appendChild(loadingRow);
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/${market}/spot?page=${currentPage}&page_size=${pageSize}`);
        const result = await response.json();
        
        // 移除加载提示
        const loadingIndicator = document.getElementById('loading-indicator');
        if (loadingIndicator) {
            loadingIndicator.remove();
        }
        
        if (result.code === 0) {
            if (currentPage === 1) {
                tbody.innerHTML = '';
            }
            
            appendStockList(result.data);
            
            // 检查是否还有更多数据
            if (result.pagination) {
                hasMore = currentPage < result.pagination.total_pages;
                if (hasMore) {
                    currentPage++;
                }
            } else {
                hasMore = false;
            }
            
            // 如果没有更多数据，显示提示
            if (!hasMore && currentPage > 1) {
                const endRow = document.createElement('tr');
                endRow.innerHTML = '<td colspan="6" style="text-align: center; padding: 20px; color: #94a3b8;">已加载全部数据</td>';
                tbody.appendChild(endRow);
            }
        } else {
            if (currentPage === 1) {
                tbody.innerHTML = `<tr><td colspan="6" class="loading">加载失败: ${result.message}</td></tr>`;
            }
        }
    } catch (error) {
        if (currentPage === 1) {
            tbody.innerHTML = `<tr><td colspan="6" class="loading">加载失败: ${error.message}</td></tr>`;
        }
        hasMore = false;
    } finally {
        isLoading = false;
    }
}

function appendStockList(stocks) {
    const tbody = document.getElementById('stock-list');
    if (stocks.length === 0 && tbody.children.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">暂无数据</td></tr>';
        return;
    }
    
    const watchlist = getWatchlist();
    
    stocks.forEach(stock => {
        const tr = document.createElement('tr');
        const isInWatchlist = watchlist.some(s => s.code === stock.code);
        // 存储完整的股票数据到data属性中
        tr.setAttribute('data-stock', JSON.stringify(stock));
        tr.style.cursor = 'pointer';
        tr.innerHTML = `
            <td>${stock.code}</td>
            <td>${stock.name}</td>
            <td>${stock.price?.toFixed(2) || '-'}</td>
            <td class="${stock.pct >= 0 ? 'up' : 'down'}">
                ${stock.pct?.toFixed(2) || '-'}%
            </td>
            <td>${formatVolume(stock.volume)}</td>
            <td>
                <button class="add-watchlist-btn" data-code="${stock.code}" data-name="${stock.name}" style="padding: 4px 8px; background: ${isInWatchlist ? '#94a3b8' : '#10b981'}; color: white; border: none; border-radius: 4px; cursor: pointer; ${isInWatchlist ? 'opacity: 0.6;' : ''}" onclick="event.stopPropagation();">${isInWatchlist ? '已添加' : '加入自选'}</button>
            </td>
        `;
        
        // 添加单击事件
        tr.addEventListener('click', function(e) {
            // 如果点击的是按钮，不触发
            if (e.target.tagName === 'BUTTON' || e.target.closest('button')) {
                return;
            }
            e.preventDefault();
            const stockData = JSON.parse(this.getAttribute('data-stock'));
            openKlineModal(stockData.code, stockData.name, stockData);
        });
        
        tbody.appendChild(tr);
    });
    
    // 添加自选按钮点击事件
    document.querySelectorAll('.add-watchlist-btn').forEach(btn => {
        btn.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            const code = this.getAttribute('data-code');
            const name = this.getAttribute('data-name');
            if (!watchlist.some(s => s.code === code)) {
                addToWatchlist(code, name);
            }
        };
    });
}

function formatVolume(vol) {
    if (!vol) return '-';
    if (vol >= 100000000) return (vol / 100000000).toFixed(2) + '亿';
    if (vol >= 10000) return (vol / 10000).toFixed(2) + '万';
    return vol.toString();
}

async function handleSearch() {
    const keyword = document.getElementById('search-input').value;
    if (keyword.length < 2) {
        resetAndLoadMarket();
        return;
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/search?keyword=${encodeURIComponent(keyword)}`);
        const result = await response.json();
        
        const tbody = document.getElementById('stock-list');
        if (result.code === 0) {
            tbody.innerHTML = '';
            hasMore = false; // 搜索结果不启用无限加载
            appendStockList(result.data);
            
            // 更新按钮状态（检查是否已在自选）
            const watchlist = getWatchlist();
            document.querySelectorAll('.add-watchlist-btn').forEach(btn => {
                const code = btn.getAttribute('data-code');
                if (watchlist.some(s => s.code === code)) {
                    btn.textContent = '已添加';
                    btn.style.background = '#94a3b8';
                    btn.disabled = true;
                }
            });
        }
    } catch (error) {
        console.error('搜索失败:', error);
    }
}

// K线模态弹窗模块
let currentKlineCode = null;
let currentKlineName = null;

function initKlineModal() {
    const periodSelect = document.getElementById('chart-period');
    if (periodSelect) {
        periodSelect.addEventListener('change', () => {
            if (currentKlineCode) {
                loadChart(currentKlineCode);
            }
        });
    }
}

function openKlineModal(code, name, stockData = null) {
    currentKlineCode = code;
    currentKlineName = name;
    const modal = document.getElementById('kline-modal');
    const title = document.getElementById('kline-modal-title');
    const detailInfo = document.getElementById('stock-detail-info');
    
    if (!modal) {
        console.error('K线模态弹窗不存在');
        return;
    }
    
    if (title) {
        title.textContent = `${name} (${code}) - K线图`;
    }
    
    // 显示股票详情
    if (detailInfo && stockData) {
        renderStockDetail(stockData);
    } else if (detailInfo) {
        // 如果没有传入详情，尝试获取
        loadStockDetail(code).then(data => {
            if (data) {
                renderStockDetail(data);
            }
        });
    }
    
    modal.style.display = 'flex';
    loadChart(code);
}

// 加载股票详情
async function loadStockDetail(code) {
    try {
        const response = await apiFetch(`${API_BASE}/api/market/a/spot?page=1&page_size=5000`);
        const result = await response.json();
        if (result.code === 0 && result.data) {
            return result.data.find(s => String(s.code).trim() === String(code).trim());
        }
    } catch (error) {
        console.error('加载股票详情失败:', error);
    }
    return null;
}

// 渲染股票详情
function renderStockDetail(stock) {
    const detailInfo = document.getElementById('stock-detail-info');
    if (!detailInfo) return;
    
    const formatValue = (value, unit = '') => {
        if (value === null || value === undefined || isNaN(value)) return '-';
        if (typeof value === 'number') {
            if (unit === '亿') {
                return (value / 100000000).toFixed(2) + '亿';
            } else if (unit === '万') {
                return (value / 10000).toFixed(2) + '万';
            } else if (unit === '%') {
                return value.toFixed(2) + '%';
            } else if (unit === '元') {
                return value.toFixed(2) + '元';
            }
            return value.toFixed(2);
        }
        return value || '-';
    };
    
    detailInfo.innerHTML = `
        <div class="stock-detail-row">
            <span class="detail-item-inline">
                <span class="detail-label-inline">最新价</span>
                <span class="detail-value-inline" style="color: ${stock.pct >= 0 ? '#10b981' : '#ef4444'}; font-weight: 600;">
                    ${formatValue(stock.price, '元')}
                </span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">涨跌幅</span>
                <span class="detail-value-inline" style="color: ${stock.pct >= 0 ? '#10b981' : '#ef4444'};">
                    ${stock.pct >= 0 ? '+' : ''}${formatValue(stock.pct, '%')}
                </span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">涨跌额</span>
                <span class="detail-value-inline" style="color: ${stock.change >= 0 ? '#10b981' : '#ef4444'};">
                    ${stock.change >= 0 ? '+' : ''}${formatValue(stock.change, '元')}
                </span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">今开</span>
                <span class="detail-value-inline">${formatValue(stock.open, '元')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">昨收</span>
                <span class="detail-value-inline">${formatValue(stock.pre_close, '元')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">最高</span>
                <span class="detail-value-inline" style="color: #10b981;">${formatValue(stock.high, '元')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">最低</span>
                <span class="detail-value-inline" style="color: #ef4444;">${formatValue(stock.low, '元')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">成交量</span>
                <span class="detail-value-inline">${formatVolume(stock.volume)}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">成交额</span>
                <span class="detail-value-inline">${formatValue(stock.amount, '万')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">振幅</span>
                <span class="detail-value-inline">${formatValue(stock.amplitude, '%')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">量比</span>
                <span class="detail-value-inline">${formatValue(stock.volume_ratio)}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">换手率</span>
                <span class="detail-value-inline">${formatValue(stock.turnover, '%')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">市盈率</span>
                <span class="detail-value-inline">${formatValue(stock.pe)}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">总市值</span>
                <span class="detail-value-inline">${formatValue(stock.market_cap, '亿')}</span>
            </span>
            <span class="detail-item-inline">
                <span class="detail-label-inline">流通市值</span>
                <span class="detail-value-inline">${formatValue(stock.circulating_market_cap, '亿')}</span>
            </span>
        </div>
    `;
}

function closeKlineModal() {
    const modal = document.getElementById('kline-modal');
    if (modal) {
        modal.style.display = 'none';
    }
    
    // 清理图表
    if (chart) {
        chart.remove();
        chart = null;
        candleSeries = null;
        volumeSeries = null;
    }
    
    currentKlineCode = null;
    currentKlineName = null;
}

// 将closeKlineModal暴露到全局
window.closeKlineModal = closeKlineModal;

async function loadChart(code) {
    const periodSelect = document.getElementById('chart-period');
    const period = periodSelect ? periodSelect.value || 'daily' : 'daily';
    const container = document.getElementById('chart-container');
    
    if (!container) {
        console.error('K线容器不存在');
        return;
    }
    
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">加载中...</div>';
    
    try {
        // 从配置中获取K线数据年限，默认1年
        let klineYears = 1;
        try {
            const configRes = await apiFetch(`${API_BASE}/api/config`);
            if (configRes.ok) {
                const configData = await configRes.json();
                klineYears = configData.kline_years ?? 1;
            }
        } catch (e) {
            console.warn('获取K线年限配置失败，使用默认值1年:', e);
        }
        
        // 根据配置的年限加载数据
        const endDate = new Date();
        const startDate = new Date();
        startDate.setFullYear(startDate.getFullYear() - klineYears); // 根据配置加载数据
        
        const startDateStr = startDate.toISOString().split('T')[0].replace(/-/g, '');
        const endDateStr = endDate.toISOString().split('T')[0].replace(/-/g, '');
        
        // 添加超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
        
        const response = await apiFetch(`${API_BASE}/api/market/a/kline?code=${code}&period=${period}&start_date=${startDateStr}&end_date=${endDateStr}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        const result = await response.json();
        
        if (result.code === 0 && result.data && result.data.length > 0) {
            // 根据年限计算最大数据量（每年约250个交易日）
            const maxDataCount = Math.ceil(klineYears * 250);
            const displayData = result.data.slice(-maxDataCount);
            renderChart(displayData);
            
            // 加载技术指标
            loadIndicators(code);
        } else {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">无法获取K线数据（代码：${code}）<br/>可能原因：股票代码不存在或数据源暂时不可用</div>`;
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">加载超时，请稍后重试</div>';
        } else {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${error.message}</div>`;
        }
    }
}

function renderChart(data) {
    const container = document.getElementById('chart-container');
    container.innerHTML = '';
    
    // 检查 LightweightCharts 是否可用
    if (!window.LightweightCharts || !window.LightweightCharts.createChart) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">K线图库加载失败，请刷新页面</div>';
        console.error('LightweightCharts not loaded');
        return;
    }
    
    // 确保容器有宽度
    const containerWidth = container.clientWidth || 800;
    
    // 销毁旧图表
    if (chart) {
        chart.remove();
        chart = null;
    }
    
    chart = window.LightweightCharts.createChart(container, {
        width: containerWidth,
        height: 500,
        layout: {
            background: { type: 'solid', color: '#1e293b' },
            textColor: '#cbd5f5',
        },
        grid: {
            vertLines: { color: '#334155' },
            horzLines: { color: '#334155' },
        },
        rightPriceScale: {
            borderColor: '#334155',
        },
        timeScale: {
            borderColor: '#334155',
            timeVisible: true,
        },
        // 恢复默认的缩放/滚动行为：无需点击即可使用鼠标滚轮、拖拽等操作
        handleScroll: {
            mouseWheel: true,
            pressedMouseMove: true,
        },
        handleScale: {
            axisPressedMouseMove: true,
            mouseWheel: true,
            pinch: true,
        },
    });
    
    candleSeries = chart.addCandlestickSeries({
        upColor: '#ef4444',
        downColor: '#22c55e',
        borderVisible: false,
        wickUpColor: '#ef4444',
        wickDownColor: '#22c55e',
    });
    
    // 为成交量创建独立的右侧价格轴，只占底部20%的空间
    // 从localStorage加载成交量可见性设置
    const savedVolumeVisible = localStorage.getItem('volumeVisible');
    const initialVolumeVisible = savedVolumeVisible !== null ? savedVolumeVisible === 'true' : volumeVisible;
    
    volumeSeries = chart.addHistogramSeries({
        color: '#3b82f6',
        priceFormat: {
            type: 'volume',
        },
        priceScaleId: 'volume',  // 使用独立的成交量价格轴
        scaleMargins: {
            top: 0.80,  // K线图占80%空间，成交量占底部20%
            bottom: 0,
        },
        visible: initialVolumeVisible,  // 设置初始可见性
    });
    
    // 配置成交量价格轴（右侧）
    chart.priceScale('volume').applyOptions({
        scaleMargins: {
            top: 0.80,  // K线图占80%空间
            bottom: 0,
        },
    });
    
    // 转换数据格式并过滤无效数据
    const candleData = [];
    const volumeData = [];
    
    data.forEach(d => {
        // 确保日期格式正确 (YYYY-MM-DD)
        let dateStr = String(d.date || '');
        if (dateStr.length === 8 && !dateStr.includes('-')) {
            // 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
            dateStr = `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`;
        }
        
        const open = parseFloat(d.open);
        const high = parseFloat(d.high);
        const low = parseFloat(d.low);
        const close = parseFloat(d.close);
        const volume = parseFloat(d.volume || 0);
        
        // 只添加有效数据（放宽条件，只要日期和价格有效即可）
        if (dateStr && (dateStr.length >= 8 || dateStr.includes('-'))) {
            if (!isNaN(open) && !isNaN(close)) {
                // 如果high/low缺失，用open/close代替
                const validHigh = !isNaN(high) ? high : Math.max(open, close);
                const validLow = !isNaN(low) ? low : Math.min(open, close);
                
                candleData.push({
                    time: dateStr,
                    open: open,
                    high: validHigh,
                    low: validLow,
                    close: close,
                });
                
                volumeData.push({
                    time: dateStr,
                    value: volume || 0,
                    color: close >= open ? '#22c55e' : '#ef4444',
                });
            }
        }
    });
    
    // 按时间排序（确保时间顺序正确）
    candleData.sort((a, b) => a.time.localeCompare(b.time));
    volumeData.sort((a, b) => a.time.localeCompare(b.time));
    
    console.log('K线数据条数:', candleData.length);
    
    if (candleData.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">K线数据格式错误</div>';
        return;
    }
    
    try {
        // 分批设置数据，避免一次性渲染太多导致卡顿
        if (candleData.length > 500) {
            // 如果数据量大于500，先设置前500条，然后异步设置剩余数据
            candleSeries.setData(candleData.slice(0, 500));
            volumeSeries.setData(volumeData.slice(0, 500));
            
            setTimeout(() => {
                candleSeries.setData(candleData);
                volumeSeries.setData(volumeData);
                chart.timeScale().fitContent();
                // 更新EMA和成交量显示状态
                if (volumeSeries) {
                    const savedVolumeVisible = localStorage.getItem('volumeVisible');
                    const isVisible = savedVolumeVisible !== null ? savedVolumeVisible === 'true' : volumeVisible;
                    volumeSeries.applyOptions({ visible: isVisible });
                }
                setTimeout(() => {
                    updateEMA();
                }, 100);
            }, 200);
        } else {
            candleSeries.setData(candleData);
            volumeSeries.setData(volumeData);
        }
        
        chart.timeScale().fitContent();
        
        // 更新EMA和成交量显示状态
        if (volumeSeries) {
            const savedVolumeVisible = localStorage.getItem('volumeVisible');
            const isVisible = savedVolumeVisible !== null ? savedVolumeVisible === 'true' : volumeVisible;
            volumeSeries.applyOptions({ visible: isVisible });
        }
        setTimeout(() => {
            updateEMA();
        }, 100);
    } catch (err) {
        console.error('设置K线数据失败:', err);
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">K线渲染失败: ${err.message}</div>`;
    }
}

async function loadIndicators(code) {
    try {
        const response = await apiFetch(`${API_BASE}/api/market/a/indicators?code=${code}`);
        const result = await response.json();
        
        if (result.code === 0) {
            renderIndicators(result.data);
        }
    } catch (error) {
        console.error('加载指标失败:', error);
    }
}

// EMA配置状态
let emaConfig = {
    enabled: false,  // 默认关闭
    values: [20, 50, 100]  // 根据Pine Script默认值
};
let volumeVisible = false;  // 默认关闭
let emaSeries = [];

function renderIndicators(indicators) {
    const volumeContainer = document.getElementById('volume-controls');
    const emaContainer = document.getElementById('ema-controls');
    if (!volumeContainer || !emaContainer) return;
    
    // 从localStorage加载配置
    const savedEmaConfig = localStorage.getItem('emaConfig');
    if (savedEmaConfig) {
        emaConfig = JSON.parse(savedEmaConfig);
    }
    const savedVolumeVisible = localStorage.getItem('volumeVisible');
    if (savedVolumeVisible !== null) {
        volumeVisible = savedVolumeVisible === 'true';
    }
    
    // 成交量控制内容
    volumeContainer.innerHTML = `
        <label class="indicator-switch">
            <input type="checkbox" id="volume-toggle" ${volumeVisible ? 'checked' : ''}>
            <span>成交量显示</span>
        </label>
    `;
    
    // EMA 控制内容
    emaContainer.innerHTML = `
        <div class="indicator-switch">
            <input type="checkbox" id="ema-toggle" ${emaConfig.enabled ? 'checked' : ''}>
            <span>EMA</span>
        </div>
        <div class="indicator-control-body" id="ema-config-group" style="${emaConfig.enabled ? '' : 'display: none;'}">
            <div class="ema-inputs">
                <label>EMA配置：</label>
                <input type="number" id="ema1" value="${emaConfig.values[0]}" min="1" max="500" placeholder="周期1">
                <input type="number" id="ema2" value="${emaConfig.values[1]}" min="1" max="500" placeholder="周期2">
                <input type="number" id="ema3" value="${emaConfig.values[2]}" min="1" max="500" placeholder="周期3">
            </div>
        </div>
    `;
    
    // 绑定事件
    document.getElementById('volume-toggle').addEventListener('change', function(e) {
        volumeVisible = e.target.checked;
        localStorage.setItem('volumeVisible', volumeVisible);
        if (volumeSeries) {
            volumeSeries.applyOptions({ visible: volumeVisible });
        }
    });
    
    document.getElementById('ema-toggle').addEventListener('change', function(e) {
        emaConfig.enabled = e.target.checked;
        localStorage.setItem('emaConfig', JSON.stringify(emaConfig));
        const emaGroup = document.getElementById('ema-config-group');
        if (emaGroup) {
            emaGroup.style.display = emaConfig.enabled ? '' : 'none';
        }
        updateEMA();
    });
    
    // EMA 数值输入：输入即生效（无需“应用”按钮）
    const emaInputs = ['ema1', 'ema2', 'ema3'];
    const defaultPeriods = [20, 50, 100];
    emaInputs.forEach((id, index) => {
        const inputEl = document.getElementById(id);
        if (!inputEl) return;
        inputEl.addEventListener('input', (e) => {
            const raw = parseInt(e.target.value, 10);
            const period = Number.isFinite(raw) && raw > 0 ? raw : defaultPeriods[index];
            emaConfig.values[index] = period;
            // 确保输入框里也回显合法数值
            if (raw !== period) {
                e.target.value = period;
            }
            localStorage.setItem('emaConfig', JSON.stringify(emaConfig));
            if (emaConfig.enabled) {
                updateEMA();
            }
        });
    });
    
    // 初始化显示状态
    if (volumeSeries) {
        volumeSeries.applyOptions({ visible: volumeVisible });
    }
    updateEMA();
    
    // 绑定折叠行为（点击“成交量”或“EMA”头部时展开/收起）
    document.querySelectorAll('.indicator-collapse').forEach(el => {
        el.addEventListener('click', () => {
            const targetId = el.getAttribute('data-target');
            const content = document.getElementById(targetId);
            if (!content) return;
            const arrow = el.querySelector('.indicator-arrow');
            const isVisible = content.style.display === 'block';
            content.style.display = isVisible ? 'none' : 'block';
            if (arrow) {
                arrow.textContent = isVisible ? '▼' : '▲';
            }
        });
    });
}

function updateEMA() {
    if (!chart || !candleSeries) return;
    
    // 清除现有EMA线
    emaSeries.forEach(series => {
        chart.removeSeries(series);
    });
    emaSeries = [];
    
    if (!emaConfig.enabled) return;
    
    // 获取K线数据
    const klineData = candleSeries.data();
    if (!klineData || klineData.length === 0) return;
    
    // 计算EMA（根据Pine Script标准EMA计算）
    emaConfig.values.forEach((period, index) => {
        const emaValues = calculateEMA(klineData, period);
        if (emaValues.length > 0) {
            // 根据Pine Script代码的颜色：black, green, red
            const colors = ['#000000', '#10b981', '#ef4444'];
            const emaLine = chart.addLineSeries({
                color: colors[index % colors.length],
                lineWidth: 1,
                title: `EMA${period}`,
            });
            emaLine.setData(emaValues);
            emaSeries.push(emaLine);
        }
    });
}

function calculateEMA(data, period) {
    if (!data || data.length < period) return [];
    
    const result = [];
    let multiplier = 2 / (period + 1);
    let ema = data[0].close;
    
    data.forEach((item, index) => {
        if (index === 0) {
            ema = item.close;
        } else {
            ema = (item.close - ema) * multiplier + ema;
        }
        result.push({
            time: item.time,
            value: ema
        });
    });
    
    return result;
}

// 自选股模块
function initWatchlist() {
    loadWatchlist();
    
    // 定期更新自选股行情（每30秒）
    setInterval(() => {
        const watchlistTab = document.getElementById('watchlist-tab');
        if (watchlistTab && watchlistTab.classList.contains('active')) {
            updateWatchlistPrices();
        }
    }, 30000);
}

// 加载自选股列表
function loadWatchlist() {
    const watchlist = getWatchlist();
    const container = document.getElementById('watchlist-container');
    
    if (!container) return;
    
    if (watchlist.length === 0) {
        container.innerHTML = `
            <div class="watchlist-placeholder">
                <div style="font-size: 48px; margin-bottom: 16px;">⭐</div>
                <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">暂无自选股</div>
                <div style="font-size: 14px; color: #64748b;">在行情页点击"加入自选"按钮添加股票</div>
            </div>
        `;
        return;
    }
    
    container.innerHTML = `
        <table class="stock-table">
            <thead>
                <tr>
                    <th>代码</th>
                    <th>名称</th>
                    <th>最新价</th>
                    <th>涨跌幅</th>
                    <th>成交量</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody id="watchlist-stock-list">
                ${watchlist.map(stock => `
                    <tr>
                        <td>${stock.code}</td>
                        <td>${stock.name}</td>
                        <td class="watchlist-price" data-code="${stock.code}">加载中...</td>
                        <td class="watchlist-pct" data-code="${stock.code}">-</td>
                        <td class="watchlist-volume" data-code="${stock.code}">-</td>
                        <td>
                            <button class="remove-watchlist-btn" data-code="${stock.code}" style="padding: 4px 8px; background: #ef4444; color: white; border: none; border-radius: 4px; cursor: pointer;" onclick="event.stopPropagation();">移除</button>
                        </td>
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
    
    // 绑定单击事件和移除按钮事件
    document.querySelectorAll('#watchlist-stock-list tr').forEach(row => {
        row.style.cursor = 'pointer';
        row.addEventListener('click', async function(e) {
            // 如果点击的是按钮，不触发
            if (e.target.tagName === 'BUTTON' || e.target.closest('button')) {
                return;
            }
            e.preventDefault();
            const code = this.querySelector('td:first-child').textContent.trim();
            const name = this.querySelector('td:nth-child(2)').textContent.trim();
            
            // 尝试从行情数据中获取完整信息
            try {
                const response = await apiFetch(`${API_BASE}/api/market/a/spot?page=1&page_size=5000`);
                const result = await response.json();
                if (result.code === 0 && result.data) {
                    const stock = result.data.find(s => String(s.code).trim() === code);
                    if (stock) {
                        openKlineModal(code, name, stock);
                        return;
                    }
                }
            } catch (error) {
                console.error('获取股票详情失败:', error);
            }
            
            // 如果获取失败，使用基本信息
            openKlineModal(code, name, { code, name });
        });
    });
    
    document.querySelectorAll('.remove-watchlist-btn').forEach(btn => {
        btn.onclick = function(e) {
            e.preventDefault();
            const code = this.getAttribute('data-code');
            removeFromWatchlist(code);
        };
    });
    
    // 更新实时行情
    updateWatchlistPrices();
}

// 获取自选股列表
function getWatchlist() {
    try {
        const data = localStorage.getItem('watchlist');
        return data ? JSON.parse(data) : [];
    } catch (e) {
        return [];
    }
}

// 保存自选股列表
function saveWatchlist(watchlist) {
    localStorage.setItem('watchlist', JSON.stringify(watchlist));
}

// 添加到自选股
function addToWatchlist(code, name) {
    const watchlist = getWatchlist();
    if (watchlist.some(s => s.code === code)) {
        alert('该股票已在自选列表中');
        return;
    }
    watchlist.push({ code, name, addTime: Date.now() });
    saveWatchlist(watchlist);
    
    // 更新按钮状态
    document.querySelectorAll(`.add-watchlist-btn[data-code="${code}"]`).forEach(btn => {
        btn.textContent = '已添加';
        btn.style.background = '#94a3b8';
        btn.disabled = true;
    });
    
    // 如果当前在自选页，刷新列表
    if (document.getElementById('watchlist-tab') && document.getElementById('watchlist-tab').classList.contains('active')) {
        loadWatchlist();
    }
}

// 从自选股移除
function removeFromWatchlist(code) {
    const watchlist = getWatchlist();
    const newWatchlist = watchlist.filter(s => s.code !== code);
    saveWatchlist(newWatchlist);
    loadWatchlist();
    
    // 更新行情页按钮状态
    document.querySelectorAll(`.add-watchlist-btn[data-code="${code}"]`).forEach(btn => {
        btn.textContent = '加入自选';
        btn.style.background = '#10b981';
        btn.disabled = false;
    });
}

// 更新自选股实时行情
async function updateWatchlistPrices() {
    const watchlist = getWatchlist();
    if (watchlist.length === 0) return;
    
    try {
        // 获取所有自选股代码
        const codes = watchlist.map(s => String(s.code).trim());
        
        // 尝试获取所有数据（分页获取）
        let allStocks = [];
        let page = 1;
        const pageSize = 500;
        let hasMore = true;
        
        while (hasMore && page <= 10) { // 最多获取10页，避免无限循环
            try {
                const response = await apiFetch(`${API_BASE}/api/market/a/spot?page=${page}&page_size=${pageSize}`);
                const result = await response.json();
                
                if (result.code === 0 && result.data && result.data.length > 0) {
                    allStocks = allStocks.concat(result.data);
                    
                    // 检查是否还有更多数据
                    if (result.pagination) {
                        hasMore = page < result.pagination.total_pages;
                    } else {
                        hasMore = result.data.length === pageSize;
                    }
                    page++;
                } else {
                    hasMore = false;
                }
            } catch (e) {
                console.error(`获取第${page}页数据失败:`, e);
                hasMore = false;
            }
        }
        
        // 更新每个自选股的价格
        watchlist.forEach(watchStock => {
            const watchCode = String(watchStock.code).trim();
            const stock = allStocks.find(s => {
                const stockCode = String(s.code || '').trim();
                return stockCode === watchCode;
            });
            
            const priceEl = document.querySelector(`.watchlist-price[data-code="${watchStock.code}"]`);
            const pctEl = document.querySelector(`.watchlist-pct[data-code="${watchStock.code}"]`);
            const volumeEl = document.querySelector(`.watchlist-volume[data-code="${watchStock.code}"]`);
            
            if (stock) {
                // 找到数据，更新显示
                if (priceEl) {
                    const price = stock.price;
                    priceEl.textContent = (price !== null && price !== undefined && !isNaN(price)) ? price.toFixed(2) : '-';
                }
                if (pctEl) {
                    const pct = stock.pct;
                    if (pct !== null && pct !== undefined && !isNaN(pct)) {
                        pctEl.textContent = `${pct.toFixed(2)}%`;
                        pctEl.className = `watchlist-pct ${pct >= 0 ? 'up' : 'down'}`;
                    } else {
                        pctEl.textContent = '-';
                        pctEl.className = 'watchlist-pct';
                    }
                }
                if (volumeEl) {
                    volumeEl.textContent = formatVolume(stock.volume);
                }
            } else {
                // 未找到数据，显示提示
                if (priceEl) priceEl.textContent = '数据不可用';
                if (pctEl) {
                    pctEl.textContent = '-';
                    pctEl.className = 'watchlist-pct';
                }
                if (volumeEl) volumeEl.textContent = '-';
            }
        });
    } catch (error) {
        console.error('更新自选股行情失败:', error);
        // 显示错误提示
        const watchlist = getWatchlist();
        watchlist.forEach(watchStock => {
            const priceEl = document.querySelector(`.watchlist-price[data-code="${watchStock.code}"]`);
            if (priceEl) priceEl.textContent = '加载失败';
        });
    }
}

// 选股模块
function initStrategy() {
    document.getElementById('select-btn').addEventListener('click', runSelection);
}

async function runSelection() {
    const threshold = parseInt(document.getElementById('threshold-input').value);
    const maxCount = parseInt(document.getElementById('max-count-input').value);
    const container = document.getElementById('selected-stocks');
    
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">选股中...</div>';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/strategy/select?threshold=${threshold}&max_count=${maxCount}`);
        const result = await response.json();
        
        if (result.code === 0) {
            renderSelectedStocks(result.data);
        } else {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">选股失败: ${result.message}</div>`;
        }
    } catch (error) {
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">选股失败: ${error.message}</div>`;
    }
}

function renderSelectedStocks(stocks) {
    const container = document.getElementById('selected-stocks');
    
    if (stocks.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">未选出符合条件的股票</div>';
        return;
    }
    
    container.innerHTML = stocks.map(stock => `
        <div class="stock-card">
            <div class="info">
                <div style="font-size: 18px; font-weight: 600; color: #60a5fa;">
                    ${stock.name} (${stock.code})
                </div>
                <div style="margin-top: 5px; color: #94a3b8;">
                    价格: ${stock.price?.toFixed(2) || '-'} | 
                    涨跌幅: ${stock.pct?.toFixed(2) || '-'}%
                </div>
            </div>
            <div class="score">${stock.score}</div>
        </div>
    `).join('');
}

// AI分析模块
function initAI() {
    const analyzeBtn = document.getElementById('analyze-btn');
    const codeInput = document.getElementById('ai-code-input');
    
    analyzeBtn.addEventListener('click', () => {
        const code = codeInput.value.trim();
        if (code) {
            analyzeStock(code);
        } else {
            alert('请输入股票代码');
        }
    });
    
    // 支持回车键触发分析
    codeInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            analyzeBtn.click();
        }
    });
}

async function analyzeStock(code) {
    const container = document.getElementById('ai-analysis-result');
    const useAI = document.getElementById('use-ai-checkbox').checked;
    
    container.innerHTML = '<div class="ai-loading"><div class="ai-loading-spinner"></div><div style="margin-top: 16px; color: #94a3b8;">AI分析中，请稍候...</div></div>';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/ai/analyze/${code}?use_ai=${useAI}`);
        const result = await response.json();
        
        if (result.code === 0 && result.data) {
            renderAIAnalysis(result.data, code);
        } else {
            container.innerHTML = `
                <div class="ai-error">
                    <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                    <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">分析失败</div>
                    <div style="font-size: 14px; color: #94a3b8;">${result.message || '无法获取分析数据'}</div>
                </div>
            `;
        }
    } catch (error) {
        container.innerHTML = `
            <div class="ai-error">
                <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">分析失败</div>
                <div style="font-size: 14px; color: #94a3b8;">${error.message}</div>
            </div>
        `;
    }
}

function renderAIAnalysis(data, code) {
    const container = document.getElementById('ai-analysis-result');
    
    const trendColor = {
        '上涨': '#10b981',
        '下跌': '#ef4444',
        '震荡': '#f59e0b',
        '未知': '#94a3b8'
    }[data.trend] || '#94a3b8';
    
    const riskColor = {
        '低': '#10b981',
        '中': '#f59e0b',
        '高': '#ef4444',
        '未知': '#94a3b8'
    }[data.risk] || '#94a3b8';
    
    const confidenceLevel = data.confidence || 0;
    const confidenceColor = confidenceLevel >= 70 ? '#10b981' : confidenceLevel >= 50 ? '#f59e0b' : '#ef4444';
    
    container.innerHTML = `
        <div class="ai-analysis-content">
            <!-- 概览卡片 -->
            <div class="ai-overview">
                <div class="ai-overview-item">
                    <div class="ai-overview-label">趋势判断</div>
                    <div class="ai-overview-value" style="color: ${trendColor};">${data.trend || '未知'}</div>
                </div>
                <div class="ai-overview-item">
                    <div class="ai-overview-label">风险评级</div>
                    <div class="ai-overview-value" style="color: ${riskColor};">${data.risk || '未知'}</div>
                </div>
                <div class="ai-overview-item">
                    <div class="ai-overview-label">置信度</div>
                    <div class="ai-overview-value" style="color: ${confidenceColor};">${confidenceLevel}%</div>
                </div>
                <div class="ai-overview-item">
                    <div class="ai-overview-label">综合评分</div>
                    <div class="ai-overview-value" style="color: ${data.score >= 0 ? '#10b981' : '#ef4444'};">${data.score || 0}</div>
                </div>
            </div>
            
            <!-- 操作建议 -->
            <div class="ai-section">
                <h3 class="ai-section-title">💡 操作建议</h3>
                <div class="ai-advice ${data.advice?.includes('买入') ? 'buy' : data.advice?.includes('卖出') ? 'sell' : 'hold'}">
                    ${data.advice || '暂无建议'}
                </div>
            </div>
            
            <!-- 关键因素 -->
            ${data.key_factors && data.key_factors.length > 0 ? `
            <div class="ai-section">
                <h3 class="ai-section-title">🔑 关键因素</h3>
                <div class="ai-factors">
                    ${data.key_factors.map(factor => `
                        <div class="ai-factor-item">
                            <span class="ai-factor-icon">${factor.includes('多头') || factor.includes('上涨') ? '📈' : factor.includes('空头') || factor.includes('下跌') ? '📉' : '📊'}</span>
                            <span class="ai-factor-text">${factor}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
            ` : ''}
            
            <!-- 分析总结 -->
            ${data.summary ? `
            <div class="ai-section">
                <h3 class="ai-section-title">📝 分析总结</h3>
                <div class="ai-summary">
                    ${data.summary}
                </div>
            </div>
            ` : ''}
            
            <!-- 技术指标详情 -->
            ${data.indicators ? `
            <div class="ai-section">
                <h3 class="ai-section-title">📊 技术指标</h3>
                <div class="ai-indicators">
                    ${Object.entries(data.indicators).map(([key, value]) => `
                        <div class="ai-indicator-item">
                            <span class="ai-indicator-label">${key}:</span>
                            <span class="ai-indicator-value">${typeof value === 'number' ? value.toFixed(2) : value || 'N/A'}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
            ` : ''}
        </div>
    `;
}

// 交易模块已删除，替换为AI分析模块

// 资讯模块
function initNews() {
    document.getElementById('refresh-news-btn').addEventListener('click', loadNews);
    loadNews();
}

async function loadNews() {
    const container = document.getElementById('news-list');
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">加载中...</div>';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/news/latest`);
        const result = await response.json();
        
        if (result.code === 0) {
            renderNews(result.data);
        } else {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${result.message}</div>`;
        }
    } catch (error) {
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${error.message}</div>`;
    }
}

function renderNews(newsList) {
    const container = document.getElementById('news-list');
    
    if (newsList.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">暂无资讯</div>';
        return;
    }
    
    container.innerHTML = newsList.slice(0, 50).map(news => `
        <div class="news-item">
            <h4>${news.title || '-'}</h4>
            <div>${(news.content || '').substring(0, 200)}...</div>
            <div class="meta">
                ${news.publish_time || news.collect_time || '-'} | ${news.source || '未知来源'}
            </div>
        </div>
    `).join('');
}

// 全局函数
window.loadChart = loadChart;

// 配置模块
function initConfig() {
    const saveBtn = document.getElementById('cfg-save-btn');
    if (!saveBtn) return;

    saveBtn.addEventListener('click', saveConfig);
    loadConfig();
}

async function loadConfig() {
    const statusEl = document.getElementById('cfg-status');
    try {
        const res = await apiFetch(`${API_BASE}/api/config`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        document.getElementById('cfg-threshold').value = data.selection_threshold ?? 65;
        document.getElementById('cfg-max-count').value = data.selection_max_count ?? 30;
        document.getElementById('cfg-collector-interval').value = data.collector_interval_seconds ?? 60;
        document.getElementById('cfg-kline-years').value = data.kline_years ?? 1;
        
        // AI 配置（API Key 不回显，只在服务端保存）
        document.getElementById('cfg-ai-api-key').value = '';
        document.getElementById('cfg-ai-api-base').value = data.openai_api_base || 'https://openai.qiniu.com/v1';
        document.getElementById('cfg-ai-model').value = data.openai_model || 'deepseek/deepseek-v3.2-251201';

        // 通知渠道配置
        const channels = data.notify_channels || [];
        const telegramEnabled = data.notify_telegram_enabled !== false && channels.includes('telegram');
        const emailEnabled = data.notify_email_enabled !== false && channels.includes('email');
        const wechatEnabled = data.notify_wechat_enabled !== false && channels.includes('wechat');
        
        document.getElementById('cfg-notify-telegram').checked = telegramEnabled;
        document.getElementById('cfg-telegram-bot-token').value = data.notify_telegram_bot_token || '';
        document.getElementById('cfg-telegram-chat-id').value = data.notify_telegram_chat_id || '';
        
        document.getElementById('cfg-notify-email').checked = emailEnabled;
        document.getElementById('cfg-email-smtp-host').value = data.notify_email_smtp_host || '';
        document.getElementById('cfg-email-smtp-port').value = data.notify_email_smtp_port || '';
        document.getElementById('cfg-email-user').value = data.notify_email_user || '';
        // 密码不加载，保持为空（已隐藏）
        document.getElementById('cfg-email-password').value = '';
        document.getElementById('cfg-email-to').value = data.notify_email_to || '';
        
        document.getElementById('cfg-notify-wechat').checked = wechatEnabled;
        document.getElementById('cfg-wechat-webhook-url').value = data.notify_wechat_webhook_url || '';

        // 同步选股面板默认值
        const thresholdInput = document.getElementById('threshold-input');
        const maxCountInput = document.getElementById('max-count-input');
        if (thresholdInput) thresholdInput.value = data.selection_threshold ?? 65;
        if (maxCountInput) maxCountInput.value = data.selection_max_count ?? 30;

        if (statusEl) statusEl.textContent = '配置已从服务器加载。';
    } catch (error) {
        console.error('加载配置失败:', error);
        if (statusEl) statusEl.textContent = `加载配置失败: ${error.message}`;
    }
}

async function saveConfig() {
    const statusEl = document.getElementById('cfg-status');
    const threshold = parseInt(document.getElementById('cfg-threshold').value);
    const maxCount = parseInt(document.getElementById('cfg-max-count').value);
    const interval = parseInt(document.getElementById('cfg-collector-interval').value);
    const klineYears = parseFloat(document.getElementById('cfg-kline-years').value);

    const channels = [];
    const telegramEnabled = document.getElementById('cfg-notify-telegram').checked;
    const emailEnabled = document.getElementById('cfg-notify-email').checked;
    const wechatEnabled = document.getElementById('cfg-notify-wechat').checked;
    
    if (telegramEnabled) channels.push('telegram');
    if (emailEnabled) channels.push('email');
    if (wechatEnabled) channels.push('wechat');

    try {
        if (statusEl) statusEl.textContent = '保存中...';
        const res = await apiFetch(`${API_BASE}/api/config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                selection_threshold: threshold,
                selection_max_count: maxCount,
                collector_interval_seconds: interval,
                kline_years: klineYears,
                openai_api_key: document.getElementById('cfg-ai-api-key').value.trim() || null,
                openai_api_base: document.getElementById('cfg-ai-api-base').value.trim() || null,
                openai_model: document.getElementById('cfg-ai-model').value.trim() || null,
                notify_channels: channels,
                notify_telegram_enabled: telegramEnabled,
                notify_telegram_bot_token: document.getElementById('cfg-telegram-bot-token').value.trim() || null,
                notify_telegram_chat_id: document.getElementById('cfg-telegram-chat-id').value.trim() || null,
                notify_email_enabled: emailEnabled,
                notify_email_smtp_host: document.getElementById('cfg-email-smtp-host').value.trim() || null,
                notify_email_smtp_port: document.getElementById('cfg-email-smtp-port').value ? parseInt(document.getElementById('cfg-email-smtp-port').value) : null,
                notify_email_user: document.getElementById('cfg-email-user').value.trim() || null,
                notify_email_password: document.getElementById('cfg-email-password').value.trim() || null, // 如果为空则不更新密码
                notify_email_to: document.getElementById('cfg-email-to').value.trim() || null,
                notify_wechat_enabled: wechatEnabled,
                notify_wechat_webhook_url: document.getElementById('cfg-wechat-webhook-url').value.trim() || null,
            }),
        });

        if (!res.ok) {
            const errText = await res.text();
            throw new Error(errText || `HTTP ${res.status}`);
        }

        const data = await res.json();

        // 同步选股面板默认值
        const thresholdInput = document.getElementById('threshold-input');
        const maxCountInput = document.getElementById('max-count-input');
        if (thresholdInput) thresholdInput.value = data.selection_threshold ?? threshold;
        if (maxCountInput) maxCountInput.value = data.selection_max_count ?? maxCount;

        if (statusEl) statusEl.textContent = '配置已保存。若修改了采集间隔，新设置会在下一轮采集后生效。';
        alert('配置已保存');
    } catch (error) {
        console.error('保存配置失败:', error);
        if (statusEl) statusEl.textContent = `保存配置失败: ${error.message}`;
        alert(`保存配置失败: ${error.message}`);
    }
}

// 配置折叠功能
function toggleConfigSection(sectionId) {
    const content = document.getElementById(`content-${sectionId}`);
    const arrow = document.getElementById(`arrow-${sectionId}`);
    
    if (content && arrow) {
        if (content.classList.contains('hidden')) {
            content.classList.remove('hidden');
            arrow.textContent = '▼';
        } else {
            content.classList.add('hidden');
            arrow.textContent = '▶';
        }
    }
}

// 通知渠道子项折叠功能
function toggleConfigSubsection(subsectionId) {
    const content = document.getElementById(`content-${subsectionId}`);
    const arrow = document.getElementById(`arrow-${subsectionId}`);
    
    if (content && arrow) {
        if (content.classList.contains('hidden')) {
            content.classList.remove('hidden');
            arrow.textContent = '▼';
        } else {
            content.classList.add('hidden');
            arrow.textContent = '▶';
        }
    }
}

// 全局函数
window.toggleConfigSection = toggleConfigSection;
window.toggleConfigSubsection = toggleConfigSubsection;

// 登录模块
async function initAuth() {
    const overlay = document.getElementById('login-overlay');
    const form = document.getElementById('login-form');
    const messageEl = document.getElementById('login-message');

    if (!overlay || !form) {
        // 如果没有登录层，直接初始化应用（兼容老版本）
        startApp();
        return;
    }

    // 检查本地存储的登录状态（永久有效）
    const isLoggedIn = localStorage.getItem('isLoggedIn');
    let savedApiToken = localStorage.getItem('apiToken');
    let savedAdminToken = localStorage.getItem('adminToken');
    
    // 过滤掉无效的token值
    if (savedApiToken === 'null' || savedApiToken === '') savedApiToken = null;
    if (savedAdminToken === 'null' || savedAdminToken === '') savedAdminToken = null;
    
    // 如果有token（即使没有isLoggedIn标记），也尝试自动登录
    if (isLoggedIn === 'true' || savedApiToken) {
        apiToken = savedApiToken;
        adminToken = savedAdminToken;
        
        // 验证token是否有效（通过尝试访问一个需要认证的接口）
        try {
            const testRes = await apiFetch(`${API_BASE}/api/config`);
            if (testRes.ok) {
                // Token有效，直接登录
                if (isLoggedIn !== 'true') {
                    localStorage.setItem('isLoggedIn', 'true');
                }
                overlay.style.display = 'none';
                startApp();
                return;
            } else if (testRes.status === 401) {
                // Token无效，清除并显示登录界面
                console.warn('Token已失效，需要重新登录');
                localStorage.removeItem('isLoggedIn');
                localStorage.removeItem('apiToken');
                localStorage.removeItem('adminToken');
                apiToken = null;
                adminToken = null;
            }
        } catch (error) {
            // 网络错误或其他错误，可能是API未启动，先尝试使用token
            console.warn('验证token时出错，尝试使用保存的token:', error);
            if (isLoggedIn === 'true') {
                // 如果之前标记为已登录，先尝试使用
                overlay.style.display = 'none';
                startApp();
                return;
            }
        }
    }

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value;
        const password = document.getElementById('login-password').value;

        messageEl.textContent = '登录中...';

        try {
            const res = await fetch(`${API_BASE}/api/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password }),
            });

            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.detail || `HTTP ${res.status}`);
            }

            const data = await res.json();
            if (!data.success) {
                throw new Error(data.message || '登录失败');
            }

            apiToken = data.token || null;
            adminToken = data.admin_token || null;

            // 保存到本地存储（永久有效）
            localStorage.setItem('isLoggedIn', 'true');
            localStorage.setItem('apiToken', apiToken || '');
            localStorage.setItem('adminToken', adminToken || '');

            overlay.style.display = 'none';
            startApp();
        } catch (error) {
            console.error('登录失败:', error);
            messageEl.textContent = `登录失败：${error.message}`;
        }
    });
}

