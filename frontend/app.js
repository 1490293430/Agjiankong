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
    initTheme();
    const currentTab = initTabs(); // 获取当前激活的tab
    
    // 初始化所有模块
    initMarket(); // 始终初始化行情模块（即使不在行情页，也需要初始化事件监听）
    initWatchlist(); // 初始化自选股模块
    
    // 根据当前tab加载数据
    if (currentTab === 'market') {
        // 如果当前是行情页，检查是否有数据，没有才加载
        const tbody = document.getElementById('stock-list');
        if (!tbody || tbody.children.length === 0) {
            loadMarket();
        } else {
            // 检查是否有有效数据（不是loading或错误提示）
            const hasData = Array.from(tbody.children).some(tr => {
                const text = tr.textContent || '';
                const cells = tr.querySelectorAll('td');
                return cells.length > 1 && text.trim() && !text.includes('加载中') && !text.includes('加载失败') && !text.includes('暂无数据');
            });
            if (!hasData) {
                loadMarket();
            }
        }
    } else if (currentTab === 'watchlist') {
        // 如果当前是自选页，加载自选股列表（使用缓存）
        loadWatchlist(false); // 不强制刷新，使用缓存
    }
    
    initKlineModal();
    initStrategy();
    initAI();
    initNews();
    initConfig();
    initMarketStatus();
}

// 主题切换
function initTheme() {
    const body = document.body;
    const btn = document.getElementById('theme-toggle');
    const saved = localStorage.getItem('theme');
    if (saved === 'light') {
        body.classList.add('light-mode');
    }
    updateThemeButtonText(btn, body);
    if (btn) {
        btn.addEventListener('click', () => {
            body.classList.toggle('light-mode');
            const mode = body.classList.contains('light-mode') ? 'light' : 'dark';
            localStorage.setItem('theme', mode);
            updateThemeButtonText(btn, body);
            // 主题切换时更新图表主题
            updateChartTheme();
        });
    }
}

function updateThemeButtonText(btn, body) {
    if (!btn || !body) return;
    const isLight = body.classList.contains('light-mode');
    btn.textContent = isLight ? '🌞 白天' : '🌙 夜间';
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
    
    // 调试日志（仅在开发环境）
    if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1') {
        console.debug('API请求:', url, { hasApiToken: !!apiToken, hasAdminToken: !!adminToken });
    }
    
    return fetch(url, { ...options, headers });
}

// 标签切换
function initTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    const contents = document.querySelectorAll('.tab-content');
    
    // 立即从localStorage恢复上次的tab（避免闪烁）
    const savedTab = localStorage.getItem('currentTab');
    if (savedTab) {
        const savedTabElement = document.querySelector(`.tab-btn[data-tab="${savedTab}"]`);
        const savedContentElement = document.getElementById(`${savedTab}-tab`);
        
        // 如果保存的tab存在，立即切换到它
        if (savedTabElement && savedContentElement) {
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));
            
            savedTabElement.classList.add('active');
            savedContentElement.classList.add('active');
        }
    }
    
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetTab = tab.dataset.tab;
            
            tabs.forEach(t => t.classList.remove('active'));
            contents.forEach(c => c.classList.remove('active'));
            
            tab.classList.add('active');
            document.getElementById(`${targetTab}-tab`).classList.add('active');
            
            // 保存当前tab到localStorage
            localStorage.setItem('currentTab', targetTab);
            
            // 切换到自选页时，检查是否已有数据显示
            if (targetTab === 'watchlist') {
                const tbody = document.getElementById('watchlist-stock-list');
                // 如果表格已存在且有数据，不重新加载
                if (tbody && tbody.children.length > 0) {
                    console.log('自选页已有数据，跳过加载');
                    return;
                }
                // 否则使用缓存加载
                loadWatchlist(false); // 不强制刷新，使用缓存
            }
            
            // 切换到行情页时，检查是否已有数据显示
            if (targetTab === 'market') {
                const tbody = document.getElementById('stock-list');
                // 如果表格已存在且有数据（不是loading提示），不重新加载
                if (tbody && tbody.children.length > 0) {
                    const hasLoading = tbody.querySelector('.loading');
                    const hasData = Array.from(tbody.children).some(tr => {
                        const text = tr.textContent || '';
                        return text.trim() && !text.includes('加载中') && !text.includes('加载失败');
                    });
                    if (hasData && !hasLoading) {
                        console.log('行情页已有数据，跳过加载');
                        return;
                    }
                }
                // 如果表格为空或只有loading/错误提示，加载数据
                // 延迟加载，确保tab切换动画完成
                setTimeout(() => {
                    // 再次检查是否仍在行情页
                    const marketTab = document.getElementById('market-tab');
                    if (marketTab && marketTab.classList.contains('active')) {
                        loadMarket();
                    }
                }, 100);
            }
        });
    });
    
    // 返回当前激活的tab，供其他模块使用
    return savedTab || 'market';
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
    
    if (!marketSelect || !searchInput || !refreshBtn) {
        console.warn('行情页元素不存在，跳过初始化');
        return;
    }
    
    refreshBtn.addEventListener('click', () => resetAndLoadMarket());
    marketSelect.addEventListener('change', () => resetAndLoadMarket());
    searchInput.addEventListener('input', handleSearch);
    
    // 监听滚动事件实现无限加载
    if (container) {
        container.addEventListener('scroll', () => {
            // 检查行情页是否激活
            const marketTab = document.getElementById('market-tab');
            if (!marketTab || !marketTab.classList.contains('active')) {
                return;
            }
            
            const scrollTop = container.scrollTop;
            const scrollHeight = container.scrollHeight;
            const clientHeight = container.clientHeight;
            
            // 距离底部100px时加载下一页
            if (scrollTop + clientHeight >= scrollHeight - 100 && !isLoading && hasMore) {
                loadMarket();
            }
        });
    }
    
    // 注意：不在这里加载数据，由startApp()根据当前tab决定是否加载
    // 但需要设置自动刷新定时器（如果当前是行情页）
    const marketTab = document.getElementById('market-tab');
    if (marketTab && marketTab.classList.contains('active')) {
        // 无感自动刷新：每30秒静默刷新当前页数据（不重置分页）
        marketRefreshInterval = setInterval(() => {
            if (!isLoading && currentPage === 1) {
                silentRefreshMarket();
            }
        }, 30000); // 30秒刷新一次
    }
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
    // 检查行情页是否激活，如果不在行情页，不加载数据
    const marketTab = document.getElementById('market-tab');
    if (!marketTab || !marketTab.classList.contains('active')) {
        console.log('行情页未激活，跳过加载');
        return;
    }
    
    if (isLoading) {
        console.log('行情数据正在加载中，跳过重复请求');
        return;
    }
    
    const tbody = document.getElementById('stock-list');
    if (!tbody) {
        console.warn('行情页表格不存在，跳过加载');
        return;
    }
    
    // 检查是否已有有效数据（不是loading或错误提示）
    if (tbody.children.length > 0 && currentPage === 1) {
        const hasLoading = tbody.querySelector('.loading');
        const hasError = Array.from(tbody.children).some(tr => {
            const text = tr.textContent || '';
            return text.includes('加载失败') || text.includes('请求超时') || text.includes('网络错误');
        });
        const hasData = Array.from(tbody.children).some(tr => {
            const text = tr.textContent || '';
            const cells = tr.querySelectorAll('td');
            // 如果有多个td且不是loading/错误提示，认为有数据
            return cells.length > 1 && text.trim() && !text.includes('加载中') && !text.includes('加载失败') && !text.includes('暂无数据');
        });
        
        if (hasData && !hasLoading && !hasError) {
            console.log('行情页已有数据，跳过加载');
            return;
        }
    }
    
    const marketSelect = document.getElementById('market-select');
    if (!marketSelect) {
        console.warn('行情页选择器不存在，跳过加载');
        return;
    }
    
    isLoading = true;
    const market = marketSelect.value || 'a';
    currentMarket = market;
    
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
        // 添加超时控制，避免请求卡住
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            controller.abort();
        }, 10000); // 10秒超时
        
        const response = await apiFetch(`${API_BASE}/api/market/${market}/spot?page=${currentPage}&page_size=${pageSize}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        // 再次检查行情页是否仍然激活
        if (!marketTab || !marketTab.classList.contains('active')) {
            console.log('行情页已切换，取消加载');
            isLoading = false;
            return;
        }
        
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
            
            if (result.data && result.data.length > 0) {
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
                // 数据为空
                if (currentPage === 1) {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px; color: #94a3b8;">暂无数据</td></tr>';
                }
                hasMore = false;
            }
        } else {
            // API返回错误
            if (currentPage === 1) {
                tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #ef4444;">加载失败: ${result.message || '未知错误'}</td></tr>`;
            }
            hasMore = false;
        }
    } catch (error) {
        // 再次检查行情页是否仍然激活
        if (!marketTab || !marketTab.classList.contains('active')) {
            console.log('行情页已切换，取消错误处理');
            isLoading = false;
            return;
        }
        
        // 移除加载提示
        const loadingIndicator = document.getElementById('loading-indicator');
        if (loadingIndicator) {
            loadingIndicator.remove();
        }
        
        if (currentPage === 1) {
            const errorMsg = error.name === 'AbortError' ? '请求超时，请稍后重试' : `加载失败: ${error.message || '网络错误'}`;
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #ef4444;">${errorMsg}</td></tr>`;
        }
        hasMore = false;
        console.error('加载行情数据失败:', error);
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
let currentKlineStockData = null;

function initKlineModal() {
    const periodSelect = document.getElementById('chart-period');
    if (periodSelect) {
        // 加载保存的周期选择
        const savedPeriod = localStorage.getItem('klineChartPeriod') || 'daily';
        if (savedPeriod && ['1h', 'daily', 'weekly', 'monthly'].includes(savedPeriod)) {
            periodSelect.value = savedPeriod;
        }
        
        periodSelect.addEventListener('change', () => {
            // 保存周期选择
            localStorage.setItem('klineChartPeriod', periodSelect.value);
            if (currentKlineCode) {
                loadChart(currentKlineCode);
            }
        });
    }
    
    // 绑定刷新按钮
    const refreshBtn = document.getElementById('kline-refresh-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            if (currentKlineCode) {
                console.log('刷新K线数据:', currentKlineCode);
                loadChart(currentKlineCode);
            }
        });
    }
    
    // 检查是否有保存的K线状态，页面刷新后自动恢复
    try {
        const savedKlineState = localStorage.getItem('klineModalState');
        if (savedKlineState) {
            const state = JSON.parse(savedKlineState);
            if (state.code && state.name) {
                // 延迟打开，确保DOM已完全加载
                setTimeout(() => {
                    // 尝试从当前页面数据中恢复stockData
                    // 如果找不到，至少用code和name打开
                    openKlineModal(state.code, state.name, state.stockData || null);
                    console.log('已恢复K线模态弹窗状态:', state.code, state.name);
                }, 100);
            }
        }
    } catch (e) {
        console.warn('恢复K线模态弹窗状态失败:', e);
    }
}

function openKlineModal(code, name, stockData = null) {
    currentKlineCode = code;
    currentKlineName = name;
    currentKlineStockData = stockData; // 保存stockData供loadChart使用
    
    // 恢复保存的周期选择
    const periodSelect = document.getElementById('chart-period');
    if (periodSelect) {
        const savedPeriod = localStorage.getItem('klineChartPeriod') || 'daily';
        if (savedPeriod && ['1h', 'daily', 'weekly', 'monthly'].includes(savedPeriod)) {
            periodSelect.value = savedPeriod;
        }
    }
    
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
    
    // 等待模态框完全显示后再加载图表
    // 使用简单的延迟，确保DOM已渲染
    setTimeout(() => {
        loadChart(code);
    }, 200);
}

// 加载股票详情
async function loadStockDetail(code) {
    try {
        // 先尝试从缓存获取
        let allStocks = getCachedMarketData();
        
        // 如果缓存不存在或为空，从服务器获取
        if (!allStocks || allStocks.length === 0) {
            allStocks = await fetchMarketDataFromServer();
            if (allStocks && allStocks.length > 0) {
                saveCachedMarketData(allStocks);
            }
        }
        
        if (allStocks && allStocks.length > 0) {
            return allStocks.find(s => String(s.code).trim() === String(code).trim());
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
        const container = document.getElementById('chart-container');
        // 清理事件监听器
        if (container && window.chartEventHandlers && window.chartEventHandlers[container.id]) {
            const handlers = window.chartEventHandlers[container.id];
            if (handlers.wheel) container.removeEventListener('wheel', handlers.wheel);
            if (handlers.resize) window.removeEventListener('resize', handlers.resize);
            delete window.chartEventHandlers[container.id];
        }
        chart.remove();
        chart = null;
        candleSeries = null;
        volumeSeries = null;
    }
    
    // 清除保存的K线状态
    try {
        localStorage.removeItem('klineModalState');
    } catch (e) {
        console.warn('清除K线模态弹窗状态失败:', e);
    }
    
    currentKlineCode = null;
    currentKlineName = null;
    currentKlineStockData = null;
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
        // 判断市场类型
        // 1. 优先使用stockData中的market字段
        // 2. 如果没有，根据代码格式判断（港股代码通常是5位数字，A股代码通常是6位数字）
        let market = 'a'; // 默认A股
        if (currentKlineStockData && currentKlineStockData.market) {
            market = currentKlineStockData.market.toLowerCase() === 'hk' ? 'hk' : 'a';
        } else {
            const codeStr = String(code).trim();
            // 港股代码通常是5位数字（如00700）或4位数字（如700）
            // A股代码通常是6位数字
            const isHK = codeStr.length === 5 && codeStr.startsWith('0');
            market = isHK ? 'hk' : 'a';
        }
        
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
        const timeoutId = setTimeout(() => controller.abort(), 15000); // 15秒超时（增加超时时间）
        
        let response, result;
        try {
            // 根据市场类型选择对应的API接口
            response = await apiFetch(`${API_BASE}/api/market/${market}/kline?code=${code}&period=${period}&start_date=${startDateStr}&end_date=${endDateStr}`, {
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            result = await response.json();
        } catch (fetchError) {
            clearTimeout(timeoutId);
            
            // 如果是超时错误，提供重试提示
            if (fetchError.name === 'AbortError') {
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                    <div>请求超时，请稍后重试</div>
                    <button id="retry-kline-btn" style="margin-top: 16px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                        重试
                    </button>
                </div>`;
                
                // 绑定重试按钮
                const retryBtn = document.getElementById('retry-kline-btn');
                if (retryBtn) {
                    retryBtn.addEventListener('click', () => {
                        loadChart(code);
                    });
                }
                return;
            }
            
            // 其他网络错误
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                <div>网络错误: ${fetchError.message || '连接失败'}</div>
                <button id="retry-kline-btn" style="margin-top: 16px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                    重试
                </button>
            </div>`;
            
            // 绑定重试按钮
            const retryBtn = document.getElementById('retry-kline-btn');
            if (retryBtn) {
                retryBtn.addEventListener('click', () => {
                    loadChart(code);
                });
            }
            console.error('K线数据请求失败:', fetchError);
            return;
        }
        
        console.log('K线API响应:', { code, market, period, resultCode: result.code, dataLength: result.data?.length });
        
        if (result.code === 0 && result.data && result.data.length > 0) {
            // 根据年限计算最大数据量（每年约250个交易日）
            const maxDataCount = Math.ceil(klineYears * 250);
            const allData = result.data.slice(-maxDataCount);
            
            console.log('准备渲染K线数据，总条数:', allData.length);
            
            // 检查数据有效性
            if (allData.length === 0) {
                console.warn('K线数据为空，原始数据长度:', result.data.length, '限制数量:', maxDataCount);
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">K线数据为空（代码：${code}）<br/>可能原因：数据时间范围不匹配或数据尚未采集<br/>请尝试采集K线数据</div>`;
                return;
            }
            
            // 直接渲染完整数据，避免多次跳动
            try {
                renderChart(allData);
                
                // 加载技术指标（异步，不阻塞K线显示）
                setTimeout(() => {
                    loadIndicators(code);
                }, 500);
            } catch (renderError) {
                console.error('渲染K线图失败:', renderError);
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                    <div>渲染K线图失败: ${renderError.message || '未知错误'}</div>
                    <button id="retry-kline-btn" style="margin-top: 16px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                        重试
                    </button>
                </div>`;
                
                // 绑定重试按钮
                const retryBtn = document.getElementById('retry-kline-btn');
                if (retryBtn) {
                    retryBtn.addEventListener('click', () => {
                        loadChart(code);
                    });
                }
            }
        } else {
            const errorMsg = result.message || '未知错误';
            console.error('获取K线数据失败:', { code, market, period, errorMsg, result });
            
            // 更详细的错误信息和日志
            if (result.data === null || result.data === undefined) {
                console.warn('API返回的data字段为null或undefined');
            } else if (Array.isArray(result.data) && result.data.length === 0) {
                console.warn('API返回的data数组为空');
            } else if (!Array.isArray(result.data)) {
                console.warn('API返回的data不是数组:', typeof result.data, result.data);
            }
            
            // 更详细的错误信息
            let errorDetail = `无法获取K线数据（代码：${code}）<br/>错误：${errorMsg}`;
            if (result.code !== 0) {
                errorDetail += `<br/>错误代码：${result.code}`;
            }
            if (result.data && Array.isArray(result.data) && result.data.length === 0) {
                errorDetail += `<br/>数据为空，可能原因：该股票尚未采集K线数据`;
            } else if (!result.data) {
                errorDetail += `<br/>可能原因：股票代码不存在或数据源暂时不可用`;
            }
            errorDetail += `<br/><br/>💡 提示：可以在选股页面点击"采集K线数据"按钮进行数据采集`;
            
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                ${errorDetail}
                <button id="retry-kline-btn" style="margin-top: 16px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                    重试
                </button>
            </div>`;
            
            // 绑定重试按钮
            const retryBtn = document.getElementById('retry-kline-btn');
            if (retryBtn) {
                retryBtn.addEventListener('click', () => {
                    loadChart(code);
                });
            }
        }
    } catch (error) {
        console.error('K线数据加载异常:', error);
        let errorMsg = '加载失败';
        if (error.name === 'AbortError') {
            errorMsg = '请求超时，请稍后重试';
        } else if (error.message) {
            errorMsg = `加载失败: ${error.message}`;
        }
        
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
            <div>${errorMsg}</div>
            <button id="retry-kline-btn" style="margin-top: 16px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
                重试
            </button>
        </div>`;
        
        // 绑定重试按钮
        const retryBtn = document.getElementById('retry-kline-btn');
        if (retryBtn) {
            retryBtn.addEventListener('click', () => {
                loadChart(code);
            });
        }
    }
}

// 获取当前主题下的图表颜色配置
function getChartTheme() {
    const isLight = document.body.classList.contains('light-mode');
    if (isLight) {
        // 白天模式：白色背景
        return {
            background: '#ffffff',
            textColor: '#1f2937',
            gridColor: '#e2e8f0',
            borderColor: '#cbd5e1',
        };
    } else {
        // 黑夜模式：深色背景
        return {
            background: '#1e293b',
            textColor: '#cbd5f5',
            gridColor: '#334155',
            borderColor: '#334155',
        };
    }
}

// 更新图表主题
function updateChartTheme() {
    if (!chart) return;
    
    const theme = getChartTheme();
    chart.applyOptions({
        layout: {
            background: { type: 'solid', color: theme.background },
            textColor: theme.textColor,
        },
        grid: {
            vertLines: { color: theme.gridColor },
            horzLines: { color: theme.gridColor },
        },
        rightPriceScale: {
            borderColor: theme.borderColor,
        },
        timeScale: {
            borderColor: theme.borderColor,
        },
    });
}

function renderChart(data) {
    const container = document.getElementById('chart-container');
    if (!container) {
        console.error('K线容器不存在');
        return;
    }
    
    container.innerHTML = '';
    
    // 检查 LightweightCharts 是否可用
    if (!window.LightweightCharts || !window.LightweightCharts.createChart) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">K线图库加载失败，请刷新页面</div>';
        console.error('LightweightCharts not loaded');
        return;
    }
    
    // 确保容器有宽度和高度
    // 获取容器的实际尺寸，如果为0则使用默认值
    let containerWidth, containerHeight;
    const containerRect = container.getBoundingClientRect();
    containerWidth = containerRect.width || container.offsetWidth || container.clientWidth || 800;
    containerHeight = containerRect.height || container.offsetHeight || container.clientHeight || 500;
    
    // 如果容器尺寸为0或过小，使用默认尺寸
    if (containerWidth < 100 || containerHeight < 100) {
        console.warn('容器尺寸不足，使用默认尺寸', { width: containerWidth, height: containerHeight });
        containerWidth = 800;
        containerHeight = 500;
    }
    
    // 直接渲染
    renderChartInternal(data, container, containerWidth, containerHeight);
}

// 内部渲染函数
function renderChartInternal(data, container, containerWidth, containerHeight) {
    // 销毁旧图表
    if (chart) {
        // 清理事件监听器
        if (window.chartEventHandlers && window.chartEventHandlers[container.id]) {
            const handlers = window.chartEventHandlers[container.id];
            if (handlers.wheel) container.removeEventListener('wheel', handlers.wheel);
            if (handlers.resize) window.removeEventListener('resize', handlers.resize);
            delete window.chartEventHandlers[container.id];
        }
        chart.remove();
        chart = null;
    }
    
    // 获取当前主题配置
    const theme = getChartTheme();
    
    chart = window.LightweightCharts.createChart(container, {
        width: containerWidth,
        height: Math.max(containerHeight, 400), // 确保最小高度
        layout: {
            background: { type: 'solid', color: theme.background },
            textColor: theme.textColor,
        },
        grid: {
            vertLines: { color: theme.gridColor },
            horzLines: { color: theme.gridColor },
        },
        rightPriceScale: {
            borderColor: theme.borderColor,
            // 禁用自动缩放，手动控制缩放避免多次跳动
            autoScale: false, // 禁用自动缩放，手动控制
            scaleMargins: {
                top: 0.1,
                bottom: 0.1,
            },
        },
        timeScale: {
            borderColor: theme.borderColor,
            timeVisible: true,
            // 配置时间格式，使用正确的日期格式
            rightOffset: 0,
        },
        // 配置本地化选项，修复日期时间显示
        localization: {
            dateFormat: 'yyyy-MM-dd',
            timeFormat: 'HH:mm:ss',
            locale: 'zh-CN',
        },
        // 配置交叉线，使其跟随K线而不是EMA
        crosshair: {
            mode: window.LightweightCharts?.CrosshairMode?.Normal || 0, // Normal模式：跟随鼠标，但会吸附到数据点
            vertLine: {
                color: '#758696',
                width: 1,
                style: window.LightweightCharts?.LineStyle?.Dashed || 1, // 虚线
                labelBackgroundColor: '#4C525E',
            },
            horzLine: {
                color: '#758696',
                width: 1,
                style: window.LightweightCharts?.LineStyle?.Dashed || 1, // 虚线
                labelBackgroundColor: '#4C525E',
            },
        },
        // 恢复默认的缩放/滚动行为：无需点击即可使用鼠标滚轮、拖拽等操作
        // 移除handleScroll和handleScale限制，允许在任何地方拖动和缩放
        handleScroll: {
            mouseWheel: true,
            pressedMouseMove: true,
            horzTouchDrag: true,
            vertTouchDrag: true,
            // 启用Shift+滚轮垂直移动价格轴
            shiftVertTouchDrag: true,
        },
        handleScale: {
            axisPressedMouseMove: {
                time: true,  // 时间轴可以拖动
                price: true, // 价格轴可以拖动（垂直移动）
            },
            axisTouchDrag: {
                time: true,
                price: true,
            },
            axisDoubleClickReset: true,
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
        lastValueVisible: false, // 隐藏K线数值标签
        priceLineVisible: false, // 隐藏价格线
        crosshairMarkerVisible: true, // 确保K线的交叉标记可见，让交叉线跟随K线
        crosshairMarkerRadius: 4, // 设置交叉标记大小
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
        lastValueVisible: false,  // 隐藏成交量数值标签框
        priceLineVisible: false,  // 隐藏价格线
    });
    
    // 配置成交量价格轴（右侧）
    chart.priceScale('volume').applyOptions({
        scaleMargins: {
            top: 0.80,  // K线图占80%空间
            bottom: 0,
        },
        // 隐藏成交量价格轴的边框、标记线和标签数值显示框
        borderVisible: false,
        ticksVisible: false,
        visible: false,  // 完全隐藏价格轴，包括数值标签
    });
    
    // 转换数据格式并过滤无效数据
    const candleData = [];
    const volumeData = [];
    
    // 辅助函数：将日期字符串转换为LightweightCharts支持的时间格式
    const parseTime = (dateStr) => {
        if (!dateStr) return null;
        
        dateStr = String(dateStr).trim();
        
        // 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
        if (dateStr.length === 8 && !dateStr.includes('-') && !dateStr.includes('/')) {
            dateStr = `${dateStr.slice(0,4)}-${dateStr.slice(4,6)}-${dateStr.slice(6,8)}`;
        }
        
        // 尝试解析日期
        let date;
        if (dateStr.includes('-')) {
            date = new Date(dateStr);
        } else if (dateStr.includes('/')) {
            date = new Date(dateStr.replace(/\//g, '-'));
        } else {
            date = new Date(dateStr);
        }
        
        if (isNaN(date.getTime())) {
            console.warn('无法解析日期:', dateStr);
            return null;
        }
        
        // 对于日线数据，使用 'YYYY-MM-DD' 格式字符串（LightweightCharts推荐格式）
        // 对于小时/分钟数据，使用 Unix 时间戳（秒）
        if (dateStr.includes(' ') || dateStr.includes('T') || dateStr.includes(':')) {
            // 包含时间部分，使用时间戳（秒）
            return Math.floor(date.getTime() / 1000);
        } else {
            // 只有日期，确保格式为 YYYY-MM-DD
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }
    };
    
    let skippedCount = 0;
    let skippedReasons = {};
    
    data.forEach((d, index) => {
        // 优先使用time字段（支持小时级别：YYYY-MM-DD HH:MM:SS），否则使用date字段
        let dateStr = String(d.time || d.date || '');
        
        const timeValue = parseTime(dateStr);
        if (!timeValue) {
            skippedCount++;
            skippedReasons['无效日期'] = (skippedReasons['无效日期'] || 0) + 1;
            if (index < 5) {
                console.warn(`跳过无效日期数据[${index}]:`, d);
            }
            return; // 跳过无效日期
        }
        
        const open = parseFloat(d.open);
        const high = parseFloat(d.high);
        const low = parseFloat(d.low);
        const close = parseFloat(d.close);
        const volume = parseFloat(d.volume || 0);
        
        // 只添加有效数据（必须有open和close）
        if (!isNaN(open) && !isNaN(close)) {
            // 如果high/low缺失，用open/close代替
            const validHigh = !isNaN(high) ? high : Math.max(open, close);
            const validLow = !isNaN(low) ? low : Math.min(open, close);
            
            candleData.push({
                time: timeValue,
                open: open,
                high: validHigh,
                low: validLow,
                close: close,
            });
            
            volumeData.push({
                time: timeValue,
                value: volume || 0,
                color: close >= open ? '#22c55e' : '#ef4444',
            });
        } else {
            skippedCount++;
            const reason = isNaN(open) && isNaN(close) ? 'open和close都无效' : (isNaN(open) ? 'open无效' : 'close无效');
            skippedReasons[reason] = (skippedReasons[reason] || 0) + 1;
            if (index < 5) {
                console.warn(`跳过无效价格数据[${index}]:`, d);
            }
        }
    });
    
    if (skippedCount > 0) {
        console.warn(`K线数据过滤统计: 总数据${data.length}条，有效${candleData.length}条，跳过${skippedCount}条`, skippedReasons);
    }
    
    // 按时间排序（确保时间顺序正确）
    candleData.sort((a, b) => {
        const timeA = typeof a.time === 'string' ? new Date(a.time).getTime() : a.time;
        const timeB = typeof b.time === 'string' ? new Date(b.time).getTime() : b.time;
        return timeA - timeB;
    });
    volumeData.sort((a, b) => {
        const timeA = typeof a.time === 'string' ? new Date(a.time).getTime() : a.time;
        const timeB = typeof b.time === 'string' ? new Date(b.time).getTime() : b.time;
        return timeA - timeB;
    });
    
    console.log('K线数据条数:', candleData.length);
    if (candleData.length > 0) {
        console.log('K线数据示例（前3条）:', candleData.slice(0, 3));
    }
    
    if (candleData.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">K线数据格式错误</div>';
        return;
    }
    
    try {
        // 检查图表和series是否已创建
        if (!chart) {
            console.error('图表未创建，无法设置数据');
            container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">图表初始化失败，请刷新页面</div>';
            return;
        }
        
        if (!candleSeries || !volumeSeries) {
            console.error('图表series未创建，无法设置数据');
            container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">图表series初始化失败，请刷新页面</div>';
            return;
        }
        
        // 直接设置所有数据（LightweightCharts可以处理大量数据）
        console.log('设置K线数据，条数:', candleData.length);
        
        // 确保价格轴禁用自动缩放，避免添加series时触发自动缩放
        if (chart && chart.priceScale('right')) {
            chart.priceScale('right').applyOptions({
                autoScale: false,
            });
        }
        
        // 设置数据
        candleSeries.setData(candleData);
        volumeSeries.setData(volumeData);
        
        // 更新EMA和成交量显示状态
        if (volumeSeries) {
            const savedVolumeVisible = localStorage.getItem('volumeVisible');
            const isVisible = savedVolumeVisible !== null ? savedVolumeVisible === 'true' : volumeVisible;
            volumeSeries.applyOptions({ visible: isVisible });
        }
        
        // 先绘制EMA，然后统一调用一次fitContent，避免多次缩放
        updateEMA();
        
        // 等待所有数据（K线、成交量、EMA）都设置完成后，只调用一次fitContent
        // 使用更长的延迟确保所有EMA数据都已设置完成
        setTimeout(() => {
            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    // 双重requestAnimationFrame确保所有数据都已渲染
                    if (chart && chart.timeScale()) {
                        chart.timeScale().fitContent(); // 只调用一次，适应所有内容
                    }
                    console.log('K线数据设置完成，图表应该已显示');
                });
            });
        }, 100); // 增加延迟，确保EMA数据完全设置
        
        // 监听窗口大小变化，调整图表尺寸
        const handleResize = () => {
            if (chart && container) {
                const newWidth = container.offsetWidth || container.clientWidth;
                const newHeight = container.offsetHeight || container.clientHeight;
                if (newWidth > 0 && newHeight > 0) {
                    chart.applyOptions({ width: newWidth, height: Math.max(newHeight, 400) });
                }
            }
        };
        
        // 移除之前的resize监听器（如果存在）
        if (window.chartResizeHandler) {
            window.removeEventListener('resize', window.chartResizeHandler);
        }
        window.chartResizeHandler = handleResize;
        window.addEventListener('resize', handleResize);
        
        // 添加垂直移动功能：Shift + 鼠标滚轮可以垂直移动价格轴
        // 鼠标滚轮事件：Shift + 滚轮 = 垂直移动价格轴，普通滚轮 = 水平移动时间轴
        const handleWheel = (e) => {
            // 使用 e.shiftKey 检测 Shift 键，更可靠
            if (!chart || !e.shiftKey) return;
            
            e.preventDefault();
            e.stopPropagation();
            
            const priceScale = chart.priceScale('right');
            if (!priceScale) return;
            
            // 获取当前价格范围
            const visibleRange = priceScale.getVisibleRange();
            if (!visibleRange) return;
            
            // 计算移动距离（根据滚轮方向）
            const delta = e.deltaY > 0 ? 0.1 : -0.1; // 每次移动10%的价格范围
            const priceRange = visibleRange.to - visibleRange.from;
            const moveAmount = priceRange * delta;
            
            // 更新价格范围
            priceScale.setVisibleRange({
                from: visibleRange.from + moveAmount,
                to: visibleRange.to + moveAmount,
            });
        };
        
        container.addEventListener('wheel', handleWheel, { passive: false });
        
        // 保存事件处理器，以便后续清理
        if (!window.chartEventHandlers) {
            window.chartEventHandlers = {};
        }
        window.chartEventHandlers[container.id] = {
            wheel: handleWheel,
            resize: handleResize,
        };
    } catch (err) {
        console.error('设置K线数据失败:', err);
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">K线渲染失败: ${err.message}</div>`;
    }
}

async function loadIndicators(code) {
    try {
        // 检查是否有token（如果后端需要认证）
        if (!apiToken) {
            console.warn('未登录或token未设置，跳过加载技术指标');
            return;
        }
        
        const response = await apiFetch(`${API_BASE}/api/market/a/indicators?code=${code}`);
        
        // 检查HTTP状态码
        if (!response.ok) {
            if (response.status === 401) {
                console.warn('加载指标失败 - 认证失败，可能需要重新登录');
            } else {
                const errorText = await response.text();
                console.error('加载指标失败 - HTTP错误:', response.status, errorText);
            }
            return; // 静默失败，不影响K线图显示
        }
        
        const result = await response.json();
        
        if (result.code === 0) {
            renderIndicators(result.data);
        } else {
            console.warn('加载指标失败 - API错误:', result.message || '未知错误');
        }
    } catch (error) {
        console.error('加载指标失败:', error);
        // 静默失败，不影响K线图显示
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
        el.addEventListener('click', (e) => {
            e.stopPropagation(); // 阻止事件冒泡
            const targetId = el.getAttribute('data-target');
            const content = document.getElementById(targetId);
            if (!content) {
                console.warn('找不到目标元素:', targetId);
                return;
            }
            const arrow = el.querySelector('.indicator-arrow');
            // 检查当前显示状态（考虑CSS默认display:none）
            const currentDisplay = content.style.display;
            const computedDisplay = window.getComputedStyle(content).display;
            const isVisible = currentDisplay === 'block' || (currentDisplay === '' && computedDisplay === 'block');
            
            content.style.display = isVisible ? 'none' : 'block';
            if (arrow) {
                arrow.textContent = isVisible ? '▼' : '▲';
            }
        });
    });
}

function updateEMA() {
    if (!chart || !candleSeries) {
        console.warn('updateEMA: chart或candleSeries不存在');
        return;
    }
    
    // 清除现有EMA线
    emaSeries.forEach(series => {
        try {
            chart.removeSeries(series);
        } catch (e) {
            console.warn('移除EMA线失败:', e);
        }
    });
    emaSeries = [];
    
    if (!emaConfig.enabled) {
        console.debug('EMA未启用，跳过绘制');
        return;
    }
    
    // 获取K线数据
    const klineData = candleSeries.data();
    if (!klineData || klineData.length === 0) {
        console.warn('updateEMA: K线数据为空');
        return;
    }
    
    console.debug(`updateEMA: 开始计算EMA，数据条数=${klineData.length}, EMA配置=`, emaConfig);
    
    // 计算EMA（根据Pine Script标准EMA计算）
    emaConfig.values.forEach((period, index) => {
        if (!period || period <= 0) {
            console.warn(`跳过无效的EMA周期: ${period}`);
            return;
        }
        
        const emaValues = calculateEMA(klineData, period);
        if (emaValues.length > 0) {
            // 根据Pine Script代码的颜色：black, green, red
            const colors = ['#000000', '#10b981', '#ef4444'];
            try {
                const emaLine = chart.addLineSeries({
                    color: colors[index % colors.length],
                    lineWidth: 1,
                    title: `EMA${period}`,
                    lastValueVisible: false,  // 隐藏EMA数值标签（价格栏旁边的数值）
                    priceLineVisible: false,  // 隐藏EMA横线
                    crosshairMarkerVisible: false, // 隐藏交叉标记
                    priceFormat: {
                        type: 'price',
                        precision: 2,
                        minMove: 0.01,
                    },
                });
                // 创建后立即隐藏数值显示和交叉标记，确保交叉线跟随K线
                emaLine.applyOptions({
                    lastValueVisible: false,
                    crosshairMarkerVisible: false,
                });
                // 在设置数据前，确保价格轴禁用自动缩放（防止添加series时触发自动缩放）
                if (chart && chart.priceScale('right')) {
                    chart.priceScale('right').applyOptions({
                        autoScale: false,
                    });
                }
                emaLine.setData(emaValues);
                // 设置数据后，再次确保价格轴禁用自动缩放
                if (chart && chart.priceScale('right')) {
                    chart.priceScale('right').applyOptions({
                        autoScale: false,
                    });
                }
                emaSeries.push(emaLine);
                console.debug(`EMA${period}线绘制成功，数据点=${emaValues.length}`);
            } catch (e) {
                console.error(`绘制EMA${period}线失败:`, e);
            }
        } else {
            console.warn(`EMA${period}计算结果为空`);
        }
    });
    
    // 确保K线的交叉标记可见，让交叉线跟随K线而不是EMA
    if (candleSeries) {
        candleSeries.applyOptions({
            crosshairMarkerVisible: true,
        });
    }
}

function calculateEMA(data, period) {
    if (!data || data.length < period) {
        console.warn(`EMA计算失败: 数据不足，需要至少${period}条数据，当前只有${data?.length || 0}条`);
        return [];
    }
    
    const result = [];
    let multiplier = 2 / (period + 1);
    let ema = parseFloat(data[0].close);
    
    if (isNaN(ema)) {
        console.error('EMA计算失败: 第一个数据点的close值无效', data[0]);
        return [];
    }
    
    data.forEach((item, index) => {
        const close = parseFloat(item.close);
        if (isNaN(close)) {
            console.warn(`EMA计算跳过无效数据点: index=${index}`, item);
            return;
        }
        
        if (index === 0) {
            ema = close;
        } else {
            ema = (close - ema) * multiplier + ema;
        }
        result.push({
            time: item.time,
            value: ema
        });
    });
    
    return result;
}

// 自选股模块缓存
const WATCHLIST_CACHE_KEY = 'watchlist_data_cache';

// 获取缓存的自选股数据（无限期缓存，除非自选股列表变化）
function getCachedWatchlistData() {
    try {
        const cached = localStorage.getItem(WATCHLIST_CACHE_KEY);
        if (!cached) return null;
        
        const { data, watchlistCodes } = JSON.parse(cached);
        
        // 检查自选股列表是否发生变化
        const currentWatchlist = getWatchlist();
        const currentCodes = currentWatchlist.map(s => String(s.code).trim()).sort().join(',');
        const cachedCodes = watchlistCodes.sort().join(',');
        
        if (currentCodes !== cachedCodes) {
            // 自选股列表已变化，清除缓存
            localStorage.removeItem(WATCHLIST_CACHE_KEY);
            return null;
        }
        
        return data;
    } catch (e) {
        console.warn('读取自选股缓存失败:', e);
        return null;
    }
}

// 保存自选股数据到缓存（无限期缓存）
function saveCachedWatchlistData(data) {
    try {
        const watchlist = getWatchlist();
        const watchlistCodes = watchlist.map(s => String(s.code).trim());
        const cacheData = {
            data: data,
            watchlistCodes: watchlistCodes
        };
        localStorage.setItem(WATCHLIST_CACHE_KEY, JSON.stringify(cacheData));
    } catch (e) {
        console.warn('保存自选股缓存失败:', e);
    }
}

// 自选股模块
function initWatchlist() {
    // 绑定刷新按钮
    const refreshBtn = document.getElementById('refresh-watchlist-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            loadWatchlist(true); // 强制刷新
        });
    }
    
    // 首次加载时使用缓存
    loadWatchlist(false);
}

// 加载自选股列表（使用和行情页一样的加载方法）
async function loadWatchlist(forceRefresh = false) {
    const watchlist = getWatchlist();
    const container = document.getElementById('watchlist-container');
    const tbody = document.getElementById('watchlist-stock-list');
    
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
    
    // 检查缓存（如果不强制刷新）
    if (!forceRefresh) {
        const cachedData = getCachedWatchlistData();
        if (cachedData && cachedData.length > 0) {
            console.log('使用缓存的自选股数据');
            renderWatchlistStocks(cachedData);
            return;
        }
    }
    
    // 确保表格结构存在
    if (!tbody) {
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
                    <tr><td colspan="6" class="loading">加载中...</td></tr>
                </tbody>
            </table>
        `;
    } else {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">加载中...</td></tr>';
    }
    
    try {
        // 获取自选股代码列表
        const watchlistCodes = watchlist.map(s => String(s.code).trim());
        
        // 分别从A股和港股获取数据
        let allStocks = [];
        
        // 获取A股数据
        try {
            let page = 1;
            let hasMore = true;
            const pageSize = 500;
            
            while (hasMore && page <= 10) {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 10000);
                
                const response = await apiFetch(`${API_BASE}/api/market/a/spot?page=${page}&page_size=${pageSize}`, {
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!response.ok) {
                    hasMore = false;
                    break;
                }
                
                const result = await response.json();
                
                if (result.code === 0 && result.data && result.data.length > 0) {
                    allStocks = allStocks.concat(result.data);
                    
                    if (result.pagination) {
                        hasMore = page < result.pagination.total_pages;
                    } else {
                        hasMore = result.data.length === pageSize;
                    }
                    page++;
                } else {
                    hasMore = false;
                }
            }
        } catch (e) {
            console.error('获取A股数据失败:', e);
        }
        
        // 获取港股数据
        try {
            let page = 1;
            let hasMore = true;
            const pageSize = 500;
            
            while (hasMore && page <= 10) {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 10000);
                
                const response = await apiFetch(`${API_BASE}/api/market/hk/spot?page=${page}&page_size=${pageSize}`, {
                    signal: controller.signal
                });
                
                clearTimeout(timeoutId);
                
                if (!response.ok) {
                    hasMore = false;
                    break;
                }
                
                const result = await response.json();
                
                if (result.code === 0 && result.data && result.data.length > 0) {
                    allStocks = allStocks.concat(result.data);
                    
                    if (result.pagination) {
                        hasMore = page < result.pagination.total_pages;
                    } else {
                        hasMore = result.data.length === pageSize;
                    }
                    page++;
                } else {
                    hasMore = false;
                }
            }
        } catch (e) {
            console.error('获取港股数据失败:', e);
        }
        
        // 筛选出自选股列表中的股票，保持自选列表的顺序
        const watchlistStocks = watchlistCodes.map(code => {
            const stock = allStocks.find(s => String(s.code).trim() === code);
            if (stock) {
                return stock;
            }
            // 如果找不到，返回基本信息
            const watchlistItem = watchlist.find(w => String(w.code).trim() === code);
            return {
                code: code,
                name: watchlistItem?.name || code,
                price: null,
                pct: null,
                volume: null,
            };
        });
        
        // 保存到缓存
        saveCachedWatchlistData(watchlistStocks);
        
        // 渲染股票列表
        renderWatchlistStocks(watchlistStocks);
        
    } catch (error) {
        console.error('加载自选股失败:', error);
        // 如果加载失败，尝试使用缓存
        const cachedData = getCachedWatchlistData();
        if (cachedData && cachedData.length > 0) {
            console.log('加载失败，使用缓存数据');
            renderWatchlistStocks(cachedData);
        } else {
            const tbodyEl = document.getElementById('watchlist-stock-list');
            if (tbodyEl) {
                tbodyEl.innerHTML = `<tr><td colspan="6" class="loading">加载失败: ${error.message}</td></tr>`;
            }
        }
    }
}

// 渲染自选股列表（复用函数）
function renderWatchlistStocks(watchlistStocks) {
    const tbodyEl = document.getElementById('watchlist-stock-list');
    
    // 如果表格已存在且有数据，且数据相同，不重新渲染（避免闪烁和重复加载）
    if (tbodyEl && tbodyEl.children.length > 0) {
        const existingRows = Array.from(tbodyEl.querySelectorAll('tr'));
        const existingCodes = existingRows.map(tr => {
            const firstTd = tr.querySelector('td:first-child');
            return firstTd ? firstTd.textContent.trim() : null;
        }).filter(code => code && code !== '暂无数据' && !code.includes('加载'));
        
        const newCodes = watchlistStocks.map(s => String(s.code).trim());
        
        // 如果数据相同，不重新渲染
        if (existingCodes.length === newCodes.length && 
            existingCodes.length > 0 &&
            existingCodes.every((code, idx) => code === newCodes[idx])) {
            console.log('自选股数据未变化，跳过渲染');
            return;
        }
    }
    
    if (!tbodyEl) {
        // 如果表格不存在，先创建
        const container = document.getElementById('watchlist-container');
        if (container) {
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
                    <tbody id="watchlist-stock-list"></tbody>
                </table>
            `;
        } else {
            return;
        }
    }
    
    // 重新获取tbodyEl（可能刚创建）
    const finalTbodyEl = document.getElementById('watchlist-stock-list');
    if (!finalTbodyEl) return;
    
    finalTbodyEl.innerHTML = '';
    
    if (watchlistStocks.length === 0) {
        finalTbodyEl.innerHTML = '<tr><td colspan="6" class="loading">暂无数据</td></tr>';
        return;
    }
    
    watchlistStocks.forEach(stock => {
        const tr = document.createElement('tr');
        tr.setAttribute('data-stock', JSON.stringify(stock));
        tr.style.cursor = 'pointer';
        tr.innerHTML = `
            <td>${stock.code}</td>
            <td>${stock.name}</td>
            <td>${stock.price !== null && stock.price !== undefined && !isNaN(stock.price) ? stock.price.toFixed(2) : '-'}</td>
            <td class="${stock.pct !== null && stock.pct !== undefined && !isNaN(stock.pct) ? (stock.pct >= 0 ? 'up' : 'down') : ''}">
                ${stock.pct !== null && stock.pct !== undefined && !isNaN(stock.pct) ? `${stock.pct.toFixed(2)}%` : '-'}
            </td>
            <td>${formatVolume(stock.volume)}</td>
            <td>
                <button class="remove-watchlist-btn" data-code="${stock.code}" style="padding: 4px 8px; background: #ef4444; color: white; border: none; border-radius: 4px; cursor: pointer;" onclick="event.stopPropagation();">移除</button>
            </td>
        `;
        
        // 添加单击事件
        tr.addEventListener('click', function(e) {
            if (e.target.tagName === 'BUTTON' || e.target.closest('button')) {
                return;
            }
            e.preventDefault();
            const stockData = JSON.parse(this.getAttribute('data-stock'));
            openKlineModal(stockData.code, stockData.name, stockData);
        });
        
        tbodyEl.appendChild(tr);
    });
    
    // 绑定移除按钮事件
    document.querySelectorAll('.remove-watchlist-btn').forEach(btn => {
        btn.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            const code = this.getAttribute('data-code');
            removeFromWatchlist(code);
        };
    });
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
    
            // 如果当前在自选页，刷新列表（清除缓存，强制刷新）
            if (document.getElementById('watchlist-tab') && document.getElementById('watchlist-tab').classList.contains('active')) {
                localStorage.removeItem(WATCHLIST_CACHE_KEY);
                loadWatchlist(true);
            }
}

// 从自选股移除
function removeFromWatchlist(code) {
    const watchlist = getWatchlist();
    const newWatchlist = watchlist.filter(s => s.code !== code);
    saveWatchlist(newWatchlist);
    // 清除缓存，重新加载
    localStorage.removeItem(WATCHLIST_CACHE_KEY);
    loadWatchlist(true);
    
    // 更新行情页按钮状态
    document.querySelectorAll(`.add-watchlist-btn[data-code="${code}"]`).forEach(btn => {
        btn.textContent = '加入自选';
        btn.style.background = '#10b981';
        btn.disabled = false;
    });
}

// 行情数据缓存管理
const MARKET_DATA_CACHE_KEY = 'market_data_cache';
const MARKET_DATA_CACHE_EXPIRY = 30000; // 30秒缓存过期时间

// 获取缓存的行情数据
function getCachedMarketData() {
    try {
        const cached = localStorage.getItem(MARKET_DATA_CACHE_KEY);
        if (!cached) return null;
        
        const { data, timestamp } = JSON.parse(cached);
        const now = Date.now();
        
        // 检查缓存是否过期
        if (now - timestamp > MARKET_DATA_CACHE_EXPIRY) {
            localStorage.removeItem(MARKET_DATA_CACHE_KEY);
            return null;
        }
        
        return data;
    } catch (e) {
        console.warn('读取缓存失败:', e);
        return null;
    }
}

// 保存行情数据到缓存
function saveCachedMarketData(data) {
    try {
        const cache = {
            data: data,
            timestamp: Date.now()
        };
        localStorage.setItem(MARKET_DATA_CACHE_KEY, JSON.stringify(cache));
    } catch (e) {
        console.warn('保存缓存失败:', e);
    }
}

// 从服务器获取行情数据
async function fetchMarketDataFromServer() {
    let allStocks = [];
    let page = 1;
    const pageSize = 500;
    let hasMore = true;
    
    while (hasMore && page <= 10) { // 最多获取10页，避免无限循环
        try {
            // 添加超时控制
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 10000); // 每页10秒超时
            
            const response = await apiFetch(`${API_BASE}/api/market/a/spot?page=${page}&page_size=${pageSize}`, {
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (!response.ok) {
                console.error(`获取第${page}页数据失败: HTTP ${response.status}`);
                hasMore = false;
                break;
            }
            
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
                // 如果返回空数据或错误，停止获取
                if (result.message) {
                    console.warn(`获取第${page}页数据: ${result.message}`);
                }
                hasMore = false;
            }
        } catch (e) {
            console.error(`获取第${page}页数据失败:`, e);
            if (e.name === 'AbortError') {
                console.error('请求超时');
            }
            hasMore = false;
        }
    }
    
    return allStocks;
}

// 更新自选股实时行情
async function updateWatchlistPrices() {
    const watchlist = getWatchlist();
    if (watchlist.length === 0) return;
    
    try {
        // 只使用缓存数据，不再主动从服务器获取
        let allStocks = getCachedMarketData();
        
        // 如果没有缓存或缓存为空，才从服务器获取一次（添加超时控制）
        if (!allStocks || allStocks.length === 0) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 15000); // 15秒超时
                
                allStocks = await fetchMarketDataFromServer();
                
                clearTimeout(timeoutId);
                
                // 保存到缓存
                if (allStocks && allStocks.length > 0) {
                    saveCachedMarketData(allStocks);
                }
            } catch (fetchError) {
                console.error('获取市场数据失败:', fetchError);
                // 如果获取失败，显示错误提示
                watchlist.forEach(watchStock => {
                    const priceEl = document.querySelector(`.watchlist-price[data-code="${watchStock.code}"]`);
                    if (priceEl) {
                        priceEl.textContent = fetchError.name === 'AbortError' ? '超时' : '获取失败';
                    }
                });
                return; // 提前返回，不继续处理
            }
        }
        // 如果有缓存，直接使用缓存数据，不再后台更新
        
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
    const selectBtn = document.getElementById('select-btn');
    const loadSelectedBtn = document.getElementById('load-selected-btn');
    const collectKlineBtn = document.getElementById('collect-kline-btn');
    
    if (selectBtn) {
        selectBtn.addEventListener('click', runSelection);
    }
    if (loadSelectedBtn) {
        loadSelectedBtn.addEventListener('click', loadSelectedStocks);
    }
    if (collectKlineBtn) {
        collectKlineBtn.addEventListener('click', () => {
            const market = document.getElementById('selection-market-select')?.value || 'A';
            const maxCount = parseInt(document.getElementById('collect-max-count-input')?.value || 6000);
            collectKlineData(market, maxCount);
        });
    }
}

async function runSelection() {
    const market = document.getElementById('selection-market-select')?.value || 'A';
    const maxCount = parseInt(document.getElementById('max-count-input')?.value || 30);
    const container = document.getElementById('selected-stocks');
    
    // 生成任务ID
    const taskId = `selection_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    // 显示进度界面
    container.innerHTML = `
        <div id="selection-progress-container" style="padding: 20px;">
            <div style="text-align: center; margin-bottom: 20px;">
                <div style="font-size: 18px; color: #60a5fa; margin-bottom: 10px;">选股进行中...</div>
                <div id="selection-progress-message" style="color: #94a3b8; margin-bottom: 10px;">初始化中...</div>
                <div style="width: 100%; max-width: 500px; margin: 0 auto; background: #1e293b; border-radius: 8px; overflow: hidden;">
                    <div id="selection-progress-bar" style="height: 8px; background: #3b82f6; width: 0%; transition: width 0.3s;"></div>
                </div>
                <div id="selection-progress-details" style="margin-top: 15px; font-size: 12px; color: #64748b;">
                    <div>进度: <span id="selection-progress-percent">0</span>%</div>
                    <div>已处理: <span id="selection-processed">0</span> / <span id="selection-total">0</span></div>
                    <div>通过: <span id="selection-passed">0</span></div>
                    <div>耗时: <span id="selection-elapsed">0</span>秒</div>
                </div>
            </div>
        </div>
    `;
    
    // 连接WebSocket获取进度
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/selection/progress`;
    let ws = null;
    
    try {
        ws = new WebSocket(wsUrl);
        
        ws.onopen = () => {
            // 发送任务ID
            ws.send(JSON.stringify({ task_id: taskId }));
        };
        
        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'selection_progress' && data.progress) {
                    updateSelectionProgress(data.progress);
                }
            } catch (e) {
                console.error('解析进度数据失败:', e);
            }
        };
        
        ws.onerror = (error) => {
            console.error('WebSocket错误:', error);
        };
        
        ws.onclose = () => {
            console.log('WebSocket连接关闭');
        };
    } catch (e) {
        console.error('WebSocket连接失败:', e);
    }
    
    // 更新进度显示的函数
    function updateSelectionProgress(progress) {
        const progressBar = document.getElementById('selection-progress-bar');
        const progressMessage = document.getElementById('selection-progress-message');
        const progressPercent = document.getElementById('selection-progress-percent');
        const processed = document.getElementById('selection-processed');
        const total = document.getElementById('selection-total');
        const passed = document.getElementById('selection-passed');
        const elapsed = document.getElementById('selection-elapsed');
        
        if (progressBar) {
            progressBar.style.width = `${progress.progress || 0}%`;
        }
        if (progressMessage) {
            progressMessage.textContent = progress.message || '处理中...';
        }
        if (progressPercent) {
            progressPercent.textContent = progress.progress || 0;
        }
        if (processed) {
            processed.textContent = progress.processed || 0;
        }
        if (total) {
            total.textContent = progress.total || 0;
        }
        if (passed) {
            passed.textContent = progress.passed || 0;
        }
        if (elapsed) {
            elapsed.textContent = progress.elapsed_time || 0;
        }
        
        // 如果完成或失败，关闭WebSocket
        if (progress.status === 'completed' || progress.status === 'failed') {
            if (ws) {
                ws.close();
            }
        }
    }
    
    try {
        // 添加超时控制（选股可能需要较长时间，设置为60秒）
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 60000); // 60秒超时
        
        const response = await apiFetch(`${API_BASE}/api/strategy/select?max_count=${maxCount}&market=${market}&task_id=${taskId}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        const result = await response.json();
        
        // 关闭WebSocket
        if (ws) {
            ws.close();
        }
        
        if (result.code === 0) {
            if (result.message && result.message.includes('市场环境不佳')) {
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: #f59e0b;">${result.message}</div>`;
            } else {
                renderSelectedStocks(result.data);
            }
        } else {
            // 如果错误提示包含"没有数据"或"kline"，显示采集按钮
            const message = result.message || '未知错误';
            let errorHtml = `<div style="text-align: center; padding: 40px; color: #ef4444;">选股失败: ${message}</div>`;
            
            if (message.includes('没有数据') || message.includes('kline') || message.includes('K线')) {
                errorHtml += `
                    <div style="text-align: center; margin-top: 20px;">
                        <button id="collect-kline-btn" style="
                            padding: 10px 20px;
                            background: #3b82f6;
                            color: white;
                            border: none;
                            border-radius: 6px;
                            cursor: pointer;
                            font-size: 14px;
                            font-weight: 500;
                            transition: background 0.2s;
                        ">📥 采集K线数据</button>
                        <div id="collect-kline-status" style="margin-top: 10px; font-size: 12px; color: #94a3b8;"></div>
                    </div>
                `;
            }
            
            container.innerHTML = errorHtml;
            
            // 绑定采集按钮事件
            setTimeout(() => {
                const collectBtn = document.getElementById('collect-kline-btn');
                if (collectBtn) {
                    collectBtn.addEventListener('click', () => {
                        const maxCount = parseInt(document.getElementById('collect-max-count-input')?.value || 6000);
                        collectKlineData(market, maxCount);
                    });
                }
            }, 0);
        }
    } catch (error) {
        // 关闭WebSocket
        if (ws) {
            ws.close();
        }
        
        let errorMessage = '选股失败';
        if (error.name === 'AbortError') {
            errorMessage = '选股超时（60秒），请检查网络连接或稍后重试';
        } else if (error.message) {
            errorMessage = `选股失败: ${error.message}`;
        }
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">${errorMessage}</div>`;
    }
}

// 采集K线数据
async function collectKlineData(market = 'A', maxCount = 6000) {
    // 优先使用选股页面的状态显示区域
    let statusEl = document.getElementById('collect-kline-status');
    let btn = document.getElementById('collect-kline-btn');
    
    // 如果选股失败时调用，使用错误消息中的状态区域
    if (!statusEl) {
        statusEl = document.getElementById('collect-kline-status');
    }
    if (!btn) {
        btn = document.getElementById('collect-kline-btn');
    }
    
    if (!btn) return;
    
    // 如果状态区域不存在，创建一个临时显示区域
    if (!statusEl) {
        const container = document.getElementById('selected-stocks');
        if (container) {
            const statusDiv = document.createElement('div');
            statusDiv.id = 'collect-kline-status-temp';
            statusDiv.style.cssText = 'text-align: center; margin-top: 10px; font-size: 12px; color: #94a3b8;';
            container.appendChild(statusDiv);
            statusEl = statusDiv;
        }
    }
    
    btn.disabled = true;
    btn.textContent = '采集中...';
    statusEl.textContent = '正在采集K线数据，请稍候...';
    statusEl.style.color = '#60a5fa';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/kline/collect?market=${market}&max_count=${maxCount}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.code === 0) {
            const taskId = result.data?.task_id;
            statusEl.textContent = `✅ ${result.message || '采集任务已启动，数据将在后台采集并保存到ClickHouse'}`;
            statusEl.style.color = '#10b981';
            btn.textContent = '采集中...';
            
            // 连接WebSocket监听进度（如果有task_id则使用，否则监听最新任务）
            connectKlineCollectProgress(taskId, statusEl, btn);
        } else {
            statusEl.textContent = `❌ 采集失败: ${result.message || '未知错误'}`;
            statusEl.style.color = '#ef4444';
            btn.disabled = false;
            btn.textContent = '📥 采集K线数据';
        }
    } catch (error) {
        statusEl.textContent = `❌ 采集失败: ${error.message || '网络错误'}`;
        statusEl.style.color = '#ef4444';
        btn.disabled = false;
        btn.textContent = '📥 采集K线数据';
    }
}

function connectKlineCollectProgress(taskId, statusEl, btn) {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsBase = wsProtocol + '//' + window.location.host;
    const ws = new WebSocket(`${wsBase}/ws/kline/collect/progress`);
    
    ws.onopen = () => {
        // 发送task_id（如果有则发送，否则监听最新任务）
        if (taskId) {
            ws.send(JSON.stringify({ task_id: taskId }));
        } else {
            // 没有task_id，监听最新任务（后端会自动选择最新的任务）
            ws.send(JSON.stringify({}));
        }
        // 显示初始状态
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #10b981; margin-bottom: 5px; font-weight: 500;">✅ 采集任务已启动</div>
                <div style="color: #60a5fa; font-size: 11px; margin-bottom: 5px;">正在连接进度监控...</div>
                <div style="color: #94a3b8; font-size: 11px;">数据正在后台采集中，请等待几分钟后再试选股</div>
            </div>
        `;
    };
    
    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'kline_collect_progress' && data.progress) {
                const progress = data.progress;
                
                if (progress.status === 'running') {
                    const progressPct = progress.progress || 0;
                    const success = progress.success || 0;
                    const failed = progress.failed || 0;
                    const total = progress.total || 0;
                    const current = progress.current || 0;
                    
                    statusEl.innerHTML = `
                        <div style="margin-top: 10px;">
                            <div style="color: #10b981; margin-bottom: 5px; font-weight: 500;">
                                ✅ 采集任务进行中
                            </div>
                            <div style="color: #60a5fa; margin-bottom: 8px; font-size: 14px; font-weight: 600;">
                                📊 正在采集: 第 <strong style="color: #3b82f6; font-size: 16px;">${current}</strong> 只 / 总共 ${total} 只
                            </div>
                            <div style="display: flex; gap: 16px; margin-bottom: 8px; font-size: 12px;">
                                <div style="color: #10b981;">
                                    ✅ 成功: <strong>${success}</strong> 只
                                </div>
                                <div style="color: ${failed > 0 ? '#ef4444' : '#94a3b8'};">
                                    ❌ 失败: <strong>${failed}</strong> 只
                                </div>
                            </div>
                            <div style="margin-top: 8px; width: 100%; background: #e2e8f0; border-radius: 4px; overflow: hidden; height: 8px;">
                                <div style="width: ${progressPct}%; background: linear-gradient(90deg, #3b82f6, #60a5fa); height: 100%; transition: width 0.3s ease; box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);"></div>
                            </div>
                            <div style="color: #94a3b8; font-size: 11px; margin-top: 8px;">
                                数据正在后台采集中，请等待完成后再试选股
                            </div>
                        </div>
                    `;
                    btn.textContent = `采集中 ${current}/${total}`;
                } else if (progress.status === 'completed') {
                    const success = progress.success || 0;
                    const failed = progress.failed || 0;
                    const total = progress.total || 0;
                    
                    statusEl.innerHTML = `
                        <div style="margin-top: 10px;">
                            <div style="color: #10b981; margin-bottom: 5px; font-weight: bold;">
                                ✅ 采集完成！
                            </div>
                            <div style="color: #10b981; font-size: 11px; margin-bottom: 2px;">
                                ✅ 成功: ${success} 只股票
                            </div>
                            <div style="color: ${failed > 0 ? '#f59e0b' : '#94a3b8'}; font-size: 11px; margin-bottom: 5px;">
                                ${failed > 0 ? `⚠️ 失败: ${failed} 只股票` : '无失败'}
                            </div>
                            <div style="color: #94a3b8; font-size: 11px;">
                                总计: ${total} 只股票，现在可以开始选股了
                            </div>
                        </div>
                    `;
                    btn.disabled = false;
                    btn.textContent = '✅ 采集完成';
                    btn.style.background = '#10b981';
                    ws.close();
                } else if (progress.status === 'failed') {
                    statusEl.innerHTML = `
                        <div style="margin-top: 10px;">
                            <div style="color: #ef4444; margin-bottom: 5px;">
                                ❌ 采集失败
                            </div>
                            <div style="color: #94a3b8; font-size: 11px;">
                                ${progress.message || '采集过程中发生错误'}
                            </div>
                        </div>
                    `;
                    btn.disabled = false;
                    btn.textContent = '📥 重新采集';
                    ws.close();
                }
            }
        } catch (error) {
            console.error('解析进度消息失败:', error);
        }
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket连接错误:', error);
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #f59e0b; margin-bottom: 5px;">⚠️ 进度监控连接失败</div>
                <div style="color: #94a3b8; font-size: 11px;">数据仍在后台采集中，请稍后手动刷新</div>
            </div>
        `;
    };
    
    ws.onclose = () => {
        console.log('K线采集进度WebSocket连接已关闭');
    };
}

async function loadSelectedStocks() {
    const container = document.getElementById('selected-stocks');
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">加载中...</div>';
    
    try {
        // 添加超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时
        
        const response = await apiFetch(`${API_BASE}/api/strategy/selected`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        const result = await response.json();
        
        if (result.code === 0 && result.data) {
            const data = result.data;
            if (data.stocks && data.stocks.length > 0) {
                // 更新市场选择器
                if (document.getElementById('selection-market-select')) {
                    document.getElementById('selection-market-select').value = data.market || 'A';
                }
                renderSelectedStocks(data.stocks);
                showToast(`已加载上次选股结果（${data.market}股，${data.count}只）`, 'success');
            } else {
                container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">暂无保存的选股结果</div>';
            }
        } else {
            container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">暂无保存的选股结果</div>';
        }
    } catch (error) {
        let errorMessage = '加载失败';
        if (error.name === 'AbortError') {
            errorMessage = '加载超时，请检查网络连接或稍后重试';
        } else if (error.message) {
            errorMessage = `加载失败: ${error.message}`;
        }
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">${errorMessage}</div>`;
    }
}

function renderSelectedStocks(stocks) {
    const container = document.getElementById('selected-stocks');
    
    if (stocks.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">未选出符合条件的股票</div>';
        return;
    }
    
    container.innerHTML = stocks.map(stock => {
        const indicators = stock.indicators || {};
        const indicatorsInfo = indicators.ma60 ? `MA60: ${indicators.ma60?.toFixed(2)} | 量比: ${indicators.vol_ratio?.toFixed(2)} | RSI: ${indicators.rsi?.toFixed(1)}` : '';
        
        return `
        <div class="stock-card">
            <div class="info">
                <div style="font-size: 18px; font-weight: 600; color: #60a5fa;">
                    ${stock.name} (${stock.code})
                </div>
                <div style="margin-top: 5px; color: #94a3b8;">
                    价格: ${stock.price?.toFixed(2) || '-'} | 
                    涨跌幅: ${stock.pct?.toFixed(2) || '-'}%
                </div>
                ${indicatorsInfo ? `<div style="margin-top: 4px; font-size: 12px; color: #64748b;">${indicatorsInfo}</div>` : ''}
            </div>
        </div>
    `;
    }).join('');
}

// AI分析模块
function initAI() {
    const analyzeBtn = document.getElementById('analyze-btn');
    const codeInput = document.getElementById('ai-code-input');
    const clearBtn = document.getElementById('ai-clear-btn');
    
    analyzeBtn.addEventListener('click', () => {
        const code = codeInput.value.trim();
        if (code) {
            // 仅分析单只股票
            analyzeStock([code]);
        } else {
            // 未输入代码时，自动分析自选页所有自选股票
            const watchlist = getWatchlist();
            if (!watchlist || watchlist.length === 0) {
                showToast('自选列表为空，请先在行情页添加自选股票', 'error');
                return;
            }
            const codes = watchlist.map(s => String(s.code).trim()).filter(c => c);
            if (codes.length === 0) {
                showToast('自选列表中没有有效的股票代码', 'error');
                return;
            }
            analyzeStock(codes);
        }
    });
    
    // 支持回车键触发分析
    codeInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            analyzeBtn.click();
        }
    });

    if (clearBtn) {
        clearBtn.addEventListener('click', clearAIAnalysis);
    }
    
    const statsBtn = document.getElementById('ai-stats-btn');
    if (statsBtn) {
        statsBtn.addEventListener('click', loadStockStatistics);
    }

    // 初始化时加载历史AI分析结果
    loadAIAnalysisHistory();

    // 初始化自动分析定时任务
    initAutoAnalyzeScheduler();
}

async function analyzeStock(codes, options = {}) {
    const container = document.getElementById('ai-analysis-result');
    const codeList = Array.isArray(codes) ? codes : [codes];

    if (!codeList || codeList.length === 0) {
        container.innerHTML = `
            <div class="ai-error">
                <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">分析失败</div>
                <div style="font-size: 14px; color: #94a3b8;">没有需要分析的股票代码</div>
            </div>
        `;
        return;
    }

    const isBatch = codeList.length > 1;
    const loadingText = isBatch
        ? `正在分析自选的 ${codeList.length} 只股票，请稍候...`
        : 'AI分析中，请稍候...';

    container.innerHTML = `
        <div class="ai-loading">
            <div class="ai-loading-spinner"></div>
            <div style="margin-top: 16px; color: #94a3b8;">${loadingText}</div>
        </div>
    `;
    
    try {
        let result;
        if (isBatch) {
            // 批量分析接口（自选股）
            const notifyFlag = options.notify === true ? 'true' : 'false';
            const response = await apiFetch(`${API_BASE}/api/ai/analyze/batch?notify=${notifyFlag}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    codes: codeList,
                }),
            });
            result = await response.json();
            if (result.code === 0 && Array.isArray(result.data)) {
                await renderAIAnalysisBatch(result.data);
            } else {
                container.innerHTML = `
                    <div class="ai-error">
                        <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                        <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">分析失败</div>
                        <div style="font-size: 14px; color: #94a3b8;">${result.message || '无法获取分析数据'}</div>
                    </div>
                `;
            }
        } else {
            // 单只股票分析接口
            const singleCode = codeList[0];
            const response = await apiFetch(`${API_BASE}/api/ai/analyze/${singleCode}`);
            result = await response.json();
            
            if (result.code === 0 && result.data) {
                renderAIAnalysis(result.data, singleCode);
            } else {
                container.innerHTML = `
                    <div class="ai-error">
                        <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                        <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">分析失败</div>
                        <div style="font-size: 14px; color: #94a3b8;">${result.message || '无法获取分析数据'}</div>
                    </div>
                `;
            }
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

function buildAIAnalysisHtml(data, code, name, planId = null, stats = null) {
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
    const title = name ? `${name} (${code})` : code || '';
    
    // 信号颜色
    const signal = data.signal || '';
    const signalColor = {
        '买入': '#10b981',
        '关注': '#3b82f6',
        '观望': '#f59e0b',
        '回避': '#ef4444'
    }[signal] || '#94a3b8';

    return `
        <div class="ai-analysis-content" data-code="${code || ''}">
            ${title ? `
            <div style="font-size: 16px; font-weight: 600; color: #60a5fa; margin-bottom: 8px;">
                ${title}
            </div>
            ` : ''}
            
            <!-- 胜率统计 -->
            ${stats && stats.total > 0 ? `
            <div class="ai-section" style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); border: 1px solid #334155; border-radius: 8px; padding: 16px; margin-bottom: 16px;">
                <h3 class="ai-section-title" style="margin-bottom: 12px;">📊 历史胜率统计</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px;">
                    <div style="text-align: center;">
                        <div style="font-size: 24px; font-weight: bold; color: ${stats.win_rate >= 60 ? '#10b981' : stats.win_rate >= 50 ? '#f59e0b' : '#ef4444'};">
                            ${stats.win_rate}%
                        </div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">胜率</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 20px; font-weight: bold; color: #60a5fa;">${stats.total}</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">总交易</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 20px; font-weight: bold; color: #10b981;">${stats.win_count}</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">盈利</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 20px; font-weight: bold; color: #ef4444;">${stats.loss_count}</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">亏损</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 18px; font-weight: bold; color: #10b981;">+${stats.avg_profit}%</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">平均收益</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 18px; font-weight: bold; color: #ef4444;">${stats.avg_loss}%</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">平均亏损</div>
                    </div>
                </div>
            </div>
            ` : ''}
            
            <!-- 概览卡片 -->
            <div class="ai-overview">
                <div class="ai-overview-item">
                    <div class="ai-overview-label">信号</div>
                    <div class="ai-overview-value" style="color: ${signalColor}; font-weight: 600;">${signal || '未知'}</div>
                </div>
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
            
            <!-- 交易点位 -->
            ${data.signal === '买入' && data.buy_price ? `
            <div class="ai-section" style="background: linear-gradient(135deg, #065f46 0%, #064e3b 100%); border: 2px solid #10b981; border-radius: 8px; padding: 16px;">
                <h3 class="ai-section-title" style="color: #10b981; margin-bottom: 12px;">💰 AI交易点位</h3>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 12px;">
                    <div style="text-align: center; padding: 12px; background: rgba(16, 185, 129, 0.1); border-radius: 6px;">
                        <div style="font-size: 12px; color: #94a3b8; margin-bottom: 4px;">买入价</div>
                        <div style="font-size: 20px; font-weight: bold; color: #10b981;">¥${data.buy_price.toFixed(2)}</div>
                    </div>
                    <div style="text-align: center; padding: 12px; background: rgba(59, 130, 246, 0.1); border-radius: 6px;">
                        <div style="font-size: 12px; color: #94a3b8; margin-bottom: 4px;">止盈价</div>
                        <div style="font-size: 20px; font-weight: bold; color: #3b82f6;">¥${data.sell_price.toFixed(2)}</div>
                    </div>
                    <div style="text-align: center; padding: 12px; background: rgba(239, 68, 68, 0.1); border-radius: 6px;">
                        <div style="font-size: 12px; color: #94a3b8; margin-bottom: 4px;">止损价</div>
                        <div style="font-size: 20px; font-weight: bold; color: #ef4444;">¥${data.stop_loss.toFixed(2)}</div>
                    </div>
                </div>
                ${data.reason ? `
                <div style="font-size: 13px; color: #94a3b8; padding: 8px; background: rgba(0, 0, 0, 0.2); border-radius: 4px;">
                    💡 ${data.reason}
                </div>
                ` : ''}
                ${planId ? `
                <div style="margin-top: 8px; font-size: 12px; color: #10b981;">
                    ✅ 交易计划已创建 (ID: ${planId})
                </div>
                ` : ''}
            </div>
            ` : data.signal && data.signal !== '买入' ? `
            <div class="ai-section" style="background: rgba(148, 163, 184, 0.1); border: 1px solid #334155; border-radius: 8px; padding: 16px;">
                <h3 class="ai-section-title">💡 交易建议</h3>
                <div style="color: #94a3b8; font-size: 14px;">
                    ${data.reason || '当前信号不明确，暂不给出交易点位'}
                </div>
            </div>
            ` : ''}
            
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

async function renderAIAnalysis(data, code, name) {
    const container = document.getElementById('ai-analysis-result');
    
    // 加载该股票的胜率统计
    let stats = null;
    try {
        const res = await apiFetch(`${API_BASE}/api/trading/statistics?code=${code}`);
        if (res.ok) {
            const result = await res.json();
            if (result.code === 0 && result.data) {
                stats = result.data;
            }
        }
    } catch (e) {
        console.warn(`获取${code}统计失败:`, e);
    }
    
    container.innerHTML = buildAIAnalysisHtml(data, code, name, null, stats);
}

async function renderAIAnalysisBatch(items) {
    const container = document.getElementById('ai-analysis-result');
    
    if (!items || items.length === 0) {
        container.innerHTML = `
            <div class="ai-placeholder">
                <div style="font-size: 48px; margin-bottom: 16px;">🤖</div>
                <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">AI股票分析</div>
                <div style="font-size: 14px; color: #64748b;">没有可分析的自选股票</div>
            </div>
        `;
        return;
    }

    let successItems = items.filter(item => item && item.success && item.analysis);
    const failedItems = items.filter(item => !item || !item.success || !item.analysis);

    // 根据评分从高到低排序，只展示前50条，避免页面太长
    successItems = successItems
        .map(item => ({
            ...item,
            _score: (item.analysis && typeof item.analysis.score === 'number') ? item.analysis.score : 0,
        }))
        .sort((a, b) => b._score - a._score)
        .slice(0, 50);

    // 加载每只股票的胜率统计
    const statsMap = {};
    for (const item of successItems) {
        try {
            const res = await apiFetch(`${API_BASE}/api/trading/statistics?code=${item.code}`);
            if (res.ok) {
                const data = await res.json();
                if (data.code === 0 && data.data) {
                    statsMap[item.code] = data.data;
                }
            }
        } catch (e) {
            console.warn(`获取${item.code}统计失败:`, e);
        }
    }

    // 生成表格行HTML
    const tableRows = successItems.map(item => {
        const analysis = item.analysis;
        const stats = statsMap[item.code];
        
        const signal = analysis.signal || '未知';
        const signalColor = {
            '买入': '#10b981',
            '关注': '#3b82f6',
            '观望': '#f59e0b',
            '回避': '#ef4444'
        }[signal] || '#94a3b8';
        
        const trendColor = {
            '上涨': '#10b981',
            '下跌': '#ef4444',
            '震荡': '#f59e0b',
            '未知': '#94a3b8'
        }[analysis.trend] || '#94a3b8';
        
        const riskColor = {
            '低': '#10b981',
            '中': '#f59e0b',
            '高': '#ef4444',
            '未知': '#94a3b8'
        }[analysis.risk] || '#94a3b8';
        
        const scoreColor = analysis.score >= 0 ? '#10b981' : '#ef4444';
        const confidenceColor = (analysis.confidence || 0) >= 70 ? '#10b981' : (analysis.confidence || 0) >= 50 ? '#f59e0b' : '#ef4444';
        
        // 胜率显示
        const winRateHtml = stats && stats.total > 0 
            ? `<span style="color: ${stats.win_rate >= 60 ? '#10b981' : stats.win_rate >= 50 ? '#f59e0b' : '#ef4444'}; font-weight: 600;">${stats.win_rate}%</span>`
            : '<span style="color: #94a3b8;">-</span>';
        
        // 交易点位显示
        const tradingPointsHtml = analysis.signal === '买入' && analysis.buy_price
            ? `
                <div style="display: flex; flex-direction: column; gap: 2px; font-size: 11px;">
                    <div><span style="color: #94a3b8;">买:</span> <span style="color: #10b981; font-weight: 600;">¥${analysis.buy_price.toFixed(2)}</span></div>
                    <div><span style="color: #94a3b8;">盈:</span> <span style="color: #3b82f6; font-weight: 600;">¥${analysis.sell_price.toFixed(2)}</span></div>
                    <div><span style="color: #94a3b8;">损:</span> <span style="color: #ef4444; font-weight: 600;">¥${analysis.stop_loss.toFixed(2)}</span></div>
                </div>
            `
            : '<span style="color: #94a3b8;">-</span>';
        
        // 交易理由
        const reasonHtml = analysis.reason 
            ? `<div style="font-size: 12px; color: #cbd5f5; line-height: 1.4; max-width: 180px;">${analysis.reason}</div>`
            : '<span style="color: #94a3b8;">-</span>';
        
        return `
            <tr style="border-bottom: 1px solid #334155;">
                <td style="padding: 8px;">
                    <div style="font-weight: 600; color: #60a5fa; font-size: 13px;">${item.code}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-top: 2px;">${item.name || '-'}</div>
                </td>
                <td style="padding: 8px; text-align: center;">
                    <span style="color: ${signalColor}; font-weight: 600; font-size: 13px;">${signal}</span>
                </td>
                <td style="padding: 8px; text-align: center;">
                    <span style="color: ${trendColor}; font-size: 12px;">${analysis.trend || '未知'}</span>
                </td>
                <td style="padding: 8px; text-align: center;">
                    <span style="color: ${riskColor}; font-size: 12px;">${analysis.risk || '未知'}</span>
                </td>
                <td style="padding: 8px; text-align: center;">
                    <span style="color: ${scoreColor}; font-weight: 600; font-size: 13px;">${analysis.score || 0}</span>
                </td>
                <td style="padding: 8px; text-align: center;">
                    <span style="color: ${confidenceColor}; font-size: 12px;">${analysis.confidence || 0}%</span>
                </td>
                <td style="padding: 8px; text-align: center;">
                    ${tradingPointsHtml}
                </td>
                <td style="padding: 8px; text-align: center;">
                    ${winRateHtml}
                </td>
                <td style="padding: 8px;">
                    <div style="font-size: 12px; color: #cbd5f5; line-height: 1.4; max-width: 200px; word-wrap: break-word;">
                        ${analysis.advice || '暂无建议'}
                    </div>
                </td>
                <td style="padding: 8px;">
                    ${reasonHtml}
                </td>
            </tr>
        `;
    }).join('');

    const failedHtml = failedItems.length
        ? `
        <tr style="background: rgba(239, 68, 68, 0.1);">
            <td colspan="10" style="padding: 8px; color: #ef4444; font-weight: 600;">
                ⚠️ 分析失败的股票 (${failedItems.length}只)
            </td>
        </tr>
        ${failedItems.map(item => `
            <tr style="border-bottom: 1px solid #334155;">
                <td style="padding: 8px;">
                    <span style="color: #e5e7eb;">${item.code || '-'}</span>
                    ${item.name ? ` <span style="color: #94a3b8;">（${item.name}）</span>` : ''}
                </td>
                <td colspan="9" style="padding: 8px; color: #ef4444;">
                    ${item.message || '分析失败'}
                </td>
            </tr>
        `).join('')}
        `
        : '';

    container.innerHTML = `
        <div style="background: #1e293b; border-radius: 8px; overflow: hidden; border: 1px solid #334155;">
            <div style="padding: 16px; border-bottom: 1px solid #334155;">
                <h3 style="margin: 0; color: #60a5fa; font-size: 18px; font-weight: 600;">📊 AI分析结果 (${successItems.length}只股票)</h3>
            </div>
            <div style="overflow-x: auto;">
                <table class="ai-analysis-table">
                    <thead>
                        <tr>
                            <th style="text-align: left; padding: 8px;">代码/名称</th>
                            <th style="text-align: center; padding: 8px;">信号</th>
                            <th style="text-align: center; padding: 8px;">趋势</th>
                            <th style="text-align: center; padding: 8px;">风险</th>
                            <th style="text-align: center; padding: 8px;">评分</th>
                            <th style="text-align: center; padding: 8px;">置信度</th>
                            <th style="text-align: center; padding: 8px;">交易点位</th>
                            <th style="text-align: center; padding: 8px;">胜率</th>
                            <th style="text-align: left; padding: 8px;">操作建议</th>
                            <th style="text-align: left; padding: 8px;">交易理由</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                        ${failedHtml}
                    </tbody>
                </table>
            </div>
        </div>
    `;
}

// 从服务端加载历史AI分析结果
async function loadAIAnalysisHistory() {
    const container = document.getElementById('ai-analysis-result');
    if (!container) return;
    try {
        const res = await apiFetch(`${API_BASE}/api/ai/analysis`);
        if (!res.ok) return;
        const data = await res.json();
        if (data.code === 0 && Array.isArray(data.data) && data.data.length > 0) {
            await renderAIAnalysisBatch(data.data);
        }
    } catch (e) {
        console.warn('加载历史AI分析结果失败:', e);
    }
}

// 清除所有AI分析结果
async function clearAIAnalysis() {
    // 需要确认
    const ok = window.confirm('确认清除所有已保存的 AI 分析结果吗？此操作不可恢复。');
    if (!ok) return;

    const container = document.getElementById('ai-analysis-result');
    try {
        const res = await apiFetch(`${API_BASE}/api/ai/analysis/clear`, {
            method: 'POST',
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data.code === 0) {
            if (container) {
                container.innerHTML = `
                    <div class="ai-placeholder">
                        <div style="font-size: 48px; margin-bottom: 16px;">🤖</div>
                        <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">AI股票分析</div>
                        <div style="font-size: 14px; color: #64748b;">AI分析结果已清除，输入股票代码或点击开始分析重新生成</div>
                    </div>
                `;
            }
            showToast('AI分析结果已清除', 'success');
        } else {
            showToast(`清除AI分析结果失败：${data.message || '未知错误'}`, 'error');
        }
    } catch (e) {
        console.error('清除AI分析结果失败:', e);
        showToast(`清除AI分析结果失败：${e.message}`, 'error');
    }
}

// 加载股票胜率统计
async function loadStockStatistics() {
    const container = document.getElementById('ai-analysis-result');
    if (!container) return;
    
    container.innerHTML = `
        <div class="ai-loading">
            <div class="ai-loading-spinner"></div>
            <div style="margin-top: 16px; color: #94a3b8;">正在加载胜率统计...</div>
        </div>
    `;
    
    try {
        const res = await apiFetch(`${API_BASE}/api/trading/statistics/stocks`);
        if (!res.ok) {
            throw new Error('获取胜率统计失败');
        }
        
        const data = await res.json();
        if (data.code === 0 && Array.isArray(data.data)) {
            renderStockStatistics(data.data);
        } else {
            const backButtonId = 'back-to-analysis-btn-error';
            container.innerHTML = `
                <div style="position: relative; background: #1e293b; border-radius: 8px; padding: 24px; border: 1px solid #334155;">
                    <button id="${backButtonId}" style="
                        position: absolute;
                        top: 16px;
                        right: 16px;
                        padding: 8px 16px;
                        background: #3b82f6;
                        color: white;
                        border: none;
                        border-radius: 6px;
                        cursor: pointer;
                        font-size: 14px;
                        font-weight: 500;
                        transition: background 0.2s;
                        z-index: 10;
                    ">← 返回AI分析</button>
                    <div class="ai-error">
                        <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                        <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">加载失败</div>
                        <div style="font-size: 14px; color: #94a3b8;">${data.message || '无法获取胜率统计'}</div>
                    </div>
                </div>
            `;
            setTimeout(() => {
                const btn = document.getElementById(backButtonId);
                if (btn) {
                    btn.addEventListener('click', loadAIAnalysisHistory);
                    btn.addEventListener('mouseenter', () => btn.style.background = '#2563eb');
                    btn.addEventListener('mouseleave', () => btn.style.background = '#3b82f6');
                }
            }, 0);
        }
    } catch (e) {
        console.error('加载胜率统计失败:', e);
        const backButtonId = 'back-to-analysis-btn-error';
        container.innerHTML = `
            <div style="position: relative; background: #1e293b; border-radius: 8px; padding: 24px; border: 1px solid #334155;">
                <button id="${backButtonId}" style="
                    position: absolute;
                    top: 16px;
                    right: 16px;
                    padding: 8px 16px;
                    background: #3b82f6;
                    color: white;
                    border: none;
                    border-radius: 6px;
                    cursor: pointer;
                    font-size: 14px;
                    font-weight: 500;
                    transition: background 0.2s;
                    z-index: 10;
                ">← 返回AI分析</button>
                <div class="ai-error">
                    <div style="font-size: 48px; margin-bottom: 16px;">⚠️</div>
                    <div style="font-size: 18px; color: #ef4444; margin-bottom: 8px;">加载失败</div>
                    <div style="font-size: 14px; color: #94a3b8;">${e.message}</div>
                </div>
            </div>
        `;
        setTimeout(() => {
            const btn = document.getElementById(backButtonId);
            if (btn) {
                btn.addEventListener('click', loadAIAnalysisHistory);
                btn.addEventListener('mouseenter', () => btn.style.background = '#2563eb');
                btn.addEventListener('mouseleave', () => btn.style.background = '#3b82f6');
            }
        }, 0);
    }
}

// 渲染股票胜率统计
function renderStockStatistics(stats, sortBy = 'win_rate') {
    const container = document.getElementById('ai-analysis-result');
    
    // 排序逻辑
    let sortedStats = [...stats];
    if (sortBy === 'time') {
        // 按时间排序（最近的在前面）
        sortedStats.sort((a, b) => {
            const timeA = a.latest_buy_time || '';
            const timeB = b.latest_buy_time || '';
            return timeB.localeCompare(timeA); // 降序：最近的在前面
        });
    } else if (sortBy === 'win_rate') {
        // 按胜率排序（从高到低）
        sortedStats.sort((a, b) => {
            if (b.win_rate !== a.win_rate) {
                return b.win_rate - a.win_rate;
            }
            return b.total - a.total; // 胜率相同时按总交易数降序
        });
    }
    
    // 格式化时间显示
    const formatTime = (timeStr) => {
        if (!timeStr) return '-';
        try {
            const date = new Date(timeStr);
            return date.toLocaleString('zh-CN', { 
                year: 'numeric', 
                month: '2-digit', 
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            });
        } catch (e) {
            return timeStr;
        }
    };
    
    // 返回按钮和排序选择框HTML
    const backButtonId = 'back-to-analysis-btn';
    const sortSelectId = 'stats-sort-select';
    const controlsHtml = `
        <div style="position: absolute; top: 16px; right: 16px; display: flex; align-items: center; gap: 12px; z-index: 10;">
            <select id="${sortSelectId}" style="
                padding: 6px 10px;
                background: #1e293b;
                color: #e5e7eb;
                border: 1px solid #334155;
                border-radius: 4px;
                font-size: 13px;
                cursor: pointer;
                outline: none;
            ">
                <option value="win_rate" ${sortBy === 'win_rate' ? 'selected' : ''}>胜率排序</option>
                <option value="time" ${sortBy === 'time' ? 'selected' : ''}>时间排序</option>
            </select>
            <button id="${backButtonId}" style="
                padding: 8px 16px;
                background: #3b82f6;
                color: white;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 500;
                transition: background 0.2s;
            ">
                ← 返回AI分析
            </button>
        </div>
    `;
    
    if (!sortedStats || sortedStats.length === 0) {
        container.innerHTML = `
            <div style="position: relative; background: #1e293b; border-radius: 8px; padding: 24px; border: 1px solid #334155;">
                ${controlsHtml}
                <div class="ai-placeholder" style="padding-top: 0;">
                    <div style="font-size: 48px; margin-bottom: 16px;">📊</div>
                    <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">胜率统计</div>
                    <div style="font-size: 14px; color: #64748b;">暂无交易记录</div>
                </div>
            </div>
        `;
        
        // 绑定事件
        setTimeout(() => {
            const btn = document.getElementById(backButtonId);
            const select = document.getElementById(sortSelectId);
            if (btn) {
                btn.addEventListener('click', loadAIAnalysisHistory);
                btn.addEventListener('mouseenter', () => btn.style.background = '#2563eb');
                btn.addEventListener('mouseleave', () => btn.style.background = '#3b82f6');
            }
            if (select) {
                select.addEventListener('change', (e) => {
                    renderStockStatistics(stats, e.target.value);
                });
            }
        }, 0);
        return;
    }
    
    // 生成表格行HTML
    const tableRows = sortedStats.map(s => {
        const winRateColor = s.win_rate >= 60 ? '#10b981' : s.win_rate >= 50 ? '#f59e0b' : '#ef4444';
        
        return `
            <tr style="border-bottom: 1px solid #334155;">
                <td style="padding: 10px;">
                    <div style="font-weight: 600; color: #60a5fa; font-size: 14px;">${s.code}</div>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: ${winRateColor}; font-weight: 600; font-size: 14px;">${s.win_rate}%</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #60a5fa; font-size: 13px;">${s.total}</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #10b981; font-size: 13px;">${s.win_count}</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #ef4444; font-size: 13px;">${s.loss_count}</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: ${s.avg_profit >= 0 ? '#10b981' : '#ef4444'}; font-size: 13px;">
                        ${s.avg_profit >= 0 ? '+' : ''}${s.avg_profit}%
                    </span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #ef4444; font-size: 13px;">${s.avg_loss}%</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #10b981; font-size: 13px;">+${s.max_profit}%</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #ef4444; font-size: 13px;">${s.max_loss}%</span>
                </td>
                <td style="padding: 10px; text-align: center;">
                    <span style="color: #94a3b8; font-size: 12px;">${formatTime(s.latest_buy_time)}</span>
                </td>
            </tr>
        `;
    }).join('');
    
    const html = `
        <div style="position: relative; background: #1e293b; border-radius: 8px; padding: 24px; border: 1px solid #334155;">
            ${controlsHtml}
            <div style="margin-bottom: 16px; padding-right: 280px;">
                <h2 style="font-size: 20px; color: #e5e7eb; margin-bottom: 16px; margin-top: 0;">📊 AI交易胜率统计</h2>
                <div style="font-size: 13px; color: #94a3b8; margin-bottom: 16px;">
                    共 ${sortedStats.length} 只股票有交易记录
                </div>
            </div>
            <div style="overflow-x: auto;">
                <table class="ai-analysis-table" style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="border-bottom: 2px solid #334155;">
                            <th style="text-align: left; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">股票代码</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">胜率</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">总交易</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">盈利</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">亏损</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">平均收益</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">平均亏损</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">最大盈利</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">最大亏损</th>
                            <th style="text-align: center; padding: 10px; color: #cbd5f5; font-weight: 600; font-size: 13px;">最近买入</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                </table>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
    
    // 绑定事件
    setTimeout(() => {
        const btn = document.getElementById(backButtonId);
        const select = document.getElementById(sortSelectId);
        if (btn) {
            btn.addEventListener('click', loadAIAnalysisHistory);
            btn.addEventListener('mouseenter', () => btn.style.background = '#2563eb');
            btn.addEventListener('mouseleave', () => btn.style.background = '#3b82f6');
        }
        if (select) {
            select.addEventListener('change', (e) => {
                renderStockStatistics(stats, e.target.value);
            });
        }
    }, 0);
}

// 每日自动分析自选股
let autoAnalyzeTimer = null;

function initAutoAnalyzeScheduler() {
    // 避免重复初始化
    if (autoAnalyzeTimer) {
        clearInterval(autoAnalyzeTimer);
        autoAnalyzeTimer = null;
    }

    // 先加载一次配置，再启动定时器
    scheduleAutoAnalyze();
    autoAnalyzeTimer = setInterval(scheduleAutoAnalyze, 60 * 1000); // 每分钟检查一次
}

async function scheduleAutoAnalyze() {
    try {
        const res = await apiFetch(`${API_BASE}/api/config`);
        if (!res.ok) return;
        const cfg = await res.json();
        const timeStr = cfg.ai_auto_analyze_time;

        if (!timeStr) {
            return; // 未配置自动分析时间
        }

        const [cfgHour, cfgMinute] = timeStr.split(':').map(v => parseInt(v, 10));
        if (
            !Number.isInteger(cfgHour) ||
            !Number.isInteger(cfgMinute) ||
            cfgHour < 0 || cfgHour > 23 ||
            cfgMinute < 0 || cfgMinute > 59
        ) {
            console.warn('AI 自动分析时间配置格式不正确，应为 HH:MM');
            return;
        }

        const now = new Date();
        const curHour = now.getHours();
        const curMinute = now.getMinutes();

        // 只在精确到分钟匹配时触发
        if (curHour !== cfgHour || curMinute !== cfgMinute) {
            return;
        }

        const todayKey = now.toISOString().slice(0, 10); // YYYY-MM-DD
        const lastRunDate = localStorage.getItem('aiAutoAnalyzeLastDate');
        if (lastRunDate === todayKey) {
            // 今天已经自动分析过了，避免重复触发
            return;
        }

        const watchlist = getWatchlist();
        if (!watchlist || watchlist.length === 0) {
            console.info('自动分析：自选列表为空，跳过本次分析');
            localStorage.setItem('aiAutoAnalyzeLastDate', todayKey);
            return;
        }

        const codes = watchlist.map(s => String(s.code).trim()).filter(c => c);
        if (!codes.length) {
            console.info('自动分析：自选列表中没有有效的股票代码，跳过本次分析');
            localStorage.setItem('aiAutoAnalyzeLastDate', todayKey);
            return;
        }

        console.info(`自动分析：开始分析自选的 ${codes.length} 只股票`);
        localStorage.setItem('aiAutoAnalyzeLastDate', todayKey);
        // 自动分析启用通知（后端会根据 AI 通知配置决定实际渠道）
        analyzeStock(codes, { notify: true });
    } catch (e) {
        console.warn('自动分析调度失败:', e);
    }
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
    
    // 转义HTML特殊字符，避免XSS
    const escapeHtml = (text) => {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    };
    
    // 显示所有资讯（不限制数量），内容长度增加到500字符
    container.innerHTML = newsList.map((news, index) => {
        const content = news.content || '';
        // 如果内容超过500字符，显示前500字符并提供展开功能
        const shouldTruncate = content.length > 500;
        const displayContent = shouldTruncate ? content.substring(0, 500) : content;
        const contentId = `news-content-${index}`;
        const btnId = `news-expand-btn-${index}`;
        
        return `
        <div class="news-item">
            <h4>${escapeHtml(news.title || '-')}</h4>
            <div class="news-content" id="${contentId}">${escapeHtml(displayContent)}${shouldTruncate ? '...' : ''}</div>
            ${shouldTruncate ? `<button class="news-expand-btn" id="${btnId}" data-full-content="${escapeHtml(content)}">展开全文</button>` : ''}
            <div class="meta">
                ${escapeHtml(news.publish_time || news.collect_time || '-')} | ${escapeHtml(news.source || '未知来源')}
            </div>
        </div>
        `;
    }).join('');
    
    // 绑定展开按钮事件
    container.querySelectorAll('.news-expand-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const contentId = this.id.replace('news-expand-btn-', 'news-content-');
            const contentDiv = document.getElementById(contentId);
            const fullContent = this.getAttribute('data-full-content');
            if (contentDiv && fullContent) {
                contentDiv.textContent = fullContent;
                this.remove();
            }
        });
    });
}

// 全局函数
window.loadChart = loadChart;

// 配置模块
function initConfig() {
    const saveBtn = document.getElementById('cfg-save-btn');
    if (!saveBtn) return;

    saveBtn.addEventListener('click', saveConfig);
    loadConfig();

    const testBtn = document.getElementById('cfg-notify-test-btn');
    if (testBtn) {
        testBtn.addEventListener('click', testNotifyChannels);
    }
    
    // 绑定修改密码按钮
    const changePasswordBtn = document.getElementById('cfg-change-password-btn');
    if (changePasswordBtn) {
        changePasswordBtn.addEventListener('click', changePassword);
    }
}

async function loadConfig() {
    const statusEl = document.getElementById('cfg-status');
    try {
        const res = await apiFetch(`${API_BASE}/api/config`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        document.getElementById('cfg-selection-market').value = data.selection_market ?? 'A';
        document.getElementById('cfg-selection-max-count').value = data.selection_max_count ?? 30;
        document.getElementById('cfg-collector-interval').value = data.collector_interval_seconds ?? 60;
        document.getElementById('cfg-kline-years').value = data.kline_years ?? 1;
        
        // 筛选策略配置
        document.getElementById('cfg-filter-volume-ratio-min').value = data.filter_volume_ratio_min ?? 1.2;
        document.getElementById('cfg-filter-volume-ratio-max').value = data.filter_volume_ratio_max ?? 5.0;
        document.getElementById('cfg-filter-rsi-min').value = data.filter_rsi_min ?? 40;
        document.getElementById('cfg-filter-rsi-max').value = data.filter_rsi_max ?? 65;
        document.getElementById('cfg-filter-williams-r-enable').checked = data.filter_williams_r_enable !== false;
        document.getElementById('cfg-filter-break-high-enable').checked = data.filter_break_high_enable !== false;
        document.getElementById('cfg-filter-boll-enable').checked = data.filter_boll_enable !== false;
        
        // AI 配置（API Key 不回显，只在服务端保存）
        document.getElementById('cfg-ai-api-key').value = '';
        document.getElementById('cfg-ai-api-base').value = data.openai_api_base || 'https://openai.qiniu.com/v1';
        document.getElementById('cfg-ai-model').value = data.openai_model || 'deepseek/deepseek-v3.2-251201';

        // AI 通知渠道开关
        const aiNotifyTelegram = data.ai_notify_telegram === true;
        const aiNotifyEmail = data.ai_notify_email === true;
        const aiNotifyWechat = data.ai_notify_wechat === true;
        const aiAutoTime = data.ai_auto_analyze_time || '';
        const aiDataPeriod = data.ai_data_period || 'daily';
        const aiDataCount = data.ai_data_count || 500;
        const aiBatchSize = data.ai_batch_size || 5;

        const aiNotifyTelegramEl = document.getElementById('cfg-ai-notify-telegram');
        const aiNotifyEmailEl = document.getElementById('cfg-ai-notify-email');
        const aiNotifyWechatEl = document.getElementById('cfg-ai-notify-wechat');
        const aiAutoTimeEl = document.getElementById('cfg-ai-auto-analyze-time');
        const aiDataPeriodDailyEl = document.getElementById('cfg-ai-data-period-daily');
        const aiDataPeriodHourlyEl = document.getElementById('cfg-ai-data-period-hourly');
        const aiDataCountEl = document.getElementById('cfg-ai-data-count');
        const aiBatchSizeEl = document.getElementById('cfg-ai-batch-size');

        if (aiNotifyTelegramEl) aiNotifyTelegramEl.checked = aiNotifyTelegram;
        if (aiNotifyEmailEl) aiNotifyEmailEl.checked = aiNotifyEmail;
        if (aiNotifyWechatEl) aiNotifyWechatEl.checked = aiNotifyWechat;
        if (aiAutoTimeEl) aiAutoTimeEl.value = aiAutoTime;
        if (aiDataPeriodDailyEl) aiDataPeriodDailyEl.checked = aiDataPeriod === 'daily';
        if (aiDataPeriodHourlyEl) aiDataPeriodHourlyEl.checked = aiDataPeriod === '1h';
        if (aiDataCountEl) aiDataCountEl.value = aiDataCount;
        if (aiBatchSizeEl) aiBatchSizeEl.value = aiBatchSize;

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
        const maxCountInput = document.getElementById('max-count-input');
        const marketSelect = document.getElementById('selection-market-select');
        if (marketSelect) marketSelect.value = data.selection_market ?? 'A';
        if (maxCountInput) maxCountInput.value = data.selection_max_count ?? 30;

        if (statusEl) statusEl.textContent = '配置已从服务器加载。';
    } catch (error) {
        console.error('加载配置失败:', error);
        if (statusEl) statusEl.textContent = `加载配置失败: ${error.message}`;
    }
}

async function saveConfig() {
    const statusEl = document.getElementById('cfg-status');
    const selectionMarket = document.getElementById('cfg-selection-market').value;
    const maxCount = parseInt(document.getElementById('cfg-selection-max-count').value);
    const interval = parseInt(document.getElementById('cfg-collector-interval').value);
    const klineYears = parseFloat(document.getElementById('cfg-kline-years').value);
    
    // 筛选策略配置
    const filterVolumeRatioMin = parseFloat(document.getElementById('cfg-filter-volume-ratio-min').value);
    const filterVolumeRatioMax = parseFloat(document.getElementById('cfg-filter-volume-ratio-max').value);
    const filterRsiMin = parseInt(document.getElementById('cfg-filter-rsi-min').value);
    const filterRsiMax = parseInt(document.getElementById('cfg-filter-rsi-max').value);
    const filterWilliamsREnable = document.getElementById('cfg-filter-williams-r-enable').checked;
    const filterBreakHighEnable = document.getElementById('cfg-filter-break-high-enable').checked;
    const filterBollEnable = document.getElementById('cfg-filter-boll-enable').checked;

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
                selection_market: selectionMarket,
                selection_max_count: maxCount,
                filter_volume_ratio_min: filterVolumeRatioMin,
                filter_volume_ratio_max: filterVolumeRatioMax,
                filter_rsi_min: filterRsiMin,
                filter_rsi_max: filterRsiMax,
                filter_williams_r_enable: filterWilliamsREnable,
                filter_break_high_enable: filterBreakHighEnable,
                filter_boll_enable: filterBollEnable,
                collector_interval_seconds: interval,
                kline_years: klineYears,
                // AI 配置
                openai_api_key: document.getElementById('cfg-ai-api-key').value.trim() || null,
                openai_api_base: document.getElementById('cfg-ai-api-base').value.trim() || null,
                openai_model: document.getElementById('cfg-ai-model').value.trim() || null,
                ai_auto_analyze_time: document.getElementById('cfg-ai-auto-analyze-time').value.trim() || null,
                ai_data_period: document.querySelector('input[name="cfg-ai-data-period"]:checked')?.value || 'daily',
                ai_data_count: parseInt(document.getElementById('cfg-ai-data-count').value) || 500,
                ai_batch_size: parseInt(document.getElementById('cfg-ai-batch-size').value) || 5,
                ai_notify_telegram: document.getElementById('cfg-ai-notify-telegram').checked,
                ai_notify_email: document.getElementById('cfg-ai-notify-email').checked,
                ai_notify_wechat: document.getElementById('cfg-ai-notify-wechat').checked,
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
        const maxCountInput = document.getElementById('max-count-input');
        const marketSelect = document.getElementById('selection-market-select');
        if (marketSelect) marketSelect.value = data.selection_market ?? 'A';
        if (maxCountInput) maxCountInput.value = data.selection_max_count ?? maxCount;

        if (statusEl) statusEl.textContent = '配置已保存。若修改了采集间隔，新设置会在下一轮采集后生效。';
        showToast('配置已保存', 'success');
    } catch (error) {
        console.error('保存配置失败:', error);
        if (statusEl) statusEl.textContent = `保存配置失败: ${error.message}`;
        showToast(`保存配置失败: ${error.message}`, 'error');
    }
}

// 修改密码
async function changePassword() {
    const oldPassword = document.getElementById('cfg-old-password').value.trim();
    const newPassword = document.getElementById('cfg-new-password').value.trim();
    const confirmPassword = document.getElementById('cfg-confirm-password').value.trim();
    const statusEl = document.getElementById('cfg-password-status');
    
    // 验证输入
    if (!oldPassword) {
        if (statusEl) statusEl.textContent = '请输入当前密码';
        showToast('请输入当前密码', 'error');
        return;
    }
    
    if (!newPassword) {
        if (statusEl) statusEl.textContent = '请输入新密码';
        showToast('请输入新密码', 'error');
        return;
    }
    
    if (newPassword.length < 6) {
        if (statusEl) statusEl.textContent = '新密码至少需要6个字符';
        showToast('新密码至少需要6个字符', 'error');
        return;
    }
    
    if (newPassword !== confirmPassword) {
        if (statusEl) statusEl.textContent = '两次输入的新密码不一致';
        showToast('两次输入的新密码不一致', 'error');
        return;
    }
    
    try {
        if (statusEl) statusEl.textContent = '修改中...';
        
        const res = await apiFetch(`${API_BASE}/api/auth/change-password`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                old_password: oldPassword,
                new_password: newPassword
            }),
        });
        
        if (!res.ok) {
            const errText = await res.text();
            let errorMsg = errText || `HTTP ${res.status}`;
            try {
                const errJson = JSON.parse(errText);
                errorMsg = errJson.detail || errorMsg;
            } catch (e) {
                // 不是JSON格式，使用原始文本
            }
            throw new Error(errorMsg);
        }
        
        const data = await res.json();
        
        if (data.success) {
            if (statusEl) statusEl.textContent = '密码修改成功';
            showToast('密码修改成功', 'success');
            // 清空输入框
            document.getElementById('cfg-old-password').value = '';
            document.getElementById('cfg-new-password').value = '';
            document.getElementById('cfg-confirm-password').value = '';
        } else {
            throw new Error(data.message || '密码修改失败');
        }
    } catch (error) {
        console.error('修改密码失败:', error);
        if (statusEl) statusEl.textContent = `修改失败: ${error.message}`;
        showToast(`修改密码失败: ${error.message}`, 'error');
    }
}

// 测试通知渠道（根据勾选的通知渠道发送一条测试消息）
async function testNotifyChannels() {
    const telegramChecked = document.getElementById('cfg-notify-telegram')?.checked;
    const emailChecked = document.getElementById('cfg-notify-email')?.checked;
    const wechatChecked = document.getElementById('cfg-notify-wechat')?.checked;

    const channels = [];
    if (telegramChecked) channels.push('telegram');
    if (emailChecked) channels.push('email');
    if (wechatChecked) channels.push('wechat');

    if (channels.length === 0) {
        alert('请先在通知配置中勾选至少一个渠道（Telegram / 邮箱 / 企业微信）。');
        return;
    }

    try {
        const res = await apiFetch(`${API_BASE}/api/notify/test`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ channels }),
        });

        if (!res.ok) {
            const text = await res.text();
            throw new Error(text || `HTTP ${res.status}`);
        }

        const data = await res.json();
        if (data.code !== 0) {
            showToast(`测试通知失败：${data.message || '未知错误'}`, 'error');
            return;
        }

        const results = data.data || {};
        const parts = [];
        ['telegram', 'email', 'wechat'].forEach((ch) => {
            if (channels.includes(ch)) {
                const ok = results[ch];
                parts.push(`${ch}: ${ok ? '成功' : '失败'}`);
            }
        });

        showToast(`测试通知已发送：${parts.join('，') || '无结果返回'}`, 'success');
    } catch (e) {
        console.error('测试通知失败:', e);
        showToast(`测试通知失败：${e.message}`, 'error');
    }
}

// 全局悬浮提示框
let toastTimer = null;
function showToast(message, type = 'info') {
    const toast = document.getElementById('global-toast');
    if (!toast) return;
    
    // 清除之前的类型类
    toast.classList.remove('success', 'error', 'info');
    
    // 根据类型添加相应的类
    if (type === 'success') {
        toast.classList.add('success');
    } else if (type === 'error') {
        toast.classList.add('error');
    }
    
    toast.textContent = message;
    toast.style.display = 'block';
    toast.classList.add('show');
    
    if (toastTimer) {
        clearTimeout(toastTimer);
    }
    toastTimer = setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => {
            toast.style.display = 'none';
            toast.classList.remove('success', 'error', 'info');
        }, 200);
    }, 3000);
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

// 市场状态模块
let marketStatusInterval = null;

function initMarketStatus() {
    console.log('initMarketStatus: 初始化市场状态模块');
    // 立即更新一次
    updateMarketStatus();
    
    // 每10秒更新一次市场状态
    marketStatusInterval = setInterval(updateMarketStatus, 10000);
}

async function updateMarketStatus() {
    console.log('updateMarketStatus: 函数被调用');
    const aStatusEl = document.getElementById('market-status-a');
    const hkStatusEl = document.getElementById('market-status-hk');
    
    console.log('updateMarketStatus: 元素查找结果', { aStatusEl: !!aStatusEl, hkStatusEl: !!hkStatusEl });
    
    if (!aStatusEl || !hkStatusEl) {
        console.warn('市场状态元素未找到，aStatusEl:', aStatusEl, 'hkStatusEl:', hkStatusEl);
        return;
    }
    
    console.log('updateMarketStatus: 开始请求市场状态', { hasToken: !!apiToken });
    try {
        // 设置超时，避免长时间等待（增加到10秒）
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            console.warn('updateMarketStatus: 请求超时，取消请求');
            controller.abort();
        }, 10000); // 10秒超时
        
        console.log('updateMarketStatus: 发送请求到', `${API_BASE}/api/market/status`);
        const res = await apiFetch(`${API_BASE}/api/market/status`, {
            signal: controller.signal
        });
        
        console.log('updateMarketStatus: 收到响应', res.status, res.ok);
        
        clearTimeout(timeoutId);
        
        if (!res.ok) {
            const errorText = await res.text().catch(() => '');
            console.error('获取市场状态失败:', res.status, errorText);
            // 如果是401错误，需要登录或token失效，显示"未登录"
            if (res.status === 401) {
                console.warn('市场状态API需要认证');
                aStatusEl.textContent = '需登录';
                aStatusEl.className = 'market-status-value closed';
                hkStatusEl.textContent = '需登录';
                hkStatusEl.className = 'market-status-value closed';
                return;
            }
            // 其他错误显示"未知"
            aStatusEl.textContent = '未知';
            aStatusEl.className = 'market-status-value closed';
            hkStatusEl.textContent = '未知';
            hkStatusEl.className = 'market-status-value closed';
            return;
        }
        
        const data = await res.json();
        console.log('updateMarketStatus: 响应数据', data);
        if (data.code === 0 && data.data) {
            const aStatus = data.data.a;
            const hkStatus = data.data.hk;
            
            console.log('updateMarketStatus: 更新状态', { aStatus, hkStatus });
            
            // 更新A股状态
            aStatusEl.textContent = aStatus.status || '未知';
            aStatusEl.className = 'market-status-value ' + (aStatus.is_trading ? 'trading' : 'closed');
            
            // 更新港股状态
            hkStatusEl.textContent = hkStatus.status || '未知';
            hkStatusEl.className = 'market-status-value ' + (hkStatus.is_trading ? 'trading' : 'closed');
            
            console.log('updateMarketStatus: 状态更新完成');
        } else {
            // 显示错误状态
            console.error('市场状态数据格式错误:', data);
            aStatusEl.textContent = '未知';
            aStatusEl.className = 'market-status-value closed';
            hkStatusEl.textContent = '未知';
            hkStatusEl.className = 'market-status-value closed';
        }
    } catch (error) {
        console.error('updateMarketStatus: 捕获到错误', error);
        if (error.name === 'AbortError') {
            console.warn('获取市场状态超时');
            // 超时时显示"超时"
            if (aStatusEl) {
                aStatusEl.textContent = '超时';
                aStatusEl.className = 'market-status-value closed';
            }
            if (hkStatusEl) {
                hkStatusEl.textContent = '超时';
                hkStatusEl.className = 'market-status-value closed';
            }
        } else {
            console.error('更新市场状态失败:', error);
            // 显示错误状态
            if (aStatusEl) {
                aStatusEl.textContent = '错误';
                aStatusEl.className = 'market-status-value closed';
            }
            if (hkStatusEl) {
                hkStatusEl.textContent = '错误';
                hkStatusEl.className = 'market-status-value closed';
            }
        }
    } finally {
        console.log('updateMarketStatus: 函数执行完成');
    }
}

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

