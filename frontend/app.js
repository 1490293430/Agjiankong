import { createChart, ColorType } from 'https://unpkg.com/lightweight-charts@4.1.2/dist/lightweight-charts.esm.production.js';

const API_BASE = window.location.origin;
let apiToken = null;
let adminToken = null;
let chart = null;
let candleSeries = null;
let volumeSeries = null;
let ws = null;

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    initAuth();
});

function startApp() {
    initTabs();
    initMarket();
    initChart();
    initStrategy();
    initTrading();
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
        });
    });
}

// 行情模块
async function initMarket() {
    const marketSelect = document.getElementById('market-select');
    const searchInput = document.getElementById('search-input');
    const refreshBtn = document.getElementById('refresh-btn');
    
    refreshBtn.addEventListener('click', loadMarket);
    marketSelect.addEventListener('change', loadMarket);
    searchInput.addEventListener('input', handleSearch);
    
    await loadMarket();
}

async function loadMarket() {
    const market = document.getElementById('market-select').value;
    const tbody = document.getElementById('stock-list');
    tbody.innerHTML = '<tr><td colspan="6" class="loading">加载中...</td></tr>';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/${market}/spot`);
        const result = await response.json();
        
        if (result.code === 0) {
            renderStockList(result.data);
        } else {
            tbody.innerHTML = `<tr><td colspan="6" class="loading">加载失败: ${result.message}</td></tr>`;
        }
    } catch (error) {
        tbody.innerHTML = `<tr><td colspan="6" class="loading">加载失败: ${error.message}</td></tr>`;
    }
}

function renderStockList(stocks) {
    const tbody = document.getElementById('stock-list');
    tbody.innerHTML = stocks.slice(0, 100).map(stock => `
        <tr>
            <td>${stock.code}</td>
            <td>${stock.name}</td>
            <td>${stock.price?.toFixed(2) || '-'}</td>
            <td class="${stock.pct >= 0 ? 'up' : 'down'}">
                ${stock.pct?.toFixed(2) || '-'}%
            </td>
            <td>${formatVolume(stock.volume)}</td>
            <td>
                <button onclick="loadChart('${stock.code}')" style="padding: 4px 8px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">查看K线</button>
            </td>
        </tr>
    `).join('');
}

function formatVolume(vol) {
    if (!vol) return '-';
    if (vol >= 100000000) return (vol / 100000000).toFixed(2) + '亿';
    if (vol >= 10000) return (vol / 10000).toFixed(2) + '万';
    return vol.toString();
}

async function handleSearch() {
    const keyword = document.getElementById('search-input').value;
    if (keyword.length < 2) return;
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/search?keyword=${encodeURIComponent(keyword)}`);
        const result = await response.json();
        
        if (result.code === 0) {
            renderStockList(result.data);
        }
    } catch (error) {
        console.error('搜索失败:', error);
    }
}

// K线图模块
function initChart() {
    document.getElementById('load-chart-btn').addEventListener('click', () => {
        const code = document.getElementById('chart-code-input').value;
        if (code) {
            loadChart(code);
        }
    });
}

async function loadChart(code) {
    const period = document.getElementById('chart-period').value;
    const container = document.getElementById('chart-container');
    
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">加载中...</div>';
    
    try {
        // 获取K线数据
        const response = await apiFetch(`${API_BASE}/api/market/a/kline?code=${code}&period=${period}`);
        const result = await response.json();
        
        if (result.code === 0 && result.data.length > 0) {
            renderChart(result.data);
            
            // 加载技术指标
            loadIndicators(code);
        } else {
            container.innerHTML = '<div style="text-align: center; padding: 40px; color: #ef4444;">无法获取K线数据</div>';
        }
    } catch (error) {
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${error.message}</div>`;
    }
}

function renderChart(data) {
    const container = document.getElementById('chart-container');
    container.innerHTML = '';
    
    chart = createChart(container, {
        width: container.clientWidth,
        height: 600,
        layout: {
            background: { type: ColorType.Solid, color: '#1e293b' },
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
    });
    
    candleSeries = chart.addCandlestickSeries({
        upColor: '#ef4444',
        downColor: '#22c55e',
        borderVisible: false,
        wickUpColor: '#ef4444',
        wickDownColor: '#22c55e',
    });
    
    volumeSeries = chart.addHistogramSeries({
        color: '#3b82f6',
        priceFormat: {
            type: 'volume',
        },
        priceScaleId: '',
        scaleMargins: {
            top: 0.8,
            bottom: 0,
        },
    });
    
    // 转换数据格式
    const candleData = data.map(d => ({
        time: d.date,
        open: parseFloat(d.open),
        high: parseFloat(d.high),
        low: parseFloat(d.low),
        close: parseFloat(d.close),
    }));
    
    const volumeData = data.map(d => ({
        time: d.date,
        value: parseFloat(d.volume || 0),
        color: parseFloat(d.close) >= parseFloat(d.open) ? '#22c55e' : '#ef4444',
    }));
    
    candleSeries.setData(candleData);
    volumeSeries.setData(volumeData);
    
    chart.timeScale().fitContent();
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

function renderIndicators(indicators) {
    const container = document.getElementById('indicators-info');
    container.innerHTML = `
        <div class="indicator-item">
            <div class="label">MA5</div>
            <div class="value">${indicators.ma5?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">MA10</div>
            <div class="value">${indicators.ma10?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">MA20</div>
            <div class="value">${indicators.ma20?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">RSI</div>
            <div class="value">${indicators.rsi?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">MACD DIF</div>
            <div class="value">${indicators.macd_dif?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">MACD DEA</div>
            <div class="value">${indicators.macd_dea?.toFixed(2) || '-'}</div>
        </div>
        <div class="indicator-item">
            <div class="label">成交量比</div>
            <div class="value">${indicators.vol_ratio?.toFixed(2) || '-'}</div>
        </div>
    `;
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

// 交易模块
function initTrading() {
    loadAccountInfo();
    loadPositions();
    
    document.getElementById('submit-order-btn').addEventListener('click', submitOrder);
    document.getElementById('reset-account-btn').addEventListener('click', resetAccount);
}

async function loadAccountInfo() {
    try {
        const response = await apiFetch(`${API_BASE}/api/trading/account`);
        const result = await response.json();
        
        if (result.success) {
            const info = result.data;
            document.getElementById('account-info-content').innerHTML = `
                <div>初始资金: ${info.initial_cash.toFixed(2)} 元</div>
                <div>可用资金: ${info.cash.toFixed(2)} 元</div>
                <div>持仓数量: ${info.position_count} 只</div>
                <div>交易次数: ${info.total_trades} 笔</div>
            `;
        }
    } catch (error) {
        console.error('加载账户信息失败:', error);
    }
}

async function loadPositions() {
    try {
        const response = await apiFetch(`${API_BASE}/api/trading/positions`);
        const result = await response.json();
        
        if (result.success) {
            const data = result.data;
            const container = document.getElementById('positions-content');
            
            if (data.positions.length === 0) {
                container.innerHTML = '<div style="color: #94a3b8;">暂无持仓</div>';
            } else {
                container.innerHTML = `
                    <div style="margin-bottom: 10px;">
                        <div>总资产: ${data.total_asset.toFixed(2)} 元</div>
                        <div>盈亏: ${data.profit >= 0 ? '+' : ''}${data.profit.toFixed(2)} 元 (${data.profit_rate.toFixed(2)}%)</div>
                    </div>
                    ${data.positions.map(pos => `
                        <div style="background: #334155; padding: 10px; border-radius: 4px; margin-bottom: 10px;">
                            <div>${pos.code} - ${pos.qty}手</div>
                            <div style="font-size: 12px; color: #94a3b8;">
                                市值: ${pos.market_value.toFixed(2)} 元
                            </div>
                        </div>
                    `).join('')}
                `;
            }
        }
    } catch (error) {
        console.error('加载持仓失败:', error);
    }
}

async function submitOrder() {
    const code = document.getElementById('order-code').value;
    const action = document.getElementById('order-action').value;
    const price = parseFloat(document.getElementById('order-price').value);
    const qty = parseInt(document.getElementById('order-qty').value);
    
    if (!code || !price || !qty) {
        alert('请填写完整信息');
        return;
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/trading/order`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action, code, price, qty })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('订单提交成功');
            loadAccountInfo();
            loadPositions();
        } else {
            alert(`订单提交失败: ${result.message}`);
        }
    } catch (error) {
        alert(`订单提交失败: ${error.message}`);
    }
}

async function resetAccount() {
    if (!confirm('确定要重置账户吗？这将清空所有持仓和交易记录！')) {
        return;
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/trading/reset`, { method: 'POST' });
        const result = await response.json();
        
        if (result.success) {
            alert('账户已重置');
            loadAccountInfo();
            loadPositions();
        }
    } catch (error) {
        alert(`重置失败: ${error.message}`);
    }
}

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
        document.getElementById('cfg-collector-interval').value = data.collector_interval_seconds ?? 5;

        const channels = data.notify_channels || [];
        document.getElementById('cfg-notify-telegram').checked = channels.includes('telegram');
        document.getElementById('cfg-notify-email').checked = channels.includes('email');
        document.getElementById('cfg-notify-wechat').checked = channels.includes('wechat');

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

    const channels = [];
    if (document.getElementById('cfg-notify-telegram').checked) channels.push('telegram');
    if (document.getElementById('cfg-notify-email').checked) channels.push('email');
    if (document.getElementById('cfg-notify-wechat').checked) channels.push('wechat');

    try {
        if (statusEl) statusEl.textContent = '保存中...';
        const res = await apiFetch(`${API_BASE}/api/config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                selection_threshold: threshold,
                selection_max_count: maxCount,
                collector_interval_seconds: interval,
                notify_channels: channels,
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

// 登录模块
function initAuth() {
    const overlay = document.getElementById('login-overlay');
    const form = document.getElementById('login-form');
    const messageEl = document.getElementById('login-message');

    if (!overlay || !form) {
        // 如果没有登录层，直接初始化应用（兼容老版本）
        startApp();
        return;
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

            overlay.style.display = 'none';
            startApp();
        } catch (error) {
            console.error('登录失败:', error);
            messageEl.textContent = `登录失败：${error.message}`;
        }
    });
}

