console.log('[全局] app.js 开始加载...');

console.log('[全局] ========== app.js 开始加载 ==========');
console.log('[全局] 当前时间:', new Date().toISOString());
console.log('[全局] 页面URL:', window.location.href);

// 筛选项折叠功能
function toggleFilterItem(headerEl) {
    const filterItem = headerEl.closest('.filter-item');
    if (!filterItem || filterItem.classList.contains('filter-item-simple')) return;
    
    // 关闭其他已展开的筛选项
    document.querySelectorAll('.filter-item.expanded').forEach(item => {
        if (item !== filterItem) {
            item.classList.remove('expanded');
        }
    });
    
    // 切换当前项
    filterItem.classList.toggle('expanded');
}
window.toggleFilterItem = toggleFilterItem;

// 点击外部关闭展开的筛选项
document.addEventListener('click', function(e) {
    if (!e.target.closest('.filter-item')) {
        document.querySelectorAll('.filter-item.expanded').forEach(item => {
            item.classList.remove('expanded');
        });
    }
});

// 筛选配置折叠功能 - 必须在文件开头定义，确保HTML onclick可以调用
function toggleSelectionConfig() {
    const section = document.querySelector('.selection-config-section');
    const content = document.getElementById('selection-config-content');
    const arrow = document.getElementById('selection-config-arrow');
    
    if (!content || !arrow) {
        console.error('筛选配置元素未找到');
        return;
    }
    
    if (content.classList.contains('hidden')) {
        // 展开
        content.classList.remove('hidden');
        if (section) section.classList.add('expanded');
        arrow.textContent = '▼';
    } else {
        // 折叠
        content.classList.add('hidden');
        if (section) section.classList.remove('expanded');
        arrow.textContent = '▶';
    }
}
window.toggleSelectionConfig = toggleSelectionConfig;

// 保存选股配置
async function saveSelectionConfig() {
    const btn = document.getElementById('save-selection-config-btn');
    if (btn) {
        btn.disabled = true;
        btn.textContent = '保存中...';
    }
    
    try {
        // 收集配置数据
        const config = {
            selection_max_count: parseInt(document.getElementById('selection-max-count')?.value || '30'),
            filter_rsi_min: parseInt(document.getElementById('filter-rsi-min')?.value || '30'),
            filter_rsi_max: parseInt(document.getElementById('filter-rsi-max')?.value || '75'),
            filter_volume_ratio_min: parseFloat(document.getElementById('filter-volume-ratio-min')?.value || '0.8'),
            filter_volume_ratio_max: parseFloat(document.getElementById('filter-volume-ratio-max')?.value || '8'),
        };
        
        console.log('[选股配置] 保存配置:', config);
        
        const res = await apiFetch(`${API_BASE}/api/config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        
        if (!res.ok) {
            const errData = await res.json().catch(() => ({}));
            throw new Error(errData.detail || `HTTP ${res.status}`);
        }
        
        // 后端返回的是RuntimeConfig对象，不是{code, data}格式
        const data = await res.json();
        if (data.selection_max_count !== undefined) {
            // 返回了配置对象，说明保存成功
            showToast('选股配置保存成功', 'success');
        } else {
            throw new Error('返回数据格式异常');
        }
    } catch (error) {
        console.error('[选股配置] 保存失败:', error);
        showToast('保存失败: ' + error.message, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = '💾 保存配置';
        }
    }
}
window.saveSelectionConfig = saveSelectionConfig;

// 加载选股配置（从服务器读取并填充到表单）
async function loadSelectionConfig() {
    try {
        const res = await apiFetch(`${API_BASE}/api/config`);
        if (!res.ok) return;
        
        const data = await res.json();
        
        // 填充选股配置
        const maxCountEl = document.getElementById('selection-max-count');
        if (maxCountEl) maxCountEl.value = data.selection_max_count || 30;
        
        const rsiMinEl = document.getElementById('filter-rsi-min');
        if (rsiMinEl) rsiMinEl.value = data.filter_rsi_min || 30;
        
        const rsiMaxEl = document.getElementById('filter-rsi-max');
        if (rsiMaxEl) rsiMaxEl.value = data.filter_rsi_max || 75;
        
        const volumeMinEl = document.getElementById('filter-volume-ratio-min');
        if (volumeMinEl) volumeMinEl.value = data.filter_volume_ratio_min || 0.8;
        
        const volumeMaxEl = document.getElementById('filter-volume-ratio-max');
        if (volumeMaxEl) volumeMaxEl.value = data.filter_volume_ratio_max || 8;
        
        // 更新预览显示
        updateFilterPreviews();
        
        console.log('[选股配置] 配置加载成功');
    } catch (error) {
        console.error('[选股配置] 加载失败:', error);
    }
}
window.loadSelectionConfig = loadSelectionConfig;

// 更新筛选项预览显示
function updateFilterPreviews() {
    // RSI预览
    const rsiMin = document.getElementById('filter-rsi-min')?.value || '30';
    const rsiMax = document.getElementById('filter-rsi-max')?.value || '75';
    const rsiPreview = document.querySelector('[data-filter="rsi"] .filter-item-preview');
    if (rsiPreview) rsiPreview.textContent = `(${rsiMin}-${rsiMax})`;
    
    // 量比预览
    const volMin = document.getElementById('filter-volume-ratio-min')?.value || '0.8';
    const volMax = document.getElementById('filter-volume-ratio-max')?.value || '8';
    const volPreview = document.querySelector('[data-filter="volume-ratio"] .filter-item-preview');
    if (volPreview) volPreview.textContent = `(${volMin}-${volMax})`;
    
    // BIAS预览
    const biasMin = document.getElementById('filter-bias-min')?.value || '-6';
    const biasMax = document.getElementById('filter-bias-max')?.value || '6';
    const biasPreview = document.querySelector('[data-filter="bias"] .filter-item-preview');
    if (biasPreview) biasPreview.textContent = `(${biasMin}~${biasMax})`;
    
    // ADX预览
    const adxMin = document.getElementById('filter-adx-min')?.value || '25';
    const adxPreview = document.querySelector('[data-filter="adx"] .filter-item-preview');
    if (adxPreview) adxPreview.textContent = `> ${adxMin}`;
}
window.updateFilterPreviews = updateFilterPreviews;

// 强制折叠筛选配置
function collapseSelectionConfig() {
    const section = document.querySelector('.selection-config-section');
    const content = document.getElementById('selection-config-content');
    const arrow = document.getElementById('selection-config-arrow');
    
    if (!content || !arrow) return;
    
    content.classList.add('hidden');
    if (section) section.classList.remove('expanded');
    arrow.textContent = '▶';
}
window.collapseSelectionConfig = collapseSelectionConfig;

const { createChart, ColorType } = window.LightweightCharts || {};
console.log('[全局] LightweightCharts 可用:', !!createChart);

const API_BASE = window.location.origin;
console.log('[全局] API_BASE:', API_BASE);

let apiToken = null;
let adminToken = null;
let chart = null;
let candleSeries = null;
let volumeSeries = null;
let ws = null;

console.log('[全局] app.js 全局变量初始化完成');

// 全局SSE连接管理器（单条SSE连接推送所有数据）
let sseConnection = null;
let currentSseTab = null;  // 当前SSE连接的页面
let sseTaskId = null;  // 当前SSE连接的任务ID

// SSE重连延迟控制（防止频繁重连）
let sseReconnectTimer = null;
let sseReconnectDelay = 1000; // 初始延迟1秒

// 更新SSE连接状态显示
function updateSSEStatus(status) {
    const indicator = document.getElementById('sse-status-indicator');
    const statusText = document.getElementById('sse-status-text');
    
    if (!indicator || !statusText) {
        // 如果元素不存在，延迟重试
        setTimeout(() => updateSSEStatus(status), 100);
        return;
    }
    
    // 移除所有状态类
    indicator.classList.remove('connected', 'connecting', 'disconnected');
    
    switch (status) {
        case 'connected':
            indicator.classList.add('connected');
            statusText.textContent = '已连接';
            statusText.className = 'market-status-value';
            break;
        case 'connecting':
            indicator.classList.add('connecting');
            statusText.textContent = '连接中...';
            statusText.className = 'market-status-value loading';
            break;
        case 'disconnected':
        default:
            indicator.classList.add('disconnected');
            statusText.textContent = '未连接';
            statusText.className = 'market-status-value closed';
            break;
    }
    
    console.log('[SSE状态] 更新状态:', status);
}

// 关闭SSE连接
function closeSSEConnection() {
    if (sseConnection) {
        try {
            // 只有在连接状态不是CLOSED时才关闭
            if (sseConnection.readyState !== EventSource.CLOSED) {
                sseConnection.close();
                console.log('[SSE] 关闭SSE连接, readyState:', sseConnection.readyState);
            } else {
                console.log('[SSE] 连接已关闭，无需再次关闭');
            }
        } catch (e) {
            console.warn('[SSE] 关闭SSE连接失败:', e);
        }
        sseConnection = null;
    }
    currentSseTab = null;
    sseTaskId = null;
    
    // 清除重连定时器
    if (sseReconnectTimer) {
        clearTimeout(sseReconnectTimer);
        sseReconnectTimer = null;
    }
    
    // 更新状态显示
    updateSSEStatus('disconnected');
}

// 连接SSE（统一推送服务）
// 全局SSE连接，推送所有类型数据，不依赖current_tab
function connectSSE() {
    // 如果连接已存在且正常，不需要重新连接
    if (sseConnection) {
        const isOpen = sseConnection.readyState === EventSource.OPEN || sseConnection.readyState === EventSource.CONNECTING;
        if (isOpen) {
            console.log('[SSE] 全局连接已存在且正常，跳过重新连接', { readyState: sseConnection.readyState });
            // 更新状态显示
            if (sseConnection.readyState === EventSource.OPEN) {
                updateSSEStatus('connected');
            } else {
                updateSSEStatus('connecting');
            }
            return;
        }
        
        // 如果连接状态不正常，先关闭
        if (sseConnection.readyState === EventSource.CLOSED) {
            console.log('[SSE] 连接已关闭，清理状态');
            sseConnection = null;
        }
    }
    
    // 清除重连定时器
    if (sseReconnectTimer) {
        clearTimeout(sseReconnectTimer);
        sseReconnectTimer = null;
    }
    
    // 构建SSE URL（不传current_tab，让服务器推送所有数据）
    const sseUrl = `${API_BASE}/api/sse/stream`;
    console.log('[SSE] 建立全局SSE连接（推送所有类型数据）:', sseUrl);
    
    try {
        sseConnection = new EventSource(sseUrl);
        currentSseTab = null; // 不再跟踪tab，因为推送所有数据
        sseTaskId = null;
        
        sseConnection.onopen = () => {
            console.log('[SSE] 连接已建立:', sseUrl);
            // 连接成功后重置重连延迟
            sseReconnectDelay = 1000;
            // 更新SSE状态显示
            updateSSEStatus('connected');
        };
        
        sseConnection.onmessage = (event) => {
            try {
                // 跳过心跳消息
                if (event.data.trim() === '' || event.data.startsWith(':')) {
                    console.debug('[SSE接收] 收到心跳消息');
                    return;
                }
                
                // 处理JSON中的NaN值（JSON.parse不支持NaN，需要先替换）
                let dataStr = event.data;
                // 替换 NaN、Infinity、-Infinity 为 null（JSON标准不支持这些值）
                dataStr = dataStr.replace(/:\s*NaN\s*([,}])/g, ': null$1');
                dataStr = dataStr.replace(/:\s*Infinity\s*([,}])/g, ': null$1');
                dataStr = dataStr.replace(/:\s*-Infinity\s*([,}])/g, ': null$1');
                
                const message = JSON.parse(dataStr);
                const messageType = message.type || 'unknown';
                const messageSize = event.data.length;
                
                // 根据消息类型记录详细信息
                if (messageType === 'market') {
                    const data = message.data || {};
                    const aCount = Array.isArray(data.a) ? data.a.length : 0;
                    const hkCount = Array.isArray(data.hk) ? data.hk.length : 0;
                    console.log(`[SSE接收] 收到市场行情更新: A股=${aCount}只, 港股=${hkCount}只, 数据大小=${messageSize}字节`);
                    if (aCount > 0) {
                        const aSamples = data.a.slice(0, 3).map(s => `${s.code || 'N/A'}:${s.price || 'N/A'}`);
                        console.debug(`[SSE接收] A股示例:`, aSamples);
                    }
                    if (hkCount > 0) {
                        const hkSamples = data.hk.slice(0, 3).map(s => `${s.code || 'N/A'}:${s.price || 'N/A'}`);
                        console.debug(`[SSE接收] 港股示例:`, hkSamples);
                    }
                } else if (messageType === 'watchlist_sync') {
                    const action = message.action || 'unknown';
                    const watchlistData = message.data || [];
                    const watchlistCount = Array.isArray(watchlistData) ? watchlistData.length : 0;
                    console.log(`[SSE接收] 收到自选股同步: action=${action}, 数量=${watchlistCount}只, 数据大小=${messageSize}字节`);
                    if (watchlistCount > 0) {
                        const codes = watchlistData.slice(0, 10).map(s => s.code || 'N/A');
                        console.debug(`[SSE接收] 自选股代码:`, codes);
                    }
                } else if (messageType === 'market_status') {
                    const statusData = message.data || {};
                    const aStatus = statusData.a?.status || 'unknown';
                    const hkStatus = statusData.hk?.status || 'unknown';
                    console.log(`[SSE接收] 收到市场状态更新: A股=${aStatus}, 港股=${hkStatus}, 数据大小=${messageSize}字节`);
                } else {
                    console.log(`[SSE接收] 收到消息: type=${messageType}, 数据大小=${messageSize}字节`);
                    console.debug(`[SSE接收] 消息内容:`, message);
                }
                
                // 根据消息类型处理
                handleSSEMessage(message);
            } catch (e) {
                console.error('[SSE接收] 解析消息失败:', e, '原始数据:', event.data?.substring(0, 200));
            }
        };
        
        sseConnection.onerror = (error) => {
            console.error('[SSE] 连接错误:', error, 'readyState:', sseConnection?.readyState);
            
            // 根据连接状态更新显示
            if (sseConnection) {
                if (sseConnection.readyState === EventSource.CONNECTING) {
                    updateSSEStatus('connecting');
                } else if (sseConnection.readyState === EventSource.CLOSED) {
                    updateSSEStatus('disconnected');
                }
            } else {
                updateSSEStatus('disconnected');
            }
            
            // 如果连接断开，尝试重新连接（使用指数退避避免频繁重连）
            if (sseConnection && sseConnection.readyState === EventSource.CLOSED) {
                console.log(`[SSE] 连接已关闭，${sseReconnectDelay/1000}秒后尝试重新连接`);
                
                // 清除之前的重连定时器
                if (sseReconnectTimer) {
                    clearTimeout(sseReconnectTimer);
                }
                
                // 显示重连中状态
                updateSSEStatus('connecting');
                
                sseReconnectTimer = setTimeout(() => {
                    // 重新连接全局SSE（不依赖tab）
                    console.log(`[SSE] 重新连接全局SSE`);
                    sseReconnectDelay = Math.min(sseReconnectDelay * 2, 30000); // 最大30秒
                    connectSSE();
                    sseReconnectTimer = null;
                }, sseReconnectDelay);
            }
        };
        
    } catch (e) {
        console.error('[SSE] 连接失败:', e);
        updateSSEStatus('disconnected');
    }
}

// 处理SSE消息（根据当前激活的tab决定是否处理）
function handleSSEMessage(message) {
    const messageType = message.type || 'unknown';
    
    // 获取当前激活的tab
    const currentTabBtn = document.querySelector('.tab-btn.active');
    const currentTab = currentTabBtn ? currentTabBtn.getAttribute('data-tab') : null;
    
    console.log(`[SSE处理] 收到消息: type=${messageType}, 当前tab=${currentTab}`);
    
    switch (messageType) {
        case 'market':
            // 市场行情数据更新（只在行情页处理）
            if (currentTab === 'market') {
                console.log(`[SSE处理] 处理市场行情更新`);
                handleMarketUpdate(message.data);
            } else {
                console.log(`[SSE处理] 跳过市场行情更新（当前不在行情页）`);
            }
            break;
        case 'watchlist_sync':
            // 自选股同步（始终处理，因为会影响按钮状态）
            const action = message.action || 'unknown';
            const dataCount = Array.isArray(message.data) ? message.data.length : 0;
            console.log(`[SSE处理] 处理自选股同步: action=${action}, 数量=${dataCount}只`);
            handleWatchlistSync(message.action, message.data);
            break;
        case 'market_status':
            // 市场状态更新（始终处理，因为显示在顶部）
            const statusData = message.data || {};
            const aStatus = statusData.a?.status || 'unknown';
            const hkStatus = statusData.hk?.status || 'unknown';
            console.log(`[SSE处理] 处理市场状态更新: A股=${aStatus}, 港股=${hkStatus}`);
            handleMarketStatusUpdate(message.data);
            break;
        case 'news':
            // 资讯更新（只在资讯页处理）
            if (currentTab === 'news') {
                const newsAction = message.action || 'unknown';
                const newsCount = Array.isArray(message.data) ? message.data.length : 0;
                console.log(`[SSE处理] 处理资讯更新: action=${newsAction}, 数量=${newsCount}条`);
                handleNewsUpdate(message.action, message.data);
            } else {
                console.log(`[SSE处理] 跳过资讯更新（当前不在资讯页）`);
            }
            break;
        case 'kline_collect_progress':
            // K线采集进度（始终处理）
            console.log(`[SSE处理] 处理K线采集进度: task_id=${message.task_id}, progress=${message.progress}`);
            handleKlineCollectProgress(message.task_id, message.progress);
            break;
        case 'spot_collect_progress':
            // 实时快照采集进度（始终处理）
            console.log(`[SSE处理] 处理实时快照采集进度: task_id=${message.task_id}, progress=${message.progress}`);
            handleSpotCollectProgress(message.task_id, message.progress);
            break;
        case 'spot_collect_result':
            // 实时数据采集结果（始终处理，显示在顶部状态栏）
            console.log(`[SSE处理] 处理实时数据采集结果:`, message.data);
            handleSpotCollectResult(message.data);
            break;
        case 'selection_progress':
            // 选股进度（始终处理）
            console.log(`[SSE处理] 处理选股进度: task_id=${message.task_id}, data=`, message.data);
            handleSelectionProgress(message.task_id, message.data);
            break;
        default:
            console.warn(`[SSE处理] 未知消息类型: ${messageType}`, message);
    }
}

// 处理市场状态更新（SSE推送）
function handleMarketStatusUpdate(data) {
    console.log('[SSE] 收到市场状态更新:', data);
    
    const aStatusEl = document.getElementById('market-status-a');
    const hkStatusEl = document.getElementById('market-status-hk');
    
    if (!aStatusEl || !hkStatusEl) {
        console.warn('[SSE] 市场状态元素未找到');
        return;
    }
    
    if (data && data.a) {
        const aStatus = data.a;
        // 构建状态文本，包含下一个开盘时间
        let statusText = aStatus.status || '未知';
        if (!aStatus.is_trading && aStatus.next_open) {
            statusText += ` (${aStatus.next_open}开)`;
        }
        aStatusEl.textContent = statusText;
        aStatusEl.className = 'market-status-value ' + (aStatus.is_trading ? 'trading' : 'closed');
        aStatusEl.title = aStatus.next_open_full ? `下次开盘: ${aStatus.next_open_full}` : '';
        console.log('[SSE] A股状态已更新:', statusText);
    }
    
    if (data && data.hk) {
        const hkStatus = data.hk;
        // 构建状态文本，包含下一个开盘时间
        let statusText = hkStatus.status || '未知';
        if (!hkStatus.is_trading && hkStatus.next_open) {
            statusText += ` (${hkStatus.next_open}开)`;
        }
        hkStatusEl.textContent = statusText;
        hkStatusEl.className = 'market-status-value ' + (hkStatus.is_trading ? 'trading' : 'closed');
        hkStatusEl.title = hkStatus.next_open_full ? `下次开盘: ${hkStatus.next_open_full}` : '';
        console.log('[SSE] 港股状态已更新:', statusText);
    }
}

// 市场行情更新防抖定时器
let marketUpdateTimer = null;

// 处理市场行情数据更新（SSE推送，无感刷新）
function handleMarketUpdate(data) {
    const tbody = document.getElementById('stock-list');
    if (!tbody) return;
    
    const marketTab = document.getElementById('market-tab');
    if (!marketTab || !marketTab.classList.contains('active')) {
        return;  // 不在行情页，不更新
    }
    
    // 使用防抖，避免频繁更新（100ms内多次更新只执行最后一次）
    if (marketUpdateTimer) {
        clearTimeout(marketUpdateTimer);
    }
    
    marketUpdateTimer = setTimeout(() => {
        _doMarketUpdate(data);
        marketUpdateTimer = null;
    }, 100);
}

// 执行市场行情更新（内部函数）
function _doMarketUpdate(data) {
    const tbody = document.getElementById('stock-list');
    if (!tbody) return;
    
    const container = document.querySelector('.stock-list-container');
    
    // 保存当前滚动位置
    let savedScrollTop = 0;
    if (container) {
        // 检查是容器滚动还是window滚动
        if (container.scrollHeight > container.clientHeight) {
            savedScrollTop = container.scrollTop;
        } else {
            savedScrollTop = window.pageYOffset || document.documentElement.scrollTop;
        }
    }
    
    const marketSelect = document.getElementById('market-select');
    const currentMarket = marketSelect ? marketSelect.value || 'a' : 'a';
    
    // 根据当前选择的市场获取对应数据
    const stocks = currentMarket === 'a' ? (data.a || []) : (data.hk || []);
    
    if (stocks.length === 0) return;
    
    // 只更新第一页的数据（避免影响滚动位置和分页）
    const existingRows = Array.from(tbody.querySelectorAll('tr'));
    const updateCount = Math.min(stocks.length, existingRows.length);
    
    // 构建股票代码到数据的映射
    const stockMap = {};
    stocks.forEach(stock => {
        stockMap[stock.code] = stock;
    });
    
    // 更新现有行的数据（无感刷新，只更新变化的字段）
    for (let i = 0; i < updateCount; i++) {
        const row = existingRows[i];
        if (!row) continue;
        
        const stockData = JSON.parse(row.getAttribute('data-stock') || '{}');
        const code = stockData.code;
        
        if (code && stockMap[code]) {
            const updatedStock = stockMap[code];
            
            // 只更新数据有变化的字段，避免不必要的DOM操作
            const cells = row.querySelectorAll('td');
            if (cells.length >= 5) {
                // 更新价格
                const priceCell = cells[2];
                const newPrice = updatedStock.price?.toFixed(2) || '-';
                if (priceCell.textContent !== newPrice) {
                    priceCell.textContent = newPrice;
                }
                
                // 更新涨跌幅
                const pctCell = cells[3];
                const newPct = updatedStock.pct?.toFixed(2) || '-';
                const newPctText = newPct + '%';
                if (pctCell.textContent !== newPctText) {
                    pctCell.textContent = newPctText;
                    pctCell.className = updatedStock.pct >= 0 ? 'up' : 'down';
                }
                
                // 更新成交量
                const volumeCell = cells[4];
                const newVolume = formatVolume(updatedStock.volume);
                if (volumeCell.textContent !== newVolume) {
                    volumeCell.textContent = newVolume;
                }
            }
            
            // 更新data-stock属性
            row.setAttribute('data-stock', JSON.stringify(updatedStock));
        }
    }
    
    // 恢复滚动位置（如果发生了变化）
    if (container && savedScrollTop > 0) {
        requestAnimationFrame(() => {
            if (container.scrollHeight > container.clientHeight) {
                container.scrollTop = savedScrollTop;
            } else {
                window.scrollTo(0, savedScrollTop);
            }
        });
    }
    
    // 更新按钮状态（每次市场数据更新后都要更新，因为自选股可能变化）
    updateWatchlistButtonStates();
}

// 自选股同步更新防抖定时器
let watchlistSyncTimer = null;

// 处理自选股同步（SSE推送，无感刷新）
function handleWatchlistSync(action, data) {
    console.log('[SSE] 自选股同步:', action, '数据数量:', data?.length || 0);
    
    if (action === 'init' || action === 'update') {
        // 使用防抖，避免频繁更新（200ms内多次更新只执行最后一次）
        if (watchlistSyncTimer) {
            clearTimeout(watchlistSyncTimer);
        }
        
        watchlistSyncTimer = setTimeout(() => {
            _doWatchlistSync(data);
            watchlistSyncTimer = null;
        }, 200);
    }
}

// 执行自选股同步更新（内部函数）
function _doWatchlistSync(data) {
    console.log('[SSE] ========== 执行自选股同步更新（无感刷新） ==========');
    const serverData = data || [];
    const localData = getWatchlist();
    const localCodes = localData.map(s => s.code).sort().join(',');
    const serverCodes = serverData.map(s => s.code).sort().join(',');
    
    console.log('[SSE] 本地自选股:', localCodes);
    console.log('[SSE] 服务器自选股:', serverCodes);
    
    // 如果数据有变化，更新本地缓存
    if (localCodes !== serverCodes) {
        console.log('[SSE] ✅ 检测到数据变化，通过SSE无感更新UI');
        localStorage.setItem('watchlist', JSON.stringify(serverData));
        
        // 更新按钮状态（无论在哪一页都要更新）
        console.log('[SSE] 更新所有页面的按钮状态');
        updateWatchlistButtonStates();
        
        // 如果当前在自选页，直接通过SSE数据更新列表（无感刷新，不需要重新请求）
        const watchlistTab = document.getElementById('watchlist-tab');
        if (watchlistTab && watchlistTab.classList.contains('active')) {
            console.log('[SSE] 当前在自选页，使用SSE数据无感更新列表（不显示加载状态，保持滚动位置）');
            // 保存当前滚动位置
            const container = document.getElementById('watchlist-container');
            const savedScrollTop = container ? container.scrollTop : 0;
            
            // 清除缓存，使用SSE推送的数据直接渲染
            localStorage.removeItem(WATCHLIST_CACHE_KEY);
            // 直接使用SSE推送的数据渲染，不需要重新请求，实现无感刷新
            renderWatchlistStocksFromSSE(serverData).then(() => {
                // 恢复滚动位置
                if (container && savedScrollTop > 0) {
                    // 延迟恢复，确保DOM已更新
                    setTimeout(() => {
                        container.scrollTop = savedScrollTop;
                    }, 50);
                }
            });
        } else {
            console.log('[SSE] 当前不在自选页，只更新按钮状态');
        }
    } else {
        console.log('[SSE] ⚠️ 数据无变化，跳过更新');
    }
}

// 从SSE数据直接渲染自选股列表（不需要重新请求服务器，支持无限滚动）
async function renderWatchlistStocksFromSSE(watchlistData) {
    console.log('[SSE] 从SSE数据直接渲染自选股列表，数量:', watchlistData.length);
    
    // 保存当前滚动位置
    const container = document.getElementById('watchlist-container');
    const savedScrollTop = container ? container.scrollTop : 0;
    
    if (!watchlistData || watchlistData.length === 0) {
        if (container) {
            container.innerHTML = `
                <div class="watchlist-placeholder">
                    <div style="font-size: 48px; margin-bottom: 16px;">⭐</div>
                    <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">暂无自选股</div>
                    <div style="font-size: 14px; color: #64748b;">在行情页点击"加入自选"按钮添加股票</div>
                </div>
            `;
        }
        watchlistAllStocks = [];
        watchlistRenderedCount = 0;
        return Promise.resolve();
    }
    
    // 批量获取股票行情数据
    const codes = watchlistData.map(s => s.code);
    console.log('[SSE] 批量获取股票行情，代码:', codes);
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/spot/batch`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ codes })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const result = await response.json();
        if (result.code === 0) {
            const stocksWithData = result.data || [];
            console.log('[SSE] 批量获取成功，共', stocksWithData.length, '只股票有行情数据');
            
            // 合并自选股信息和行情数据
            const watchlistStocks = watchlistData.map(watchlistItem => {
                const stockData = stocksWithData.find(s => s.code === watchlistItem.code);
                return {
                    ...watchlistItem,
                    ...stockData,
                    // 确保有基本信息
                    name: stockData?.name || watchlistItem.name || watchlistItem.code,
                    code: watchlistItem.code
                };
            });
            
            // 使用无限滚动渲染（forceRender=true重置状态，silent=true不显示日志）
            renderWatchlistStocks(watchlistStocks, true, true);
            
            // 恢复滚动位置
            if (container && savedScrollTop > 0) {
                // 延迟恢复，确保DOM已更新
                setTimeout(() => {
                    container.scrollTop = savedScrollTop;
                }, 100);
            }
            
            return Promise.resolve();
        } else {
            throw new Error(result.message || '批量查询失败');
        }
    } catch (error) {
        console.error('[SSE] 批量获取股票行情失败:', error);
        // 即使获取行情失败，也使用基本信息渲染（支持无限滚动）
        renderWatchlistStocks(watchlistData.map(item => ({
            ...item,
            name: item.name || item.code
        })), true, true);
        
        // 恢复滚动位置
        if (container && savedScrollTop > 0) {
            setTimeout(() => {
                container.scrollTop = savedScrollTop;
            }, 100);
        }
        
        return Promise.resolve();
    }
}

// 处理资讯更新（SSE推送，无感刷新，支持无限滚动）
function handleNewsUpdate(action, data) {
    console.log('[SSE] ========== 处理资讯更新（无感刷新） ==========');
    const newsData = data || [];
    console.log('[SSE] 收到资讯数据:', action, '数量:', newsData.length);
    
    if (action === 'init' || action === 'update') {
        // 如果当前在资讯页，无感更新列表（支持无限滚动）
        const newsTab = document.getElementById('news-tab');
        if (newsTab && newsTab.classList.contains('active')) {
            console.log('[SSE] 当前在资讯页，使用SSE数据无感更新列表（支持无限滚动）');
            renderNews(newsData);
        } else {
            console.log('[SSE] 当前不在资讯页，跳过更新');
        }
    }
}

// 处理K线采集进度（SSE推送）
function handleKlineCollectProgress(taskId, progress) {
    console.log('[SSE] K线采集进度:', taskId, progress);
    
    // 获取状态显示元素
    const statusEl = document.getElementById('collect-kline-status');
    const btn = document.getElementById('collect-kline-btn');
    
    if (!statusEl) {
        console.log('[SSE] K线采集进度: 状态元素不存在，跳过更新');
        return;
    }
    
    if (!progress) {
        return;
    }
    
    const dataSource = progress.data_source || '';
    const success = progress.success || 0;
    const failed = progress.failed || 0;
    const total = progress.total || 0;
    const current = progress.current || 0;
    const progressPct = progress.progress || 0;
    
    if (progress.status === 'running') {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #10b981; margin-bottom: 5px; font-weight: 500;">
                    ✅ 采集任务进行中
                </div>
                ${dataSource ? `<div style="color: #f59e0b; font-size: 11px; margin-bottom: 5px;">📡 数据源: ${dataSource}</div>` : ''}
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
        if (btn) {
            btn.textContent = `采集中 ${current}/${total}`;
            btn.disabled = true;
        }
    } else if (progress.status === 'completed') {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #10b981; margin-bottom: 5px; font-weight: bold;">
                    ✅ 采集完成！
                </div>
                ${dataSource ? `<div style="color: #f59e0b; font-size: 11px; margin-bottom: 5px;">📡 数据源: ${dataSource}</div>` : ''}
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
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📥 批量采集';
        }
    } else if (progress.status === 'cancelled') {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #f59e0b; margin-bottom: 5px; font-weight: bold;">
                    ⏹️ 采集已停止
                </div>
                ${dataSource ? `<div style="color: #f59e0b; font-size: 11px; margin-bottom: 5px;">📡 数据源: ${dataSource}</div>` : ''}
                <div style="color: #94a3b8; font-size: 11px; margin-bottom: 2px;">
                    已处理: ${current}/${total} 只股票
                </div>
                <div style="color: #10b981; font-size: 11px; margin-bottom: 2px;">
                    ✅ 成功: ${success} 只
                </div>
                <div style="color: ${failed > 0 ? '#f59e0b' : '#94a3b8'}; font-size: 11px;">
                    ${failed > 0 ? `⚠️ 失败: ${failed} 只` : '无失败'}
                </div>
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📥 批量采集';
        }
    } else if (progress.status === 'failed') {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #ef4444; margin-bottom: 5px;">
                    ❌ 采集失败
                </div>
                ${dataSource ? `<div style="color: #f59e0b; font-size: 11px; margin-bottom: 5px;">📡 数据源: ${dataSource}</div>` : ''}
                <div style="color: #94a3b8; font-size: 11px;">
                    ${progress.message || '采集过程中发生错误'}
                </div>
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📥 重新采集';
        }
    } else if (progress.status === 'idle') {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #94a3b8; margin-bottom: 5px; font-weight: 500;">
                    💤 暂无采集任务
                </div>
                <div style="color: #64748b; font-size: 11px; margin-bottom: 5px;">📡 数据源: ${dataSource || '空闲'}</div>
                <div style="color: #94a3b8; font-size: 11px;">
                    点击"批量采集"按钮开始采集K线数据
                </div>
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📥 批量采集';
        }
    }
}

// 处理实时快照采集进度（SSE推送）
function handleSpotCollectProgress(taskId, progress) {
    console.log('[SSE] 实时快照采集进度:', taskId, progress);
    
    const statusEl = document.getElementById('spot-collect-status');
    const btn = document.getElementById('collect-spot-btn');
    
    if (!statusEl) {
        console.log('[SSE] 实时快照采集进度: 状态元素不存在，跳过更新');
        return;
    }
    
    if (!progress) {
        return;
    }
    
    const message = progress.message || '';
    const dataSource = progress.data_source || '';
    const aCount = progress.a_count || 0;
    const hkCount = progress.hk_count || 0;
    
    // 数据源显示
    const sourceHtml = dataSource ? `<span style="color: #60a5fa; margin-left: 8px;">[${dataSource}]</span>` : '';
    
    if (progress.status === 'running') {
        statusEl.innerHTML = `
            <div style="color: #10b981; font-weight: 500;">
                ⏳ ${message}${sourceHtml}
            </div>
        `;
        if (btn) {
            btn.disabled = true;
            btn.textContent = '采集中...';
        }
    } else if (progress.status === 'completed') {
        statusEl.innerHTML = `
            <div style="color: #10b981; font-weight: 500;">
                ✅ ${message}${sourceHtml}
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📊 采集实时快照';
        }
        // 30秒后清除状态
        setTimeout(() => {
            if (statusEl) statusEl.innerHTML = '';
        }, 30000);
    } else if (progress.status === 'failed') {
        statusEl.innerHTML = `
            <div style="color: #ef4444; font-weight: 500;">
                ❌ ${message}
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📊 采集实时快照';
        }
    } else if (progress.status === 'cancelled') {
        statusEl.innerHTML = `
            <div style="color: #f59e0b; font-weight: 500;">
                ⏹️ ${message || '采集已停止'}
            </div>
        `;
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📊 采集实时快照';
        }
        // 10秒后清除状态
        setTimeout(() => {
            if (statusEl) statusEl.innerHTML = '';
        }, 10000);
    }
}

// 采集实时快照
async function collectSpotData() {
    const btn = document.getElementById('collect-spot-btn');
    const statusEl = document.getElementById('spot-collect-status');
    
    if (btn) {
        btn.disabled = true;
        btn.textContent = '启动中...';
    }
    
    if (statusEl) {
        statusEl.innerHTML = '<div style="color: #60a5fa;">正在启动采集任务...</div>';
    }
    
    try {
        const res = await apiFetch(`${API_BASE}/api/market/spot/collect`, {
            method: 'POST'
        });
        
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}`);
        }
        
        const data = await res.json();
        if (data.code !== 0) {
            throw new Error(data.message || '启动失败');
        }
        
        // 任务已启动，等待SSE推送进度
        console.log('[实时快照] 采集任务已启动:', data.data?.task_id);
        
    } catch (error) {
        console.error('[实时快照] 启动采集失败:', error);
        if (statusEl) {
            statusEl.innerHTML = `<div style="color: #ef4444;">❌ 启动失败: ${error.message}</div>`;
        }
        if (btn) {
            btn.disabled = false;
            btn.textContent = '📊 采集实时快照';
        }
    }
}

// 处理实时数据采集结果（显示在顶部状态栏）
function handleSpotCollectResult(data) {
    console.log('[SSE] 实时数据采集结果:', data);
    
    const container = document.getElementById('spot-collect-result');
    const aTextEl = document.getElementById('spot-result-a-text');
    const aTimeEl = document.getElementById('spot-result-a-time');
    const hkTextEl = document.getElementById('spot-result-hk-text');
    const hkTimeEl = document.getElementById('spot-result-hk-time');
    const sourceEl = document.getElementById('spot-collect-result-source');
    
    if (!container) {
        console.warn('[SSE] 实时数据采集结果元素未找到');
        return;
    }
    
    const success = data.success;
    const time = data.time || '';
    const source = data.source || '';
    const hkSource = data.hk_source || '';
    const aCount = data.a_count || 0;
    const hkCount = data.hk_count || 0;
    const aTime = data.a_time || time;
    const hkTime = data.hk_time || time;
    
    // 更新显示
    container.style.display = 'flex';
    container.className = 'spot-collect-result ' + (success ? 'success' : 'failed');
    
    // A股状态
    if (aTextEl) {
        const aSuccess = aCount > 0;
        aTextEl.textContent = (aSuccess ? '✅ ' : '❌ ') + aCount + '只';
        aTextEl.className = 'spot-result-value ' + (aSuccess ? 'success' : 'failed');
    }
    if (aTimeEl) aTimeEl.textContent = aTime;
    
    // 港股状态
    if (hkTextEl) {
        const hkSuccess = hkCount > 0;
        hkTextEl.textContent = (hkSuccess ? '✅ ' : '❌ ') + hkCount + '只';
        hkTextEl.className = 'spot-result-value ' + (hkSuccess ? 'success' : 'failed');
    }
    if (hkTimeEl) hkTimeEl.textContent = hkTime;
    
    // 数据源（显示A股和港股数据源）
    if (sourceEl) {
        let sourceText = source || '未知';
        if (hkSource && hkCount > 0) {
            sourceText = `A:${source || '未知'} H:${hkSource}`;
        }
        sourceEl.textContent = sourceText;
    }
    
    console.log(`[SSE] 实时数据采集结果已更新: A股=${aCount}只(${aTime}), 港股=${hkCount}只(${hkTime}), A股源=${source}, 港股源=${hkSource}`);
}

// 加载上次的采集结果（页面刷新后恢复显示）
async function loadSpotCollectResult() {
    try {
        const res = await apiFetch(`${API_BASE}/api/spot/collect/result`);
        if (!res.ok) return;
        
        const data = await res.json();
        if (data.code === 0 && data.data) {
            console.log('[启动] 加载上次采集结果:', data.data);
            handleSpotCollectResult(data.data);
        }
    } catch (error) {
        console.debug('[启动] 加载采集结果失败:', error);
    }
}

// 处理选股进度（SSE推送）
function handleSelectionProgress(taskId, progressData) {
    console.log('[SSE] 选股进度:', taskId, progressData);
    
    // 不再过滤 task_id，始终显示最新的选股进度
    // 这样刷新页面后也能看到正在进行的选股任务
    
    // 显示进度容器
    const progressContainer = document.getElementById('selection-progress-container');
    if (progressContainer && progressContainer.style.display === 'none') {
        progressContainer.style.display = 'block';
    }
    
    // 更新进度显示
    const statusEl = document.getElementById('selection-status');
    const progressBar = document.getElementById('selection-progress-bar');
    const progressText = document.getElementById('selection-progress-text');
    
    if (!progressData) return;
    
    const { status, stage, message, progress, total, processed, passed, selected, elapsed_time } = progressData;
    
    // 更新状态文本（添加阶段图标）
    if (statusEl) {
        let displayMessage = message || '选股中...';
        // 添加阶段图标
        if (displayMessage.includes('市场环境')) {
            displayMessage = '🌍 ' + displayMessage;
        } else if (displayMessage.includes('第一层')) {
            displayMessage = '🔍 ' + displayMessage;
        } else if (displayMessage.includes('第二层')) {
            displayMessage = '📊 ' + displayMessage;
        } else if (displayMessage.includes('筛选')) {
            displayMessage = '⚡ ' + displayMessage;
        } else if (status === 'completed') {
            displayMessage = '✅ ' + displayMessage;
        } else if (status === 'failed') {
            displayMessage = '❌ ' + displayMessage;
        }
        statusEl.innerHTML = displayMessage;
        statusEl.className = 'selection-status ' + (status === 'completed' ? 'success' : (status === 'failed' ? 'error' : 'running'));
    }
    
    // 更新进度条（添加颜色变化）
    if (progressBar) {
        const targetWidth = progress || 0;
        progressBar.style.width = `${targetWidth}%`;
        
        // 根据进度添加颜色变化
        if (status === 'completed') {
            progressBar.className = 'selection-progress-fill success';
        } else if (status === 'failed') {
            progressBar.className = 'selection-progress-fill error';
        } else {
            progressBar.className = 'selection-progress-fill';
            // 动态颜色
            if (targetWidth < 30) {
                progressBar.style.background = 'linear-gradient(90deg, #ef4444 0%, #f97316 100%)';
            } else if (targetWidth < 70) {
                progressBar.style.background = 'linear-gradient(90deg, #f59e0b 0%, #eab308 100%)';
            } else {
                progressBar.style.background = 'linear-gradient(90deg, #10b981 0%, #059669 100%)';
            }
        }
    }
    
    // 更新进度文本
    if (progressText) {
        let text = `${progress || 0}%`;
        if (processed !== undefined && total) {
            text += ` (${processed}/${total})`;
        }
        if (passed !== undefined) {
            text += ` 通过: ${passed}`;
        }
        if (elapsed_time !== undefined) {
            text += ` - ${typeof elapsed_time === 'number' ? elapsed_time.toFixed(1) : elapsed_time}秒`;
        }
        progressText.textContent = text;
    }
    
    // 如果选股完成或失败，3秒后隐藏进度条
    if (status === 'completed' || status === 'failed') {
        console.log('[SSE] 选股' + (status === 'completed' ? '完成' : '失败') + '，选中:', selected || 0, '只股票');
        setTimeout(() => {
            if (progressContainer) {
                progressContainer.style.display = 'none';
            }
        }, 3000);
    }
}

// 页面卸载时关闭SSE连接
window.addEventListener('beforeunload', closeSSEConnection);
window.addEventListener('pagehide', closeSSEConnection);

// 初始化
document.addEventListener('DOMContentLoaded', async () => {
    console.log('[全局] DOMContentLoaded 事件触发');
    try {
        console.log('[全局] 开始初始化认证...');
        await initAuth();
        console.log('[全局] 认证初始化完成');
    } catch (error) {
        console.error('[全局] 初始化认证失败:', error);
        // 即使认证失败，也尝试启动应用
        try {
            console.log('[全局] 尝试直接启动应用（无认证）');
            startApp();
        } catch (e) {
            console.error('[全局] 启动应用失败:', e);
        }
    }
    
    // 监听浏览器返回按钮，处理页面导航和K线图关闭
    window.addEventListener('popstate', (event) => {
        const state = event.state || {};
        const path = window.location.pathname;
        
        // 检查是否从K线图页面返回
        const wasKlinePage = state.klineModal || path.startsWith('/kline/');
        const isKlinePage = path.startsWith('/kline/');
        
        // 如果从K线图页面返回，关闭模态框
        const modal = document.getElementById('kline-modal');
        if (wasKlinePage && !isKlinePage && modal && modal.style.display !== 'none') {
            // 关闭K线图模态框
            modal.style.display = 'none';
            
            // 清理图表
            if (chart) {
                const container = document.getElementById('chart-container');
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
            
            // 清除状态
            try {
                localStorage.removeItem('klineModalState');
            } catch (e) {
                console.warn('清除K线模态弹窗状态失败:', e);
            }
            
            currentKlineCode = null;
            currentKlineName = null;
            currentKlineStockData = null;
        }
        
        // 处理tab切换（根据路径判断）
        if (state.tab) {
            switchToTab(state.tab, false); // false表示不添加历史记录
        } else if (path && path !== '/') {
            // 解析路径，如 /market, /watchlist, /kline/000001
            if (path.startsWith('/kline/')) {
                // K线图页面，切换到对应的tab
                const savedTab = state.tab || localStorage.getItem('currentTab') || 'market';
                switchToTab(savedTab, false);
            } else {
                // 其他路径，尝试切换到对应的tab
                const pathTab = path.replace('/', '').split('/')[0]; // 获取第一个路径段
                const validTabs = ['market', 'watchlist', 'strategy', 'ai', 'news', 'config'];
                if (validTabs.includes(pathTab)) {
                    switchToTab(pathTab, false);
                } else {
                    // 无效路径，切换到默认tab
                    switchToTab('market', false);
                }
            }
        } else {
            // 根路径，切换到默认tab
            switchToTab('market', false);
        }
    });
    
    // 初始化时根据URL路径设置tab（这个逻辑由initTabs处理，这里不需要重复）
    // 注意：initTabs会在startApp中调用，所以这里不需要处理
});

function startApp() {
    console.log('[启动] startApp函数被调用');
    try {
        initTheme();
        const currentTab = initTabs(); // 获取当前激活的tab
        console.log('[启动] 当前tab:', currentTab);
    
    // 监听自选股变化事件（同一标签页内的同步）
    window.addEventListener('watchlistChanged', (e) => {
        const { action, code } = e.detail;
        console.log(`[自选] 自选股变化事件: ${action}, 代码: ${code}`);
        
        // 更新按钮状态
        updateWatchlistButtonStates();
        
        // 不再手动刷新自选页，依赖SSE推送来更新（无感刷新）
        // SSE会在服务器保存成功后自动推送更新，_doWatchlistSync会处理更新
        console.log('[自选] 等待SSE推送更新（无感刷新）');
    });
    
    // 初始化所有模块
    initMarket(); // 始终初始化行情模块（即使不在行情页，也需要初始化事件监听）
    initWatchlist(); // 初始化自选股模块
    
    // 根据当前tab加载数据（首次加载）
    if (currentTab === 'market') {
        // 如果当前是行情页，检查是否有数据，没有才加载
        const tbody = document.getElementById('stock-list');
        if (!tbody || tbody.children.length === 0) {
            loadMarket(); // 首次加载后会连接SSE
        } else {
            // 检查是否有有效数据（不是loading或错误提示）
            const hasData = Array.from(tbody.children).some(tr => {
                const text = tr.textContent || '';
                const cells = tr.querySelectorAll('td');
                return cells.length > 1 && text.trim() && !text.includes('加载中') && !text.includes('加载失败') && !text.includes('暂无数据');
            });
            if (!hasData) {
                loadMarket(); // 首次加载后会连接SSE
            } else {
                // SSE已全局连接，无需重新连接
            }
        }
    } else if (currentTab === 'watchlist') {
        // 如果当前是自选页，先显示缓存数据，SSE会推送更新
        console.log('[自选] 当前是自选页，先显示缓存数据');
        const cachedData = getCachedWatchlistData();
        const localWatchlist = getWatchlist();
        if (cachedData && cachedData.length > 0 && localWatchlist.length > 0) {
            renderWatchlistStocks(cachedData, false, true);
        } else if (localWatchlist.length > 0) {
            loadWatchlist(false);
        }
    } else if (currentTab === 'news') {
        // 如果当前是资讯页，主动加载一次数据（避免页面为空）
        console.log('[资讯] 当前是资讯页，主动加载一次数据');
        loadNews();
    }
    
    // 建立全局SSE连接（推送所有类型数据，不依赖tab）
    console.log('[启动] 建立全局SSE连接');
    connectSSE();
    
        initKlineModal();
        initStrategy();
        initAI();
        initNews();
        initConfig();
        console.log('[启动] 准备初始化市场状态模块');
        initMarketStatus();
        
        // 初始化SSE状态显示（初始状态为未连接）
        updateSSEStatus('disconnected');
        
        // 加载上次的采集结果（持久化显示）
        loadSpotCollectResult();
        
        console.log('[启动] startApp函数执行完成');
    } catch (error) {
        console.error('[启动] startApp执行出错:', error);
        // 即使出错也尝试初始化市场状态
        try {
            console.log('[启动] 尝试单独初始化市场状态模块');
            initMarketStatus();
        } catch (e) {
            console.error('[启动] 初始化市场状态模块失败:', e);
        }
    }
}

// 主题切换
let themeInitialized = false;
function initTheme() {
    if (themeInitialized) {
        return; // 已经初始化过，避免重复初始化
    }
    
    const body = document.body;
    const btn = document.getElementById('theme-toggle');
    
    if (!btn) {
        console.warn('[主题] 主题切换按钮不存在，将在DOM加载后重试');
        // 延迟重试，确保DOM已加载
        setTimeout(() => {
            initTheme();
        }, 100);
        return;
    }
    
    themeInitialized = true;
    
    const saved = localStorage.getItem('theme');
    // 如果主题已在页面加载前设置（通过head中的脚本），这里只是确保应用（不会重复添加）
    if (saved === 'light' && !body.classList.contains('light-mode')) {
        body.classList.add('light-mode');
    }
    
    updateThemeButtonText(btn, body);
    
    // 绑定点击事件
    btn.addEventListener('click', () => {
        body.classList.toggle('light-mode');
        const mode = body.classList.contains('light-mode') ? 'light' : 'dark';
        localStorage.setItem('theme', mode);
        updateThemeButtonText(btn, body);
        // 主题切换时更新图表主题
        updateChartTheme();
        console.log('[主题] 主题已切换为:', mode);
    });
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

// 切换到指定tab（支持History API，使用路径模式）
function switchToTab(targetTab, addHistory = true) {
    const tabs = document.querySelectorAll('.tab-btn');
    const contents = document.querySelectorAll('.tab-content');
    
    // 移除所有active类
    tabs.forEach(t => t.classList.remove('active'));
    contents.forEach(c => c.classList.remove('active'));
    
    // 设置目标tab为active
    const targetBtn = document.querySelector(`.tab-btn[data-tab="${targetTab}"]`);
    const targetContent = document.getElementById(`${targetTab}-tab`);
    
    if (targetBtn && targetContent) {
        targetBtn.classList.add('active');
        targetContent.classList.add('active');
        
        // 保存当前tab到localStorage
        localStorage.setItem('currentTab', targetTab);
        
        // 更新URL（使用路径模式，如 /market, /watchlist）
        if (window.history) {
            const url = `/${targetTab}${window.location.search}`;
            if (addHistory && window.history.pushState) {
                // 添加历史记录（用户操作）
                window.history.pushState({ tab: targetTab }, '', url);
            } else if (window.history.replaceState) {
                // 替换当前历史记录（初始化或程序化切换）
                window.history.replaceState({ tab: targetTab }, '', url);
            }
        }
        
        // 切换到自选页时，先显示缓存数据，通过SSE实时推送更新
        if (targetTab === 'watchlist') {
            console.log('[自选] 切换到自选页，使用SSE实时推送（SSE已连接，无需重连）');
            
            // 先使用缓存数据快速显示（如果存在）
            const cachedData = getCachedWatchlistData();
            const localWatchlist = getWatchlist();
            
            if (cachedData && cachedData.length > 0 && localWatchlist.length > 0) {
                console.log('[自选] 使用缓存数据快速显示，共', cachedData.length, '只股票');
                // 先渲染缓存数据（无感显示）
                renderWatchlistStocks(cachedData, false, true);
            } else if (localWatchlist.length > 0) {
                // 如果没有缓存但有自选列表，直接加载（不强制同步，避免频繁请求）
                console.log('[自选] 无缓存数据，直接加载');
                loadWatchlist(false); // 使用现有数据，通过SSE实时更新
            } else {
                // 如果自选列表为空，显示占位符
                console.log('[自选] 自选列表为空');
                loadWatchlist(false);
            }
            
            // SSE已全局连接，无需重新连接
        }
        
        // 切换到行情页时，使用SSE实时推送
        if (targetTab === 'market') {
            console.log('[行情] 切换到行情页，使用SSE实时推送（SSE已连接，无需重连）');
            
            // 直接使用本地缓存更新按钮状态（避免频繁同步）
            updateWatchlistButtonStates();
            
            // 如果表格为空，先加载一次初始数据
            const tbody = document.getElementById('stock-list');
            if (!tbody || tbody.children.length === 0) {
                console.log('[行情] 行情页表格为空，加载初始数据');
                currentPage = 1;
                hasMore = true;
                loadMarket();
            } else {
                // 如果已有数据，检查是否需要刷新（避免频繁请求）
                const firstRow = tbody.querySelector('tr');
                if (!firstRow || firstRow.textContent.includes('加载中') || firstRow.textContent.includes('加载失败')) {
                    console.log('[行情] 行情页数据异常，重新加载');
                    currentPage = 1;
                    hasMore = true;
                    loadMarket();
                }
            }
            
            // SSE已全局连接，无需重新连接
        }
        
        // 切换到资讯页时，如果数据为空则主动加载一次
        if (targetTab === 'news') {
            console.log('[资讯] 切换到资讯页，使用SSE实时推送（SSE已连接，无需重连）');
            // SSE已全局连接，无需重新连接
            // 如果数据为空，主动加载一次（避免页面为空）
            const newsList = document.getElementById('news-list');
            if (newsList) {
                const hasData = newsList.children.length > 0 && 
                               !newsList.innerHTML.includes('暂无资讯') && 
                               !newsList.innerHTML.includes('加载中');
                if (!hasData) {
                    console.log('[资讯] 切换到资讯页，数据为空，主动加载一次');
                    loadNews();
                }
            }
        }
        
        // 切换到配置页时，加载配置
        if (targetTab === 'config') {
            console.log('[配置] 切换到配置页');
            // 确保配置模块已初始化（如果还没有初始化）
            if (!configInitialized) {
                initConfig();
            }
            // 如果配置未加载，重新加载
            loadConfig();
        }
    }
}

// 标签切换
function initTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    
    // 立即从localStorage恢复上次的tab（避免闪烁）
    const savedTab = localStorage.getItem('currentTab') || 'market';
    const path = window.location.pathname;
    
    // 根据路径确定初始tab
    let initialTab = savedTab;
    if (path && path !== '/') {
        if (path.startsWith('/kline/')) {
            // K线图页面，使用保存的tab
            initialTab = savedTab;
        } else {
            const pathTab = path.replace('/', '').split('/')[0];
            const validTabs = ['market', 'watchlist', 'strategy', 'ai', 'news', 'config'];
            if (validTabs.includes(pathTab)) {
                initialTab = pathTab;
            }
        }
    }
    
    // 确保URL路径正确（如果路径不正确，使用replaceState更新）
    const currentPath = window.location.pathname;
    const expectedPath = `/${initialTab}`;
    
    if (currentPath !== expectedPath && !currentPath.startsWith('/kline/')) {
        // 如果当前路径与期望的路径不一致，且不是K线图页面，则更新URL
        if (window.history && window.history.replaceState) {
            window.history.replaceState({ tab: initialTab }, '', `${expectedPath}${window.location.search}`);
        }
    }
    
    // 立即切换到初始tab（不添加历史记录，因为这是首次加载）
    // switchToTab内部也会更新URL，但这里我们已经更新了，避免重复
    switchToTab(initialTab, false);
    
    // 为每个tab按钮添加点击事件
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetTab = tab.dataset.tab;
            switchToTab(targetTab, true); // 点击切换时添加历史记录
        });
    });
    
    // 返回当前激活的tab，供其他模块使用
    return initialTab;
}

// 行情模块
let currentPage = 1;
const pageSize = 30;
let isLoading = false;
let hasMore = true;
let currentMarket = 'a';

// 已移除marketRefreshInterval，改用SSE实时推送

// 行情页滚动处理函数（提升到全局作用域，供loadMarket使用）
let marketScrollTimer = null;
function handleMarketScroll() {
    // 检查行情页是否激活
    const marketTab = document.getElementById('market-tab');
    if (!marketTab || !marketTab.classList.contains('active')) {
        return;
    }
    
    // 防抖处理
    if (marketScrollTimer) {
        clearTimeout(marketScrollTimer);
    }
    
    marketScrollTimer = setTimeout(() => {
        // 重新获取容器引用（可能DOM已更新）
        const currentContainer = document.querySelector('.stock-list-container');
        let scrollTop, scrollHeight, clientHeight;
        let usingContainer = false;
        
        // 优先使用容器滚动（移动端和桌面端都支持）
        const isMobile = window.innerWidth <= 768;
        
        if (currentContainer) {
            const containerScrollHeight = currentContainer.scrollHeight;
            const containerClientHeight = currentContainer.clientHeight;
            
            // 移动端：始终优先使用容器滚动（因为移动端CSS设置了overflow-y: auto）
            // 桌面端：只有当容器可以滚动时才使用容器滚动
            if (isMobile) {
                // 移动端：只要容器存在且有内容，就使用容器滚动
                if (containerScrollHeight > 0 && containerClientHeight > 0) {
                    scrollTop = currentContainer.scrollTop;
                    scrollHeight = containerScrollHeight;
                    clientHeight = containerClientHeight;
                    usingContainer = true;
                }
            } else {
                // 桌面端：只有当容器可以滚动时才使用容器滚动
                const threshold = 5; // 桌面端5px容差
                if (containerScrollHeight > containerClientHeight + threshold) {
                    scrollTop = currentContainer.scrollTop;
                    scrollHeight = containerScrollHeight;
                    clientHeight = containerClientHeight;
                    usingContainer = true;
                }
            }
        }
        
        // 如果容器无法滚动，使用window滚动（仅桌面端fallback）
        if (!usingContainer && !isMobile) {
            scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            scrollHeight = document.documentElement.scrollHeight;
            clientHeight = window.innerHeight;
        } else if (!usingContainer && isMobile) {
            // 移动端：如果容器滚动失败，也尝试window滚动作为备用
            // 但这种情况应该很少发生
            scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            scrollHeight = document.documentElement.scrollHeight;
            clientHeight = window.innerHeight;
            console.warn('[行情] 移动端：容器滚动检测失败，使用window滚动作为备用');
        }
        
        // 距离底部100px时加载下一页
        const distanceToBottom = scrollHeight - (scrollTop + clientHeight);
        
        // 详细日志（仅在接近底部时输出，避免日志过多）
        if (distanceToBottom < 200) {
            console.log('[行情] 滚动检测:', { 
                distanceToBottom: distanceToBottom.toFixed(2),
                scrollTop: scrollTop.toFixed(2),
                scrollHeight,
                clientHeight,
                isLoading, 
                hasMore, 
                currentPage,
                usingContainer: usingContainer ? 'container' : 'window',
                containerScrollHeight: currentContainer?.scrollHeight,
                containerClientHeight: currentContainer?.clientHeight,
                containerScrollTop: currentContainer?.scrollTop,
                shouldLoad: distanceToBottom <= 100 && !isLoading && hasMore
            });
        }
        
        if (distanceToBottom <= 100 && !isLoading && hasMore) {
            console.log('[行情] ✅ 触发无限滚动，加载下一页，当前页:', currentPage);
            loadMarket();
        }
    }, 150); // 增加防抖时间到150ms，减少频繁触发
}

// 设置滚动监听器（提升到全局作用域，供loadMarket使用）
let scrollListenersSetup = false;
let containerScrollListenerSetup = false;
function setupMarketScrollListeners() {
    const currentContainer = document.querySelector('.stock-list-container');
    
    // 监听容器滚动（移动端和桌面端都支持）
    const isMobile = window.innerWidth <= 768;
    
    if (currentContainer) {
        // 移除旧的监听器（如果存在）- 使用命名函数引用确保能正确移除
        if (containerScrollListenerSetup) {
            currentContainer.removeEventListener('scroll', handleMarketScroll, { passive: true });
        }
        // 添加新的监听器
        currentContainer.addEventListener('scroll', handleMarketScroll, { passive: true });
        containerScrollListenerSetup = true;
        
        const rowCount = document.getElementById('stock-list')?.children.length || 0;
        const canScroll = currentContainer.scrollHeight > currentContainer.clientHeight + (isMobile ? 1 : 5);
        console.log('[行情] ✅ 已设置容器滚动监听器', {
            clientHeight: currentContainer.clientHeight,
            scrollHeight: currentContainer.scrollHeight,
            scrollTop: currentContainer.scrollTop,
            canScroll: canScroll,
            rowCount: rowCount,
            isMobile: isMobile,
            containerStyle: window.getComputedStyle(currentContainer).overflowY
        });
    } else {
        console.warn('[行情] ⚠️ 容器不存在，无法设置滚动监听');
    }
    
    // window滚动监听器只设置一次（避免重复添加）
    // 移动端也设置window滚动作为备用（虽然主要使用容器滚动）
    if (!scrollListenersSetup) {
        window.addEventListener('scroll', handleMarketScroll, { passive: true });
        console.log('[行情] ✅ 已设置window滚动监听器（备用）', { isMobile: isMobile });
        scrollListenersSetup = true;
    }
}

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
    
    // 立即设置一次
    setupMarketScrollListeners();
    
    // 延迟再次设置（确保DOM完全加载）
    setTimeout(setupMarketScrollListeners, 500);
    
    // 移动端额外多次延迟设置，确保容器高度计算正确和滚动监听器正确绑定
    if (window.innerWidth <= 768) {
        setTimeout(() => {
            setupMarketScrollListeners();
            console.log('[行情] 移动端：延迟1000ms设置滚动监听器');
            setTimeout(() => {
                setupMarketScrollListeners();
                console.log('[行情] 移动端：延迟1500ms设置滚动监听器');
            }, 500);
        }, 1000);
    }
    
    // 监听窗口大小变化（包括移动端横竖屏切换）
    let resizeTimer = null;
    window.addEventListener('resize', () => {
        if (resizeTimer) {
            clearTimeout(resizeTimer);
        }
        resizeTimer = setTimeout(() => {
            console.log('[行情] 窗口大小变化，重新设置滚动监听器');
            setupMarketScrollListeners();
            // 移动端额外延迟
            if (window.innerWidth <= 768) {
                setTimeout(setupMarketScrollListeners, 300);
            }
        }, 300);
    });
    
    // 监听tab切换，重新设置滚动监听器
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.getAttribute('data-tab');
            if (tab === 'market') {
                setTimeout(() => {
                    setupMarketScrollListeners();
                    // 移动端额外延迟
                    if (window.innerWidth <= 768) {
                        setTimeout(setupMarketScrollListeners, 300);
                    }
                }, 100);
            }
        });
    });
    
    // 注意：不在这里加载数据，由startApp()根据当前tab决定是否加载
    // 不再使用定时刷新，改用SSE实时推送
}

// 已移除silentRefreshMarket函数，改用SSE实时推送实现无感刷新

function resetAndLoadMarket() {
    currentPage = 1;
    hasMore = true;
    document.getElementById('stock-list').innerHTML = '';
    loadMarket();
}

// 初始化时更新按钮状态
function updateWatchlistButtonStates() {
    const watchlist = getWatchlist();
    const watchlistCodes = new Set(watchlist.map(s => String(s.code).trim()));
    
    console.log('[按钮状态] 更新按钮状态，当前自选股:', Array.from(watchlistCodes));
    
    document.querySelectorAll('.add-watchlist-btn').forEach(btn => {
        const code = String(btn.getAttribute('data-code') || '').trim();
        if (!code) {
            console.warn('[按钮状态] 按钮缺少data-code属性:', btn);
            return;
        }
        
        const isInWatchlist = watchlistCodes.has(code);
        
        if (isInWatchlist) {
            btn.textContent = '已添加';
            btn.style.background = '#94a3b8';
            btn.disabled = true;
            btn.style.cursor = 'not-allowed';
            btn.style.opacity = '0.6';
        } else {
            btn.textContent = '加入自选';
            btn.style.background = '#10b981';
            btn.disabled = false;
            btn.style.cursor = 'pointer';
            btn.style.opacity = '1';
        }
        
        // 确保按钮可以点击（移除可能存在的阻止点击的样式）
        btn.style.pointerEvents = isInWatchlist ? 'none' : 'auto';
    });
    
    console.log('[按钮状态] 按钮状态更新完成，共更新', document.querySelectorAll('.add-watchlist-btn').length, '个按钮');
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
    
    // 行情页每次都刷新，不再检查是否已有数据
    // 但如果当前正在加载中，跳过重复请求（已在函数开头检查isLoading）
    
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
            controller.abort('Request timeout after 10 seconds');
        }, 10000); // 10秒超时
        
        console.log(`[行情] 加载行情数据: market=${market}, page=${currentPage}, pageSize=${pageSize}`);
        const response = await apiFetch(`${API_BASE}/api/market/${market}/spot?page=${currentPage}&page_size=${pageSize}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        console.log(`[行情] 收到响应: status=${response.status}, ok=${response.ok}`);
        
        // 再次检查行情页是否仍然激活
        if (!marketTab || !marketTab.classList.contains('active')) {
            console.log('行情页已切换，取消加载');
            isLoading = false;
            return;
        }
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        console.log(`[行情] 解析结果: code=${result.code}, dataLength=${result.data?.length || 0}`);
        
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
                
                // 如果是第一页且首次加载，连接SSE实时推送
                if (currentPage === 1) {
                    connectSSE('market');
                }
                
                // 数据加载完成后，检查容器状态并重新设置滚动监听器
                // 使用多个延迟确保DOM完全更新（特别是移动端）
                setTimeout(() => {
                    const container = document.querySelector('.stock-list-container');
                    const tbodyEl = document.getElementById('stock-list');
                    if (container && tbodyEl) {
                        const rowCount = tbodyEl.children.length;
                        const canScroll = container.scrollHeight > container.clientHeight + 5;
                        console.log('[行情] 数据加载完成，容器状态:', {
                            clientHeight: container.clientHeight,
                            scrollHeight: container.scrollHeight,
                            scrollTop: container.scrollTop,
                            canScroll: canScroll,
                            rowCount: rowCount,
                            currentPage: currentPage,
                            hasMore: hasMore,
                            isMobile: window.innerWidth <= 768
                        });
                        
                        // 重新设置滚动监听器（确保监听器已绑定到最新的DOM）
                        setupMarketScrollListeners();
                        
                        // 移动端额外延迟多次，确保容器高度计算正确和滚动监听器正确绑定
                        if (window.innerWidth <= 768) {
                            setTimeout(() => {
                                setupMarketScrollListeners();
                                console.log('[行情] 移动端：二次设置滚动监听器');
                                
                                // 第三次设置，确保万无一失
                                setTimeout(() => {
                                    setupMarketScrollListeners();
                                    console.log('[行情] 移动端：三次设置滚动监听器');
                                }, 300);
                            }, 300);
                        }
                    }
                }, 200);
                
                // 检查是否还有更多数据
                if (result.pagination) {
                    hasMore = currentPage < result.pagination.total_pages;
                    console.log(`[行情] 分页信息: 当前页=${currentPage}, 总页数=${result.pagination.total_pages}, hasMore=${hasMore}`);
                    if (hasMore) {
                        currentPage++;
                        console.log(`[行情] ✅ 还有更多数据，下一页=${currentPage}`);
                    } else {
                        console.log(`[行情] ⚠️ 没有更多数据了，当前页=${currentPage}, 总页数=${result.pagination.total_pages}`);
                    }
                } else {
                    // 如果没有分页信息，根据返回的数据量判断
                    hasMore = result.data.length === pageSize;
                    console.log(`[行情] 无分页信息，根据数据量判断: 返回${result.data.length}条, pageSize=${pageSize}, hasMore=${hasMore}`);
                    if (hasMore) {
                        currentPage++;
                        console.log(`[行情] ✅ 还有更多数据，下一页=${currentPage}`);
                    } else {
                        console.log(`[行情] ⚠️ 没有更多数据了，返回数据量=${result.data.length}, pageSize=${pageSize}`);
                    }
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
        console.error('[行情] 加载失败:', error);
        
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
            let errorMsg = '加载失败';
            if (error.name === 'AbortError' || error.message?.includes('aborted')) {
                errorMsg = '请求超时，请稍后重试';
            } else if (error.message) {
                errorMsg = `加载失败: ${error.message}`;
            } else {
                errorMsg = '网络错误，请检查网络连接';
            }
            tbody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #ef4444;">${errorMsg}<br/><button onclick="location.reload()" style="margin-top: 10px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">刷新页面</button></td></tr>`;
        }
        hasMore = false;
    } finally {
        isLoading = false;
        console.log('[行情] loadMarket完成, isLoading=false, currentPage=', currentPage, ', hasMore=', hasMore);
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
                <button class="add-watchlist-btn" data-code="${stock.code}" data-name="${stock.name}" style="padding: 4px 8px; background: ${isInWatchlist ? '#94a3b8' : '#10b981'}; color: white; border: none; border-radius: 4px; cursor: ${isInWatchlist ? 'not-allowed' : 'pointer'}; ${isInWatchlist ? 'opacity: 0.6; pointer-events: none;' : 'opacity: 1; pointer-events: auto;'}" ${isInWatchlist ? 'disabled' : ''}>${isInWatchlist ? '已添加' : '加入自选'}</button>
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
        // 移除旧的事件监听器（通过克隆节点）
        const newBtn = btn.cloneNode(true);
        btn.parentNode.replaceChild(newBtn, btn);
        
        newBtn.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            const code = String(this.getAttribute('data-code') || '').trim();
            const name = String(this.getAttribute('data-name') || code).trim();
            
            if (!code) {
                console.error('[自选] 按钮缺少data-code属性');
                return;
            }
            
            // 检查是否已在自选列表中
            const currentWatchlist = getWatchlist();
            if (currentWatchlist.some(s => String(s.code).trim() === code)) {
                console.log('[自选] 股票已在自选列表中:', code);
                return;
            }
            
            console.log('[自选] 添加股票到自选:', code, name);
            addToWatchlist(code, name);
        };
    });
}

function formatVolume(vol) {
    if (!vol) return '-';
    if (vol >= 100000000) return (vol / 100000000).toFixed(2) + '亿';
    if (vol >= 10000) return (vol / 10000).toFixed(2) + '万';
    return vol.toString();
}

function formatAmount(amount) {
    if (!amount) return '-';
    if (amount >= 100000000) return (amount / 100000000).toFixed(2) + '亿';
    if (amount >= 10000) return (amount / 10000).toFixed(2) + '万';
    return amount.toFixed(2);
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
                const code = String(btn.getAttribute('data-code') || '').trim();
                const isInWatchlist = watchlist.some(s => String(s.code).trim() === code);
                
                if (isInWatchlist) {
                    btn.textContent = '已添加';
                    btn.style.background = '#94a3b8';
                    btn.disabled = true;
                    btn.style.cursor = 'not-allowed';
                    btn.style.opacity = '0.6';
                    btn.style.pointerEvents = 'none';
                } else {
                    btn.textContent = '加入自选';
                    btn.style.background = '#10b981';
                    btn.disabled = false;
                    btn.style.cursor = 'pointer';
                    btn.style.opacity = '1';
                    btn.style.pointerEvents = 'auto';
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
    
    // 初始化指标控制面板内容（在打开模态框之前就填充，不依赖指标数据加载）
    initIndicatorPanels();
    
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

// 初始化指标控制面板内容（独立函数，不依赖指标数据）
function initIndicatorPanels() {
    const volumeContainer = document.getElementById('volume-controls');
    const emaContainer = document.getElementById('ema-controls');
    if (!volumeContainer || !emaContainer) {
        console.warn('指标控制面板容器不存在');
        return;
    }
    
    // 从localStorage加载配置
    const savedEmaConfig = localStorage.getItem('emaConfig');
    if (savedEmaConfig) {
        try {
            emaConfig = JSON.parse(savedEmaConfig);
        } catch (e) {
            console.warn('解析EMA配置失败:', e);
        }
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
    
    // 绑定事件（每次重新绑定，因为innerHTML会清除事件）
    const volumeToggle = document.getElementById('volume-toggle');
    if (volumeToggle) {
        // 移除旧的事件监听器（如果存在）
        const newVolumeToggle = volumeToggle.cloneNode(true);
        volumeToggle.parentNode.replaceChild(newVolumeToggle, volumeToggle);
        newVolumeToggle.addEventListener('change', function(e) {
            volumeVisible = e.target.checked;
            localStorage.setItem('volumeVisible', volumeVisible);
            if (volumeSeries) {
                volumeSeries.applyOptions({ visible: volumeVisible });
            }
        });
    }
    
    const emaToggle = document.getElementById('ema-toggle');
    if (emaToggle) {
        // 移除旧的事件监听器（如果存在）
        const newEmaToggle = emaToggle.cloneNode(true);
        emaToggle.parentNode.replaceChild(newEmaToggle, emaToggle);
        newEmaToggle.addEventListener('change', function(e) {
            emaConfig.enabled = e.target.checked;
            localStorage.setItem('emaConfig', JSON.stringify(emaConfig));
            const emaGroup = document.getElementById('ema-config-group');
            if (emaGroup) {
                emaGroup.style.display = emaConfig.enabled ? '' : 'none';
            }
            updateEMA();
        });
    }
    
    // EMA 数值输入：输入即生效（无需"应用"按钮）
    const emaInputs = ['ema1', 'ema2', 'ema3'];
    const defaultPeriods = [20, 50, 100];
    emaInputs.forEach((id, index) => {
        const inputEl = document.getElementById(id);
        if (!inputEl) return;
        // 移除旧的事件监听器（如果存在）
        const newInputEl = inputEl.cloneNode(true);
        inputEl.parentNode.replaceChild(newInputEl, inputEl);
        newInputEl.addEventListener('input', (e) => {
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
    
    // 绑定折叠行为（点击"成交量"或"EMA"头部时展开/收起）
    // 直接绑定到每个toggle按钮，确保事件正常工作
    // 使用事件委托，避免重复绑定问题
    const controlsBar = document.querySelector('.kline-controls-bar');
    if (controlsBar) {
        // 移除旧的事件监听器（如果存在）
        if (controlsBar._indicatorToggleHandler) {
            controlsBar.removeEventListener('click', controlsBar._indicatorToggleHandler);
        }
        
        // 创建新的事件处理函数
        controlsBar._indicatorToggleHandler = (e) => {
            // 检查点击的是否是indicator-toggle或其子元素
            const toggle = e.target.closest('.indicator-toggle');
            if (!toggle) return;
            
            e.stopPropagation(); // 阻止事件冒泡
            e.preventDefault(); // 阻止默认行为
            
            const targetId = toggle.getAttribute('data-target');
            const content = document.getElementById(targetId);
            if (!content) {
                console.warn('找不到目标元素:', targetId);
                return;
            }
            
            console.log('点击了indicator-toggle:', targetId, '当前状态:', toggle.classList.contains('active'));
            
            // 切换active类
            const isActive = toggle.classList.contains('active');
            if (isActive) {
                toggle.classList.remove('active');
                content.classList.remove('active');
                console.log('关闭面板:', targetId);
            } else {
                // 关闭其他已打开的panel
                document.querySelectorAll('.indicator-toggle.active').forEach(otherToggle => {
                    otherToggle.classList.remove('active');
                    const otherPanel = document.getElementById(otherToggle.getAttribute('data-target'));
                    if (otherPanel) otherPanel.classList.remove('active');
                });
                
                toggle.classList.add('active');
                content.classList.add('active');
                console.log('打开面板:', targetId, '元素存在:', !!content, '有active类:', content.classList.contains('active'));
            }
        };
        
        controlsBar.addEventListener('click', controlsBar._indicatorToggleHandler);
    }
    
    // 点击外部关闭panel（使用事件委托，避免重复绑定）
    // 使用全局变量标记，避免重复绑定
    if (!window.klineExternalClickBound) {
        window.klineExternalClickBound = true;
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.kline-indicators-group')) {
                document.querySelectorAll('.indicator-toggle.active').forEach(toggle => {
                    toggle.classList.remove('active');
                    const panel = document.getElementById(toggle.getAttribute('data-target'));
                    if (panel) panel.classList.remove('active');
                });
            }
        });
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
    
    // 使用 history API 添加一个历史记录，用于处理返回按钮（使用路径模式）
    if (window.history && window.history.pushState) {
        const currentTab = localStorage.getItem('currentTab') || 'market';
        const url = `/kline/${code}${window.location.search}`;
        window.history.pushState({ klineModal: true, code: code, name: name, tab: currentTab }, '', url);
    }
    
    modal.style.display = 'flex';
    
    // 在移动端，确保模态框内容从顶部可见（不被地址栏遮挡）
    // 1. 立即滚动到顶部
    if (modal) {
        modal.scrollTop = 0;
    }
    
    // 2. 动态设置模态框高度，使用实际窗口高度（不考虑地址栏）
    const setModalHeight = () => {
        const modalContent = document.querySelector('.kline-modal-content');
        if (modalContent) {
            // 使用window.innerHeight（实际可视区域高度）而不是100vh
            const actualHeight = window.innerHeight;
            modalContent.style.height = `${actualHeight}px`;
            modalContent.style.maxHeight = `${actualHeight}px`;
            console.log('[K线] 设置模态框高度:', actualHeight);
        }
    };
    
    // 立即设置高度
    setModalHeight();
    
    // 监听窗口大小变化（地址栏显示/隐藏时）
    const handleResize = () => {
        setModalHeight();
        // 确保滚动到顶部
        if (modal) {
            modal.scrollTop = 0;
        }
        // 确保内容区域也滚动到顶部
        const modalContent = document.querySelector('.kline-modal-content');
        if (modalContent) {
            modalContent.scrollTop = 0;
        }
    };
    
    // 移除旧的监听器（如果存在）
    if (window._klineModalResizeHandler) {
        window.removeEventListener('resize', window._klineModalResizeHandler);
        window.removeEventListener('orientationchange', window._klineModalResizeHandler);
    }
    
    // 添加新的监听器
    window.addEventListener('resize', handleResize);
    window.addEventListener('orientationchange', handleResize);
    
    // 保存清理函数，关闭模态框时移除监听器
    window._klineModalResizeHandler = handleResize;
    
    // 等待模态框完全显示后再初始化面板和加载图表
    // 使用requestAnimationFrame + setTimeout确保DOM已完全渲染（特别是手机端）
    requestAnimationFrame(() => {
        setTimeout(() => {
            // 再次确保滚动到顶部和高度设置（防止浏览器自动调整）
            setModalHeight();
            if (modal) {
                modal.scrollTop = 0;
            }
            // 确保指标面板已初始化（在模态框显示后）
            initIndicatorPanels();
            loadChart(code);
        }, 150); // 稍微增加延迟，确保手机端布局稳定
    });
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

function closeKlineModal(event) {
    // 阻止事件冒泡和默认行为，防止触发浏览器返回
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    
    const modal = document.getElementById('kline-modal');
    if (!modal || modal.style.display === 'none') {
        return false; // 如果已经关闭，直接返回
    }
    
    // 清理窗口大小变化监听器
    if (window._klineModalResizeHandler) {
        window.removeEventListener('resize', window._klineModalResizeHandler);
        window.removeEventListener('orientationchange', window._klineModalResizeHandler);
        window._klineModalResizeHandler = null;
    }
    
    // 如果当前历史记录是K线图状态，替换为之前的tab页面（使用路径模式）
    if (window.history && window.history.replaceState) {
        const state = window.history.state || {};
        const currentTab = state.tab || localStorage.getItem('currentTab') || 'market';
        const url = `/${currentTab}${window.location.search}`;
        window.history.replaceState({ tab: currentTab }, '', url);
    }
    
    modal.style.display = 'none';
    
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
    
    // 返回 false 确保不会触发其他操作
    return false;
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
        
        console.log(`[K线] 加载K线数据: ${code}, 市场: ${market}, 周期: ${period}, 日期范围: ${startDateStr} ~ ${endDateStr}`);
        
        // 添加超时控制（增加到30秒，避免504超时）
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            console.warn('[K线] 请求超时，取消请求');
            controller.abort('K线数据请求超时（30秒）');
        }, 30000); // 30秒超时（增加超时时间，避免504错误）
        
        let response, result;
        try {
            // 根据市场类型选择对应的API接口
            const klineUrl = `${API_BASE}/api/market/${market}/kline?code=${code}&period=${period}&start_date=${startDateStr}&end_date=${endDateStr}`;
            console.log(`[K线] 请求URL: ${klineUrl}`);
            
            response = await apiFetch(klineUrl, {
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            console.log(`[K线] 收到响应: status=${response.status}, ok=${response.ok}`);
            
            if (!response.ok) {
                const errorText = await response.text().catch(() => '');
                console.error(`[K线] HTTP错误: ${response.status}, ${errorText}`);
                throw new Error(`HTTP ${response.status}: ${response.statusText || errorText}`);
            }
            
            result = await response.json();
            console.log(`[K线] 解析结果: code=${result.code}, dataLength=${result.data?.length || 0}`);
        } catch (fetchError) {
            clearTimeout(timeoutId);
            
            console.error('K线数据请求失败:', fetchError);
            
            // 如果是超时错误或被取消，提供重试提示
            if (fetchError.name === 'AbortError' || fetchError.message?.includes('aborted')) {
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                    <div style="font-size: 18px; margin-bottom: 12px;">⏱️ 请求超时</div>
                    <div style="color: #94a3b8; margin-bottom: 16px;">服务器响应时间过长，请稍后重试</div>
                    <button id="retry-kline-btn" style="padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">
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
            
            // 其他网络错误 - 提供更详细的错误信息
            let errorMessage = '连接失败';
            let errorDetail = '';
            
            if (fetchError.message) {
                errorMessage = fetchError.message;
            }
            
            // 检查是否是网络连接问题
            if (fetchError.message && fetchError.message.includes('Failed to fetch')) {
                errorMessage = '无法连接到服务器';
                errorDetail = '<div style="color: #94a3b8; margin-top: 8px; font-size: 13px;">请检查网络连接或服务器状态</div>';
            } else if (fetchError.message && fetchError.message.includes('401')) {
                errorMessage = '认证失败';
                errorDetail = '<div style="color: #94a3b8; margin-top: 8px; font-size: 13px;">请重新登录</div>';
            } else if (fetchError.message && fetchError.message.includes('404')) {
                errorMessage = '接口不存在';
                errorDetail = '<div style="color: #94a3b8; margin-top: 8px; font-size: 13px;">请检查API地址是否正确</div>';
            } else if (fetchError.message && fetchError.message.includes('500')) {
                errorMessage = '服务器错误';
                errorDetail = '<div style="color: #94a3b8; margin-top: 8px; font-size: 13px;">服务器内部错误，请稍后重试</div>';
            }
            
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">
                <div style="font-size: 18px; margin-bottom: 12px;">❌ ${errorMessage}</div>
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
                    try {
                        loadIndicators(code).catch(err => {
                            console.error('加载指标失败（异步）:', err);
                            // 静默失败，不影响K线图显示
                        });
                    } catch (err) {
                        console.error('调用loadIndicators失败:', err);
                        // 静默失败，不影响K线图显示
                    }
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
    // 获取容器的实际尺寸，如果为0则延迟重试
    let containerWidth, containerHeight;
    const containerRect = container.getBoundingClientRect();
    containerWidth = containerRect.width || container.offsetWidth || container.clientWidth || 0;
    containerHeight = containerRect.height || container.offsetHeight || container.clientHeight || 0;
    
    // 如果容器尺寸为0或过小，延迟重试（可能是模态框还没完全显示）
    if (containerWidth < 100 || containerHeight < 100) {
        console.warn('容器尺寸不足，延迟重试', { width: containerWidth, height: containerHeight });
        // 使用requestAnimationFrame等待下一个渲染周期
        requestAnimationFrame(() => {
            setTimeout(() => {
                const retryRect = container.getBoundingClientRect();
                containerWidth = retryRect.width || container.offsetWidth || container.clientWidth || window.innerWidth - 40;
                containerHeight = retryRect.height || container.offsetHeight || container.clientHeight || Math.max(window.innerHeight * 0.6, 400);
                
                // 如果还是不够，使用窗口尺寸的合理比例（手机端）
                if (containerWidth < 100) {
                    containerWidth = window.innerWidth - 40;
                }
                if (containerHeight < 100) {
                    containerHeight = Math.max(window.innerHeight * 0.6, 400);
                }
                
                renderChartInternal(data, container, containerWidth, containerHeight);
            }, 100);
        });
        return;
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
            } else if (response.status === 500) {
                // 服务器错误，可能是数据格式问题，记录但不影响K线图
                console.error('加载指标失败 - 服务器错误:', response.status);
                try {
                    const errorText = await response.text();
                    console.error('错误详情:', errorText.substring(0, 200)); // 只显示前200字符
                } catch (e) {
                    // 忽略解析错误
                }
            } else {
                const errorText = await response.text();
                console.error('加载指标失败 - HTTP错误:', response.status, errorText.substring(0, 200));
            }
            return; // 静默失败，不影响K线图显示
        }
        
        const result = await response.json();
        
        if (result.code === 0 && result.data) {
            try {
                renderIndicators(result.data);
            } catch (renderError) {
                console.error('渲染指标失败:', renderError);
                // 渲染失败不影响K线图
            }
        } else {
            console.warn('加载指标失败 - API错误:', result.message || '未知错误');
        }
    } catch (error) {
        console.error('加载指标失败:', error);
        // 静默失败，不影响K线图显示
        // 确保不会因为指标加载失败而导致整个页面崩溃
        if (error instanceof TypeError && error.message.includes('JSON')) {
            console.warn('指标数据格式错误，跳过显示');
        }
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
    // 指标数据已加载，更新显示状态（面板内容已在initIndicatorPanels中初始化）
    if (volumeSeries) {
        volumeSeries.applyOptions({ visible: volumeVisible });
    }
    updateEMA();
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
// 同步锁，防止重复同步
let isSyncingWatchlist = false;
let lastSyncTime = 0;
const SYNC_COOLDOWN = 5000; // 5秒冷却时间

function initWatchlist() {
    console.log('[自选] 初始化自选股模块');
    
    // 初始化自选页无限滚动（监听容器滚动，而不是window滚动）
    let watchlistScrollTimer = null;
    
    // 监听容器滚动事件
    function setupWatchlistScrollListener() {
        const container = document.getElementById('watchlist-container');
        if (!container) {
            // 如果容器不存在，延迟重试
            setTimeout(setupWatchlistScrollListener, 100);
            return;
        }
        
        // 移除旧的监听器（如果存在）
        container.removeEventListener('scroll', handleWatchlistScroll);
        
        // 添加新的监听器
        container.addEventListener('scroll', handleWatchlistScroll);
        console.log('[自选] 滚动监听器已设置，监听容器:', container);
    }
    
    // 滚动处理函数
    function handleWatchlistScroll() {
        const watchlistTab = document.getElementById('watchlist-tab');
        if (!watchlistTab || !watchlistTab.classList.contains('active')) {
            return;
        }
        
        const container = document.getElementById('watchlist-container');
        if (!container) return;
        
        // 防抖处理
        if (watchlistScrollTimer) {
            clearTimeout(watchlistScrollTimer);
        }
        
        watchlistScrollTimer = setTimeout(() => {
            // 检查是否滚动到底部
            const scrollTop = container.scrollTop;
            const scrollHeight = container.scrollHeight;
            const clientHeight = container.clientHeight;
            
            // 距离底部200px时加载下一批
            if (scrollTop + clientHeight >= scrollHeight - 200 && 
                !watchlistIsLoading && 
                watchlistRenderedCount < watchlistAllStocks.length) {
                console.log('[自选] 触发无限滚动，加载下一批');
                watchlistIsLoading = true;
                requestAnimationFrame(() => {
                    renderWatchlistStocksBatch();
                    watchlistIsLoading = false;
                });
            }
        }, 100);
    }
    
    // 初始设置监听器
    setupWatchlistScrollListener();
    
    // 当tab切换时重新设置监听器（因为容器可能被重新创建）
    const tabBtns = document.querySelectorAll('.tab-btn');
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.getAttribute('data-tab');
            if (tab === 'watchlist') {
                setTimeout(setupWatchlistScrollListener, 100);
            }
        });
    });
    
    // 页面加载时从服务器同步自选股列表（带防抖）
    console.log('[自选] 开始从服务器同步自选股列表...');
    
    // 检查冷却时间
    const now = Date.now();
    if (now - lastSyncTime < SYNC_COOLDOWN) {
        console.log('[自选] 同步冷却中，跳过本次同步');
        return;
    }
    
    // 如果正在同步，跳过
    if (isSyncingWatchlist) {
        console.log('[自选] 正在同步中，跳过重复请求');
        return;
    }
    
    isSyncingWatchlist = true;
    lastSyncTime = now;
    
    syncWatchlistFromServer().then(serverData => {
        isSyncingWatchlist = false;
        if (serverData !== null) {
            console.log('[自选] 从服务器同步成功，共', serverData.length, '只股票');
            // 更新按钮状态
            updateWatchlistButtonStates();
            
            // 不再手动刷新自选页，依赖SSE推送来更新（无感刷新）
            // 如果当前在自选页，SSE会在连接时推送初始数据，后续变化也会通过SSE推送
            console.log('[自选] 等待SSE推送更新（无感刷新）');
        } else {
            console.log('[自选] 从服务器同步失败或数据为空，使用本地缓存');
        }
    }).catch(err => {
        isSyncingWatchlist = false;
        console.error('[自选] 从服务器同步失败:', err);
    });
    
    // SSE连接已在全局管理，当切换到自选页时会通过connectSSE('watchlist')连接
    
    // 监听 localStorage 变化，实现跨标签页同步
    window.addEventListener('storage', (e) => {
        if (e.key === 'watchlist') {
            console.log('[自选] 检测到跨标签页自选股列表变化，同步更新');
            // 更新按钮状态
            updateWatchlistButtonStates();
            
            // 不再手动刷新自选页，依赖SSE推送来更新（无感刷新）
            // 跨标签页的变化会通过SSE推送同步，_doWatchlistSync会处理更新
            console.log('[自选] 等待SSE推送更新（无感刷新）');
            
            // 如果当前在行情页，更新按钮状态
            const marketTab = document.getElementById('market-tab');
            if (marketTab && marketTab.classList.contains('active')) {
                updateWatchlistButtonStates();
            }
        }
    });
    
    // 注意：首次加载数据已经在startApp中根据当前tab处理，这里不需要再次调用
    // 避免在非自选页时也触发数据加载，导致显示加载状态
}

// 加载自选股列表（使用和行情页一样的加载方法）
async function loadWatchlist(forceRefresh = false) {
    console.log('[自选] loadWatchlist: 开始加载，forceRefresh=', forceRefresh);
    
    // 检查当前是否在自选页，如果不在则跳过加载（避免在不应该加载时显示加载状态）
    const watchlistTab = document.getElementById('watchlist-tab');
    if (!watchlistTab || !watchlistTab.classList.contains('active')) {
        console.log('[自选] loadWatchlist: 当前不在自选页，跳过加载');
        // 即使不在自选页，也要更新按钮状态
        updateWatchlistButtonStates();
        return;
    }
    
    const watchlist = getWatchlist();
    console.log('[自选] loadWatchlist: 当前自选列表:', watchlist.map(s => s.code), '共', watchlist.length, '只');
    const container = document.getElementById('watchlist-container');
    const tbody = document.getElementById('watchlist-stock-list');
    
    if (!container) {
        console.warn('[自选] loadWatchlist: 容器不存在，退出');
        return;
    }
    
    if (watchlist.length === 0) {
        console.log('[自选] loadWatchlist: 自选列表为空，显示占位符');
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
                console.log('[自选] loadWatchlist: 使用缓存的自选股数据，共', cachedData.length, '只');
                renderWatchlistStocks(cachedData, false, true); // silent=true 静默渲染
                return;
            }
        } else {
            console.log('[自选] loadWatchlist: 强制刷新，跳过缓存检查');
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
        
        // 使用批量查询接口，直接查询自选股的行情数据（大幅提升加载速度）
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort('Request timeout after 15 seconds'), 15000); // 15秒超时
            
            const response = await apiFetch(`${API_BASE}/api/market/spot/batch`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(watchlistCodes),
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const result = await response.json();
            
            if (result.code === 0 && Array.isArray(result.data)) {
                // 构建代码到股票数据的映射（用于快速查找）
                const stockMap = {};
                result.data.forEach(stock => {
                    const code = String(stock.code || '').trim();
                    if (code) {
                        stockMap[code] = stock;
                    }
                });
                
                // 按照自选列表的顺序构建结果，保持原有顺序
                const watchlistStocks = watchlistCodes.map(code => {
                    const stock = stockMap[code];
                    if (stock) {
                        return stock;
                    }
                    // 如果找不到，返回基本信息（可能股票已退市或数据不存在）
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
                
                // 渲染股票列表（强制刷新时强制渲染）
                console.log('[自选] loadWatchlist: 准备渲染，forceRefresh=', forceRefresh);
                renderWatchlistStocks(watchlistStocks, forceRefresh);
                return; // 成功返回
            } else {
                throw new Error(result.message || '批量查询失败');
            }
        } catch (fetchError) {
            console.error('批量查询自选股行情失败:', fetchError);
            
            // 如果是AbortError（请求被取消），不抛出错误，而是尝试使用缓存
            if (fetchError.name === 'AbortError' || fetchError.message?.includes('aborted')) {
                console.warn('[自选] 请求被取消，尝试使用缓存数据');
                const cachedData = getCachedWatchlistData();
                if (cachedData && cachedData.length > 0) {
                    console.log('[自选] loadWatchlist: 使用缓存数据，共', cachedData.length, '只');
                    renderWatchlistStocks(cachedData, forceRefresh);
                    return;
                }
                // 如果没有缓存，显示友好提示而不是错误
                const tbodyEl = document.getElementById('watchlist-stock-list');
                if (tbodyEl) {
                    tbodyEl.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px; color: #94a3b8;">请求超时，请稍后刷新</td></tr>';
                }
                return;
            }
            
            // 如果批量查询失败，尝试使用缓存
            const cachedData = getCachedWatchlistData();
            if (cachedData && cachedData.length > 0) {
                console.log('[自选] loadWatchlist: 批量查询失败，使用缓存数据，共', cachedData.length, '只');
                renderWatchlistStocks(cachedData, forceRefresh);
                return;
            }
            // 如果缓存也没有，抛出错误进入下面的错误处理
            throw fetchError;
        }
        
    } catch (error) {
        console.error('加载自选股失败:', error);
        
        // 如果是AbortError（请求被取消），显示友好提示
        if (error.name === 'AbortError' || error.message?.includes('aborted')) {
            const tbodyEl = document.getElementById('watchlist-stock-list');
            if (tbodyEl) {
                tbodyEl.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 20px; color: #94a3b8;">请求超时，请稍后刷新</td></tr>';
            }
            return;
        }
        
        // 如果加载失败，尝试使用缓存
        const cachedData = getCachedWatchlistData();
        if (cachedData && cachedData.length > 0) {
            console.log('[自选] loadWatchlist: 加载失败，使用缓存数据，共', cachedData.length, '只');
            renderWatchlistStocks(cachedData, forceRefresh);
        } else {
            const tbodyEl = document.getElementById('watchlist-stock-list');
            if (tbodyEl) {
                const errorMsg = error.message || '未知错误';
                tbodyEl.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #ef4444;">加载失败: ${errorMsg}</td></tr>`;
            }
        }
    }
}

// 自选股无限滚动相关变量
let watchlistAllStocks = []; // 所有自选股数据
let watchlistRenderedCount = 0; // 已渲染的数量
let watchlistPageSize = 30; // 每批渲染的数量
let watchlistIsLoading = false; // 是否正在加载

// 渲染自选股列表（支持无限滚动）
function renderWatchlistStocks(watchlistStocks, forceRender = false, silent = false) {
    const tbodyEl = document.getElementById('watchlist-stock-list');
    const container = document.getElementById('watchlist-container');
    
    // 保存滚动位置（仅在强制渲染时保存，避免影响正常滚动）
    let savedScrollTop = 0;
    if (forceRender && container) {
        savedScrollTop = container.scrollTop;
    }
    
    if (!silent) {
        console.log('[自选] renderWatchlistStocks: 准备渲染', watchlistStocks.length, '只股票, forceRender=', forceRender);
    }
    
    // 如果强制渲染，重置无限滚动状态
    if (forceRender) {
        watchlistAllStocks = watchlistStocks;
        watchlistRenderedCount = 0;
        if (tbodyEl) {
            tbodyEl.innerHTML = '';
        }
    } else {
        // 如果不是强制渲染，检查数据是否有变化
        const existingRows = tbodyEl ? Array.from(tbodyEl.querySelectorAll('tr')) : [];
        const existingCodes = existingRows.map(tr => {
            const firstTd = tr.querySelector('td:first-child');
            return firstTd ? firstTd.textContent.trim() : null;
        }).filter(code => code && code !== '暂无数据' && !code.includes('加载'));
        
        const newCodes = watchlistStocks.map(s => String(s.code).trim());
        
        if (!silent) {
            console.log('[自选] renderWatchlistStocks: 现有代码:', existingCodes.length, '新代码:', newCodes.length);
        }
        
        // 如果数据相同且已全部渲染，不重新渲染（无感更新）
        if (existingCodes.length === newCodes.length && 
            existingCodes.length > 0 &&
            existingCodes.every((code, idx) => code === newCodes[idx]) &&
            watchlistRenderedCount >= watchlistAllStocks.length) {
            if (!silent) {
                console.log('[自选] renderWatchlistStocks: 数据未变化且已全部渲染，跳过渲染');
            }
            return;
        }
        
        // 数据有变化，更新全部数据并重置渲染
        watchlistAllStocks = watchlistStocks;
        watchlistRenderedCount = 0;
        if (tbodyEl) {
            tbodyEl.innerHTML = '';
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
    
    if (watchlistAllStocks.length === 0) {
        finalTbodyEl.innerHTML = '<tr><td colspan="6" class="loading">暂无数据</td></tr>';
        return;
    }
    
    // 渲染第一批数据（无限滚动）
    renderWatchlistStocksBatch();
    
    if (!silent) {
        console.log('[自选] renderWatchlistStocks: 开始分批渲染，总数:', watchlistAllStocks.length);
    }
    
    // 恢复滚动位置（仅在强制渲染时恢复）
    if (forceRender && container && savedScrollTop > 0) {
        // 延迟恢复，确保DOM已更新
        setTimeout(() => {
            container.scrollTop = savedScrollTop;
        }, 100);
    }
    
    // 更新按钮状态（确保按钮状态正确）
    updateWatchlistButtonStates();
}

// 分批渲染自选股（无限滚动）
function renderWatchlistStocksBatch() {
    if (watchlistIsLoading) return;
    
    const tbodyEl = document.getElementById('watchlist-stock-list');
    if (!tbodyEl) return;
    
    const watchlistTab = document.getElementById('watchlist-tab');
    if (!watchlistTab || !watchlistTab.classList.contains('active')) {
        return; // 不在自选页，不渲染
    }
    
    // 计算本次要渲染的范围
    const start = watchlistRenderedCount;
    const end = Math.min(start + watchlistPageSize, watchlistAllStocks.length);
    const batch = watchlistAllStocks.slice(start, end);
    
    if (batch.length === 0) {
        // 已全部渲染完成
        if (watchlistRenderedCount > 0 && watchlistRenderedCount >= watchlistAllStocks.length) {
            // 移除加载提示
            const loadingRow = tbodyEl.querySelector('tr.loading-more');
            if (loadingRow) {
                loadingRow.remove();
            }
        }
        return;
    }
    
    // 移除之前的加载提示
    const loadingRow = tbodyEl.querySelector('tr.loading-more');
    if (loadingRow) {
        loadingRow.remove();
    }
    
    // 渲染本批数据
    batch.forEach(stock => {
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
    
    watchlistRenderedCount = end;
    
    // 绑定移除按钮事件
    document.querySelectorAll('.remove-watchlist-btn').forEach(btn => {
        // 移除旧的事件监听器（通过克隆节点）
        const newBtn = btn.cloneNode(true);
        btn.parentNode.replaceChild(newBtn, btn);
        
        newBtn.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            const code = String(this.getAttribute('data-code') || '').trim();
            
            if (!code) {
                console.error('[自选] 移除按钮缺少data-code属性');
                return;
            }
            
            console.log('[自选] 移除股票:', code);
            removeFromWatchlist(code);
        };
    });
    
    // 如果还有更多数据，添加加载提示
    if (watchlistRenderedCount < watchlistAllStocks.length) {
        const loadingTr = document.createElement('tr');
        loadingTr.className = 'loading-more';
        loadingTr.innerHTML = '<td colspan="6" style="text-align: center; padding: 10px; color: #94a3b8;">加载中...</td>';
        tbodyEl.appendChild(loadingTr);
    }
    
    console.log(`[自选] 已渲染 ${watchlistRenderedCount}/${watchlistAllStocks.length} 只股票`);
}

// 从本地缓存快速获取自选股列表（同步，用于UI渲染）
function getWatchlistFromCache() {
    try {
        const data = localStorage.getItem('watchlist');
        return data ? JSON.parse(data) : [];
    } catch (e) {
        return [];
    }
}

// 从服务器同步自选股列表（异步，用于初始化）
async function syncWatchlistFromServer() {
    try {
        const url = `${API_BASE}/api/watchlist`;
        console.log('[自选] syncWatchlistFromServer: 请求URL:', url);
        const response = await apiFetch(url);
        console.log('[自选] syncWatchlistFromServer: 响应状态:', response.status, response.statusText);
        
        if (response.ok) {
            const result = await response.json();
            console.log('[自选] syncWatchlistFromServer: 响应数据:', result);
            if (result.code === 0 && Array.isArray(result.data)) {
                console.log('[自选] syncWatchlistFromServer: 同步成功，共', result.data.length, '只股票');
                // 保存到本地缓存
                localStorage.setItem('watchlist', JSON.stringify(result.data));
                return result.data;
            } else {
                console.warn('[自选] syncWatchlistFromServer: 响应格式错误:', result);
            }
        } else {
            console.warn('[自选] syncWatchlistFromServer: HTTP错误:', response.status);
        }
    } catch (e) {
        console.error('[自选] syncWatchlistFromServer: 异常:', e);
    }
    console.log('[自选] syncWatchlistFromServer: 同步失败，返回null');
    return null;
}

// 获取自选股列表（兼容旧代码，返回本地缓存）
function getWatchlist() {
    return getWatchlistFromCache();
}

// 保存自选股列表（同时保存到服务器和本地）
async function saveWatchlist(watchlist) {
    console.log('[自选] saveWatchlist: 开始保存，股票数量:', watchlist.length);
    // 先保存到本地缓存（快速响应）
    localStorage.setItem('watchlist', JSON.stringify(watchlist));
    console.log('[自选] saveWatchlist: 已保存到本地缓存');
    
    // 同步保存到服务器（等待响应，确保数据同步）
    try {
        const url = `${API_BASE}/api/watchlist`;
        const payload = { stocks: watchlist };
        console.log('[自选] saveWatchlist: 请求URL:', url);
        console.log('[自选] saveWatchlist: 请求数据:', JSON.stringify(payload));
        
        const response = await apiFetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        console.log('[自选] saveWatchlist: 响应状态:', response.status, response.statusText);
        
        if (response.ok) {
            const result = await response.json();
            console.log('[自选] saveWatchlist: 响应数据:', result);
            if (result.code === 0) {
                console.log('[自选] saveWatchlist: 保存成功，服务器返回', result.data?.length || 0, '只股票');
                return true;
            } else {
                console.warn('[自选] saveWatchlist: 服务器返回错误:', result.message);
                return false;
            }
        } else {
            console.warn('[自选] saveWatchlist: HTTP错误:', response.status, response.statusText);
            const errorText = await response.text().catch(() => '');
            console.warn('[自选] saveWatchlist: 错误响应体:', errorText);
            return false;
        }
    } catch (e) {
        console.error('[自选] saveWatchlist: 异常:', e);
        // 即使服务器保存失败，本地已保存，不影响使用
        return false;
    }
}

// 添加到自选股
async function addToWatchlist(code, name) {
    console.log('[自选] 开始添加股票到自选:', code, name);
    const watchlist = getWatchlist();
    console.log('[自选] 当前自选列表:', watchlist.map(s => s.code));
    
    if (watchlist.some(s => s.code === code)) {
        console.log('[自选] 股票已在自选列表中，跳过');
        alert('该股票已在自选列表中');
        return;
    }
    
    watchlist.push({ code, name, addTime: Date.now() });
    console.log('[自选] 添加到列表后，共', watchlist.length, '只股票');
    
    // 等待保存到服务器完成（确保数据同步）
    console.log('[自选] 开始保存到服务器...');
    const saved = await saveWatchlist(watchlist);
    if (saved) {
        console.log('[自选] 保存到服务器成功');
    } else {
        // 如果保存失败，提示用户（但不阻止操作，因为本地已保存）
        console.warn('[自选] 保存到服务器失败，但已保存到本地');
    }
    
    // 触发自定义事件，通知当前标签页的其他部分更新
    console.log('[自选] 触发watchlistChanged事件');
    window.dispatchEvent(new CustomEvent('watchlistChanged', { detail: { action: 'add', code, name } }));
    
    // 更新按钮状态
    updateWatchlistButtonStates();
    
    // 不再手动刷新自选页，依赖SSE推送来更新（无感刷新）
    // SSE会在服务器保存成功后自动推送更新，_doWatchlistSync会处理更新
    console.log('[自选] 添加完成，等待SSE推送更新（无感刷新）');
}

// 从自选股移除（无感移除：立即删除，后台保存）
async function removeFromWatchlist(code) {
    // 如果当前在自选页，先找到对应的行
    const watchlistTab = document.getElementById('watchlist-tab');
    const isInWatchlistPage = watchlistTab && watchlistTab.classList.contains('active');
    
    let targetRow = null;
    let rowData = null; // 保存行数据，用于失败时恢复
    
    if (isInWatchlistPage) {
        // 找到对应的行
        const tbody = document.getElementById('watchlist-stock-list');
        if (tbody) {
            const rows = Array.from(tbody.querySelectorAll('tr'));
            targetRow = rows.find(tr => {
                const firstTd = tr.querySelector('td:first-child');
                return firstTd && firstTd.textContent.trim() === String(code).trim();
            });
            
            // 保存行的HTML和数据，用于失败时恢复
            if (targetRow) {
                rowData = {
                    html: targetRow.outerHTML,
                    nextSibling: targetRow.nextSibling
                };
            }
        }
    }
    
    // 立即更新本地缓存（乐观更新）
    const watchlist = getWatchlist();
    const newWatchlist = watchlist.filter(s => s.code !== code);
    localStorage.setItem('watchlist', JSON.stringify(newWatchlist));
    
    // 如果当前在自选页，立即从DOM中删除对应的行（无感移除）
    if (isInWatchlistPage && targetRow) {
        // 添加淡出动画（可选，让移除更平滑）
        targetRow.style.transition = 'opacity 0.2s ease-out';
        targetRow.style.opacity = '0';
        
        // 延迟删除，让动画完成
        setTimeout(() => {
            targetRow.remove();
            
            // 检查是否还有数据
            const tbody = document.getElementById('watchlist-stock-list');
            if (tbody && tbody.children.length === 0) {
                // 如果没有数据了，显示空状态
                const container = document.getElementById('watchlist-container');
                if (container) {
                    container.innerHTML = `
                        <div class="watchlist-placeholder">
                            <div style="font-size: 48px; margin-bottom: 16px;">⭐</div>
                            <div style="font-size: 18px; color: #94a3b8; margin-bottom: 8px;">暂无自选股</div>
                            <div style="font-size: 14px; color: #64748b;">在行情页点击"加入自选"按钮添加股票</div>
                        </div>
                    `;
                }
            }
        }, 200);
    }
    
    // 立即更新按钮状态
    updateWatchlistButtonStates();
    
    // 触发自定义事件，通知当前标签页的其他部分更新
    window.dispatchEvent(new CustomEvent('watchlistChanged', { detail: { action: 'remove', code } }));
    
    // 清除缓存
    localStorage.removeItem(WATCHLIST_CACHE_KEY);
    
    // 后台异步保存到服务器（不阻塞UI）
    try {
        await saveWatchlist(newWatchlist);
    } catch (error) {
        console.error('保存自选股到服务器失败:', error);
        // 如果保存失败，恢复本地缓存和DOM
        localStorage.setItem('watchlist', JSON.stringify(watchlist));
        
        if (isInWatchlistPage && targetRow && rowData) {
            // 恢复行（如果还没删除）
            const tbody = document.getElementById('watchlist-stock-list');
            if (tbody) {
                // 如果行已经被删除，重新插入
                if (!targetRow.parentNode) {
                    const tempDiv = document.createElement('div');
                    tempDiv.innerHTML = rowData.html;
                    const restoredRow = tempDiv.firstElementChild;
                    
                    if (rowData.nextSibling && rowData.nextSibling.parentNode) {
                        tbody.insertBefore(restoredRow, rowData.nextSibling);
                    } else {
                        tbody.appendChild(restoredRow);
                    }
                    
                    // 重新绑定事件
                    const removeBtn = restoredRow.querySelector('.remove-watchlist-btn');
                    if (removeBtn) {
                        removeBtn.onclick = function(e) {
                            e.preventDefault();
                            e.stopPropagation();
                            const code = this.getAttribute('data-code');
                            removeFromWatchlist(code);
                        };
                    }
                    
                    // 重新绑定行点击事件
                    restoredRow.addEventListener('click', function(e) {
                        if (e.target.tagName === 'BUTTON' || e.target.closest('button')) {
                            return;
                        }
                        e.preventDefault();
                        const stockData = JSON.parse(this.getAttribute('data-stock'));
                        openKlineModal(stockData.code, stockData.name, stockData);
                    });
                } else {
                    // 如果行还在，恢复样式
                    targetRow.style.opacity = '';
                    targetRow.style.transition = '';
                }
            }
        }
        
        // 不再手动刷新，依赖SSE推送来更新（无感刷新）
        // 如果列表被清空，SSE会在下次推送时更新
        console.log('[自选] 等待SSE推送更新（无感刷新）');
        
        // 恢复按钮状态
        updateWatchlistButtonStates();
        
        // 静默失败，不打扰用户（因为本地已经更新了）
        console.warn('移除操作已应用到本地，但服务器同步失败。将在下次同步时自动修复。');
    }
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
            const timeoutId = setTimeout(() => controller.abort('Request timeout after 10 seconds'), 10000); // 每页10秒超时
            
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
            const timeoutId = setTimeout(() => controller.abort('Request timeout after 15 seconds'), 15000); // 15秒超时
                
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

// 初始化筛选配置
function initSelectionConfig() {
    // 设置默认折叠状态
    const content = document.getElementById('selection-config-content');
    const arrow = document.getElementById('selection-config-arrow');
    
    if (content && arrow) {
        // 默认折叠状态
        content.classList.add('hidden');
        arrow.classList.add('collapsed');
        arrow.textContent = '▶';
    }
    
    // 默认值
    const defaults = {
        'filter-volume-ratio-enable': true,
        'filter-volume-ratio-min': '0.8',
        'filter-volume-ratio-max': '8.0',
        'filter-rsi-enable': true,
        'filter-rsi-min': '30',
        'filter-rsi-max': '75',
        'filter-ma-enable': false,
        'filter-ma-period': '20',
        'filter-ma-condition': 'above',
        'filter-ema-enable': false,
        'filter-ema-period': '12',
        'filter-ema-condition': 'above',
        'filter-macd-enable': false,
        'filter-macd-condition': 'golden',
        'filter-kdj-enable': false,
        'filter-kdj-condition': 'golden',
        'filter-bias-enable': false,
        'filter-bias-min': '-6',
        'filter-bias-max': '6',
        'filter-williams-r-enable': false,
        'filter-break-high-enable': false,
        'filter-boll-enable': false,
        'filter-boll-condition': 'expanding',
        'filter-adx-enable': false,
        'filter-adx-min': '25',
        'filter-ichimoku-enable': false,
        'filter-ichimoku-condition': 'above_cloud',
        'selection-max-count': '30'
    };
    
    // 尝试从localStorage加载保存的配置
    let savedConfig = null;
    try {
        const savedConfigStr = localStorage.getItem('selectionConfig');
        if (savedConfigStr) {
            savedConfig = JSON.parse(savedConfigStr);
            console.log('[选股配置] 从localStorage加载配置:', savedConfig);
        }
    } catch (e) {
        console.warn('加载筛选配置失败，使用默认值:', e);
    }
    
    // 应用配置值（优先使用保存的配置，否则使用默认值）
    Object.entries(defaults).forEach(([id, defaultValue]) => {
        const element = document.getElementById(id);
        if (!element) return;
        
        // 将id转换为camelCase格式（与保存的配置键匹配）
        // 例如: 'filter-volume-ratio-enable' -> 'volumeRatioEnable'
        const camelKey = id
            .replace('filter-', '')
            .replace('selection-', '')
            .replace(/-([a-z])/g, (g) => g[1].toUpperCase());
        
        // 获取值：优先使用保存的配置，否则使用默认值
        let value = defaultValue;
        if (savedConfig && savedConfig[camelKey] !== undefined) {
            value = savedConfig[camelKey];
        }
        
        // 应用值
        if (element.type === 'checkbox') {
            element.checked = value === true || value === 'true';
        } else {
            element.value = value;
        }
    });
    
    // 添加范围输入的联动逻辑
    const volumeMinInput = document.getElementById('filter-volume-ratio-min');
    const volumeMaxInput = document.getElementById('filter-volume-ratio-max');
    const rsiMinInput = document.getElementById('filter-rsi-min');
    const rsiMaxInput = document.getElementById('filter-rsi-max');
    
    // 量比范围验证
    if (volumeMinInput && volumeMaxInput) {
        volumeMinInput.addEventListener('change', () => {
            const min = parseFloat(volumeMinInput.value);
            const max = parseFloat(volumeMaxInput.value);
            if (min >= max) {
                volumeMaxInput.value = (min + 1).toFixed(1);
            }
        });
        
        volumeMaxInput.addEventListener('change', () => {
            const min = parseFloat(volumeMinInput.value);
            const max = parseFloat(volumeMaxInput.value);
            if (max <= min) {
                volumeMinInput.value = Math.max(0.1, max - 1).toFixed(1);
            }
        });
    }
    
    // RSI范围验证
    if (rsiMinInput && rsiMaxInput) {
        rsiMinInput.addEventListener('change', () => {
            const min = parseInt(rsiMinInput.value);
            const max = parseInt(rsiMaxInput.value);
            if (min >= max) {
                rsiMaxInput.value = Math.min(100, min + 10);
            }
            updateFilterPreviews();
        });
        
        rsiMaxInput.addEventListener('change', () => {
            const min = parseInt(rsiMinInput.value);
            const max = parseInt(rsiMaxInput.value);
            if (max <= min) {
                rsiMinInput.value = Math.max(0, max - 10);
            }
            updateFilterPreviews();
        });
    }
    
    // 量比范围变化时更新预览
    if (volumeMinInput) {
        volumeMinInput.addEventListener('change', updateFilterPreviews);
    }
    if (volumeMaxInput) {
        volumeMaxInput.addEventListener('change', updateFilterPreviews);
    }
    
    // BIAS范围变化时更新预览
    const biasMinInput = document.getElementById('filter-bias-min');
    const biasMaxInput = document.getElementById('filter-bias-max');
    if (biasMinInput) biasMinInput.addEventListener('change', updateFilterPreviews);
    if (biasMaxInput) biasMaxInput.addEventListener('change', updateFilterPreviews);
    
    // ADX变化时更新预览
    const adxMinInput = document.getElementById('filter-adx-min');
    if (adxMinInput) adxMinInput.addEventListener('change', updateFilterPreviews);
}

// 选股模块
function initStrategy() {
    const selectBtn = document.getElementById('select-btn');
    const loadSelectedBtn = document.getElementById('load-selected-btn');
    const collectKlineBtn = document.getElementById('collect-kline-btn');
    const singleBatchCollectBtn = document.getElementById('single-batch-collect-kline-btn');
    
    // 初始化筛选配置
    initSelectionConfig();
    
    // 从服务器加载选股配置（持久化配置）
    loadSelectionConfig();
    
    // 加载保存的选股结果
    const savedResults = loadSelectionResults();
    if (savedResults && savedResults.length > 0) {
        console.log('[选股] 恢复上次选股结果');
        renderSelectedStocks(savedResults, false); // false 表示不重复保存
    }
    
    // 配置按钮事件
    const resetConfigBtn = document.getElementById('reset-config-btn');
    const saveConfigBtn = document.getElementById('save-config-btn');
    
    if (resetConfigBtn) {
        resetConfigBtn.addEventListener('click', () => {
            if (confirm('确认重置所有筛选配置为默认值吗？')) {
                // 清除localStorage中的配置
                localStorage.removeItem('selectionConfig');
                // 重新应用默认值
                initSelectionConfig();
                showToast('筛选配置已重置', 'success');
            }
        });
    }
    
    if (saveConfigBtn) {
        saveConfigBtn.addEventListener('click', () => {
            // 保存所有筛选配置到localStorage
            const config = {
                // 量比
                volumeRatioEnable: document.getElementById('filter-volume-ratio-enable')?.checked,
                volumeRatioMin: document.getElementById('filter-volume-ratio-min')?.value,
                volumeRatioMax: document.getElementById('filter-volume-ratio-max')?.value,
                // RSI
                rsiEnable: document.getElementById('filter-rsi-enable')?.checked,
                rsiMin: document.getElementById('filter-rsi-min')?.value,
                rsiMax: document.getElementById('filter-rsi-max')?.value,
                // MA
                maEnable: document.getElementById('filter-ma-enable')?.checked,
                maPeriod: document.getElementById('filter-ma-period')?.value,
                maCondition: document.getElementById('filter-ma-condition')?.value,
                // EMA
                emaEnable: document.getElementById('filter-ema-enable')?.checked,
                emaPeriod: document.getElementById('filter-ema-period')?.value,
                emaCondition: document.getElementById('filter-ema-condition')?.value,
                // MACD
                macdEnable: document.getElementById('filter-macd-enable')?.checked,
                macdCondition: document.getElementById('filter-macd-condition')?.value,
                // KDJ
                kdjEnable: document.getElementById('filter-kdj-enable')?.checked,
                kdjCondition: document.getElementById('filter-kdj-condition')?.value,
                // BIAS
                biasEnable: document.getElementById('filter-bias-enable')?.checked,
                biasMin: document.getElementById('filter-bias-min')?.value,
                biasMax: document.getElementById('filter-bias-max')?.value,
                // 威廉指标
                williamsREnable: document.getElementById('filter-williams-r-enable')?.checked,
                // 突破高点
                breakHighEnable: document.getElementById('filter-break-high-enable')?.checked,
                // 布林带
                bollEnable: document.getElementById('filter-boll-enable')?.checked,
                bollCondition: document.getElementById('filter-boll-condition')?.value,
                // ADX
                adxEnable: document.getElementById('filter-adx-enable')?.checked,
                adxMin: document.getElementById('filter-adx-min')?.value,
                // 一目均衡表
                ichimokuEnable: document.getElementById('filter-ichimoku-enable')?.checked,
                ichimokuCondition: document.getElementById('filter-ichimoku-condition')?.value,
                // 选股数量
                selectionMaxCount: document.getElementById('selection-max-count')?.value
            };
            
            localStorage.setItem('selectionConfig', JSON.stringify(config));
            showToast('筛选配置已保存', 'success');
            
            // 保存成功后自动折叠配置面板
            collapseSelectionConfig();
        });
    }
    
    if (selectBtn) {
        selectBtn.addEventListener('click', runSelection);
    }
    if (loadSelectedBtn) {
        loadSelectedBtn.addEventListener('click', loadSelectedStocks);
    }
    
    // 初始化选股页无限滚动
    function setupSelectionScrollListener() {
        const selectedStocksContainer = document.getElementById('selected-stocks');
        if (!selectedStocksContainer) {
            return;
        }
        
        // 监听容器滚动事件
        selectedStocksContainer.addEventListener('scroll', () => {
            const strategyTab = document.getElementById('strategy-tab');
            if (!strategyTab || !strategyTab.classList.contains('active')) {
                return;
            }
            
            // 检查是否滚动到底部
            const scrollTop = selectedStocksContainer.scrollTop;
            const scrollHeight = selectedStocksContainer.scrollHeight;
            const clientHeight = selectedStocksContainer.clientHeight;
            
            // 距离底部200px时加载下一批
            if (scrollTop + clientHeight >= scrollHeight - 200 && 
                !selectedIsLoading && 
                selectedRenderedCount < selectedAllStocks.length) {
                console.log('[选股] 触发无限滚动，加载下一批');
                selectedIsLoading = true;
                requestAnimationFrame(() => {
                    renderSelectedStocksBatch();
                    selectedIsLoading = false;
                });
            }
        });
        console.log('[选股] 滚动监听器已设置');
    }
    
    // 初始设置
    setupSelectionScrollListener();
    
    // 当tab切换到选股页时重新设置监听器
    const tabBtns = document.querySelectorAll('.tab-btn');
    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.getAttribute('data-tab');
            if (tab === 'strategy') {
                setTimeout(setupSelectionScrollListener, 100);
            }
        });
    });
    if (collectKlineBtn) {
        collectKlineBtn.addEventListener('click', () => {
            // 默认同时采集A股和港股
            const market = 'ALL';
            const maxCount = parseInt(document.getElementById('collect-max-count-input')?.value || 6000);
            collectKlineData(market, maxCount);
            // 停止按钮始终可用，无需操作
        });
    }
    if (singleBatchCollectBtn) {
        singleBatchCollectBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            collectSingleBatchKline().catch(err => {
                console.error('单个批量采集失败:', err);
                showToast(`采集失败: ${err.message || '未知错误'}`, 'error');
            });
            // 停止按钮始终可用，无需操作
        });
    }
    const stopCollectBtn = document.getElementById('stop-collect-kline-btn');
    if (stopCollectBtn) {
        stopCollectBtn.addEventListener('click', stopKlineCollect);
    }
    
    // 页面加载时，进度状态会通过SSE推送
    // 不需要额外检查，SSE连接会自动推送最新状态
}

async function runSelection() {
    const selectBtn = document.getElementById('select-btn');
    const market = 'A'; // 默认A股
    const maxCount = parseInt(document.getElementById('selection-max-count')?.value) || 30;
    const container = document.getElementById('selected-stocks');
    
    // 收集筛选配置
    const filterConfig = {
        // 量比
        volume_ratio_enable: document.getElementById('filter-volume-ratio-enable')?.checked || false,
        volume_ratio_min: parseFloat(document.getElementById('filter-volume-ratio-min')?.value) || 0.8,
        volume_ratio_max: parseFloat(document.getElementById('filter-volume-ratio-max')?.value) || 8.0,
        // RSI
        rsi_enable: document.getElementById('filter-rsi-enable')?.checked || false,
        rsi_min: parseInt(document.getElementById('filter-rsi-min')?.value) || 30,
        rsi_max: parseInt(document.getElementById('filter-rsi-max')?.value) || 75,
        // MA
        ma_enable: document.getElementById('filter-ma-enable')?.checked || false,
        ma_period: document.getElementById('filter-ma-period')?.value || '20',
        ma_condition: document.getElementById('filter-ma-condition')?.value || 'above',
        // EMA
        ema_enable: document.getElementById('filter-ema-enable')?.checked || false,
        ema_period: document.getElementById('filter-ema-period')?.value || '12',
        ema_condition: document.getElementById('filter-ema-condition')?.value || 'above',
        // MACD
        macd_enable: document.getElementById('filter-macd-enable')?.checked || false,
        macd_condition: document.getElementById('filter-macd-condition')?.value || 'golden',
        // KDJ
        kdj_enable: document.getElementById('filter-kdj-enable')?.checked || false,
        kdj_condition: document.getElementById('filter-kdj-condition')?.value || 'golden',
        // BIAS
        bias_enable: document.getElementById('filter-bias-enable')?.checked || false,
        bias_min: parseFloat(document.getElementById('filter-bias-min')?.value) || -6,
        bias_max: parseFloat(document.getElementById('filter-bias-max')?.value) || 6,
        // 威廉指标
        williams_r_enable: document.getElementById('filter-williams-r-enable')?.checked || false,
        // 突破高点
        break_high_enable: document.getElementById('filter-break-high-enable')?.checked || false,
        // 布林带
        boll_enable: document.getElementById('filter-boll-enable')?.checked || false,
        boll_condition: document.getElementById('filter-boll-condition')?.value || 'expanding',
        // ADX
        adx_enable: document.getElementById('filter-adx-enable')?.checked || false,
        adx_min: parseFloat(document.getElementById('filter-adx-min')?.value) || 25,
        // 一目均衡表
        ichimoku_enable: document.getElementById('filter-ichimoku-enable')?.checked || false,
        ichimoku_condition: document.getElementById('filter-ichimoku-condition')?.value || 'above_cloud',
    };
    
    console.log('筛选配置:', filterConfig);
    
    // 禁用选股按钮，显示加载状态
    if (selectBtn) {
        selectBtn.disabled = true;
        selectBtn.innerHTML = '🔄 选股中...';
        selectBtn.style.opacity = '0.7';
        selectBtn.style.cursor = 'not-allowed';
    }
    
    // 生成任务ID
    const taskId = `selection_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    // 恢复按钮状态的函数
    const restoreButton = () => {
        if (selectBtn) {
            selectBtn.disabled = false;
            selectBtn.innerHTML = '🎯 开始选股';
            selectBtn.style.opacity = '1';
            selectBtn.style.cursor = 'pointer';
        }
    };
    
    // 显示进度容器
    const progressContainer = document.getElementById('selection-progress-container');
    if (progressContainer) {
        progressContainer.style.display = 'block';
        // 重置进度
        const progressBar = document.getElementById('selection-progress-bar');
        const statusEl = document.getElementById('selection-status');
        const progressText = document.getElementById('selection-progress-text');
        if (progressBar) progressBar.style.width = '0%';
        if (statusEl) {
            statusEl.textContent = '正在初始化选股引擎...';
            statusEl.className = 'selection-status running';
        }
        if (progressText) progressText.textContent = '0%';
    }
    
    // 显示加载状态
    container.innerHTML = `
        <div class="selection-loading">
            <div class="ai-loading-spinner"></div>
            <div style="margin-top: 16px; color: #94a3b8;">正在选股中，请稍候...</div>
            <div style="margin-top: 8px; color: #64748b; font-size: 12px;">进度将通过SSE实时推送</div>
        </div>
    `;
    
    // 确保SSE连接已建立（进度通过SSE推送）
    if (!sseConnection || sseConnection.readyState !== EventSource.OPEN) {
        console.log('[选股] SSE未连接，尝试连接...');
        connectSSE();
    }
    
    // 保存当前任务ID到全局变量，用于SSE消息过滤
    window.currentSelectionTaskId = taskId;
    console.log('[选股] 任务ID:', taskId, '进度将通过SSE推送');
    
    // 隐藏进度容器的函数（选股完成后调用）
    const hideProgressContainer = () => {
        setTimeout(() => {
            if (progressContainer) {
                progressContainer.style.display = 'none';
            }
        }, 3000); // 3秒后隐藏
    };
    
    // 注意：进度更新由 handleSelectionProgress 函数处理（在SSE消息处理中）
    // 这里不再需要 updateSelectionProgress 函数，因为 handleSelectionProgress 已经处理了
    
    // 临时变量用于跟踪进度状态
    let selectionCompleted = false;
    
    try {
        // 选股不设置超时，由后端控制（可能需要很长时间处理全部股票）
        console.log('发送选股请求:', `${API_BASE}/api/strategy/select?max_count=${maxCount}&market=${market}&task_id=${taskId}`);
        const startTime = Date.now();
        
        const response = await apiFetch(`${API_BASE}/api/strategy/select?max_count=${maxCount}&market=${market}&task_id=${taskId}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(filterConfig)
        });
        
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`选股请求完成，耗时: ${elapsed}秒`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        console.log('选股结果:', result);
        
        // 清除当前任务ID
        window.currentSelectionTaskId = null;
        
        // 隐藏进度容器
        hideProgressContainer();
        
        if (result.code === 0) {
            if (result.message && result.message.includes('市场环境不佳')) {
                container.innerHTML = `
                    <div class="selection-error">
                        <div class="error-icon">⚠️</div>
                        <div class="error-title" style="color: #f59e0b;">市场环境不佳</div>
                        <div class="error-message">${result.message}</div>
                        <div class="error-detail">耗时: ${elapsed}秒</div>
                    </div>
                `;
            } else {
                console.log(`选股成功，找到${result.data.length}只股票，耗时${elapsed}秒`);
                renderSelectedStocks(result.data);
            }
        } else {
            // 如果错误提示包含"没有数据"或"kline"，显示采集按钮
            const message = result.message || '未知错误';
            let errorHtml = `
                <div class="selection-error">
                    <div class="error-icon" style="color: #ef4444;">❌</div>
                    <div class="error-title" style="color: #ef4444;">选股失败</div>
                    <div class="error-message">${message}</div>
                    <div class="error-detail">耗时: ${elapsed}秒</div>
                </div>
            `;
            
            if (message.includes('没有数据') || message.includes('kline') || message.includes('K线')) {
                errorHtml += `
                    <div style="text-align: center; margin-top: 20px;">
                        <button id="collect-kline-btn" class="selection-retry-btn">
                            📥 批量采集K线数据
                        </button>
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
                        collectKlineData('ALL', maxCount);
                    });
                }
            }, 100);
        }
        
        // 恢复按钮状态
        restoreButton();
    } catch (error) {
        console.error('选股请求失败:', error);
        
        // 清除当前任务ID
        window.currentSelectionTaskId = null;
        
        // 隐藏进度容器
        hideProgressContainer();
        
        let errorMessage = '选股请求失败';
        let errorDetail = error.message || '未知错误';
        
        if (error.name === 'AbortError') {
            errorMessage = '选股请求被取消';
            errorDetail = '请求已被取消，请重试';
        } else if (error.message.includes('Failed to fetch')) {
            errorMessage = '网络连接失败';
            errorDetail = '请检查网络连接或服务器状态';
        }
        
        container.innerHTML = `
            <div class="selection-error">
                <div class="error-icon" style="color: #ef4444;">🔥</div>
                <div class="error-title" style="color: #ef4444;">${errorMessage}</div>
                <div class="error-message">${errorDetail}</div>
                <button onclick="runSelection()" class="selection-retry-btn">
                    🔄 重试选股
                </button>
            </div>
        `;
        
        // 恢复按钮状态
        restoreButton();
    }
}

// 单个股票采集K线数据
async function collectSingleStockKline() {
    const codeInput = document.getElementById('single-collect-code-input');
    const marketSelect = document.getElementById('single-collect-market-select');
    const periodSelect = document.getElementById('single-collect-period-select');
    const statusEl = document.getElementById('collect-kline-status');
    const btn = document.getElementById('single-collect-kline-btn');
    
    if (!codeInput || !btn) {
        console.error('单个采集：缺少必要的DOM元素');
        showToast('页面元素加载失败，请刷新页面重试', 'error');
        return;
    }
    
    const code = codeInput.value.trim();
    const market = marketSelect?.value || 'A';
    const period = periodSelect?.value || 'daily';
    
    if (!code) {
        if (statusEl) {
            statusEl.innerHTML = '<div style="color: #ef4444; margin-top: 10px;">❌ 请输入股票代码</div>';
        }
        showToast('请输入股票代码', 'error');
        return;
    }
    
    btn.disabled = true;
    btn.textContent = '采集中...';
    if (statusEl) {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #60a5fa; margin-bottom: 5px; font-weight: 500;">正在采集 ${code} 的K线数据...</div>
                <div style="color: #94a3b8; font-size: 11px;">请稍候，数据正在采集中</div>
            </div>
        `;
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/kline/collect/single?code=${code}&market=${market}&period=${period}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.code === 0) {
            const data = result.data || {};
            const count = data.count || 0;
            const latestDate = data.latest_date || '';
            if (statusEl) {
                statusEl.innerHTML = `
                    <div style="margin-top: 10px;">
                        <div style="color: #10b981; margin-bottom: 5px; font-weight: bold;">✅ 采集成功！</div>
                        <div style="color: #10b981; font-size: 12px; margin-bottom: 2px;">股票代码: ${code}</div>
                        <div style="color: #10b981; font-size: 12px; margin-bottom: 2px;">数据条数: ${count} 条</div>
                        ${latestDate ? `<div style="color: #94a3b8; font-size: 11px;">最新日期: ${latestDate}</div>` : ''}
                    </div>
                `;
            }
            showToast(`成功采集 ${code}，共 ${count} 条`, 'success');
        } else {
            if (statusEl) {
                statusEl.innerHTML = `
                    <div style="margin-top: 10px;">
                        <div style="color: #ef4444; margin-bottom: 5px;">❌ 采集失败</div>
                        <div style="color: #94a3b8; font-size: 11px;">${result.message || '未知错误'}</div>
                    </div>
                `;
            }
            showToast(`采集失败: ${result.message || '未知错误'}`, 'error');
        }
    } catch (error) {
        if (statusEl) {
            statusEl.innerHTML = `
                <div style="margin-top: 10px;">
                    <div style="color: #ef4444; margin-bottom: 5px;">❌ 采集失败</div>
                    <div style="color: #94a3b8; font-size: 11px;">${error.message || '网络错误'}</div>
                </div>
            `;
        }
        showToast(`采集失败: ${error.message || '网络错误'}`, 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = '📥 单个采集';
    }
}

// 单个批量采集K线数据（从akshare获取列表，循环采集）
async function collectSingleBatchKline() {
    const batchSizeInput = document.getElementById('single-batch-size-input');
    const marketSelect = document.getElementById('single-batch-market-select');
    const periodSelect = document.getElementById('single-batch-period-select');
    const statusEl = document.getElementById('collect-kline-status');
    const btn = document.getElementById('single-batch-collect-kline-btn');
    
    if (!batchSizeInput || !btn) {
        console.error('单个批量采集：缺少必要的DOM元素');
        showToast('页面元素加载失败，请刷新页面重试', 'error');
        return;
    }
    
    const batchSize = parseInt(batchSizeInput.value) || 10;
    const market = marketSelect?.value || 'ALL';
    const period = periodSelect?.value || 'daily';
    
    if (batchSize < 1 || batchSize > 100) {
        if (statusEl) {
            statusEl.innerHTML = '<div style="color: #ef4444; margin-top: 10px;">❌ 单个数量应在1-100之间</div>';
        }
        showToast('单个数量应在1-100之间', 'error');
        return;
    }
    
    btn.disabled = true;
    btn.textContent = '采集中...';
    if (statusEl) {
        statusEl.innerHTML = `
            <div style="margin-top: 10px;">
                <div style="color: #60a5fa; margin-bottom: 5px; font-weight: 500;">正在启动单个批量采集...</div>
                <div style="color: #94a3b8; font-size: 11px;">正在从akshare获取股票列表，请稍候</div>
            </div>
        `;
    }
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/kline/collect/batch-single?batch_size=${batchSize}&market=${market}&period=${period}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.code === 0) {
            // 使用和批量采集一样的进度显示
            if (statusEl) {
                statusEl.innerHTML = `
                    <div style="margin-top: 10px;">
                        <div style="color: #10b981; margin-bottom: 5px; font-weight: 500;">✅ 采集任务已启动</div>
                        <div style="color: #60a5fa; font-size: 11px; margin-bottom: 5px;">进度将通过SSE实时推送...</div>
                        <div style="color: #94a3b8; font-size: 11px;">数据正在后台采集中，每次${batchSize}只股票</div>
                    </div>
                `;
            }
            btn.textContent = '采集中...';
            // 进度通过SSE推送，由handleKlineCollectProgress处理
        } else {
            if (statusEl) {
                statusEl.innerHTML = `
                    <div style="margin-top: 10px;">
                        <div style="color: #ef4444; margin-bottom: 5px;">❌ 启动失败</div>
                        <div style="color: #94a3b8; font-size: 11px;">${result.message || '未知错误'}</div>
                    </div>
                `;
            }
            showToast(`启动失败: ${result.message || '未知错误'}`, 'error');
            btn.disabled = false;
            btn.textContent = '📥 单个批量采集';
            // 停止按钮始终可用，无需禁用
        }
    } catch (error) {
        if (statusEl) {
            statusEl.innerHTML = `
                <div style="margin-top: 10px;">
                    <div style="color: #ef4444; margin-bottom: 5px;">❌ 启动失败</div>
                    <div style="color: #94a3b8; font-size: 11px;">${error.message || '网络错误'}</div>
                </div>
            `;
        }
        showToast(`启动失败: ${error.message || '网络错误'}`, 'error');
        btn.disabled = false;
        btn.textContent = '📥 单个批量采集';
        // 停止按钮始终可用，无需禁用
    }
}

// 批量采集K线数据
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
            statusEl.innerHTML = `
                <div style="margin-top: 10px;">
                    <div style="color: #10b981; margin-bottom: 5px; font-weight: 500;">✅ 采集任务已启动</div>
                    <div style="color: #60a5fa; font-size: 11px; margin-bottom: 5px;">进度将通过SSE实时推送...</div>
                    <div style="color: #94a3b8; font-size: 11px;">${result.message || '数据将在后台采集并保存到ClickHouse'}</div>
                </div>
            `;
            btn.textContent = '采集中...';
            // 进度通过SSE推送，由handleKlineCollectProgress处理
        } else {
            statusEl.textContent = `❌ 采集失败: ${result.message || '未知错误'}`;
            statusEl.style.color = '#ef4444';
            btn.disabled = false;
            btn.textContent = '📥 批量采集';
            // 停止按钮始终可用，无需禁用
        }
    } catch (error) {
        statusEl.textContent = `❌ 采集失败: ${error.message || '网络错误'}`;
        statusEl.style.color = '#ef4444';
        btn.disabled = false;
        btn.textContent = '📥 批量采集';
        // 停止按钮始终可用，无需禁用
    }
}

// 停止K线采集
async function stopKlineCollect() {
    const stopBtn = document.getElementById('stop-collect-kline-btn');
    if (!stopBtn) return;
    
    // 如果正在处理，防止重复点击
    if (stopBtn.textContent === '停止中...') {
        return;
    }
    
    stopBtn.textContent = '停止中...';
    
    try {
        const response = await apiFetch(`${API_BASE}/api/market/kline/collect/stop`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.code === 0) {
            showToast('已发送停止信号，采集任务将停止', 'success');
        } else {
            showToast(`停止失败: ${result.message || '未知错误'}`, 'error');
        }
    } catch (error) {
        showToast(`停止失败: ${error.message || '网络错误'}`, 'error');
    } finally {
        // 恢复按钮文本（允许再次点击）
        stopBtn.textContent = '🛑 停止采集';
    }
}

async function loadSelectedStocks() {
    const container = document.getElementById('selected-stocks');
    
    // 优先从 localStorage 加载
    const savedResults = loadSelectionResults();
    if (savedResults && savedResults.length > 0) {
        renderSelectedStocks(savedResults, false);
        showToast(`已加载本地保存的选股结果（${savedResults.length}只）`, 'success');
        return;
    }
    
    // 如果本地没有，从服务器加载
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

// 选股页无限滚动相关变量
let selectedAllStocks = []; // 所有选股结果
let selectedRenderedCount = 0; // 已渲染的数量
let selectedPageSize = 20; // 每批渲染的数量
let selectedIsLoading = false; // 是否正在加载

// 保存选股结果到 localStorage
function saveSelectionResults(stocks) {
    try {
        const data = {
            stocks: stocks,
            timestamp: Date.now()
        };
        localStorage.setItem('selectionResults', JSON.stringify(data));
        console.log('[选股] 结果已保存到本地存储，共', stocks.length, '只');
    } catch (e) {
        console.warn('[选股] 保存结果失败:', e);
    }
}

// 从 localStorage 加载选股结果
function loadSelectionResults() {
    try {
        const saved = localStorage.getItem('selectionResults');
        if (saved) {
            const data = JSON.parse(saved);
            // 检查数据是否有效
            if (data.stocks && data.stocks.length > 0) {
                console.log('[选股] 从本地存储加载结果，共', data.stocks.length, '只');
                return data.stocks;
            }
        }
    } catch (e) {
        console.warn('[选股] 加载结果失败:', e);
    }
    return null;
}

function renderSelectedStocks(stocks, saveToStorage = true) {
    const container = document.getElementById('selected-stocks');
    
    if (stocks.length === 0) {
        container.innerHTML = `
            <div class="selection-error">
                <div class="error-icon">🤔</div>
                <div class="error-title" style="color: #94a3b8;">未找到符合条件的股票</div>
                <div class="error-message">当前筛选条件较为严格，建议调整筛选参数后重试</div>
                <button onclick="runSelection()" class="selection-retry-btn">
                    🔄 重新选股
                </button>
            </div>
        `;
        selectedAllStocks = [];
        selectedRenderedCount = 0;
        return;
    }
    
    // 保存到 localStorage
    if (saveToStorage) {
        saveSelectionResults(stocks);
    }
    
    // 获取勾选的筛选指标
    const enabledFilters = getEnabledFilters();
    
    // 构建表头
    let headerHtml = '<th>代码/名称</th><th>现价</th><th>涨跌幅</th>';
    enabledFilters.forEach(filter => {
        headerHtml += `<th>${filter.label}</th>`;
    });
    
    // 表格式布局
    container.innerHTML = `
        <div class="selected-stocks-header">
            <div class="selected-stocks-info">
                <span class="selected-count">🎯 共筛选出 <strong>${stocks.length}</strong> 只股票</span>
            </div>
        </div>
        <div class="selected-stocks-table-wrapper">
            <table class="selected-stocks-table">
                <thead>
                    <tr>${headerHtml}</tr>
                </thead>
                <tbody id="selected-stocks-list"></tbody>
            </table>
        </div>
    `;
    
    // 保存启用的筛选器供分批渲染使用
    window.selectedEnabledFilters = enabledFilters;
    
    // 重置无限滚动状态
    selectedAllStocks = stocks;
    selectedRenderedCount = 0;
    
    // 渲染第一批数据（无限滚动）
    renderSelectedStocksBatch();
    
    console.log(`[选股] 开始分批渲染，总数: ${stocks.length}`);
}

// 获取启用的筛选指标
function getEnabledFilters() {
    const filters = [];
    
    // 量比
    if (document.getElementById('filter-volume-ratio-enable')?.checked) {
        filters.push({
            id: 'volume-ratio',
            label: '量比',
            getValue: (stock) => stock.vol_ratio?.toFixed(2) || stock.indicators?.vol_ratio?.toFixed(2) || stock.volume_ratio?.toFixed(2) || '-'
        });
    }
    
    // RSI
    if (document.getElementById('filter-rsi-enable')?.checked) {
        filters.push({
            id: 'rsi',
            label: 'RSI',
            getValue: (stock) => stock.rsi?.toFixed(1) || stock.indicators?.rsi?.toFixed(1) || '-'
        });
    }
    
    // MA均线
    if (document.getElementById('filter-ma-enable')?.checked) {
        const period = document.getElementById('filter-ma-period')?.value || '20';
        filters.push({
            id: 'ma',
            label: `MA${period}`,
            getValue: (stock) => stock[`ma${period}`]?.toFixed(2) || stock.indicators?.[`ma${period}`]?.toFixed(2) || '-'
        });
    }
    
    // EMA均线
    if (document.getElementById('filter-ema-enable')?.checked) {
        const period = document.getElementById('filter-ema-period')?.value || '12';
        filters.push({
            id: 'ema',
            label: `EMA${period}`,
            getValue: (stock) => stock[`ema${period}`]?.toFixed(2) || stock.indicators?.[`ema${period}`]?.toFixed(2) || '-'
        });
    }
    
    // MACD
    if (document.getElementById('filter-macd-enable')?.checked) {
        filters.push({
            id: 'macd',
            label: 'MACD',
            getValue: (stock) => {
                const dif = stock.macd_dif ?? stock.indicators?.macd_dif;
                if (dif === undefined || dif === null) return '-';
                return dif > 0 ? '多' : '空';
            }
        });
    }
    
    // KDJ
    if (document.getElementById('filter-kdj-enable')?.checked) {
        filters.push({
            id: 'kdj',
            label: 'KDJ',
            getValue: (stock) => {
                const k = stock.kdj_k ?? stock.indicators?.kdj_k;
                const d = stock.kdj_d ?? stock.indicators?.kdj_d;
                if (k === undefined || d === undefined) return '-';
                if (k > d) return '金叉';
                return '死叉';
            }
        });
    }
    
    // BIAS乖离率
    if (document.getElementById('filter-bias-enable')?.checked) {
        filters.push({
            id: 'bias',
            label: 'BIAS',
            getValue: (stock) => stock.bias?.toFixed(2) || stock.indicators?.bias?.toFixed(2) || '-'
        });
    }
    
    // 威廉指标
    if (document.getElementById('filter-williams-r-enable')?.checked) {
        filters.push({
            id: 'williams-r',
            label: '威廉%R',
            getValue: (stock) => stock.williams_r?.toFixed(1) || stock.indicators?.williams_r?.toFixed(1) || '-'
        });
    }
    
    // 突破高点
    if (document.getElementById('filter-break-high-enable')?.checked) {
        filters.push({
            id: 'break-high',
            label: '突破高点',
            getValue: (stock) => stock.indicators?.break_high_20d ? '是' : '-'
        });
    }
    
    // 布林带
    if (document.getElementById('filter-boll-enable')?.checked) {
        filters.push({
            id: 'boll',
            label: '布林带',
            getValue: (stock) => {
                const expanding = stock.indicators?.boll_expanding;
                if (expanding) return '开口';
                return '收口';
            }
        });
    }
    
    // ADX趋势
    if (document.getElementById('filter-adx-enable')?.checked) {
        filters.push({
            id: 'adx',
            label: 'ADX',
            getValue: (stock) => stock.indicators?.adx?.toFixed(1) || '-'
        });
    }
    
    // 一目均衡表
    if (document.getElementById('filter-ichimoku-enable')?.checked) {
        filters.push({
            id: 'ichimoku',
            label: '一目均衡',
            getValue: (stock) => {
                const above = stock.indicators?.ichimoku_above_cloud;
                if (above === true) return '云上';
                if (above === false) return '云下';
                return '-';
            }
        });
    }
    
    return filters;
}

// 分批渲染选股结果（无限滚动）
function renderSelectedStocksBatch() {
    if (selectedIsLoading) return;
    
    const container = document.getElementById('selected-stocks-list');
    if (!container) return;
    
    const strategyTab = document.getElementById('strategy-tab');
    if (!strategyTab || !strategyTab.classList.contains('active')) {
        return; // 不在选股页，不渲染
    }
    
    // 计算本次要渲染的范围
    const start = selectedRenderedCount;
    const end = Math.min(start + selectedPageSize, selectedAllStocks.length);
    const batch = selectedAllStocks.slice(start, end);
    
    if (batch.length === 0) {
        return;
    }
    
    const enabledFilters = window.selectedEnabledFilters || [];
    
    // 渲染表格行
    batch.forEach((stock, index) => {
        const tr = document.createElement('tr');
        tr.className = 'stock-row';
        tr.setAttribute('data-stock', JSON.stringify(stock));
        
        const pct = stock.pct || 0;
        const changeClass = pct >= 0 ? 'up' : 'down';
        const changeText = pct >= 0 ? `+${pct.toFixed(2)}%` : `${pct.toFixed(2)}%`;
        
        // 基础列：代码/名称、现价、涨跌幅
        let rowHtml = `
            <td class="stock-info-cell">
                <div class="stock-code">${stock.code || 'N/A'}</div>
                <div class="stock-name">${stock.name || '-'}</div>
            </td>
            <td class="price-cell">¥${stock.price ? stock.price.toFixed(2) : '-'}</td>
            <td class="change-cell ${changeClass}">${changeText}</td>
        `;
        
        // 动态添加启用的指标列
        enabledFilters.forEach(filter => {
            const value = filter.getValue(stock);
            rowHtml += `<td class="indicator-cell">${value}</td>`;
        });
        
        tr.innerHTML = rowHtml;
        
        // 添加点击事件
        tr.addEventListener('click', () => {
            console.log(`[选股] 点击股票: ${stock.code} ${stock.name}`);
            showKlineModal(stock.code, stock.name || stock.code, stock);
        });
        
        container.appendChild(tr);
    });
    
    // 更新已渲染数量
    selectedRenderedCount = end;
    
    console.log(`[选股] 已渲染 ${selectedRenderedCount}/${selectedAllStocks.length} 只股票`);
}

// AI分析模块
function initAI() {
    console.log('[AI] initAI 开始初始化');
    const analyzeBtn = document.getElementById('analyze-btn');
    const codeInput = document.getElementById('ai-code-input');
    const clearBtn = document.getElementById('ai-clear-btn');
    const watchlistCheckbox = document.getElementById('ai-source-watchlist');
    const selectionCheckbox = document.getElementById('ai-source-selection');
    
    console.log('[AI] 元素查找结果:', {
        analyzeBtn: !!analyzeBtn,
        codeInput: !!codeInput,
        watchlistCheckbox: !!watchlistCheckbox,
        selectionCheckbox: !!selectionCheckbox
    });
    
    // 从localStorage加载选择框状态
    try {
        const savedConfig = localStorage.getItem('aiSourceConfig');
        if (savedConfig) {
            const config = JSON.parse(savedConfig);
            if (watchlistCheckbox) watchlistCheckbox.checked = config.watchlist ?? true;
            if (selectionCheckbox) selectionCheckbox.checked = config.selection ?? false;
        }
    } catch (e) {
        console.warn('加载AI来源配置失败:', e);
    }
    
    // 保存选择框状态的函数
    const saveSourceConfig = () => {
        const config = {
            watchlist: watchlistCheckbox?.checked ?? true,
            selection: selectionCheckbox?.checked ?? false
        };
        localStorage.setItem('aiSourceConfig', JSON.stringify(config));
    };
    
    // 监听选择框变化
    if (watchlistCheckbox) {
        watchlistCheckbox.addEventListener('change', saveSourceConfig);
    }
    if (selectionCheckbox) {
        selectionCheckbox.addEventListener('change', saveSourceConfig);
    }
    
    if (!analyzeBtn) {
        console.error('[AI] 找不到分析按钮!');
        return;
    }
    
    analyzeBtn.addEventListener('click', () => {
        console.log('[AI] 点击了开始分析按钮');
        const code = codeInput.value.trim();
        if (code) {
            // 输入了代码，仅分析单只股票
            console.log('[AI] 分析单只股票:', code);
            analyzeStock([code]);
            return;
        }
        
        // 未输入代码，根据选择框决定分析哪些股票
        const useWatchlist = watchlistCheckbox?.checked;
        const useSelection = selectionCheckbox?.checked;
        
        console.log('[AI] 选择框状态:', { useWatchlist, useSelection });
        
        if (!useWatchlist && !useSelection) {
            showToast('请勾选自选股或选股结果，或输入股票代码', 'warning');
            return;
        }
        
        let codes = [];
        
        // 获取自选股
        if (useWatchlist) {
            const watchlist = getWatchlist();
            console.log('[AI] 自选股列表:', watchlist);
            if (watchlist && watchlist.length > 0) {
                const watchlistCodes = watchlist.map(s => String(s.code).trim()).filter(c => c);
                codes = codes.concat(watchlistCodes);
            }
        }
        
        // 获取选股结果
        if (useSelection) {
            console.log('[AI] 选股结果:', selectedAllStocks?.length || 0, '只');
            if (selectedAllStocks && selectedAllStocks.length > 0) {
                const selectionCodes = selectedAllStocks.map(s => String(s.code).trim()).filter(c => c);
                codes = codes.concat(selectionCodes);
            }
        }
        
        // 去重
        codes = [...new Set(codes)];
        
        console.log('[AI] 最终要分析的股票:', codes.length, '只', codes.slice(0, 5));
        
        if (codes.length === 0) {
            let msg = '';
            if (useWatchlist && useSelection) {
                msg = '自选列表和选股结果都为空';
            } else if (useWatchlist) {
                msg = '自选列表为空，请先在行情页添加自选股票';
            } else {
                msg = '选股结果为空，请先执行选股';
            }
            showToast(msg, 'error');
            return;
        }
        
        console.log('[AI] 调用 analyzeStock 函数');
        analyzeStock(codes);
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
    console.log('[AI] analyzeStock 被调用，股票数量:', Array.isArray(codes) ? codes.length : 1);
    const container = document.getElementById('ai-analysis-result');
    const codeList = Array.isArray(codes) ? codes : [codes];

    if (!codeList || codeList.length === 0) {
        console.log('[AI] 没有股票代码');
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
    // 估算时间：每5只股票约30-40秒
    const estimatedMinutes = isBatch ? Math.ceil(codeList.length / 5 * 0.6) : 1;
    const loadingText = isBatch
        ? `正在分析 ${codeList.length} 只股票`
        : 'AI分析中';
    const estimateText = isBatch
        ? `预计需要 ${estimatedMinutes} 分钟，请耐心等待...`
        : '请稍候...';

    console.log('[AI] 显示加载界面');
    container.innerHTML = `
        <div class="ai-loading">
            <div class="ai-loading-spinner"></div>
            <div style="margin-top: 16px; color: #e2e8f0; font-size: 16px;">${loadingText}</div>
            <div style="margin-top: 8px; color: #94a3b8; font-size: 14px;">${estimateText}</div>
            <div id="ai-loading-timer" style="margin-top: 12px; color: #60a5fa; font-size: 14px;">已用时: 0秒</div>
        </div>
    `;
    
    // 启动计时器显示已用时间
    const startTime = Date.now();
    const timerInterval = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const timerEl = document.getElementById('ai-loading-timer');
        if (timerEl) {
            const mins = Math.floor(elapsed / 60);
            const secs = elapsed % 60;
            timerEl.textContent = mins > 0 ? `已用时: ${mins}分${secs}秒` : `已用时: ${secs}秒`;
        } else {
            clearInterval(timerInterval);
        }
    }, 1000);
    
    try {
        let result;
        if (isBatch) {
            // 批量分析接口（自选股）
            const notifyFlag = options.notify === true ? 'true' : 'false';
            const url = `${API_BASE}/api/ai/analyze/batch?notify=${notifyFlag}`;
            console.log('[AI] 发送批量分析请求:', url, '股票数量:', codeList.length);
            const response = await apiFetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    codes: codeList,
                }),
            });
            clearInterval(timerInterval);
            console.log('[AI] 收到响应:', response.status, response.statusText);
            result = await response.json();
            console.log('[AI] 响应数据:', result.code, result.message);
            if (result.code === 0 && Array.isArray(result.data)) {
                console.log('[AI] 分析成功，渲染结果');
                await renderAIAnalysisBatch(result.data);
            } else {
                console.log('[AI] 分析失败:', result.message);
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
            const url = `${API_BASE}/api/ai/analyze/${singleCode}`;
            console.log('[AI] 发送单只分析请求:', url);
            const response = await apiFetch(url);
            clearInterval(timerInterval);
            console.log('[AI] 收到响应:', response.status, response.statusText);
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
        console.error('[AI] 请求出错:', error);
        clearInterval(timerInterval);
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
let newsInitialized = false;
function initNews() {
    if (newsInitialized) {
        return; // 已经初始化过，避免重复初始化
    }

    const refreshBtn = document.getElementById('refresh-news-btn');
    if (!refreshBtn) {
        console.warn('[资讯] 刷新按钮不存在，将在DOM加载后重试');
        // 延迟重试，确保DOM已加载
        setTimeout(() => {
            initNews();
        }, 100);
        return;
    }
    
    newsInitialized = true;
    // 刷新按钮仍然保留，但只在用户主动点击时刷新
    refreshBtn.addEventListener('click', () => {
        console.log('[资讯] 用户主动点击刷新按钮');
        loadNews();
    });
    
    // 初始化资讯页无限滚动（监听news-list容器）
    const newsList = document.getElementById('news-list');
    if (newsList) {
        newsList.addEventListener('scroll', () => {
            const newsTab = document.getElementById('news-tab');
            if (!newsTab || !newsTab.classList.contains('active')) {
                return;
            }
            
            // 检查是否滚动到底部
            const scrollTop = newsList.scrollTop;
            const scrollHeight = newsList.scrollHeight;
            const clientHeight = newsList.clientHeight;
            
            // 距离底部200px时加载下一批
            if (scrollTop + clientHeight >= scrollHeight - 200 && 
                !newsIsLoading && 
                newsRenderedCount < newsAllItems.length) {
                console.log('[资讯] 触发无限滚动，加载下一批');
                newsIsLoading = true;
                requestAnimationFrame(() => {
                    renderNewsBatch();
                    newsIsLoading = false;
                });
            }
        });
    }
    
    // 如果当前在资讯页且没有数据，主动加载一次（避免页面为空）
    const newsTab = document.getElementById('news-tab');
    const newsListEl = document.getElementById('news-list');
    if (newsTab && newsTab.classList.contains('active')) {
        // 检查是否已有数据（不是占位符或加载提示）
        const hasData = newsListEl && newsListEl.children.length > 0 && 
                       !newsListEl.innerHTML.includes('暂无资讯') && 
                       !newsListEl.innerHTML.includes('加载中');
        if (!hasData) {
            console.log('[资讯] 当前在资讯页且无数据，主动加载一次');
            loadNews();
        } else {
            console.log('[资讯] 资讯模块初始化完成，已有数据，等待SSE推送更新');
        }
    } else {
        console.log('[资讯] 资讯模块初始化完成，等待切换到资讯页或SSE推送数据');
    }
}

async function loadNews() {
    const container = document.getElementById('news-list');
    if (!container) {
        console.warn('[资讯] 资讯容器不存在');
        return;
    }
    
    // 检查是否在资讯页
    const newsTab = document.getElementById('news-tab');
    if (!newsTab || !newsTab.classList.contains('active')) {
        console.log('[资讯] 当前不在资讯页，跳过加载');
        return;
    }
    
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">加载中...</div>';
    
    try {
        console.log('[资讯] 开始加载资讯');
        const response = await apiFetch(`${API_BASE}/api/news/latest`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        console.log('[资讯] 收到响应:', result.code, '数据数量:', result.data?.length || 0);
        
        if (result.code === 0) {
            renderNews(result.data || []);
        } else {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${result.message || '未知错误'}</div>`;
        }
    } catch (error) {
        console.error('[资讯] 加载失败:', error);
        container.innerHTML = `<div style="text-align: center; padding: 40px; color: #ef4444;">加载失败: ${error.message || '网络错误'}<br/><button onclick="loadNews()" style="margin-top: 10px; padding: 8px 16px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer;">重试</button></div>`;
    }
}

// 资讯页无限滚动相关变量
let newsAllItems = []; // 所有资讯数据
let newsRenderedCount = 0; // 已渲染的数量
let newsPageSize = 20; // 每批渲染的数量
let newsIsLoading = false; // 是否正在加载

function renderNews(newsList) {
    const container = document.getElementById('news-list');
    
    if (newsList.length === 0) {
        container.innerHTML = '<div style="text-align: center; padding: 40px; color: #94a3b8;">暂无资讯</div>';
        newsAllItems = [];
        newsRenderedCount = 0;
        return;
    }
    
    // 重置无限滚动状态
    newsAllItems = newsList;
    newsRenderedCount = 0;
    container.innerHTML = '';
    
    // 渲染第一批数据（无限滚动）
    renderNewsBatch();
    
    console.log(`[资讯] 开始分批渲染，总数: ${newsList.length}`);
}

// 分批渲染资讯（无限滚动）
function renderNewsBatch() {
    if (newsIsLoading) return;
    
    const container = document.getElementById('news-list');
    if (!container) return;
    
    const newsTab = document.getElementById('news-tab');
    if (!newsTab || !newsTab.classList.contains('active')) {
        return; // 不在资讯页，不渲染
    }
    
    // 计算本次要渲染的范围
    const start = newsRenderedCount;
    const end = Math.min(start + newsPageSize, newsAllItems.length);
    const batch = newsAllItems.slice(start, end);
    
    if (batch.length === 0) {
        // 已全部渲染完成
        const loadingDiv = container.querySelector('.loading-more');
        if (loadingDiv) {
            loadingDiv.remove();
        }
        return;
    }
    
    // 移除之前的加载提示
    const loadingDiv = container.querySelector('.loading-more');
    if (loadingDiv) {
        loadingDiv.remove();
    }
    
    // 转义HTML特殊字符，避免XSS
    const escapeHtml = (text) => {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    };
    
    // 渲染本批数据
    batch.forEach((news, batchIndex) => {
        const index = start + batchIndex; // 全局索引
        const content = news.content || '';
        // 如果内容超过500字符，显示前500字符并提供展开功能
        const shouldTruncate = content.length > 500;
        const displayContent = shouldTruncate ? content.substring(0, 500) : content;
        const contentId = `news-content-${index}`;
        const btnId = `news-expand-btn-${index}`;
        
        const newsItem = document.createElement('div');
        newsItem.className = 'news-item';
        newsItem.innerHTML = `
            <h4>${escapeHtml(news.title || '-')}</h4>
            <div class="news-content" id="${contentId}">${escapeHtml(displayContent)}${shouldTruncate ? '...' : ''}</div>
            ${shouldTruncate ? `<button class="news-expand-btn" id="${btnId}" data-full-content="${escapeHtml(content)}">展开全文</button>` : ''}
            <div class="meta">
                ${escapeHtml(news.publish_time || news.collect_time || '-')} | ${escapeHtml(news.source || '未知来源')}
            </div>
        `;
        container.appendChild(newsItem);
        
        // 绑定展开按钮事件
        if (shouldTruncate) {
            const expandBtn = document.getElementById(btnId);
            if (expandBtn) {
                expandBtn.addEventListener('click', function() {
                    const contentDiv = document.getElementById(contentId);
                    const fullContent = this.getAttribute('data-full-content');
                    if (contentDiv && fullContent) {
                        contentDiv.textContent = fullContent;
                        this.remove();
                    }
                });
            }
        }
    });
    
    newsRenderedCount = end;
    
    // 如果还有更多数据，添加加载提示
    if (newsRenderedCount < newsAllItems.length) {
        const loadingDiv = document.createElement('div');
        loadingDiv.className = 'loading-more';
        loadingDiv.style.cssText = 'text-align: center; padding: 20px; color: #94a3b8;';
        loadingDiv.textContent = '加载中...';
        container.appendChild(loadingDiv);
    }
    
    console.log(`[资讯] 已渲染 ${newsRenderedCount}/${newsAllItems.length} 条资讯`);
}

// 全局函数
window.loadChart = loadChart;

// 配置模块
let configInitialized = false;
function initConfig() {
    if (configInitialized) {
        return; // 已经初始化过，避免重复初始化
    }
    
    const saveBtn = document.getElementById('cfg-save-btn');
    if (!saveBtn) {
        console.warn('[配置] 保存按钮不存在，将在DOM加载后重试');
        // 延迟重试，确保DOM已加载
        setTimeout(() => {
            initConfig();
        }, 100);
        return;
    }

    configInitialized = true;
    
    // 绑定保存按钮事件，添加错误处理
    saveBtn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        console.log('[配置] 保存按钮被点击');
        try {
            saveConfig();
        } catch (error) {
            console.error('[配置] 保存按钮点击处理失败:', error);
            showToast(`保存失败: ${error.message}`, 'error');
        }
    });
    
    console.log('[配置] 保存按钮事件已绑定');
    
    // 如果当前在配置页，立即加载
    const configTab = document.getElementById('config-tab');
    if (configTab && configTab.classList.contains('active')) {
        loadConfig();
    }

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
    // 检查是否在配置页
    const configTab = document.getElementById('config-tab');
    if (!configTab || !configTab.classList.contains('active')) {
        console.log('[配置] 当前不在配置页，跳过加载');
        return;
    }
    
    const statusEl = document.getElementById('cfg-status');
    if (statusEl) {
        statusEl.textContent = '加载中...';
    }
    
    try {
        console.log('[配置] 开始加载配置');
        const res = await apiFetch(`${API_BASE}/api/config`);
        
        if (!res.ok) {
            const errorText = await res.text().catch(() => '');
            console.error('[配置] 加载失败:', res.status, errorText);
            
            if (res.status === 401) {
                throw new Error('需要管理员权限，请重新登录');
            }
            throw new Error(`HTTP ${res.status}: ${errorText || res.statusText}`);
        }
        
        const data = await res.json();
        console.log('[配置] 配置加载成功');

        document.getElementById('cfg-collector-interval').value = data.collector_interval_seconds ?? 60;
        document.getElementById('cfg-kline-years').value = data.kline_years ?? 1;
        
        // K线数据源选择
        const klineDataSourceEl = document.getElementById('cfg-kline-data-source');
        if (klineDataSourceEl) {
            klineDataSourceEl.value = data.kline_data_source || 'auto';
        }
        
        // 实时行情数据源选择
        const spotDataSourceEl = document.getElementById('cfg-spot-data-source');
        if (spotDataSourceEl) {
            spotDataSourceEl.value = data.spot_data_source || 'auto';
        }
        
        // Tushare Token（不回显，只在服务端保存）
        document.getElementById('cfg-tushare-token').value = '';
        
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

        // 选股面板默认值已移除，使用固定值

        if (statusEl) statusEl.textContent = '配置已从服务器加载。';
    } catch (error) {
        console.error('加载配置失败:', error);
        if (statusEl) statusEl.textContent = `加载配置失败: ${error.message}`;
    }
}

async function saveConfig() {
    console.log('[配置] saveConfig函数被调用');
    
    try {
        const statusEl = document.getElementById('cfg-status');
        if (statusEl) statusEl.textContent = '保存中...';
        
        // 检查是否在配置页
        const configTab = document.getElementById('config-tab');
        if (!configTab || !configTab.classList.contains('active')) {
            console.warn('[配置] 当前不在配置页，无法保存');
            if (statusEl) statusEl.textContent = '请先切换到配置页';
            showToast('请先切换到配置页', 'error');
            return;
        }
        
        const interval = parseInt(document.getElementById('cfg-collector-interval')?.value || '60');
        const klineYears = parseFloat(document.getElementById('cfg-kline-years')?.value || '1');
        const klineDataSource = document.getElementById('cfg-kline-data-source')?.value || 'auto';
        const spotDataSource = document.getElementById('cfg-spot-data-source')?.value || 'auto';
        const tushareToken = document.getElementById('cfg-tushare-token')?.value?.trim() || null;

        const channels = [];
        const telegramEnabled = document.getElementById('cfg-notify-telegram')?.checked ?? false;
        const emailEnabled = document.getElementById('cfg-notify-email')?.checked ?? false;
        const wechatEnabled = document.getElementById('cfg-notify-wechat')?.checked ?? false;
        
        if (telegramEnabled) channels.push('telegram');
        if (emailEnabled) channels.push('email');
        if (wechatEnabled) channels.push('wechat');

        console.log('[配置] 准备保存配置', { interval, klineYears, klineDataSource, spotDataSource, hasTushareToken: !!tushareToken });
        
        const res = await apiFetch(`${API_BASE}/api/config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                collector_interval_seconds: interval,
                kline_years: klineYears,
                kline_data_source: klineDataSource,
                spot_data_source: spotDataSource,
                tushare_token: tushareToken,
                // AI 配置
                openai_api_key: document.getElementById('cfg-ai-api-key')?.value?.trim() || null,
                openai_api_base: document.getElementById('cfg-ai-api-base')?.value?.trim() || null,
                openai_model: document.getElementById('cfg-ai-model')?.value?.trim() || null,
                ai_auto_analyze_time: document.getElementById('cfg-ai-auto-analyze-time')?.value?.trim() || null,
                ai_data_period: document.querySelector('input[name="cfg-ai-data-period"]:checked')?.value || 'daily',
                ai_data_count: parseInt(document.getElementById('cfg-ai-data-count')?.value || '500'),
                ai_batch_size: parseInt(document.getElementById('cfg-ai-batch-size')?.value || '5'),
                ai_notify_telegram: document.getElementById('cfg-ai-notify-telegram')?.checked ?? false,
                ai_notify_email: document.getElementById('cfg-ai-notify-email')?.checked ?? false,
                ai_notify_wechat: document.getElementById('cfg-ai-notify-wechat')?.checked ?? false,
                notify_channels: channels,
                notify_telegram_enabled: telegramEnabled,
                notify_telegram_bot_token: document.getElementById('cfg-telegram-bot-token')?.value?.trim() || null,
                notify_telegram_chat_id: document.getElementById('cfg-telegram-chat-id')?.value?.trim() || null,
                notify_email_enabled: emailEnabled,
                notify_email_smtp_host: document.getElementById('cfg-email-smtp-host')?.value?.trim() || null,
                notify_email_smtp_port: document.getElementById('cfg-email-smtp-port')?.value ? parseInt(document.getElementById('cfg-email-smtp-port').value) : null,
                notify_email_user: document.getElementById('cfg-email-user')?.value?.trim() || null,
                notify_email_password: document.getElementById('cfg-email-password')?.value?.trim() || null, // 如果为空则不更新密码
                notify_email_to: document.getElementById('cfg-email-to')?.value?.trim() || null,
                notify_wechat_enabled: wechatEnabled,
                notify_wechat_webhook_url: document.getElementById('cfg-wechat-webhook-url')?.value?.trim() || null,
            }),
        });

        console.log('[配置] 保存请求已发送，等待响应...', res.status);

        if (!res.ok) {
            const errText = await res.text().catch(() => '');
            console.error('[配置] 保存失败:', res.status, errText);
            throw new Error(errText || `HTTP ${res.status}`);
        }

        const data = await res.json();
        console.log('[配置] 保存成功:', data);

        if (statusEl) statusEl.textContent = '配置已保存。若修改了采集间隔，新设置会在下一轮采集后生效。';
        showToast('配置已保存', 'success');
    } catch (error) {
        console.error('[配置] 保存配置失败:', error);
        const statusEl = document.getElementById('cfg-status');
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
            if (statusEl) statusEl.textContent = '密码修改成功，需要重新登录';
            showToast('密码修改成功，请重新登录', 'success');
            
            // 清除登录状态（修改密码后登录状态失效）
            localStorage.removeItem('isLoggedIn');
            localStorage.removeItem('apiToken');
            localStorage.removeItem('adminToken');
            apiToken = null;
            adminToken = null;
            
            // 清空输入框
            document.getElementById('cfg-old-password').value = '';
            document.getElementById('cfg-new-password').value = '';
            document.getElementById('cfg-confirm-password').value = '';
            
            // 延迟一下再显示登录界面，让用户看到成功提示
            setTimeout(() => {
                const loginOverlay = document.getElementById('login-overlay');
                if (loginOverlay) {
                    loginOverlay.style.display = 'flex';
                    // 刷新页面以确保所有状态都被清除
                    window.location.reload();
                }
            }, 1500);
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
    console.log('[市场状态] ========== initMarketStatus: 开始初始化市场状态模块 ==========');
    
    try {
        // 清除旧的定时器（如果存在）
        if (marketStatusInterval) {
            console.log('[市场状态] 清除旧的定时器');
            clearInterval(marketStatusInterval);
            marketStatusInterval = null;
        }
        
        // 检查DOM元素是否存在
        const aStatusEl = document.getElementById('market-status-a');
        const hkStatusEl = document.getElementById('market-status-hk');
        
        console.log('[市场状态] DOM元素检查:', { 
            aStatusEl: !!aStatusEl, 
            hkStatusEl: !!hkStatusEl,
            aStatusText: aStatusEl?.textContent,
            hkStatusText: hkStatusEl?.textContent,
            documentReady: document.readyState,
            bodyExists: !!document.body
        });
        
        if (!aStatusEl || !hkStatusEl) {
            console.error('[市场状态] DOM元素未找到！', {
                aStatusEl: aStatusEl,
                hkStatusEl: hkStatusEl,
                allElements: document.querySelectorAll('[id*="market-status"]').length
            });
            // 延迟重试
            setTimeout(() => {
                console.log('[市场状态] 延迟重试初始化');
                initMarketStatus();
            }, 500);
            return;
        }
        
        // 立即更新一次（页面加载时获取初始状态）
        console.log('[市场状态] 立即执行第一次更新');
        updateMarketStatus();
        
        // 不再轮询，后续依赖SSE推送市场状态更新
        // SSE会在 handleMarketStatusUpdate 中处理状态更新
        console.log('[市场状态] ========== 初始化完成，后续依赖SSE推送更新 ==========');
    } catch (error) {
        console.error('[市场状态] 初始化失败:', error);
        console.error('[市场状态] 错误堆栈:', error.stack);
    }
}

// 市场状态更新锁，防止重复更新
let isUpdatingMarketStatus = false;

async function updateMarketStatus() {
    console.log('[市场状态] ========== updateMarketStatus: 函数被调用 ==========');
    
    // 防止重复更新
    if (isUpdatingMarketStatus) {
        console.log('[市场状态] ⚠️ 正在更新中，跳过重复请求');
        return;
    }
    
    const aStatusEl = document.getElementById('market-status-a');
    const hkStatusEl = document.getElementById('market-status-hk');
    
    console.log('[市场状态] 元素查找结果', { 
        aStatusEl: !!aStatusEl, 
        hkStatusEl: !!hkStatusEl,
        aStatusText: aStatusEl?.textContent,
        hkStatusText: hkStatusEl?.textContent
    });
    
    if (!aStatusEl || !hkStatusEl) {
        console.warn('[市场状态] 元素未找到', { 
            aStatusEl: !!aStatusEl, 
            hkStatusEl: !!hkStatusEl,
            documentReady: document.readyState
        });
        // 如果元素不存在，等待一段时间后重试（可能是DOM还没加载完成）
        // 最多重试5次
        if (!updateMarketStatus.retryCount) {
            updateMarketStatus.retryCount = 0;
        }
        if (updateMarketStatus.retryCount < 5) {
            updateMarketStatus.retryCount++;
            console.log('[市场状态] 延迟重试', updateMarketStatus.retryCount, '/5');
            setTimeout(() => {
                updateMarketStatus();
            }, 1000);
        } else {
            console.error('[市场状态] 重试次数过多，停止重试');
        }
        return;
    }
    
    // 重置重试计数
    updateMarketStatus.retryCount = 0;
    
    // 如果元素存在但内容为空，显示"加载中..."
    if (!aStatusEl.textContent || aStatusEl.textContent === '') {
        aStatusEl.textContent = '加载中...';
        aStatusEl.className = 'market-status-value closed';
    }
    if (!hkStatusEl.textContent || hkStatusEl.textContent === '') {
        hkStatusEl.textContent = '加载中...';
        hkStatusEl.className = 'market-status-value closed';
    }
    
    isUpdatingMarketStatus = true;
    
    const requestUrl = `${API_BASE}/api/market/status`;
    console.log('[市场状态] 开始请求市场状态', { 
        url: requestUrl,
        API_BASE: API_BASE,
        hasApiToken: !!apiToken,
        hasAdminToken: !!adminToken,
        apiToken: apiToken ? apiToken.substring(0, 10) + '...' : null
    });
    
    try {
        // 设置超时，避免长时间等待（增加到10秒）
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            console.warn('[市场状态] 请求超时，取消请求');
            controller.abort('市场状态请求超时（10秒）');
        }, 10000); // 10秒超时
        
        console.log('[市场状态] 发送请求到', requestUrl);
        const res = await apiFetch(requestUrl, {
            signal: controller.signal
        });
        
        console.log('[市场状态] 收到响应', { 
            status: res.status, 
            ok: res.ok,
            statusText: res.statusText,
            headers: Object.fromEntries(res.headers.entries())
        });
        
        clearTimeout(timeoutId);
        
        if (!res.ok) {
            const errorText = await res.text().catch(() => '');
            console.error('[市场状态] 获取市场状态失败:', { 
                status: res.status, 
                statusText: res.statusText,
                errorText: errorText,
                url: requestUrl
            });
            
            // 显示错误状态
            if (aStatusEl) {
                aStatusEl.textContent = `错误(${res.status})`;
                aStatusEl.className = 'market-status-value closed';
            }
            if (hkStatusEl) {
                hkStatusEl.textContent = `错误(${res.status})`;
                hkStatusEl.className = 'market-status-value closed';
            }
            isUpdatingMarketStatus = false;
            return;
        }
        
        const data = await res.json();
        console.log('[市场状态] 响应数据', JSON.stringify(data, null, 2));
        if (data.code === 0 && data.data) {
            const aStatus = data.data.a;
            const hkStatus = data.data.hk;
            
            console.log('[市场状态] 更新状态', { 
                aStatus: aStatus, 
                hkStatus: hkStatus,
                aStatusText: aStatus.status,
                hkStatusText: hkStatus.status
            });
            
            // 更新A股状态（包含下一个开盘时间）
            let aStatusText = aStatus.status || '未知';
            if (!aStatus.is_trading && aStatus.next_open) {
                aStatusText += ` (${aStatus.next_open}开)`;
            }
            aStatusEl.textContent = aStatusText;
            aStatusEl.className = 'market-status-value ' + (aStatus.is_trading ? 'trading' : 'closed');
            aStatusEl.title = aStatus.next_open_full ? `下次开盘: ${aStatus.next_open_full}` : '';
            console.log('[市场状态] A股状态已更新:', aStatusText, aStatus.is_trading ? '交易中' : '已收盘');
            
            // 更新港股状态（包含下一个开盘时间）
            let hkStatusText = hkStatus.status || '未知';
            if (!hkStatus.is_trading && hkStatus.next_open) {
                hkStatusText += ` (${hkStatus.next_open}开)`;
            }
            hkStatusEl.textContent = hkStatusText;
            hkStatusEl.className = 'market-status-value ' + (hkStatus.is_trading ? 'trading' : 'closed');
            hkStatusEl.title = hkStatus.next_open_full ? `下次开盘: ${hkStatus.next_open_full}` : '';
            console.log('[市场状态] 港股状态已更新:', hkStatusText, hkStatus.is_trading ? '交易中' : '已收盘');
            
            console.log('[市场状态] 状态更新完成');
        } else {
            // 显示错误状态
            console.error('市场状态数据格式错误:', data);
            if (aStatusEl) {
                aStatusEl.textContent = '未知';
                aStatusEl.className = 'market-status-value closed';
            }
            if (hkStatusEl) {
                hkStatusEl.textContent = '未知';
                hkStatusEl.className = 'market-status-value closed';
            }
        }
    } catch (error) {
        console.error('[市场状态] 捕获到错误', {
            name: error.name,
            message: error.message,
            stack: error.stack,
            url: requestUrl
        });
        
        if (error.name === 'AbortError' || error.message?.includes('aborted')) {
            console.warn('[市场状态] 获取市场状态超时或被取消');
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
            console.error('[市场状态] 更新市场状态失败:', error);
            // 显示错误状态
            const errorMsg = error.message || '错误';
            if (aStatusEl) {
                aStatusEl.textContent = errorMsg.length > 10 ? '错误' : errorMsg;
                aStatusEl.className = 'market-status-value closed';
            }
            if (hkStatusEl) {
                hkStatusEl.textContent = errorMsg.length > 10 ? '错误' : errorMsg;
                hkStatusEl.className = 'market-status-value closed';
            }
        }
    } finally {
        isUpdatingMarketStatus = false;
        console.log('[市场状态] ========== updateMarketStatus 函数执行完成 ==========');
    }
}

// 在脚本加载完成后立即检查
console.log('[全局] app.js 脚本加载完成，等待DOMContentLoaded...');

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

    // 检查本地存储的登录状态，但需要验证token有效性
    const isLoggedIn = localStorage.getItem('isLoggedIn');
    let savedApiToken = localStorage.getItem('apiToken');
    let savedAdminToken = localStorage.getItem('adminToken');
    
    // 过滤掉无效的token值
    if (savedApiToken === 'null' || savedApiToken === '') savedApiToken = null;
    if (savedAdminToken === 'null' || savedAdminToken === '') savedAdminToken = null;
    
    // 如果有token，尝试验证其有效性
    if (isLoggedIn === 'true' || savedApiToken) {
        apiToken = savedApiToken;
        adminToken = savedAdminToken;
        
        // 验证token是否有效（通过尝试访问一个需要认证的接口）
        try {
            const testRes = await apiFetch(`${API_BASE}/api/config`);
            if (testRes.ok) {
                // Token有效，隐藏登录界面并启动应用
                console.log('Token验证成功，自动登录');
                overlay.style.display = 'none';
                if (isLoggedIn !== 'true') {
                    localStorage.setItem('isLoggedIn', 'true');
                }
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
                overlay.style.display = 'flex'; // 显示登录界面
            }
        } catch (error) {
            // 网络错误或其他错误，可能是API未启动
            console.warn('验证token时出错:', error);
            // 清除可能无效的登录状态，强制重新登录
            localStorage.removeItem('isLoggedIn');
            localStorage.removeItem('apiToken');
            localStorage.removeItem('adminToken');
            apiToken = null;
            adminToken = null;
            overlay.style.display = 'flex'; // 显示登录界面
        }
    } else {
        // 没有登录状态，确保显示登录界面
        overlay.style.display = 'flex';
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