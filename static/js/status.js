// 任务状态管理页面JavaScript

// 全局变量
let currentPage = 1;
let pageSize = 10;
let totalPages = 1;
let autoRefreshInterval = null;
let isAutoRefresh = false;
let currentFilter = 'all'; // all, running
let tableHelper;

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    tableHelper = new TableHelper('status-table');
    initializePage();
});

// 初始化页面
function initializePage() {
    loadStatuses();
    
    // 启动自动刷新
    startAutoRefresh();
    
    // 页面卸载时停止自动刷新
    window.addEventListener('beforeunload', function() {
        stopAutoRefresh();
    });
}

// 启动自动刷新
function startAutoRefresh() {
    // 每5秒刷新一次状态
    autoRefreshInterval = setInterval(() => {
        loadStatuses(currentPage, false); // 静默刷新，不显示加载状态
    }, 5000);
}

// 停止自动刷新
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// 加载状态列表
async function loadStatuses(page = 1, showLoading = true) {
    const table = new TableHelper('status-table');
    if (showLoading) {
        table.showLoading();
    }
    
    try {
        const skip = (page - 1) * pageSize;
        const params = new URLSearchParams({
            skip: skip,
            limit: pageSize
        });
        
        const response = await ApiClient.get(`/status/?${params}`);
        statuses = Array.isArray(response) ? response : [];
        
        // 简单分页处理
        totalPages = statuses.length < pageSize ? page : page + 1;
        currentPage = page;
        
        if (showLoading) {
            table.clear();
        }
        
        if (statuses.length === 0) {
            if (showLoading) {
                table.showEmpty('暂无任务状态记录');
            }
            updatePagination();
            return;
        }
        
        // 如果是静默刷新，只更新表格内容
        if (!showLoading) {
            updateTableContent();
        } else {
            renderTable();
        }
        
        updatePagination();
        
    } catch (error) {
        console.error('加载状态列表失败:', error);
        if (showLoading) {
            table.showEmpty('加载失败');
            Utils.showError('加载状态列表失败: ' + (error.response?.data?.detail || error.message));
        }
    }
}

// 渲染表格
function renderTable() {
    const table = new TableHelper('status-table');
    table.clear();
    
    statuses.forEach(status => {
        const row = createStatusRow(status);
        table.addRow(row);
    });
}

// 更新表格内容（静默刷新）
function updateTableContent() {
    const tbody = document.querySelector('#status-table tbody');
    if (!tbody) return;
    
    const rows = tbody.querySelectorAll('tr');
    
    statuses.forEach((status, index) => {
        if (rows[index]) {
            const row = createStatusRow(status);
            // 更新现有行
            row.forEach((cellContent, cellIndex) => {
                if (rows[index].cells[cellIndex]) {
                    rows[index].cells[cellIndex].innerHTML = cellContent;
                }
            });
        } else {
            // 添加新行
            const table = new TableHelper('status-table');
            const row = createStatusRow(status);
            table.addRow(row);
        }
    });
    
    // 移除多余的行
    for (let i = statuses.length; i < rows.length; i++) {
        if (rows[i]) {
            rows[i].remove();
        }
    }
}

// 创建状态行
function createStatusRow(status) {
    const statusBadge = getTaskStatusBadge(status.status);
    const duration = status.created_at ? 
        Utils.formatDuration((Date.now() - new Date(status.created_at).getTime()) / 1000) : '-';
    
    const actions = `
        <div class="btn-group btn-group-sm">
            <button class="btn btn-outline-info" onclick="viewStatusDetail(${status.id})" title="查看详情">
                <i class="fas fa-eye"></i>
            </button>
            ${status.status === 'RUNNING' ? `
                <button class="btn btn-outline-warning" onclick="cancelTask(${status.id})" title="取消任务">
                    <i class="fas fa-stop"></i>
                </button>
            ` : ''}
            ${status.status !== 'RUNNING' ? `
                <button class="btn btn-outline-danger" onclick="deleteStatus(${status.id})" title="删除">
                    <i class="fas fa-trash"></i>
                </button>
            ` : ''}
        </div>
    `;
    
    return [
        status.job_name || '未知任务',
        statusBadge,
        status.current_stage || '-',
        status.progress_percentage !== null ? `${status.progress_percentage}%` : '-',
        status.is_cancellation_requested ? '<span class="badge bg-warning">已请求取消</span>' : '-',
        Utils.formatDateTime(status.created_at),
        duration,
        actions
    ];
}

// 获取任务状态徽章
function getTaskStatusBadge(status) {
    const statusMap = {
        'RUNNING': { class: 'bg-primary', text: '运行中' },
        'STOP_REQUESTED': { class: 'bg-warning', text: '请求停止' },
        'STOPPED': { class: 'bg-secondary', text: '已停止' },
        'COMPLETED': { class: 'bg-success', text: '已完成' },
        'FAILED': { class: 'bg-danger', text: '失败' }
    };
    
    const config = statusMap[status] || { class: 'bg-secondary', text: status };
    return `<span class="badge ${config.class}">${config.text}</span>`;
}

// 加载任务列表
async function loadJobs() {
    try {
        jobs = await ApiClient.get('/jobs/');
    } catch (error) {
        console.error('加载任务列表失败:', error);
    }
}

// 查看状态详情
async function viewStatusDetail(statusId) {
    try {
        const status = await ApiClient.get(`/status/${statusId}`);
        
        let detailContent = `
            <div class="row mb-3">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0">基本信息</h6>
                        </div>
                        <div class="card-body">
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>任务名称:</strong></div>
                                <div class="col-sm-8">${status.job_name || '未知任务'}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>状态:</strong></div>
                                <div class="col-sm-8">${getTaskStatusBadge(status.status)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>当前阶段:</strong></div>
                                <div class="col-sm-8">${status.current_stage || '-'}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>进度:</strong></div>
                                <div class="col-sm-8">${status.progress_percentage !== null ? `${status.progress_percentage}%` : '-'}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>取消请求:</strong></div>
                                <div class="col-sm-8">${status.is_cancellation_requested ? '是' : '否'}</div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0">时间信息</h6>
                        </div>
                        <div class="card-body">
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>创建时间:</strong></div>
                                <div class="col-sm-8">${Utils.formatDateTime(status.created_at)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>更新时间:</strong></div>
                                <div class="col-sm-8">${Utils.formatDateTime(status.updated_at)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>运行时长:</strong></div>
                                <div class="col-sm-8">${status.created_at ? Utils.formatDuration((Date.now() - new Date(status.created_at).getTime()) / 1000) : '-'}</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // 如果有关联的执行日志，显示日志信息
        if (status.execution_log_id) {
            detailContent += `
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0">关联信息</h6>
                    </div>
                    <div class="card-body">
                        <div class="row mb-2">
                            <div class="col-sm-3"><strong>执行日志ID:</strong></div>
                            <div class="col-sm-9">
                                <a href="#" onclick="viewExecutionLog(${status.execution_log_id})" class="text-decoration-none">
                                    ${status.execution_log_id}
                                    <i class="fas fa-external-link-alt ms-1"></i>
                                </a>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        document.getElementById('statusDetailContent').innerHTML = detailContent;
        const modal = new bootstrap.Modal(document.getElementById('statusDetailModal'));
        modal.show();
        
    } catch (error) {
        console.error('获取状态详情失败:', error);
        Utils.showError('获取状态详情失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 查看执行日志
function viewExecutionLog(logId) {
    // 关闭当前模态框
    const modal = bootstrap.Modal.getInstance(document.getElementById('statusDetailModal'));
    if (modal) {
        modal.hide();
    }
    
    // 跳转到日志页面或打开日志详情
    window.location.href = `/logs.html?logId=${logId}`;
}

// 取消任务
async function cancelTask(statusId) {
    const status = statuses.find(s => s.id === statusId);
    if (!status) return;
    
    if (!await Utils.confirm(`确定要取消任务"${status.job_name}"吗？`)) {
        return;
    }
    
    try {
        await ApiClient.post(`/status/${statusId}/cancel`);
        Utils.showSuccess('取消请求已发送，任务将在下次检查时停止');
        loadStatuses(currentPage, false); // 静默刷新
    } catch (error) {
        console.error('取消任务失败:', error);
        Utils.showError('取消任务失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 删除状态记录
async function deleteStatus(statusId) {
    const status = statuses.find(s => s.id === statusId);
    if (!status) return;
    
    if (!await Utils.confirm(`确定要删除此状态记录吗？`)) {
        return;
    }
    
    try {
        await ApiClient.delete(`/status/${statusId}`);
        Utils.showSuccess('状态记录删除成功');
        loadStatuses(currentPage);
    } catch (error) {
        console.error('删除状态记录失败:', error);
        Utils.showError('删除状态记录失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 清理旧状态记录
async function cleanupOldStatuses() {
    if (!await Utils.confirm('确定要清理旧的状态记录吗？这将删除所有非运行状态的记录。')) {
        return;
    }
    
    try {
        const response = await ApiClient.post('/status/cleanup');
        Utils.showSuccess(`清理完成，删除了${response.deleted_count}条旧状态记录`);
        loadStatuses(currentPage);
    } catch (error) {
        console.error('清理状态记录失败:', error);
        Utils.showError('清理状态记录失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 只显示运行中的任务
function showRunningOnly() {
    loadRunningStatuses();
}

// 加载运行中的状态
async function loadRunningStatuses() {
    const table = new TableHelper('status-table');
    table.showLoading();
    
    try {
        const response = await ApiClient.get('/status/running');
        statuses = Array.isArray(response) ? response : [];
        
        table.clear();
        
        if (statuses.length === 0) {
            table.showEmpty('当前没有运行中的任务');
            return;
        }
        
        statuses.forEach(status => {
            const row = createStatusRow(status);
            table.addRow(row);
        });
        
        // 隐藏分页
        document.getElementById('pagination-container').style.display = 'none';
        
    } catch (error) {
        console.error('加载运行中状态失败:', error);
        table.showEmpty('加载失败');
        Utils.showError('加载运行中状态失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 显示所有状态
function showAllStatuses() {
    document.getElementById('pagination-container').style.display = 'block';
    loadStatuses(1);
}

// 更新分页
function updatePagination() {
    document.getElementById('current-page').textContent = currentPage;
    document.getElementById('total-pages').textContent = totalPages;
    
    const pagination = document.getElementById('pagination');
    let paginationHtml = '';
    
    // 上一页
    if (currentPage > 1) {
        paginationHtml += `
            <li class="page-item">
                <a class="page-link" href="#" onclick="loadStatuses(${currentPage - 1})">上一页</a>
            </li>
        `;
    }
    
    // 页码
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHtml += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="loadStatuses(${i})">${i}</a>
            </li>
        `;
    }
    
    // 下一页
    if (currentPage < totalPages) {
        paginationHtml += `
            <li class="page-item">
                <a class="page-link" href="#" onclick="loadStatuses(${currentPage + 1})">下一页</a>
            </li>
        `;
    }
    
    pagination.innerHTML = paginationHtml;
}

// 切换自动刷新
function toggleAutoRefresh() {
    const button = document.getElementById('auto-refresh-btn');
    
    if (autoRefreshInterval) {
        stopAutoRefresh();
        button.innerHTML = '<i class="fas fa-play"></i> 启动自动刷新';
        button.classList.remove('btn-success');
        button.classList.add('btn-outline-success');
        Utils.showInfo('自动刷新已停止');
    } else {
        startAutoRefresh();
        button.innerHTML = '<i class="fas fa-pause"></i> 停止自动刷新';
        button.classList.remove('btn-outline-success');
        button.classList.add('btn-success');
        Utils.showInfo('自动刷新已启动');
    }
}