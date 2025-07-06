// 日志管理页面JavaScript

let logs = [];
let jobs = [];
let currentFilter = 'all';
let currentPage = 1;
let pageSize = 20;
let totalPages = 1;

// 页面加载时初始化
document.addEventListener('DOMContentLoaded', function() {
    loadLogs();
    loadJobs();
    loadSummary();
});

// 加载日志列表
async function loadLogs(page = 1) {
    const table = new TableHelper('logs-table');
    table.showLoading();
    
    try {
        const skip = (page - 1) * pageSize;
        const params = new URLSearchParams({
            skip: skip,
            limit: pageSize
        });
        
        if (currentFilter !== 'all') {
            params.append('status', currentFilter);
        }
        
        const response = await ApiClient.get(`/logs/?${params}`);
        logs = Array.isArray(response) ? response : [];
        // 简单分页处理：如果返回的记录数等于页面大小，假设还有下一页
        totalPages = logs.length < pageSize ? page : page + 1;
        currentPage = page;
        
        table.clear();
        
        if (logs.length === 0) {
            table.showEmpty('暂无执行日志');
            updatePagination();
            return;
        }
        
        logs.forEach(log => {
            const statusBadge = Utils.getStatusBadge(log.status);
            const duration = log.end_time && log.start_time 
                ? Utils.formatDuration((new Date(log.end_time) - new Date(log.start_time)) / 1000)
                : (log.status === 'running' ? '运行中' : '-');
            
            const actions = `
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-info" onclick="viewLogDetail(${log.id})" title="查看详情">
                        <i class="fas fa-eye"></i>
                    </button>
                    ${log.status !== 'running' ? `
                        <button class="btn btn-outline-danger" onclick="deleteLog(${log.id})" title="删除">
                            <i class="fas fa-trash"></i>
                        </button>
                    ` : ''}
                </div>
            `;
            
            const row = [
                log.job_name || '未知任务',
                statusBadge,
                Utils.formatDateTime(log.start_time),
                log.end_time ? Utils.formatDateTime(log.end_time) : '-',
                duration,
                log.tables_processed || 0,
                Utils.formatNumber(log.records_transferred || 0),
                actions
            ];
            table.addRow(row);
        });
        
        updatePagination();
        
    } catch (error) {
        console.error('加载日志列表失败:', error);
        table.showEmpty('加载失败');
        Utils.showError('加载日志列表失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 加载任务列表（用于清理功能）
async function loadJobs() {
    try {
        jobs = await ApiClient.get('/jobs/');
        
        const jobSelect = document.getElementById('jobSelect');
        jobSelect.innerHTML = '<option value="">请选择要清理的任务</option>';
        
        jobs.forEach(job => {
            jobSelect.innerHTML += `<option value="${job.id}">${job.name}</option>`;
        });
        
    } catch (error) {
        console.error('加载任务列表失败:', error);
    }
}

// 加载统计摘要
async function loadSummary() {
    try {
        const summary = await ApiClient.get('/logs/summary/overall');
        
        document.getElementById('total-executions').textContent = summary.total_executions || 0;
        document.getElementById('successful-executions').textContent = summary.successful_executions || 0;
        document.getElementById('failed-executions').textContent = summary.failed_executions || 0;
        document.getElementById('success-rate').textContent = summary.success_rate ? 
            summary.success_rate.toFixed(1) + '%' : '0%';
        
    } catch (error) {
        console.error('加载统计摘要失败:', error);
    }
}

// 筛选日志
function filterLogs(status) {
    currentFilter = status;
    currentPage = 1;
    loadLogs(1);
}

// 查看日志详情
async function viewLogDetail(logId) {
    try {
        const log = await ApiClient.get(`/logs/${logId}`);
        
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
                                <div class="col-sm-8">${log.job_name || '未知任务'}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>执行状态:</strong></div>
                                <div class="col-sm-8">${Utils.getStatusBadge(log.status)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>开始时间:</strong></div>
                                <div class="col-sm-8">${Utils.formatDateTime(log.start_time)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>结束时间:</strong></div>
                                <div class="col-sm-8">${log.end_time ? Utils.formatDateTime(log.end_time) : '-'}</div>
                            </div>
                            ${log.end_time && log.start_time ? `
                            <div class="row mb-2">
                                <div class="col-sm-4"><strong>执行耗时:</strong></div>
                                <div class="col-sm-8">${Utils.formatDuration((new Date(log.end_time) - new Date(log.start_time)) / 1000)}</div>
                            </div>
                            ` : ''}
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h6 class="mb-0">执行统计</h6>
                        </div>
                        <div class="card-body">
                            <div class="row mb-2">
                                <div class="col-sm-6"><strong>处理表数:</strong></div>
                                <div class="col-sm-6">${log.tables_processed || 0}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-6"><strong>传输记录数:</strong></div>
                                <div class="col-sm-6">${Utils.formatNumber(log.records_transferred || 0)}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-6"><strong>数据大小:</strong></div>
                                <div class="col-sm-6">${log.data_size ? Utils.formatBytes(log.data_size) : '-'}</div>
                            </div>
                            <div class="row mb-2">
                                <div class="col-sm-6"><strong>平均速度:</strong></div>
                                <div class="col-sm-6">${calculateTransferRate(log)}</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // 如果有错误信息，显示错误详情
        if (log.error_message) {
            detailContent += `
                <div class="card">
                    <div class="card-header bg-danger text-white">
                        <h6 class="mb-0">错误信息</h6>
                    </div>
                    <div class="card-body">
                        <pre class="mb-0" style="white-space: pre-wrap; word-wrap: break-word;">${log.error_message}</pre>
                    </div>
                </div>
            `;
        }
        
        // 如果有详细日志，显示日志内容
        if (log.log_details) {
            detailContent += `
                <div class="card mt-3">
                    <div class="card-header">
                        <h6 class="mb-0">执行日志</h6>
                    </div>
                    <div class="card-body">
                        <pre class="mb-0" style="white-space: pre-wrap; word-wrap: break-word; max-height: 400px; overflow-y: auto;">${log.log_details}</pre>
                    </div>
                </div>
            `;
        }
        
        document.getElementById('logDetailContent').innerHTML = detailContent;
        const modal = new bootstrap.Modal(document.getElementById('logDetailModal'));
        modal.show();
        
    } catch (error) {
        console.error('获取日志详情失败:', error);
        Utils.showError('获取日志详情失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 计算传输速率
function calculateTransferRate(log) {
    if (!log.data_size || !log.start_time || !log.end_time) {
        return '-';
    }
    
    const duration = (new Date(log.end_time) - new Date(log.start_time)) / 1000; // 秒
    if (duration <= 0) return '-';
    
    const rate = log.data_size / duration; // 字节/秒
    return Utils.formatBytes(rate) + '/s';
}

// 删除日志
async function deleteLog(logId) {
    const log = logs.find(l => l.id === logId);
    if (!log) return;
    
    if (!await Utils.confirm(`确定要删除此执行日志吗？`)) {
        return;
    }
    
    try {
        await ApiClient.delete(`/logs/${logId}`);
        Utils.showSuccess('日志删除成功');
        loadLogs(currentPage);
        loadSummary(); // 刷新统计
    } catch (error) {
        console.error('删除日志失败:', error);
        Utils.showError('删除日志失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 执行清理
async function performCleanup() {
    const form = document.getElementById('cleanupForm');
    if (!form.checkValidity()) {
        form.reportValidity();
        return;
    }
    
    const jobId = document.getElementById('jobSelect').value;
    const keepLatest = parseInt(document.getElementById('keepLatest').value);
    
    if (!await Utils.confirm(`确定要清理任务的历史日志吗？将保留最新的${keepLatest}条记录。`)) {
        return;
    }
    
    try {
        const response = await ApiClient.post('/logs/clear', {
            job_id: parseInt(jobId),
            keep_latest: keepLatest
        });
        
        Utils.showSuccess(`清理完成，删除了${response.deleted_count}条历史日志`);
        
        // 关闭模态框并刷新
        const modal = bootstrap.Modal.getInstance(document.getElementById('cleanupModal'));
        modal.hide();
        loadLogs(currentPage);
        loadSummary();
        
    } catch (error) {
        console.error('清理日志失败:', error);
        Utils.showError('清理日志失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 导出日志
async function exportLogs() {
    try {
        const params = new URLSearchParams();
        if (currentFilter !== 'all') {
            params.append('status', currentFilter);
        }
        
        // 创建下载链接
        const url = `/logs/export?${params}`;
        const link = document.createElement('a');
        link.href = url;
        link.download = `execution_logs_${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        Utils.showSuccess('日志导出成功');
        
    } catch (error) {
        console.error('导出日志失败:', error);
        Utils.showError('导出日志失败: ' + (error.response?.data?.detail || error.message));
    }
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
                <a class="page-link" href="#" onclick="loadLogs(${currentPage - 1})">上一页</a>
            </li>
        `;
    }
    
    // 页码
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHtml += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="loadLogs(${i})">${i}</a>
            </li>
        `;
    }
    
    // 下一页
    if (currentPage < totalPages) {
        paginationHtml += `
            <li class="page-item">
                <a class="page-link" href="#" onclick="loadLogs(${currentPage + 1})">下一页</a>
            </li>
        `;
    }
    
    pagination.innerHTML = paginationHtml;
}

// 自动刷新（每30秒）
setInterval(() => {
    if (document.visibilityState === 'visible') {
        loadSummary();
        // 如果当前页有运行中的任务，刷新列表
        const hasRunning = logs.some(log => log.status === 'running');
        if (hasRunning) {
            loadLogs(currentPage);
        }
    }
}, 30000);
