// 日志管理页面JavaScript

let logs = [];
let jobs = [];
let currentFilter = 'all';
let currentPage = 1;
let pageSize = 20;
let totalPages = 1;
let progressConnections = new Map(); // 存储SSE连接

// 页面加载时初始化
document.addEventListener('DOMContentLoaded', function() {
    loadLogs();
    loadJobs();
    loadSummary();
    
    // 页面卸载时关闭所有SSE连接
    window.addEventListener('beforeunload', function() {
        progressConnections.forEach(connection => {
            connection.close();
        });
    });
});

// 连接SSE获取任务进度
function connectToJobProgress(jobId) {
    // 如果已经有连接，先关闭
    if (progressConnections.has(jobId)) {
        progressConnections.get(jobId).close();
    }
    
    const eventSource = new EventSource(`/api/jobs/${jobId}/progress`);
    progressConnections.set(jobId, eventSource);
    
    eventSource.onmessage = function(event) {
        try {
            const progress = JSON.parse(event.data);
            console.log('收到SSE进度数据:', progress); // 添加调试日志
            updateProgressDisplay(jobId, progress);
        } catch (error) {
            console.error('解析进度数据失败:', error, 'Raw data:', event.data);
        }
    };
    
    eventSource.onerror = function(error) {
        console.error('SSE连接错误:', error);
        // 连接错误时移除连接
        progressConnections.delete(jobId);
        eventSource.close();
    };
    
    eventSource.addEventListener('complete', function(event) {
        // 任务完成时关闭连接并刷新日志
        progressConnections.delete(jobId);
        eventSource.close();
        loadLogs(currentPage);
        loadSummary();
    });
}

// 更新进度显示
function updateProgressDisplay(jobId, progress) {
    console.log('更新进度显示 - JobID:', jobId, 'Progress:', progress); // 添加调试日志
    
    // 在日志表格中找到对应的行并更新进度
    const table = document.getElementById('logs-table');
    if (!table) {
        console.log('未找到日志表格');
        return;
    }
    
    const rows = table.querySelectorAll('tbody tr');
    console.log('找到表格行数:', rows.length);
    
    // 查找当前正在运行的任务
    const runningLog = logs.find(log => log.job_id === jobId && log.status === 'running');
    if (!runningLog) {
        console.log('未找到正在运行的任务，JobID:', jobId);
        return;
    }
    
    rows.forEach((row, index) => {
        const cells = row.cells;
        if (cells.length > 0) {
            // 查找正在运行的任务行（可以通过状态或其他标识符）
            const statusCell = cells[1]; // 状态列
            if (statusCell && statusCell.innerHTML.includes('运行中')) {
                console.log('找到运行中的任务行，行索引:', index);
                
                // 更新处理表数和传输记录数
                if (cells[5]) {
                    const tablesCompleted = progress.tables_completed || 0;
                    cells[5].textContent = tablesCompleted;
                    console.log('更新处理表数:', tablesCompleted);
                }
                
                if (cells[6]) {
                    const recordsProcessed = progress.records_processed || 0;
                    cells[6].textContent = Utils.formatNumber(recordsProcessed);
                    console.log('更新传输记录数:', recordsProcessed);
                }
                
                // 更新持续时间（基于日志开始时间）
                if (cells[4] && runningLog.start_time) {
                    const duration = (Date.now() - new Date(runningLog.start_time).getTime()) / 1000;
                    cells[4].textContent = Utils.formatDuration(duration);
                }
                
                // 如果有当前操作信息，可以在状态中显示
                if (progress.current_table) {
                    const statusBadge = statusCell.querySelector('.badge');
                    if (statusBadge) {
                        statusBadge.title = `正在处理: ${progress.current_table}`;
                    }
                }
                
                // 显示详细的进度信息
                if (progress.percentage !== undefined || progress.current_table_percentage !== undefined) {
                    const statusBadge = statusCell.querySelector('.badge');
                    if (statusBadge) {
                        let progressText = '运行中';
                        
                        // 如果有当前表的详细进度信息
                        if (progress.current_table_percentage !== undefined && progress.current_table_total_records > 0) {
                            const currentProcessed = progress.current_table_processed_records || 0;
                            const currentTotal = progress.current_table_total_records || 0;
                            progressText += ` (${progress.current_table_percentage}% - ${Utils.formatNumber(currentProcessed)}/${Utils.formatNumber(currentTotal)})`;
                        } else if (progress.percentage !== undefined) {
                            // 显示总体进度
                            progressText += ` (${progress.percentage}%)`;
                        }
                        
                        statusBadge.textContent = progressText;
                        
                        // 设置详细的tooltip信息
                        let tooltipText = '';
                        if (progress.current_table) {
                            tooltipText += `正在处理: ${progress.current_table}\n`;
                        }
                        if (progress.current_table_total_records > 0) {
                            tooltipText += `当前表记录数: ${Utils.formatNumber(progress.current_table_total_records)}\n`;
                            tooltipText += `已处理记录数: ${Utils.formatNumber(progress.current_table_processed_records || 0)}\n`;
                        }
                        if (progress.total_tables) {
                            tooltipText += `总表数: ${progress.total_tables}\n`;
                            tooltipText += `已完成表数: ${progress.tables_completed || 0}`;
                        }
                        
                        if (tooltipText) {
                            statusBadge.title = tooltipText;
                        }
                    }
                }
            }
        }
    });
}

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
                    ${log.status === 'running' ? `
                        <button class="btn btn-outline-warning" onclick="stopRunningLog(${log.id}, ${log.job_id})" title="停止任务">
                            <i class="fas fa-stop"></i>
                        </button>
                    ` : ''}
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
            
            // 如果任务正在运行，连接SSE获取实时进度
            if (log.status === 'running' && log.job_id) {
                connectToJobProgress(log.job_id);
            }
        });
        
        updatePagination();
        
        // SSE连接已在上面建立，无需额外刷新机制
        
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

// 定时刷新功能已移除，仅使用SSE获取实时进度

// 停止运行中的任务
async function stopRunningLog(logId, jobId) {
    const log = logs.find(l => l.id === logId);
    if (!log) return;
    
    if (!await Utils.confirm(`确定要停止任务"${log.job_name}"的执行吗？`)) {
        return;
    }
    
    try {
        // 停止任务
        await ApiClient.post(`/logs/${logId}/stop`);
        // 重置运行状态
        await ApiClient.post(`/logs/job/${jobId}/reset-running-status`);
        Utils.showSuccess('任务已停止并重置运行状态');
        loadLogs(currentPage);
        loadSummary(); // 刷新统计
    } catch (error) {
        console.error('停止任务失败:', error);
        Utils.showError('停止任务失败: ' + (error.response?.data?.detail || error.message));
    }
}
