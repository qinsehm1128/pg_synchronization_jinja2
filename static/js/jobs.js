// 任务管理页面JavaScript

let jobs = [];
let connections = [];
let editingJobId = null;
let sourceTables = [];
let editingTargetTables = [];

// 页面加载时初始化
document.addEventListener('DOMContentLoaded', function() {
    loadJobs();
    loadConnections();
});

// 加载任务列表
async function loadJobs() {
    const table = new TableHelper('jobs-table');
    table.showLoading();
    
    try {
        jobs = await ApiClient.get('/jobs/');
        
        table.clear();
        
        if (jobs.length === 0) {
            table.showEmpty('暂无同步任务，请创建第一个任务');
            return;
        }
        
        jobs.forEach(job => {
            const statusBadge = Utils.getStatusBadge(job.status);
            const nextRun = job.next_run_at ? Utils.formatDateTime(job.next_run_at) : '-';
            
            const actions = `
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-info" onclick="viewJob(${job.id})" title="查看详情">
                        <i class="fas fa-eye"></i>
                    </button>
                    <button class="btn btn-outline-success" onclick="executeJob(${job.id})" title="立即执行">
                        <i class="fas fa-play"></i>
                    </button>
                    <button class="btn btn-outline-warning" onclick="pauseResumeJob(${job.id}, '${job.status}')" title="${job.status === 'active' ? '暂停' : '恢复'}">
                        <i class="fas fa-${job.status === 'active' ? 'pause' : 'play'}"></i>
                    </button>
                    <button class="btn btn-outline-primary" onclick="editJob(${job.id})" title="编辑">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-outline-danger" onclick="deleteJob(${job.id})" title="删除">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            `;
            
            const row = [
                job.name,
                job.source_db_name || '-',
                job.destination_db_name || '-',
                job.sync_mode === 'full' ? '全量同步' : '增量同步',
                statusBadge,
                job.cron_expression || '-',
                nextRun,
                actions
            ];
            table.addRow(row);
        });
        
    } catch (error) {
        console.error('加载任务列表失败:', error);
        table.showEmpty('加载失败');
        Utils.showError('加载任务列表失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 加载数据库连接
async function loadConnections() {
    try {
        connections = await ApiClient.get('/connections/');
        
        // 更新下拉选项
        const sourceSelect = document.getElementById('sourceConnectionId');
        const targetSelect = document.getElementById('targetConnectionId');
        
        sourceSelect.innerHTML = '<option value="">请选择源数据库</option>';
        targetSelect.innerHTML = '<option value="">请选择目标数据库</option>';
        
        connections.forEach(conn => {
            const option = `<option value="${conn.id}">${conn.name} (${conn.host}:${conn.port}/${conn.database_name})</option>`;
            sourceSelect.innerHTML += option;
            targetSelect.innerHTML += option;
        });
        
    } catch (error) {
        console.error('加载连接列表失败:', error);
        Utils.showError('加载数据库连接失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 查看任务详情
async function viewJob(jobId) {
    try {
        const job = await ApiClient.get(`/jobs/${jobId}`);
        
        const detailContent = `
            <div class="row">
                <div class="col-sm-4"><strong>任务名称:</strong></div>
                <div class="col-sm-8">${job.name}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>同步模式:</strong></div>
                <div class="col-sm-8">${job.sync_mode === 'full' ? '全量同步' : '增量同步'}</div>
            </div>
            ${job.sync_mode === 'incremental' && job.where_condition ? `
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>增量同步条件:</strong></div>
                <div class="col-sm-8"><code>${job.where_condition}</code></div>
            </div>
            ` : ''}
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>冲突处理策略:</strong></div>
                <div class="col-sm-8">${{
                    'error': '报错停止',
                    'skip': '跳过冲突记录',
                    'replace': '替换现有记录',
                    'ignore': '忽略冲突继续'
                }[job.conflict_strategy] || job.conflict_strategy || '报错停止'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>执行模式:</strong></div>
                <div class="col-sm-8">${job.execution_mode === 'immediate' ? '立即执行' : '定时执行'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>源数据库:</strong></div>
                <div class="col-sm-8">${job.source_db_name || '-'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>目标数据库:</strong></div>
                <div class="col-sm-8">${job.destination_db_name || '-'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>状态:</strong></div>
                <div class="col-sm-8">${Utils.getStatusBadge(job.status)}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>调度表达式:</strong></div>
                <div class="col-sm-8">${job.cron_expression || '-'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>下次执行:</strong></div>
                <div class="col-sm-8">${job.next_run_at ? Utils.formatDateTime(job.next_run_at) : '-'}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>同步表数量:</strong></div>
                <div class="col-sm-8">${job.target_tables ? job.target_tables.length : 0}</div>
            </div>
            ${job.target_tables && job.target_tables.length > 0 ? `
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>表配置详情:</strong></div>
                <div class="col-sm-8">
                    <div class="table-responsive">
                        <table class="table table-sm table-bordered">
                            <thead class="table-light">
                                <tr>
                                    <th>表名</th>
                                    <th>状态</th>
                                    ${job.sync_mode === 'incremental' ? '<th>增量策略</th><th>增量字段</th><th>自定义条件</th>' : ''}
                                </tr>
                            </thead>
                            <tbody>
                                ${job.target_tables.map(table => `
                                    <tr>
                                        <td><code>${table.schema_name}.${table.table_name}</code></td>
                                        <td><span class="badge ${table.is_active ? 'bg-success' : 'bg-secondary'}">${table.is_active ? '启用' : '禁用'}</span></td>
                                        ${job.sync_mode === 'incremental' ? `
                                            <td><span class="badge bg-info">${{
                                                'none': '无增量',
                                                'auto_id': '自动ID',
                                                'auto_timestamp': '自动时间戳',
                                                'custom_condition': '自定义条件'
                                            }[table.incremental_strategy] || table.incremental_strategy || '无增量'}</span></td>
                                            <td>${table.incremental_field ? `<code>${table.incremental_field}</code>` : '-'}</td>
                                            <td>${table.custom_condition ? `<code>${table.custom_condition}</code>` : '-'}</td>
                                        ` : ''}
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            ` : ''}
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>创建时间:</strong></div>
                <div class="col-sm-8">${Utils.formatDateTime(job.created_at)}</div>
            </div>
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>最后更新:</strong></div>
                <div class="col-sm-8">${Utils.formatDateTime(job.updated_at)}</div>
            </div>
            ${job.description ? `
            <hr>
            <div class="row">
                <div class="col-sm-4"><strong>描述:</strong></div>
                <div class="col-sm-8">${job.description}</div>
            </div>
            ` : ''}
        `;
        
        document.getElementById('jobDetailContent').innerHTML = detailContent;
        const modal = new bootstrap.Modal(document.getElementById('jobDetailModal'));
        modal.show();
        
    } catch (error) {
        console.error('获取任务详情失败:', error);
        Utils.showError('获取任务详情失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 立即执行任务
async function executeJob(jobId) {
    if (!await Utils.confirm('确定要立即执行此任务吗？')) {
        return;
    }
    
    try {
        await ApiClient.post(`/jobs/${jobId}/run`);
        Utils.showSuccess('任务已提交执行');
        loadJobs(); // 刷新列表
    } catch (error) {
        console.error('执行任务失败:', error);
        Utils.showError('执行任务失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 暂停/恢复任务
async function pauseResumeJob(jobId, currentStatus) {
    const action = currentStatus === 'active' ? 'pause' : 'resume';
    const actionText = action === 'pause' ? '暂停' : '恢复';
    
    if (!await Utils.confirm(`确定要${actionText}此任务吗？`)) {
        return;
    }
    
    try {
        await ApiClient.post(`/jobs/${jobId}/${action}`);
        Utils.showSuccess(`任务已${actionText}`);
        loadJobs(); // 刷新列表
    } catch (error) {
        console.error(`${actionText}任务失败:`, error);
        Utils.showError(`${actionText}任务失败: ` + (error.response?.data?.detail || error.message));
    }
}

// 编辑任务
async function editJob(jobId) {
    try {
        const job = await ApiClient.get(`/jobs/${jobId}`);
        
        editingJobId = jobId;
        
        // 填充表单
        document.getElementById('jobId').value = job.id;
        document.getElementById('jobName').value = job.name;
        document.getElementById('syncMode').value = job.sync_mode;
        document.getElementById('conflictStrategy').value = job.conflict_strategy || 'error';
        document.getElementById('executionMode').value = job.execution_mode || 'scheduled';
        document.getElementById('sourceConnectionId').value = job.source_db_id;
        document.getElementById('targetConnectionId').value = job.destination_db_id;
        document.getElementById('jobDescription').value = job.description || '';
        
        // 填充WHERE条件（增量同步时）
        if (job.sync_mode === 'incremental' && job.where_condition) {
            document.getElementById('whereCondition').value = job.where_condition;
        }
        
        // 处理调度配置
        if (job.cron_expression) {
            if (job.cron_expression.includes(' ')) {
                document.getElementById('scheduleType').value = 'cron';
                document.getElementById('cronExpression').value = job.cron_expression;
            } else {
                document.getElementById('scheduleType').value = 'interval';
                document.getElementById('intervalMinutes').value = parseInt(job.cron_expression);
            }
        } else {
            document.getElementById('scheduleType').value = 'once';
        }
        
        // 触发切换函数以显示正确的配置项
        toggleSyncMode();
        toggleExecutionMode();
        toggleScheduleConfig();
        
        // 加载并填充表格配置
        await loadSourceTablesForEdit(job);
        
        // 更新模态框标题
        document.getElementById('jobModalTitle').textContent = '编辑同步任务';
        
        const modal = new bootstrap.Modal(document.getElementById('jobModal'));
        modal.show();
        
    } catch (error) {
        console.error('获取任务信息失败:', error);
        Utils.showError('获取任务信息失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 删除任务
async function deleteJob(jobId) {
    const job = jobs.find(j => j.id === jobId);
    if (!job) return;
    
    if (!await Utils.confirm(`确定要删除任务 "${job.name}" 吗？`)) {
        return;
    }
    
    try {
        await ApiClient.delete(`/jobs/${jobId}`);
        Utils.showSuccess('任务删除成功');
        loadJobs(); // 刷新列表
    } catch (error) {
        console.error('删除任务失败:', error);
        Utils.showError('删除任务失败: ' + (error.response?.data?.detail || error.message));
    }
}

// 切换执行模式显示
function toggleExecutionMode() {
    const executionMode = document.getElementById('executionMode').value;
    const scheduleConfigRow = document.getElementById('scheduleConfigRow');
    
    if (executionMode === 'scheduled') {
        scheduleConfigRow.style.display = 'block';
        document.getElementById('scheduleType').required = true;
    } else {
        scheduleConfigRow.style.display = 'none';
        document.getElementById('scheduleType').required = false;
        // 清空调度配置
        document.getElementById('scheduleType').value = '';
        document.getElementById('cronExpression').value = '';
        document.getElementById('intervalMinutes').value = '';
    }
    
    toggleScheduleConfig();
}

// 切换调度配置显示
function toggleScheduleConfig() {
    const scheduleType = document.getElementById('scheduleType').value;
    
    // 隐藏所有调度相关配置
    document.getElementById('cronExpressionDiv').style.display = 'none';
    document.getElementById('intervalDiv').style.display = 'none';
    
    // 根据选择显示对应配置
    if (scheduleType === 'cron') {
        document.getElementById('cronExpressionDiv').style.display = 'block';
    } else if (scheduleType === 'interval') {
        document.getElementById('intervalDiv').style.display = 'block';
    }
}

// 切换同步模式显示
function toggleSyncMode() {
    const syncMode = document.getElementById('syncMode').value;
    const incrementalConfigRow = document.getElementById('incrementalConfigRow');
    
    if (syncMode === 'incremental') {
        incrementalConfigRow.style.display = 'block';
        
        // 检查是否需要重新生成表格（当前表格没有增量配置时）
        const hasIncrementalConfig = document.querySelector('[id^="incremental_config_"]');
        if (!hasIncrementalConfig && sourceTables && sourceTables.length > 0) {
            // 保存当前选中状态
            const selectedTables = [];
            const checkboxes = document.querySelectorAll('#tablesContainer input[type="checkbox"]:checked');
            checkboxes.forEach(checkbox => {
                selectedTables.push(checkbox.value);
            });
            
            // 重新生成表格HTML，包含增量配置
            regenerateTablesWithIncrementalConfig(selectedTables);
        } else {
            // 显示已选中表的增量配置
            const checkboxes = document.querySelectorAll('#tablesContainer input[type="checkbox"]:checked');
            checkboxes.forEach(checkbox => {
                const tableIndex = checkbox.id.replace('table_', '');
                const configDiv = document.getElementById(`incremental_config_${tableIndex}`);
                if (configDiv) {
                    configDiv.style.display = 'block';
                }
            });
        }
    } else {
        incrementalConfigRow.style.display = 'none';
        // 清空增量同步配置
        document.getElementById('whereCondition').value = '';
        // 隐藏所有表的增量配置
        const configDivs = document.querySelectorAll('[id^="incremental_config_"]');
        configDivs.forEach(div => {
            div.style.display = 'none';
        });
    }
}

// 重新生成包含增量配置的表格
function regenerateTablesWithIncrementalConfig(selectedTables = []) {
    const container = document.getElementById('tablesContainer');
    if (!sourceTables || sourceTables.length === 0) {
        return;
    }
    
    const tablesHtml = sourceTables.map((table, index) => {
        const isChecked = selectedTables.includes(table.full_name) ? 'checked' : '';
        
        const incrementalConfig = `
            <div class="mt-2 ms-4" id="incremental_config_${index}" style="display: ${isChecked ? 'block' : 'none'};">
                <div class="row g-2">
                    <div class="col-md-4">
                        <label class="form-label text-muted small">增量策略:</label>
                        <select class="form-select form-select-sm" id="strategy_${index}" onchange="toggleIncrementalFields(${index})">
                            <option value="none">无增量</option>
                            <option value="auto_id">自动ID</option>
                            <option value="auto_timestamp">自动时间戳</option>
                            <option value="custom_condition">自定义条件</option>
                        </select>
                    </div>
                    <div class="col-md-4" id="field_container_${index}" style="display: none;">
                        <label class="form-label text-muted small">增量字段:</label>
                        <input type="text" class="form-control form-control-sm" id="field_${index}" placeholder="字段名">
                    </div>
                    <div class="col-md-4" id="condition_container_${index}" style="display: none;">
                        <label class="form-label text-muted small">自定义条件:</label>
                        <input type="text" class="form-control form-control-sm" id="condition_${index}" placeholder="WHERE条件">
                    </div>
                </div>
            </div>
        `;
        
        return `
            <div class="border rounded p-2 mb-2">
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${table.full_name}" id="table_${index}" ${isChecked} onchange="toggleTableConfig(${index})">
                    <label class="form-check-label" for="table_${index}">
                        <strong>${table.full_name}</strong>
                        <small class="text-muted d-block">Owner: ${table.table_owner}</small>
                    </label>
                </div>
                ${incrementalConfig}
            </div>
        `;
    }).join('');
    
    container.innerHTML = tablesHtml;
}

// 切换表格配置显示
function toggleTableConfig(tableIndex) {
    const checkbox = document.getElementById(`table_${tableIndex}`);
    const configDiv = document.getElementById(`incremental_config_${tableIndex}`);
    const syncMode = document.getElementById('syncMode').value;
    
    if (checkbox && configDiv && syncMode === 'incremental') {
        if (checkbox.checked) {
            configDiv.style.display = 'block';
        } else {
            configDiv.style.display = 'none';
            // 重置增量配置
            const strategySelect = document.getElementById(`strategy_${tableIndex}`);
            const fieldInput = document.getElementById(`field_${tableIndex}`);
            const conditionInput = document.getElementById(`condition_${tableIndex}`);
            
            if (strategySelect) strategySelect.value = 'none';
            if (fieldInput) fieldInput.value = '';
            if (conditionInput) conditionInput.value = '';
            
            // 隐藏字段和条件输入框
            toggleIncrementalFields(tableIndex);
        }
    }
}

// 切换增量字段显示
function toggleIncrementalFields(tableIndex) {
    const strategy = document.getElementById(`strategy_${tableIndex}`).value;
    const fieldContainer = document.getElementById(`field_container_${tableIndex}`);
    const conditionContainer = document.getElementById(`condition_container_${tableIndex}`);
    
    // 隐藏所有容器
    fieldContainer.style.display = 'none';
    conditionContainer.style.display = 'none';
    
    // 根据策略显示对应的输入框
    if (strategy === 'auto_id' || strategy === 'auto_timestamp') {
        fieldContainer.style.display = 'block';
    } else if (strategy === 'custom_condition') {
        conditionContainer.style.display = 'block';
    }
}

// 为编辑任务加载源数据库表
async function loadSourceTablesForEdit(job) {
    const sourceConnectionId = job.source_db_id;
    if (!sourceConnectionId) {
        return;
    }
    
    try {
        const response = await ApiClient.get(`/connections/${sourceConnectionId}/tables`);
        sourceTables = response.tables || [];
        
        const container = document.getElementById('tablesContainer');
        if (sourceTables.length === 0) {
            container.innerHTML = '<p class="text-muted text-center">未找到表</p>';
            return;
        }
        
        // 保存编辑时的目标表配置
        editingTargetTables = job.target_tables || [];
        
        // 使用带筛选功能的渲染方法
        renderTablesWithFilterForEdit(job.sync_mode);
        
    } catch (error) {
        console.error('加载源数据库表失败:', error);
        Utils.showError('加载源数据库表失败: ' + (error.response?.data?.detail || error.message));
        document.getElementById('tablesContainer').innerHTML = '<p class="text-danger text-center">加载失败</p>';
    }
}

// 加载源数据库表
async function loadSourceTables() {
    const sourceConnectionId = document.getElementById('sourceConnectionId').value;
    if (!sourceConnectionId) {
        Utils.showWarning('请先选择源数据库');
        return;
    }
    
    try {
        const response = await ApiClient.get(`/connections/${sourceConnectionId}/tables`);
        sourceTables = response.tables || [];
        
        const container = document.getElementById('tablesContainer');
        if (sourceTables.length === 0) {
            container.innerHTML = '<p class="text-muted text-center">未找到表</p>';
            return;
        }
        
        // 渲染表格列表（包含搜索框）
        renderTablesWithFilter();
        
    } catch (error) {
        console.error('加载源数据库表失败:', error);
        Utils.showError('加载源数据库表失败: ' + (error.response?.data?.detail || error.message));
        document.getElementById('tablesContainer').innerHTML = '<p class="text-danger text-center">加载失败</p>';
    }
}

// 渲染表格列表（带筛选功能）
function renderTablesWithFilter(filterText = '') {
    const container = document.getElementById('tablesContainer');
    const filterContainer = document.getElementById('tableFilterContainer');
    const filterInput = document.getElementById('tableFilterInput');
    const syncMode = document.getElementById('syncMode').value;
    const isIncremental = syncMode === 'incremental';
    
    // 显示筛选容器
    if (filterContainer) {
        filterContainer.style.display = 'block';
        if (filterInput) {
            filterInput.value = filterText;
        }
    }
    
    // 筛选表格
    const filteredTables = sourceTables.filter(table => 
        table.full_name.toLowerCase().includes(filterText.toLowerCase()) ||
        table.table_owner.toLowerCase().includes(filterText.toLowerCase())
    );
    
    // 更新统计信息
    const statsHtml = `<small class="text-muted d-block mb-2">找到 ${filteredTables.length} 个表（共 ${sourceTables.length} 个）</small>`;
    
    if (filteredTables.length === 0) {
        container.innerHTML = statsHtml + '<p class="text-muted text-center">没有找到匹配的表</p>';
        return;
    }
    
    const tablesHtml = filteredTables.map((table, index) => {
        const originalIndex = sourceTables.findIndex(t => t.full_name === table.full_name);
        const incrementalConfig = isIncremental ? `
            <div class="mt-2 ms-4" id="incremental_config_${originalIndex}" style="display: none;">
                <div class="row g-2">
                    <div class="col-md-4">
                        <label class="form-label text-muted small">增量策略:</label>
                        <select class="form-select form-select-sm" id="strategy_${originalIndex}" onchange="toggleIncrementalFields(${originalIndex})">
                            <option value="none">无增量</option>
                            <option value="auto_id">自动ID</option>
                            <option value="auto_timestamp">自动时间戳</option>
                            <option value="custom_condition">自定义条件</option>
                        </select>
                    </div>
                    <div class="col-md-4" id="field_container_${originalIndex}" style="display: none;">
                        <label class="form-label text-muted small">增量字段:</label>
                        <input type="text" class="form-control form-control-sm" id="field_${originalIndex}" placeholder="字段名">
                    </div>
                    <div class="col-md-4" id="condition_container_${originalIndex}" style="display: none;">
                        <label class="form-label text-muted small">自定义条件:</label>
                        <input type="text" class="form-control form-control-sm" id="condition_${originalIndex}" placeholder="WHERE条件">
                    </div>
                </div>
            </div>
        ` : '';
        
        return `
            <div class="border rounded p-2 mb-2 table-item" data-table-name="${table.full_name}">
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${table.full_name}" id="table_${originalIndex}" onchange="toggleTableConfig(${originalIndex})">
                    <label class="form-check-label" for="table_${originalIndex}">
                        <strong>${table.full_name}</strong>
                        <small class="text-muted d-block">Owner: ${table.table_owner}</small>
                    </label>
                </div>
                ${incrementalConfig}
            </div>
        `;
    }).join('');
    
    container.innerHTML = statsHtml + tablesHtml;
}

// 筛选表格
function filterTables() {
    const filterInput = document.getElementById('tableFilterInput');
    const filterText = filterInput ? filterInput.value : '';
    if (editingJobId) {
        renderTablesWithFilterForEdit(document.getElementById('syncMode').value, filterText);
    } else {
        renderTablesWithFilter(filterText);
    }
}

// 清除筛选
function clearTableFilter() {
    const filterInput = document.getElementById('tableFilterInput');
    if (filterInput) {
        filterInput.value = '';
        if (editingJobId) {
            renderTablesWithFilterForEdit(document.getElementById('syncMode').value, '');
        } else {
            renderTablesWithFilter('');
        }
    }
}

// 渲染表格列表（编辑模式，带筛选功能）
function renderTablesWithFilterForEdit(syncMode, filterText = '') {
    const container = document.getElementById('tablesContainer');
    const filterContainer = document.getElementById('tableFilterContainer');
    const filterInput = document.getElementById('tableFilterInput');
    const isIncremental = syncMode === 'incremental';
    
    // 显示筛选容器
    if (filterContainer) {
        filterContainer.style.display = 'block';
        if (filterInput) {
            filterInput.value = filterText;
        }
    }
    
    // 筛选表格
    const filteredTables = sourceTables.filter(table => 
        table.full_name.toLowerCase().includes(filterText.toLowerCase()) ||
        table.table_owner.toLowerCase().includes(filterText.toLowerCase())
    );
    
    // 更新统计信息
    const statsHtml = `<small class="text-muted d-block mb-2">找到 ${filteredTables.length} 个表（共 ${sourceTables.length} 个）</small>`;
    
    if (filteredTables.length === 0) {
        container.innerHTML = statsHtml + '<p class="text-muted text-center">没有找到匹配的表</p>';
        return;
    }
    
    const tablesHtml = filteredTables.map((table, index) => {
        const originalIndex = sourceTables.findIndex(t => t.full_name === table.full_name);
        
        // 查找对应的目标表配置
        const targetTable = editingTargetTables.find(t => 
            t.schema_name === (table.full_name.split('.')[0] || 'public') && 
            t.table_name === (table.full_name.split('.')[1] || table.full_name)
        );
        
        const isChecked = targetTable ? 'checked' : '';
        const incrementalStrategy = targetTable?.incremental_strategy || 'none';
        const incrementalField = targetTable?.incremental_field || '';
        const customCondition = targetTable?.custom_condition || '';
        
        const incrementalConfig = isIncremental ? `
            <div class="mt-2 ms-4" id="incremental_config_${originalIndex}" style="display: ${isChecked ? 'block' : 'none'};">
                <div class="row g-2">
                    <div class="col-md-4">
                        <label class="form-label text-muted small">增量策略:</label>
                        <select class="form-select form-select-sm" id="strategy_${originalIndex}" onchange="toggleIncrementalFields(${originalIndex})">
                            <option value="none" ${incrementalStrategy === 'none' ? 'selected' : ''}>无增量</option>
                            <option value="auto_id" ${incrementalStrategy === 'auto_id' ? 'selected' : ''}>自动ID</option>
                            <option value="auto_timestamp" ${incrementalStrategy === 'auto_timestamp' ? 'selected' : ''}>自动时间戳</option>
                            <option value="custom_condition" ${incrementalStrategy === 'custom_condition' ? 'selected' : ''}>自定义条件</option>
                        </select>
                    </div>
                    <div class="col-md-4" id="field_container_${originalIndex}" style="display: ${(incrementalStrategy === 'auto_id' || incrementalStrategy === 'auto_timestamp') ? 'block' : 'none'};">
                        <label class="form-label text-muted small">增量字段:</label>
                        <input type="text" class="form-control form-control-sm" id="field_${originalIndex}" placeholder="字段名" value="${incrementalField}">
                    </div>
                    <div class="col-md-4" id="condition_container_${originalIndex}" style="display: ${incrementalStrategy === 'custom_condition' ? 'block' : 'none'};">
                        <label class="form-label text-muted small">自定义条件:</label>
                        <input type="text" class="form-control form-control-sm" id="condition_${originalIndex}" placeholder="WHERE条件" value="${customCondition}">
                    </div>
                </div>
            </div>
        ` : '';
        
        return `
            <div class="border rounded p-2 mb-2 table-item" data-table-name="${table.full_name}">
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" value="${table.full_name}" id="table_${originalIndex}" ${isChecked} onchange="toggleTableConfig(${originalIndex})">
                    <label class="form-check-label" for="table_${originalIndex}">
                        <strong>${table.full_name}</strong>
                        <small class="text-muted d-block">Owner: ${table.table_owner}</small>
                    </label>
                </div>
                ${incrementalConfig}
            </div>
        `;
    }).join('');
    
    container.innerHTML = statsHtml + tablesHtml;
}

// 获取选中的表格
function getSelectedTables() {
    const selectedTables = [];
    const checkboxes = document.querySelectorAll('#tablesContainer input[type="checkbox"]:checked');
    checkboxes.forEach((checkbox, index) => {
        const tableName = checkbox.value;
        const parts = tableName.split('.');
        const tableIndex = checkbox.id.replace('table_', '');
        
        const tableConfig = {
            schema_name: parts[0] || 'public',
            table_name: parts[1] || tableName,
            is_active: true
        };
        
        // 如果是增量同步模式，收集增量配置
        const syncMode = document.getElementById('syncMode').value;
        if (syncMode === 'incremental') {
            const strategyElement = document.getElementById(`strategy_${tableIndex}`);
            const fieldElement = document.getElementById(`field_${tableIndex}`);
            const conditionElement = document.getElementById(`condition_${tableIndex}`);
            
            if (strategyElement) {
                tableConfig.incremental_strategy = strategyElement.value || 'NONE';
                tableConfig.incremental_field = fieldElement?.value?.trim() || null;
                tableConfig.custom_condition = conditionElement?.value?.trim() || null;
            }
        }
        
        selectedTables.push(tableConfig);
    });
    return selectedTables;
}

// 保存任务
async function saveJob() {
    const formData = getFormData();
    if (!formData) return;
    
    const button = event.target;
    const originalContent = Utils.showLoading(button);
    
    try {
        if (editingJobId) {
            // 更新任务
            await ApiClient.put(`/jobs/${editingJobId}`, formData);
            Utils.showSuccess('任务更新成功');
        } else {
            // 创建新任务
            await ApiClient.post('/jobs/', formData);
            Utils.showSuccess('任务创建成功');
        }
        
        // 关闭模态框并刷新列表
        const modal = bootstrap.Modal.getInstance(document.getElementById('jobModal'));
        modal.hide();
        resetForm();
        loadJobs();
        
    } catch (error) {
        console.error('保存任务失败:', error);
        Utils.showError('保存任务失败: ' + (error.response?.data?.detail || error.message));
    } finally {
        Utils.hideLoading(button, originalContent);
    }
}

// 获取表单数据
function getFormData() {
    const selectedTables = getSelectedTables();
    if (selectedTables.length === 0) {
        Utils.showWarning('请至少选择一个要同步的表');
        return null;
    }
    
    const form = document.getElementById('jobForm');
    if (!form.checkValidity()) {
        form.reportValidity();
        return null;
    }
    
    const executionMode = document.getElementById('executionMode').value;
    let scheduleExpression = null;
    
    // 只有定时执行模式才需要调度配置
    if (executionMode === 'scheduled') {
        const scheduleType = document.getElementById('scheduleType').value;
        
        if (!scheduleType) {
            Utils.showWarning('请选择调度类型');
            return null;
        }
        
        if (scheduleType === 'cron') {
            scheduleExpression = document.getElementById('cronExpression').value.trim();
            if (!scheduleExpression) {
                Utils.showWarning('请输入Cron表达式');
                return null;
            }
        } else if (scheduleType === 'interval') {
            const minutes = document.getElementById('intervalMinutes').value;
            if (!minutes || minutes < 1) {
                Utils.showWarning('请输入有效的间隔时间');
                return null;
            }
            // 将间隔转换为cron表达式 (每N分钟执行一次)
            scheduleExpression = `*/${minutes} * * * *`;
        }
    }
    
    // 获取增量同步条件
    const syncMode = document.getElementById('syncMode').value;
    let whereCondition = null;
    if (syncMode === 'incremental') {
        whereCondition = document.getElementById('whereCondition').value.trim() || null;
    }
    
    return {
        name: document.getElementById('jobName').value.trim(),
        sync_mode: syncMode,
        conflict_strategy: document.getElementById('conflictStrategy').value,
        execution_mode: executionMode,
        source_db_id: parseInt(document.getElementById('sourceConnectionId').value),
        destination_db_id: parseInt(document.getElementById('targetConnectionId').value),
        cron_expression: scheduleExpression,
        where_condition: whereCondition,
        description: document.getElementById('jobDescription').value.trim() || null,
        target_tables: selectedTables
    };
}

// 重置表单
function resetForm() {
    document.getElementById('jobForm').reset();
    document.getElementById('jobId').value = '';
    document.getElementById('jobModalTitle').textContent = '创建同步任务';
    document.getElementById('tablesContainer').innerHTML = '<p class="text-muted text-center">请先选择源数据库，然后点击"加载表"按钮</p>';
    toggleScheduleConfig();
    editingJobId = null;
}

// 模态框关闭时重置表单
document.getElementById('jobModal').addEventListener('hidden.bs.modal', function() {
    resetForm();
});

// 设置时间范围条件
function setTimeRangeCondition(timeRange) {
    const whereConditionElement = document.getElementById('whereCondition');
    if (!whereConditionElement) return;
    
    if (timeRange === 'clear') {
        // 清除条件
        whereConditionElement.value = '';
        Utils.showSuccess('已清除时间范围条件');
        return;
    }
    
    // 解析时间范围参数
    let months;
    let displayText;
    switch(timeRange) {
        case '1_year':
            months = 12;
            displayText = '1年';
            break;
        case '6_months':
            months = 6;
            displayText = '6个月';
            break;
        case '3_months':
            months = 3;
            displayText = '3个月';
            break;
        case '1_month':
            months = 1;
            displayText = '1个月';
            break;
        default:
            Utils.showError('无效的时间范围参数');
            return;
    }
    
    // 计算时间范围
    const endDate = new Date();
    const startDate = new Date();
    startDate.setMonth(startDate.getMonth() - months);
    
    // 格式化日期为 YYYY-MM-DD
    const formatDate = (date) => {
        return date.getFullYear() + '-' + 
               String(date.getMonth() + 1).padStart(2, '0') + '-' + 
               String(date.getDate()).padStart(2, '0');
    };
    
    const startDateStr = formatDate(startDate);
    
    // 生成WHERE条件（简化版本，主要使用updated_at字段）
    const condition = `updated_at >= '${startDateStr}'`;
    
    whereConditionElement.value = condition;
    
    // 显示提示信息
    Utils.showSuccess(`已设置${displayText}前至今的时间范围条件`);
}
