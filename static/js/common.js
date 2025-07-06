// 通用JavaScript函数

// API基础URL
const API_BASE_URL = '/api';

// 通用API请求函数
class ApiClient {
    static async request(method, url, data = null) {
        try {
            const config = {
                method: method,
                url: API_BASE_URL + url,
                headers: {
                    'Content-Type': 'application/json',
                }
            };

            if (data) {
                config.data = data;
            }

            const response = await axios(config);
            return response.data;
        } catch (error) {
            console.error('API请求失败:', error);
            throw error;
        }
    }

    static async get(url) {
        return this.request('GET', url);
    }

    static async post(url, data) {
        return this.request('POST', url, data);
    }

    static async put(url, data) {
        return this.request('PUT', url, data);
    }

    static async delete(url) {
        return this.request('DELETE', url);
    }
}

// 通用工具函数
class Utils {
    // 格式化日期时间
    static formatDateTime(dateString) {
        if (!dateString) return '-';
        const date = new Date(dateString);
        return date.toLocaleString('zh-CN');
    }

    // 格式化持续时间
    static formatDuration(seconds) {
        if (!seconds) return '-';
        
        // 确保输入是数字并取整
        seconds = Math.floor(Number(seconds));
        
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = seconds % 60;
        
        if (hours > 0) {
            return `${hours}时${minutes}分${secs}秒`;
        } else if (minutes > 0) {
            return `${minutes}分${secs}秒`;
        } else {
            return `${secs}秒`;
        }
    }

    // 格式化数字（添加千分位分隔符）
    static formatNumber(num) {
        if (typeof num !== 'number') return num;
        return num.toLocaleString('zh-CN');
    }

    // 格式化字节数
    static formatBytes(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
    }

    // 获取状态标签
    static getStatusBadge(status) {
        const statusMap = {
            'active': '<span class="badge status-badge status-active">激活</span>',
            'inactive': '<span class="badge status-badge status-inactive">未激活</span>',
            'paused': '<span class="badge status-badge status-paused">已暂停</span>',
            'running': '<span class="badge status-badge status-running">运行中</span>',
            'success': '<span class="badge status-badge status-success">成功</span>',
            'failed': '<span class="badge status-badge status-failed">失败</span>',
            'cancelled': '<span class="badge status-badge status-inactive">已取消</span>'
        };
        return statusMap[status] || `<span class="badge bg-secondary">${status}</span>`;
    }

    // 显示成功消息
    static showSuccess(message) {
        this.showAlert('success', message);
    }

    // 显示错误消息
    static showError(message) {
        this.showAlert('danger', message);
    }

    // 显示警告消息
    static showWarning(message) {
        this.showAlert('warning', message);
    }

    // 显示信息消息
    static showInfo(message) {
        this.showAlert('info', message);
    }

    // 显示通用提示
    static showAlert(type, message) {
        const alertId = 'alert-' + Date.now();
        const alertHtml = `
            <div id="${alertId}" class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
        
        // 创建或获取通知容器
        let alertContainer = document.getElementById('alert-container');
        if (!alertContainer) {
            alertContainer = document.createElement('div');
            alertContainer.id = 'alert-container';
            alertContainer.className = 'position-fixed top-0 end-0 p-3';
            alertContainer.style.zIndex = '9999';
            document.body.appendChild(alertContainer);
        }
        
        alertContainer.insertAdjacentHTML('beforeend', alertHtml);
        
        // 自动消失
        setTimeout(() => {
            const alert = document.getElementById(alertId);
            if (alert) {
                const bsAlert = new bootstrap.Alert(alert);
                bsAlert.close();
            }
        }, 5000);
    }

    // 确认操作
    static async confirm(message, title = '确认操作') {
        return new Promise((resolve) => {
            const modalId = 'confirmModal-' + Date.now();
            const modalHtml = `
                <div class="modal fade" id="${modalId}" tabindex="-1">
                    <div class="modal-dialog">
                        <div class="modal-content">
                            <div class="modal-header">
                                <h5 class="modal-title">${title}</h5>
                                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                            </div>
                            <div class="modal-body">
                                ${message}
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                                <button type="button" class="btn btn-danger" id="confirmBtn">确认</button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            
            document.body.insertAdjacentHTML('beforeend', modalHtml);
            const modal = new bootstrap.Modal(document.getElementById(modalId));
            
            document.getElementById('confirmBtn').addEventListener('click', () => {
                modal.hide();
                resolve(true);
            });
            
            document.getElementById(modalId).addEventListener('hidden.bs.modal', () => {
                document.getElementById(modalId).remove();
                resolve(false);
            });
            
            modal.show();
        });
    }

    // 显示加载状态
    static showLoading(element) {
        const originalContent = element.innerHTML;
        element.innerHTML = '<span class="loading"></span> 加载中...';
        element.disabled = true;
        return originalContent;
    }

    // 隐藏加载状态
    static hideLoading(element, originalContent) {
        element.innerHTML = originalContent;
        element.disabled = false;
    }

    // 防抖函数
    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    // 节流函数
    static throttle(func, limit) {
        let inThrottle;
        return function() {
            const args = arguments;
            const context = this;
            if (!inThrottle) {
                func.apply(context, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }
}

// 表格工具类
class TableHelper {
    constructor(tableId) {
        this.table = document.getElementById(tableId);
        this.tbody = this.table.querySelector('tbody');
    }

    // 清空表格
    clear() {
        this.tbody.innerHTML = '';
    }

    // 添加行
    addRow(rowData) {
        const row = this.tbody.insertRow();
        rowData.forEach(cellData => {
            const cell = row.insertCell();
            cell.innerHTML = cellData;
        });
        return row;
    }

    // 显示空状态
    showEmpty(message = '暂无数据') {
        const colCount = this.table.querySelectorAll('thead th').length;
        this.clear();
        const row = this.tbody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = colCount;
        cell.className = 'text-center text-muted py-4';
        cell.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-inbox"></i>
                <p>${message}</p>
            </div>
        `;
    }

    // 显示加载状态
    showLoading() {
        const colCount = this.table.querySelectorAll('thead th').length;
        this.clear();
        const row = this.tbody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = colCount;
        cell.className = 'text-center py-4';
        cell.innerHTML = '<span class="loading"></span> 加载中...';
    }
}

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 初始化工具提示
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 初始化弹出框
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
});

// 导出工具类供其他脚本使用
window.ApiClient = ApiClient;
window.Utils = Utils;
window.TableHelper = TableHelper;
