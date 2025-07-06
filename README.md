# PostgreSQL数据库同步平台

一个基于Web的PostgreSQL数据库同步与备份自动化平台，提供完整的数据同步管理解决方案。

## 功能特性

- 🔗 **连接管理**: 集中管理多个PostgreSQL数据库连接，支持加密存储
- 📋 **任务调度**: 灵活的同步任务配置，支持全量/增量同步
- ⏰ **定时执行**: 基于Cron表达式或间隔时间的自动调度
- 📊 **监控日志**: 详细的执行日志和统计分析
- 🎨 **友好界面**: 现代化的Web管理界面
- 🔒 **安全保障**: 连接密码加密存储，安全性高

## 技术栈

- **后端**: FastAPI + Uvicorn
- **数据库**: PostgreSQL + SQLAlchemy + Alembic
- **调度**: APScheduler
- **前端**: Bootstrap 5 + Jinja2
- **安全**: Cryptography (Fernet加密)

## 快速开始

### 1. 环境准备

确保已安装Python 3.8+和PostgreSQL数据库。

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置环境变量

复制 `.env.example` 为 `.env` 并修改配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件，重要配置项：

- `DATABASE_URL`: 平台数据库连接URL
- `ENCRYPTION_KEY`: 加密密钥（必须设置）

生成加密密钥：
```python
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

### 4. 初始化数据库

```bash
# 创建数据库迁移
alembic revision --autogenerate -m "Initial migration"

# 执行迁移
alembic upgrade head
```

### 5. 启动应用

```bash
# 开发模式
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 生产模式
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 6. 访问应用

打开浏览器访问：http://localhost:8000

## 使用说明

### 1. 添加数据库连接

1. 点击"数据库连接"菜单
2. 点击"添加连接"按钮
3. 填写数据库连接信息
4. 测试连接成功后保存

### 2. 创建同步任务

1. 点击"同步任务"菜单
2. 点击"创建任务"按钮
3. 配置源数据库、目标数据库
4. 选择同步模式（全量/增量）
5. 设置调度计划
6. 选择要同步的表
7. 保存并启动任务

### 3. 监控执行日志

1. 点击"执行日志"菜单
2. 查看任务执行历史
3. 筛选和查看详细日志
4. 清理历史日志

## 目录结构

```
pg_synchronization/
├── main.py                 # 应用入口
├── config.py              # 配置管理
├── database.py            # 数据库配置
├── models/                # 数据模型
├── routers/               # API路由
├── utils/                 # 工具函数
├── scheduler/             # 任务调度
├── sync_engine/           # 同步引擎
├── templates/             # HTML模板
├── static/                # 静态资源
├── migrations/            # 数据库迁移
├── logs/                  # 日志文件
├── requirements.txt       # 依赖清单
├── .env                   # 环境配置
└── README.md             # 项目说明
```

## API文档

启动应用后访问以下地址查看完整API文档：

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 开发指南

### 数据库迁移

```bash
# 创建新的迁移文件
alembic revision --autogenerate -m "描述"

# 执行迁移
alembic upgrade head

# 回滚迁移
alembic downgrade -1
```

### 调试模式

在 `.env` 文件中设置：
```
DEBUG=true
LOG_LEVEL=DEBUG
```

## 注意事项

1. **安全配置**: 生产环境必须设置强加密密钥
2. **数据库权限**: 确保连接用户有足够的读写权限
3. **性能考虑**: 大表同步建议在业务低峰期执行
4. **备份策略**: 建议同步前做好数据备份

## 故障排除

### 常见问题

1. **连接失败**: 检查数据库连接信息和网络连通性
2. **同步失败**: 查看详细日志，检查表结构兼容性
3. **调度不执行**: 确认调度器服务是否正常运行

### 日志查看

应用日志位于 `logs/app.log`，也可通过Web界面查看执行日志。

## 贡献

欢迎提交Issue和Pull Request！

## 许可证

MIT License
