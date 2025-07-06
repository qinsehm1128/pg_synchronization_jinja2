# 安全配置指南

## 概述

本文档提供了关于如何安全管理敏感配置信息的指南，特别是数据库凭据、API密钥和加密密钥等。

## 🚨 重要安全提醒

**永远不要将包含敏感信息的文件提交到版本控制系统！**

## 配置文件管理

### 1. 环境配置文件

- **`.env`**: 包含实际的敏感配置，**绝对不能**提交到Git
- **`.env.example`**: 配置模板，只包含变量名和示例值，可以提交到Git

### 2. 设置步骤

1. 复制 `.env.example` 为 `.env`：
   ```bash
   cp .env.example .env
   ```

2. 编辑 `.env` 文件，填入实际的配置值：
   ```bash
   # 数据库配置
   DATABASE_URL=postgresql://real_username:real_password@your_host:5432/your_database
   
   # 加密密钥
   ENCRYPTION_KEY=your_actual_encryption_key
   ```

3. 确保 `.env` 文件在 `.gitignore` 中被忽略

## 生成安全密钥

### 加密密钥生成

使用以下Python代码生成强加密密钥：

```python
from cryptography.fernet import Fernet

# 生成新的加密密钥
key = Fernet.generate_key()
print(f"ENCRYPTION_KEY={key.decode()}")
```

### 数据库密码要求

- 至少12个字符
- 包含大小写字母、数字和特殊字符
- 避免使用常见词汇或个人信息

## 如果敏感信息已泄露

### 立即行动清单

1. **更换所有泄露的凭据**
   - 数据库密码
   - API密钥
   - 加密密钥

2. **从Git历史中删除敏感文件**
   ```bash
   # 从跟踪中移除文件
   git rm --cached .env
   
   # 添加到.gitignore
   echo ".env" >> .gitignore
   
   # 提交更改
   git add .gitignore
   git commit -m "security: 移除.env文件跟踪并添加.gitignore"
   
   # 从历史记录中彻底删除（谨慎使用）
   git filter-branch --force --index-filter 'git rm --cached --ignore-unmatch .env' --prune-empty --tag-name-filter cat -- --all
   
   # 清理
   git reflog expire --expire=now --all
   git gc --prune=now --aggressive
   ```

3. **强制推送更新的历史**
   ```bash
   git push --force-with-lease origin main
   ```
   
   ⚠️ **警告**: 这会重写远程仓库历史，确保团队成员了解此操作

4. **通知团队成员**
   - 告知安全事件
   - 要求重新克隆仓库
   - 提供新的配置信息

## 最佳实践

### 开发环境

1. **使用环境变量**
   ```python
   import os
   from dotenv import load_dotenv
   
   load_dotenv()
   
   DATABASE_URL = os.getenv('DATABASE_URL')
   ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
   ```

2. **配置验证**
   ```python
   def validate_config():
       required_vars = ['DATABASE_URL', 'ENCRYPTION_KEY']
       missing = [var for var in required_vars if not os.getenv(var)]
       
       if missing:
           raise ValueError(f"Missing required environment variables: {missing}")
   ```

### 生产环境

1. **使用密钥管理服务**
   - Azure Key Vault
   - AWS Secrets Manager
   - HashiCorp Vault

2. **环境变量注入**
   - Docker secrets
   - Kubernetes secrets
   - CI/CD平台的安全变量

3. **定期轮换密钥**
   - 设置密钥过期时间
   - 建立密钥轮换流程
   - 监控密钥使用情况

## 监控和审计

### 安全检查清单

- [ ] `.env` 文件在 `.gitignore` 中
- [ ] 没有硬编码的密码或密钥
- [ ] 使用强密码和密钥
- [ ] 定期更换凭据
- [ ] 监控异常访问
- [ ] 备份重要配置（安全存储）

### 代码审查要点

- 检查是否有硬编码的敏感信息
- 确认环境变量正确使用
- 验证错误处理不会泄露敏感信息
- 确保日志不包含密码或密钥

## 应急响应

### 发现泄露时的步骤

1. **立即评估影响范围**
2. **更换所有相关凭据**
3. **检查访问日志**
4. **通知相关人员**
5. **更新安全措施**
6. **记录事件和教训**

## 联系信息

如有安全相关问题，请联系：
- 项目维护者
- 安全团队
- 系统管理员

---

**记住：安全是每个人的责任！**