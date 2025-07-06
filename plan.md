# 同步任务前端功能完善计划

## Notes
- 用户正在完善同步任务的前端功能。
- 需要支持同步模式切换的JavaScript函数。
- 编辑和查看功能需支持新的WHERE条件字段。
- jobs.js 和 jobs.html 已有部分相关改动。
- editJob/viewJob 已处理 whereCondition 字段。
- jobs.html 模板已包含 whereCondition 输入框。
- 前端功能已开发完毕，当前被后端 sync_engine.py 中 ConflictStrategy 未定义报错阻塞。
- 遇到批量插入时 psycopg2 报 can't adapt type 'dict'，details 字段需要序列化处理。
- 已修复后端批量插入 dict/list 字段无法适配问题，现已自动序列化为 JSON 字符串。
- 用户需要增量同步支持每个表独立的匹配规则（如id、时间等），并能自动获取目标表最大id/时间作为增量条件。
- 已完成JobTargetTable模型和数据库迁移脚本的扩展，支持每表独立增量同步配置（策略、字段、条件、last_sync_value）。
- 调度器遇到立即执行（IMMEDIATE）任务或无cron表达式任务时会报错，需兼容处理。

## Task List
- [x] 添加同步模式切换的JavaScript函数（jobs.js）
- [ ] 更新HTML模板以调用同步模式切换函数（jobs.html）
- [x] 更新editJob和viewJob函数以支持WHERE条件字段（jobs.js & jobs.html）
    - [x] editJob 支持填充/保存 whereCondition
    - [x] viewJob 支持展示 whereCondition
- [ ] 测试前端功能，确保切换和WHERE条件正确联动
- [x] 修复sync_engine.py中的ConflictStrategy未定义错误
- [ ] 测试批量插入，确保复杂字段（dict/list）可用
- [x] 设计JobTargetTable模型和迁移脚本，支持每表独立增量同步配置
- [x] 设计并实现每个表独立的增量同步规则（自动获取目标表最大id/时间等，支持自定义匹配字段）
- [x] 实现同步引擎支持每表独立增量同步逻辑
- [x] 修正并验证同步引擎对每表独立增量同步的调用
- [ ] 测试和验证每表独立增量同步
  - [ ] 验证数据库迁移和模型字段
  - [x] 修正调度器对无cron表达式任务的兼容性
  - [ ] 端到端测试每表独立增量同步功能

