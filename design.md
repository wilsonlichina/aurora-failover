# Aurora PostgreSQL 故障转移测试方案设计（简化版）

## 1. 项目目标
- 测量 Aurora PostgreSQL 故障转移期间的停机时间
- 对比直接连接与 RDS 代理连接的故障转移性能

## 2. 测试场景
1. **直接连接**：客户端 → Aurora 集群端点
2. **代理连接**：客户端 → RDS 代理 → Aurora 集群

## 3. 核心设计

### 3.1 连接配置
```
直接连接：
- Writer: ards-with-rdsproxy.cluster-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com
- Reader: ards-with-rdsproxy.cluster-ro-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com

RDS 代理连接：
- Writer: proxy-1753874304259-ards-with-rdsproxy.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com
- Reader: proxy-1753874304259-ards-with-rdsproxy-read-only.endpoint.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com

认证：postgres / Guoguo123
```

### 3.2 测试流程
```
1. 启动监控 → 2. 触发故障转移 → 3. 记录恢复时间 → 4. 生成报告
```

### 3.3 关键指标
- **停机时间**：从最后一次成功操作到第一次恢复成功操作的时间间隔
- **检测时间**：发现连接失败的时间
- **恢复时间**：重新建立连接的时间

## 4. 技术实现

### 4.1 核心组件
1. **连接测试器**：执行简单的 `SELECT 1` 查询
2. **时间记录器**：记录关键时间点
3. **结果分析器**：计算停机时间并生成报告

### 4.2 实现逻辑
```python
# 伪代码
while True:
    try:
        execute_query("SELECT 1")
        record_success_time()
        sleep(0.1)  # 100ms 间隔
    except Exception:
        record_failure_time()
        # 等待恢复
        while not connection_recovered():
            sleep(0.1)
        record_recovery_time()
```

### 4.3 文件结构
```
aurora-failover/
├── src/
│   ├── connection_tester.py    # 连接测试核心逻辑
│   ├── config.py              # 配置管理
│   └── reporter.py            # 结果报告
├── results/                   # 测试结果输出
├── requirements.txt
└── main.py                    # 主程序入口
```

## 5. 测试步骤
1. **准备阶段**：配置两种连接方式
2. **基线测试**：验证正常连接工作
3. **故障转移测试**：
   - 启动监控程序
   - 手动触发 Aurora 故障转移
   - 记录整个过程
4. **结果分析**：计算并对比停机时间

## 6. 输出结果
- 直接连接停机时间：X 秒
- 代理连接停机时间：Y 秒
- 性能提升：(X-Y)/X * 100%
- 详细时间线日志

## 7. 成功标准
- 能够准确测量故障转移时间（精度 100ms）
- 成功对比两种连接方式
- 生成简洁的对比报告

这个简化版本专注于核心功能，去除了复杂的架构设计，更容易实现和维护。
