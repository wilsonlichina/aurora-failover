# Aurora PostgreSQL 故障转移测试方案设计

## 1. 项目目标
- 测量 Aurora PostgreSQL 故障转移期间的停机时间
- 对比直接连接与 RDS 代理连接的故障转移性能
- 提供真实业务场景下的性能评估
- 支持 pgbench 标准负载测试

## 2. 测试场景
1. **直接连接**：客户端 → Aurora 集群端点
2. **代理连接**：客户端 → RDS 代理 → Aurora 集群
3. **业务场景测试**：模拟真实的读/写/事务操作
4. **pgbench 负载测试**：标准数据库性能基准测试

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
1. 准备阶段 → 2. 预热阶段 → 3. 正式测试 → 4. 故障转移 → 5. 结果分析
```

### 3.3 关键指标
- **停机时间**：从最后一次成功操作到第一次恢复成功操作的时间间隔
- **检测时间**：发现连接失败的时间
- **恢复时间**：重新建立连接的时间
- **TPS**：每秒事务数
- **延迟**：操作响应时间
- **成功率**：按操作类型统计的成功率

## 4. 技术实现

### 4.1 核心组件

#### ConnectionTester（业务场景测试器）
- 执行真实的业务操作（读/写/事务）
- 连接池管理（5个连接）
- 并发操作支持（默认3个线程）
- 精确的停机时间检测

#### PgbenchLoadGenerator（负载生成器）
- 标准 pgbench 测试
- 支持多种测试模式（TPC-B、只读、自定义）
- 实时性能监控
- 负载预热功能

#### FailoverTester（集成测试器）
- 独立监控每种连接类型的 downtime
- 精度达到 100ms
- 综合负载和故障转移测试
- 详细的性能分析

#### Reporter（报告生成器）
- 业务场景对比报告
- pgbench 负载测试报告
- JSON 格式详细数据
- 可视化性能对比

### 4.2 业务场景设计

#### 测试表结构
1. **business_users**：用户表
2. **business_orders**：订单表
3. **business_products**：产品表
4. **business_logs**：操作日志表

#### 操作类型及权重
- **读操作（70%）**：用户列表、订单统计、产品搜索、用户订单、最近日志
- **写操作（20%）**：更新登录信息、创建订单、更新库存、插入日志
- **事务操作（10%）**：完整的下单流程（检查库存→创建订单→更新库存→记录日志）

### 4.3 实现逻辑
```python
# 业务场景测试伪代码
def run_business_test():
    initialize_connection_pool()
    setup_test_tables()
    
    with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
        while test_running:
            operation_type = choose_operation_type()  # 根据权重选择
            future = executor.submit(execute_business_operation, operation_type)
            record_operation_result(future.result())
            
            if consecutive_failures >= threshold:
                record_downtime_start()
            elif downtime_active and operation_success:
                record_downtime_end()

# pgbench 负载测试伪代码
def run_pgbench_test():
    prepare_database()
    start_load_generation()
    
    while test_running:
        monitor_connection_downtime()  # 独立线程监控
        collect_performance_metrics()
        display_real_time_status()
```

### 4.4 文件结构
```
aurora-failover/
├── src/
│   ├── connection_tester.py    # 业务场景测试核心逻辑
│   ├── pgbench_load_generator.py # pgbench 负载生成器
│   ├── failover_tester.py      # 集成测试器
│   ├── config.py              # 配置管理
│   └── reporter.py            # 结果报告
├── results/                   # 测试结果输出
├── requirements.txt
├── main.py                    # 主程序入口
├── run_test.sh               # 快速测试脚本
├── README.md                 # 使用说明
├── design.md                 # 设计文档
└── business.md               # 业务场景设计文档
```

## 5. 测试步骤

### 5.1 业务场景测试
1. **准备阶段**：初始化连接池和测试表
2. **基线测试**：验证正常连接工作
3. **故障转移测试**：
   - 启动并发业务操作
   - 手动触发 Aurora 故障转移
   - 记录整个过程的操作成功/失败
4. **结果分析**：计算并对比停机时间和成功率

### 5.2 pgbench 负载测试
1. **准备阶段**：初始化 pgbench 测试数据
2. **预热阶段**：启动负载并等待稳定
3. **正式测试**：
   - 持续生成负载
   - 独立监控每种连接的 downtime
   - 实时显示性能指标
4. **故障转移**：手动触发并观察影响
5. **结果分析**：生成综合报告

## 6. 输出结果

### 6.1 业务场景测试报告
```
业务场景测试结果对比报告
================================

Direct 连接结果:
- 总操作数: 1,250
- 总体成功率: 97.2%
- 读操作: 875 (成功率: 98.1%)
- 写操作: 250 (成功率: 95.6%)  
- 事务操作: 125 (成功率: 94.4%)
- 平均响应时间: 0.045秒
- 检测停机时间: 12.3秒

Proxy 连接结果:
- 总操作数: 1,180
- 总体成功率: 98.8%
- 读操作: 826 (成功率: 99.2%)
- 写操作: 236 (成功率: 98.3%)
- 事务操作: 118 (成功率: 97.5%)
- 平均响应时间: 0.052秒  
- 检测停机时间: 3.1秒

性能对比:
- Proxy 停机时间减少: 74.8%
- Proxy 事务成功率提升: 3.3%
- Proxy 平均响应时间增加: 15.6%
```

### 6.2 pgbench 负载测试报告
```
Aurora PostgreSQL 故障转移 + pgbench 负载测试报告
================================================

负载性能结果:
  direct 连接:
    平均 TPS: 1234.56
    最大 TPS: 1456.78
    平均延迟: 8.12ms
    错误数量: 23

  proxy 连接:
    平均 TPS: 1198.34
    最大 TPS: 1389.45
    平均延迟: 8.45ms
    错误数量: 12

故障转移 Downtime 分析:
  direct 连接:
    总 downtime: 12.345秒
    中断次数: 1
    详细记录:
      #1: 14:23:45.123 - 14:23:57.468 (12.345秒)

  proxy 连接:
    总 downtime: 3.127秒
    中断次数: 1
    详细记录:
      #1: 14:23:46.234 - 14:23:49.361 (3.127秒)

Downtime 对比分析:
  Proxy 相对 Direct 的改善: +74.7%
  结论: RDS Proxy 在故障转移时表现更好，downtime 更短
```

## 7. 成功标准
- 能够准确测量故障转移时间（精度 100ms）
- 成功对比两种连接方式
- 提供真实业务场景下的性能数据
- 生成详细的对比报告
- 支持标准 pgbench 负载测试

## 8. 技术特点

### 8.1 精确的 Downtime 监控
- 独立线程监控每种连接类型
- 100ms 检查间隔
- 连续失败阈值检测
- 精确记录开始和结束时间

### 8.2 真实业务场景模拟
- 替代简单的 SELECT 1 心跳查询
- 模拟实际的读/写/事务操作
- 支持自定义操作权重
- 并发操作执行

### 8.3 连接池管理
- 智能连接池（5个连接）
- 自动检测和重建失效连接
- 线程安全的连接获取
- 连接状态监控

### 8.4 综合性能分析
- 按操作类型统计成功率
- 响应时间分析
- TPS 计算
- 错误统计和分类

这个设计方案提供了完整的故障转移测试解决方案，既支持真实业务场景测试，也支持标准的 pgbench 负载测试，能够全面评估 RDS Proxy 在故障转移场景下的性能表现。
