# Aurora PostgreSQL 故障转移测试工具

这是一个用于测量和比较 Aurora PostgreSQL 在故障转移期间停机时间的工具，特别用于评估 RDS 代理在减少停机时间方面的效果。

## 功能特性

- 支持直接连接和 RDS 代理连接两种测试模式
- 实时监控连接状态和响应时间
- 精确测量故障转移停机时间
- **新增：pgbench 负载测试功能**
- 自动生成详细的对比报告
- 支持自定义测试参数

## 安装依赖

### Python 依赖
```bash
pip install -r requirements.txt
```

### pgbench 安装（负载测试功能需要）
```bash
# macOS
brew install postgresql

# Ubuntu/Debian
sudo apt-get install postgresql-client

# CentOS/RHEL
sudo yum install postgresql
```

## 使用方法

### 基本连接测试（原有功能）

```bash
# 运行完整对比测试（推荐）
python main.py --mode both --duration 300

# 仅测试直接连接
python main.py --mode direct --duration 180

# 仅测试代理连接
python main.py --mode proxy --duration 180
```

### pgbench 负载测试（新功能）

```bash
# 基本负载测试
python main.py --mode both --duration 300 --enable-pgbench

# 自定义负载参数
python main.py --mode both --duration 300 --enable-pgbench \
    --pgbench-clients 20 --pgbench-jobs 4 --pgbench-scale 50

# 只读负载测试
python main.py --mode both --duration 300 --enable-pgbench \
    --pgbench-mode read-only --pgbench-clients 30

# 长时间预热测试
python main.py --mode both --duration 600 --enable-pgbench \
    --warmup-time 120 --pgbench-clients 15
```

### 参数说明

#### 基本参数
- `--mode`: 测试模式
  - `direct`: 仅测试直接连接
  - `proxy`: 仅测试代理连接
  - `both`: 同时测试两种连接方式（默认）

- `--duration`: 测试持续时间（秒），默认 300 秒

- `--interval`: 查询间隔（秒），默认 0.1 秒

#### pgbench 负载测试参数
- `--enable-pgbench`: 启用 pgbench 负载测试

- `--pgbench-clients`: pgbench 并发客户端数，默认 10

- `--pgbench-jobs`: pgbench 工作线程数，默认 2

- `--pgbench-scale`: pgbench 数据规模因子，默认 10

- `--pgbench-mode`: pgbench 测试模式
  - `tpc-b`: 标准 TPC-B 测试（默认）
  - `read-only`: 只读测试
  - `custom`: 自定义脚本测试

- `--warmup-time`: 负载预热时间（秒），默认 60 秒

### 测试流程

#### 标准连接测试流程
1. **准备阶段**：
   - 启动测试程序
   - 建立数据库连接
   - 开始连续监控

2. **故障转移阶段**：
   - 在 AWS 控制台或使用 AWS CLI 手动触发故障转移
   - 程序自动检测连接失败
   - 记录故障转移过程的关键时间点

3. **恢复阶段**：
   - 程序自动检测连接恢复
   - 记录恢复时间
   - 继续监控直到测试结束

#### pgbench 负载测试流程
1. **准备阶段**：
   - 初始化 pgbench 测试数据
   - 验证数据库连接
   - 准备负载生成器

2. **预热阶段**：
   - 启动 pgbench 负载生成
   - 等待负载稳定（默认 60 秒）
   - 监控负载性能指标

3. **正式测试阶段**：
   - 持续生成负载
   - 同时监控故障转移
   - 实时显示性能指标

4. **结果分析**：
   - 停止负载生成
   - 分析故障转移对负载的影响
   - 生成综合报告

## 手动触发故障转移

### 使用 AWS CLI

```bash
# 触发故障转移到指定实例
aws rds failover-db-cluster \
    --db-cluster-identifier ards-with-rdsproxy \
    --target-db-instance-identifier <target-instance-id> \
    --region ap-southeast-1

# 或者不指定目标实例，让 Aurora 自动选择
aws rds failover-db-cluster \
    --db-cluster-identifier ards-with-rdsproxy \
    --region ap-southeast-1
```

### 使用 AWS 控制台

1. 登录 AWS 控制台
2. 进入 RDS 服务
3. 选择 Aurora 集群 `ards-with-rdsproxy`
4. 点击 "Actions" → "Failover"
5. 选择目标实例或让系统自动选择
6. 确认执行故障转移

## 输出结果

### 控制台输出
程序运行时会实时显示：
- 连接状态
- 故障检测信息
- **负载性能指标（TPS、延迟等）**
- 恢复时间
- 基本统计信息

### 文件输出
测试完成后会在 `results/` 目录生成：

#### 标准连接测试
1. **详细结果文件**：
   - `direct_result_YYYYMMDD_HHMMSS.json`
   - `proxy_result_YYYYMMDD_HHMMSS.json`

2. **对比报告**：
   - `comparison_report_YYYYMMDD_HHMMSS.txt`

#### pgbench 负载测试
1. **综合报告**：
   - `pgbench_failover_report_YYYYMMDD_HHMMSS.txt`

### 报告内容

#### 标准测试报告
- 基本统计信息（成功率、失败次数等）
- 停机时间详细分析
- 性能改善百分比
- 详细的时间线记录

#### pgbench 负载测试报告
- **测试配置信息**
- **负载性能统计**：
  - 平均/最大/最小 TPS
  - 平均/最大/最小延迟
  - 错误统计
- **故障转移影响分析**：
  - 故障转移检测时间
  - 负载中断时长
  - 性能恢复时间
- **连接类型对比**（如果测试两种连接）

## 配置说明

数据库连接配置在 `src/config.py` 中：

```python
# Aurora 直接连接
direct_writer: ards-with-rdsproxy.cluster-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com
direct_reader: ards-with-rdsproxy.cluster-ro-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com

# RDS 代理连接
proxy_writer: proxy-1753874304259-ards-with-rdsproxy.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com
proxy_reader: proxy-1753874304259-ards-with-rdsproxy-read-only.endpoint.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com
```

## 注意事项

1. **测试环境**：建议在测试环境中进行，避免影响生产业务
2. **网络稳定性**：确保测试客户端网络连接稳定
3. **权限要求**：需要数据库连接权限和 AWS 故障转移权限
4. **测试时机**：建议在业务低峰期进行测试
5. **多次测试**：建议进行多次测试以获得更准确的平均值
6. **pgbench 数据**：首次运行 pgbench 测试时会自动初始化测试数据
7. **资源消耗**：pgbench 负载测试会消耗更多的数据库资源

## 故障排除

### 常见问题

1. **连接超时**：
   - 检查网络连接
   - 验证数据库端点地址
   - 确认安全组配置

2. **认证失败**：
   - 验证用户名和密码
   - 检查数据库用户权限

3. **导入错误**：
   - 确保已安装所有依赖
   - 检查 Python 路径配置

4. **pgbench 命令未找到**：
   - 确保已安装 PostgreSQL 客户端
   - 检查 PATH 环境变量

5. **pgbench 初始化失败**：
   - 检查数据库连接权限
   - 确认数据库有足够空间
   - 验证用户有创建表的权限

### 调试模式

如需更详细的调试信息，可以修改代码中的日志级别或添加更多调试输出。

## 项目结构

```
aurora-failover/
├── main.py                              # 主程序入口
├── requirements.txt                     # 依赖包列表
├── README.md                           # 使用说明
├── design.md                           # 设计文档
├── src/                                # 源代码目录
│   ├── __init__.py
│   ├── config.py                       # 配置管理
│   ├── connection_tester.py            # 连接测试核心逻辑
│   ├── pgbench_load_generator.py       # pgbench 负载生成器（新增）
│   ├── failover_with_pgbench_tester.py # 集成测试器（新增）
│   └── reporter.py                     # 结果报告生成
└── results/                            # 测试结果输出目录
```

## 性能基准

### pgbench 测试模式说明

- **tpc-b 模式**：标准的 TPC-B 基准测试，包含读写混合操作
- **read-only 模式**：纯读操作，适合测试读性能和读故障转移
- **custom 模式**：支持自定义 SQL 脚本，可模拟特定业务场景

### 推荐配置

- **轻量测试**：`--pgbench-clients 5 --pgbench-scale 5`
- **标准测试**：`--pgbench-clients 10 --pgbench-scale 10`（默认）
- **高负载测试**：`--pgbench-clients 50 --pgbench-scale 100`
