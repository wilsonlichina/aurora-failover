# Aurora PostgreSQL 业务场景故障转移测试方案

## 概述

将现有的简单心跳测试（SELECT 1）替换为真实的业务场景测试，通过模拟实际的业务查询和写入操作来更准确地评估 RDS Proxy 和 Direct 连接方式在故障转移期间的表现差异。

## 修改目标

- 移除简单的 `SELECT 1` 心跳查询
- 实现真实的业务场景模拟
- 保持现有的故障检测和报告功能
- 提供更有意义的性能对比数据

## 业务场景设计

### 测试表结构

#### 1. 用户表 (business_users)
```sql
CREATE TABLE business_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    login_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active'
);
```

#### 2. 订单表 (business_orders)
```sql
CREATE TABLE business_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES business_users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3. 产品表 (business_products)
```sql
CREATE TABLE business_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER DEFAULT 0,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 4. 操作日志表 (business_logs)
```sql
CREATE TABLE business_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    action VARCHAR(50) NOT NULL,
    details TEXT,
    ip_address INET,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 业务操作类型及权重

#### 读操作 (70%)
1. **用户列表查询**
   ```sql
   SELECT id, username, email, last_login, login_count, status
   FROM business_users 
   WHERE status = 'active'
   ORDER BY last_login DESC NULLS LAST
   LIMIT 20;
   ```

2. **订单统计查询**
   ```sql
   SELECT status, COUNT(*) as count, SUM(amount) as total_amount
   FROM business_orders 
   WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
   GROUP BY status;
   ```

3. **产品搜索查询**
   ```sql
   SELECT id, name, price, stock, category
   FROM business_products 
   WHERE category = ? AND stock > 0
   ORDER BY price
   LIMIT 10;
   ```

4. **用户订单查询**
   ```sql
   SELECT o.id, o.order_number, o.amount, o.status, o.created_at
   FROM business_orders o
   JOIN business_users u ON o.user_id = u.id
   WHERE u.id = ?
   ORDER BY o.created_at DESC
   LIMIT 10;
   ```

5. **最近日志查询**
   ```sql
   SELECT l.action, l.details, l.created_at, u.username
   FROM business_logs l
   LEFT JOIN business_users u ON l.user_id = u.id
   WHERE l.created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
   ORDER BY l.created_at DESC
   LIMIT 50;
   ```

#### 写操作 (20%)
1. **更新用户登录信息**
   ```sql
   UPDATE business_users 
   SET last_login = CURRENT_TIMESTAMP,
       login_count = login_count + 1
   WHERE id = ?;
   ```

2. **创建订单**
   ```sql
   INSERT INTO business_orders (user_id, order_number, amount, status)
   VALUES (?, ?, ?, 'pending');
   ```

3. **更新产品库存**
   ```sql
   UPDATE business_products 
   SET stock = GREATEST(0, stock + ?)
   WHERE id = ?;
   ```

4. **插入操作日志**
   ```sql
   INSERT INTO business_logs (user_id, action, details, ip_address)
   VALUES (?, ?, ?, ?);
   ```

#### 事务操作 (10%)
**完整的下单流程**：
1. 检查库存（带锁）
2. 创建订单
3. 更新库存
4. 记录操作日志

```sql
BEGIN;
SELECT stock, price FROM business_products WHERE id = ? FOR UPDATE;
INSERT INTO business_orders (user_id, order_number, amount, status) VALUES (?, ?, ?, 'confirmed');
UPDATE business_products SET stock = stock - ? WHERE id = ?;
INSERT INTO business_logs (user_id, action, details) VALUES (?, 'purchase', ?);
COMMIT;
```

## 技术实现方案

### 1. 修改 ConnectionTester 类

#### 核心改动
- 移除 `_execute_test_query()` 中的 `SELECT 1`
- 添加 `_execute_business_operation()` 方法
- 实现连接池管理（5个连接）
- 支持并发操作执行

#### 新增方法
```python
def _setup_test_tables(self)           # 创建和初始化测试表
def _execute_read_operation(self)      # 执行读操作
def _execute_write_operation(self)     # 执行写操作  
def _execute_transaction_operation(self) # 执行事务操作
def _choose_operation_type(self)       # 根据权重选择操作类型
def _execute_business_operation(self)  # 执行业务操作的入口
```

#### 连接池管理
- 维护5个数据库连接的连接池
- 自动检测和重建失效连接
- 线程安全的连接获取机制

### 2. 增强配置管理

#### config.py 新增配置
```python
class TestConfig:
    # 业务测试配置
    self.read_weight = 70           # 读操作权重
    self.write_weight = 20          # 写操作权重  
    self.transaction_weight = 10    # 事务操作权重
    self.concurrent_workers = 3     # 并发工作线程数
    self.min_operation_interval = 0.1  # 最小操作间隔
    self.max_operation_interval = 0.5  # 最大操作间隔
    self.connection_pool_size = 5   # 连接池大小
```

### 3. 命令行参数扩展

#### main.py 新增参数
```python
parser.add_argument('--concurrent-workers', type=int, default=3,
                   help='并发工作线程数 (默认: 3)')
parser.add_argument('--read-weight', type=int, default=70,
                   help='读操作权重百分比 (默认: 70)')
parser.add_argument('--write-weight', type=int, default=20,
                   help='写操作权重百分比 (默认: 20)')
parser.add_argument('--transaction-weight', type=int, default=10,
                   help='事务操作权重百分比 (默认: 10)')
```

### 4. 结果数据结构增强

#### 新的 BusinessOperation 数据类
```python
@dataclass
class BusinessOperation:
    operation_id: str
    operation_type: str  # 'read', 'write', 'transaction'
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    error_message: str = ""
    response_time: Optional[float] = None
    affected_rows: int = 0
```

#### 增强的 TestResult 类
```python
@dataclass  
class TestResult:
    # 原有字段保持不变
    # 新增业务统计字段
    read_operations: int = 0
    write_operations: int = 0
    transaction_operations: int = 0
    successful_reads: int = 0
    successful_writes: int = 0
    successful_transactions: int = 0
    
    # 新增属性方法
    @property
    def read_success_rate(self) -> float
    @property  
    def write_success_rate(self) -> float
    @property
    def transaction_success_rate(self) -> float
    @property
    def average_response_time(self) -> float
```

### 5. 报告功能增强

#### 新增报告内容
- 按操作类型的成功率统计
- 平均/最大/最小响应时间
- TPS (Transactions Per Second) 计算
- 故障转移期间各类操作的影响分析
- Direct vs Proxy 的详细性能对比

#### 报告示例
```
业务场景测试结果对比报告
================================

测试配置:
- 持续时间: 300秒
- 并发线程: 3
- 操作权重: 读70% 写20% 事务10%

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

## 实现步骤

### Phase 1: 核心功能实现
1. 修改 `ConnectionTester` 类，移除心跳查询
2. 实现业务操作方法
3. 添加连接池管理
4. 实现表结构初始化

### Phase 2: 配置和参数
1. 扩展 `TestConfig` 类
2. 添加命令行参数解析
3. 更新 `main.py` 主流程

### Phase 3: 结果和报告
1. 增强 `TestResult` 数据结构
2. 修改 `Reporter` 类
3. 实现业务场景报告生成

### Phase 4: 测试和优化
1. 端到端测试
2. 性能调优
3. 错误处理完善

## 使用示例

### 基本业务场景测试
```bash
python main.py --mode both --duration 300
```

### 自定义并发和权重
```bash
python main.py --mode both --duration 300 \
    --concurrent-workers 5 \
    --read-weight 60 --write-weight 30 --transaction-weight 10
```

### 结合 pgbench 负载测试
```bash
python main.py --mode both --duration 300 \
    --concurrent-workers 3 \
    --enable-pgbench --pgbench-clients 10
```

## 预期效果

1. **更真实的测试场景**：通过模拟实际业务操作，能够更准确地反映故障转移对真实业务的影响

2. **更详细的性能分析**：按操作类型统计成功率，识别哪类操作在故障转移时更容易受影响

3. **更有价值的对比数据**：Direct 和 Proxy 连接方式在处理复杂业务场景时的差异更加明显

4. **更好的决策支持**：为选择连接方式提供基于真实业务场景的数据支撑

## 风险和注意事项

1. **资源消耗**：业务场景测试比简单心跳查询消耗更多数据库资源
2. **测试数据**：需要确保测试表有足够的初始数据
3. **并发控制**：需要合理控制并发度，避免对数据库造成过大压力
4. **清理机制**：测试完成后需要清理测试数据（可选）
