"""
连接测试器核心模块 - 业务场景测试
"""

import time
import psycopg2
import threading
import random
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import TestConfig


@dataclass
class BusinessOperation:
    """业务操作记录"""
    operation_id: str
    operation_type: str  # 'read', 'write', 'transaction'
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    error_message: str = ""
    response_time: Optional[float] = None
    affected_rows: int = 0


@dataclass
class TestResult:
    """测试结果"""
    connection_type: str
    start_time: datetime
    end_time: datetime = None
    
    # 原有字段
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    downtime_periods: List[Dict] = field(default_factory=list)
    
    # 新增业务统计字段
    operations: List[BusinessOperation] = field(default_factory=list)
    read_operations: int = 0
    write_operations: int = 0
    transaction_operations: int = 0
    successful_reads: int = 0
    successful_writes: int = 0
    successful_transactions: int = 0
    
    @property
    def total_downtime(self) -> float:
        """计算总停机时间（秒）"""
        return sum(period['duration'] for period in self.downtime_periods)
    
    @property
    def success_rate(self) -> float:
        """计算成功率"""
        if self.total_attempts == 0:
            return 0.0
        return (self.successful_attempts / self.total_attempts) * 100
    
    @property
    def read_success_rate(self) -> float:
        """读操作成功率"""
        if self.read_operations == 0:
            return 0.0
        return (self.successful_reads / self.read_operations) * 100
    
    @property
    def write_success_rate(self) -> float:
        """写操作成功率"""
        if self.write_operations == 0:
            return 0.0
        return (self.successful_writes / self.write_operations) * 100
    
    @property
    def transaction_success_rate(self) -> float:
        """事务操作成功率"""
        if self.transaction_operations == 0:
            return 0.0
        return (self.successful_transactions / self.transaction_operations) * 100
    
    @property
    def average_response_time(self) -> float:
        """平均响应时间"""
        successful_ops = [op for op in self.operations if op.success and op.response_time]
        if not successful_ops:
            return 0.0
        return sum(op.response_time for op in successful_ops) / len(successful_ops)


class ConnectionTester:
    """连接测试器 - 业务场景测试"""
    
    def __init__(self, config: TestConfig, connection_type: str):
        self.config = config
        self.connection_type = connection_type
        self.db_config = config.get_config(connection_type, 'writer')
        self.is_running = False
        
        # 连接池管理
        self.connection_pool = []
        self.pool_size = getattr(config, 'connection_pool_size', 5)
        self.lock = threading.Lock()
        
        # 业务场景配置
        self.read_weight = getattr(config, 'read_weight', 70)
        self.write_weight = getattr(config, 'write_weight', 20)
        self.transaction_weight = getattr(config, 'transaction_weight', 10)
        
        # 操作间隔配置
        self.min_interval = getattr(config, 'min_operation_interval', 0.1)
        self.max_interval = getattr(config, 'max_operation_interval', 0.5)
        
    def _create_connection(self) -> psycopg2.extensions.connection:
        """创建数据库连接"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.username,
                password=self.db_config.password,
                connect_timeout=self.config.connection_timeout
            )
            conn.autocommit = False  # 业务场景需要事务控制
            return conn
        except Exception as e:
            raise ConnectionError(f"无法创建数据库连接: {e}")
    
    def _initialize_connection_pool(self):
        """初始化连接池"""
        print(f"[{self.connection_type}] 初始化连接池...")
        self.connection_pool = []
        for i in range(self.pool_size):
            try:
                conn = self._create_connection()
                self.connection_pool.append(conn)
            except Exception as e:
                print(f"[{self.connection_type}] 创建连接 {i+1} 失败: {e}")
    
    def _get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """从连接池获取连接"""
        with self.lock:
            for i, conn in enumerate(self.connection_pool):
                if conn and not conn.closed:
                    try:
                        # 测试连接是否可用
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                        return conn
                    except:
                        # 连接不可用，尝试重新创建
                        try:
                            conn.close()
                        except:
                            pass
                        try:
                            new_conn = self._create_connection()
                            self.connection_pool[i] = new_conn
                            return new_conn
                        except:
                            self.connection_pool[i] = None
            
            # 如果没有可用连接，尝试创建新连接
            try:
                return self._create_connection()
            except:
                return None
    
    def _setup_test_tables(self):
        """创建测试表"""
        conn = self._get_connection()
        if not conn:
            raise Exception("无法获取数据库连接来创建测试表")
        
        try:
            with conn.cursor() as cursor:
                # 创建用户表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS business_users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(50) UNIQUE NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP,
                        login_count INTEGER DEFAULT 0,
                        status VARCHAR(20) DEFAULT 'active'
                    )
                """)
                
                # 创建订单表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS business_orders (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES business_users(id),
                        order_number VARCHAR(50) UNIQUE NOT NULL,
                        amount DECIMAL(10,2) NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建产品表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS business_products (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        price DECIMAL(10,2) NOT NULL,
                        stock INTEGER DEFAULT 0,
                        category VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建日志表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS business_logs (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER,
                        action VARCHAR(50) NOT NULL,
                        details TEXT,
                        ip_address INET,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 插入一些初始数据
                cursor.execute("""
                    INSERT INTO business_users (username, email) 
                    SELECT 'user_' || generate_series(1, 100), 
                           'user_' || generate_series(1, 100) || '@example.com'
                    ON CONFLICT (username) DO NOTHING
                """)
                
                cursor.execute("""
                    INSERT INTO business_products (name, price, stock, category)
                    SELECT 'Product ' || generate_series(1, 50),
                           (random() * 1000)::decimal(10,2),
                           (random() * 100)::integer,
                           CASE (random() * 4)::integer 
                               WHEN 0 THEN 'Electronics'
                               WHEN 1 THEN 'Books'
                               WHEN 2 THEN 'Clothing'
                               ELSE 'Home'
                           END
                    ON CONFLICT DO NOTHING
                """)
                
                conn.commit()
                print(f"[{self.connection_type}] 测试表创建完成")
                
        except Exception as e:
            conn.rollback()
            raise Exception(f"创建测试表失败: {e}")
    
    def _execute_read_operation(self) -> BusinessOperation:
        """执行读操作"""
        operation_id = str(uuid.uuid4())[:8]
        operation = BusinessOperation(
            operation_id=operation_id,
            operation_type='read',
            start_time=datetime.now(timezone.utc)
        )
        
        conn = self._get_connection()
        if not conn:
            operation.end_time = datetime.now(timezone.utc)
            operation.error_message = "无法获取数据库连接"
            return operation
        
        try:
            start_time = time.time()
            
            with conn.cursor() as cursor:
                # 随机选择一种读操作
                read_type = random.choice([
                    'user_list',
                    'order_summary',
                    'product_search',
                    'user_orders',
                    'recent_logs'
                ])
                
                if read_type == 'user_list':
                    cursor.execute("""
                        SELECT id, username, email, last_login, login_count, status
                        FROM business_users 
                        WHERE status = 'active'
                        ORDER BY last_login DESC NULLS LAST
                        LIMIT 20
                    """)
                    
                elif read_type == 'order_summary':
                    cursor.execute("""
                        SELECT status, COUNT(*) as count, SUM(amount) as total_amount
                        FROM business_orders 
                        WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                        GROUP BY status
                    """)
                    
                elif read_type == 'product_search':
                    category = random.choice(['Electronics', 'Books', 'Clothing', 'Home'])
                    cursor.execute("""
                        SELECT id, name, price, stock, category
                        FROM business_products 
                        WHERE category = %s AND stock > 0
                        ORDER BY price
                        LIMIT 10
                    """, (category,))
                    
                elif read_type == 'user_orders':
                    user_id = random.randint(1, 100)
                    cursor.execute("""
                        SELECT o.id, o.order_number, o.amount, o.status, o.created_at
                        FROM business_orders o
                        JOIN business_users u ON o.user_id = u.id
                        WHERE u.id = %s
                        ORDER BY o.created_at DESC
                        LIMIT 10
                    """, (user_id,))
                    
                else:  # recent_logs
                    cursor.execute("""
                        SELECT l.action, l.details, l.created_at, u.username
                        FROM business_logs l
                        LEFT JOIN business_users u ON l.user_id = u.id
                        WHERE l.created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
                        ORDER BY l.created_at DESC
                        LIMIT 50
                    """)
                
                results = cursor.fetchall()
                operation.affected_rows = len(results)
            
            conn.commit()
            operation.response_time = time.time() - start_time
            operation.success = True
            
        except Exception as e:
            conn.rollback()
            operation.error_message = str(e)
            operation.response_time = time.time() - start_time if 'start_time' in locals() else 0
        
        operation.end_time = datetime.now(timezone.utc)
        return operation
    def _execute_write_operation(self) -> BusinessOperation:
        """执行写操作"""
        operation_id = str(uuid.uuid4())[:8]
        operation = BusinessOperation(
            operation_id=operation_id,
            operation_type='write',
            start_time=datetime.now(timezone.utc)
        )
        
        conn = self._get_connection()
        if not conn:
            operation.end_time = datetime.now(timezone.utc)
            operation.error_message = "无法获取数据库连接"
            return operation
        
        try:
            start_time = time.time()
            
            with conn.cursor() as cursor:
                # 随机选择一种写操作
                write_type = random.choice([
                    'update_user_login',
                    'create_order',
                    'update_product_stock',
                    'insert_log'
                ])
                
                if write_type == 'update_user_login':
                    user_id = random.randint(1, 100)
                    cursor.execute("""
                        UPDATE business_users 
                        SET last_login = CURRENT_TIMESTAMP,
                            login_count = login_count + 1
                        WHERE id = %s
                    """, (user_id,))
                    
                elif write_type == 'create_order':
                    user_id = random.randint(1, 100)
                    order_number = f"ORD-{int(time.time())}-{random.randint(1000, 9999)}"
                    amount = round(random.uniform(10.0, 1000.0), 2)
                    cursor.execute("""
                        INSERT INTO business_orders (user_id, order_number, amount, status)
                        VALUES (%s, %s, %s, 'pending')
                    """, (user_id, order_number, amount))
                    
                elif write_type == 'update_product_stock':
                    product_id = random.randint(1, 50)
                    stock_change = random.randint(-5, 10)
                    cursor.execute("""
                        UPDATE business_products 
                        SET stock = GREATEST(0, stock + %s)
                        WHERE id = %s
                    """, (stock_change, product_id))
                    
                else:  # insert_log
                    user_id = random.randint(1, 100)
                    action = random.choice(['login', 'logout', 'view_product', 'add_to_cart', 'checkout'])
                    details = f"User performed {action} action"
                    ip_address = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
                    cursor.execute("""
                        INSERT INTO business_logs (user_id, action, details, ip_address)
                        VALUES (%s, %s, %s, %s)
                    """, (user_id, action, details, ip_address))
                
                operation.affected_rows = cursor.rowcount
            
            conn.commit()
            operation.response_time = time.time() - start_time
            operation.success = True
            
        except Exception as e:
            conn.rollback()
            operation.error_message = str(e)
            operation.response_time = time.time() - start_time if 'start_time' in locals() else 0
        
        operation.end_time = datetime.now(timezone.utc)
        return operation
    
    def _execute_transaction_operation(self) -> BusinessOperation:
        """执行事务操作"""
        operation_id = str(uuid.uuid4())[:8]
        operation = BusinessOperation(
            operation_id=operation_id,
            operation_type='transaction',
            start_time=datetime.now(timezone.utc)
        )
        
        conn = self._get_connection()
        if not conn:
            operation.end_time = datetime.now(timezone.utc)
            operation.error_message = "无法获取数据库连接"
            return operation
        
        try:
            start_time = time.time()
            
            with conn.cursor() as cursor:
                # 模拟一个完整的业务事务：创建订单并更新库存
                user_id = random.randint(1, 100)
                product_id = random.randint(1, 50)
                quantity = random.randint(1, 5)
                
                # 1. 检查库存
                cursor.execute("""
                    SELECT stock, price FROM business_products WHERE id = %s FOR UPDATE
                """, (product_id,))
                result = cursor.fetchone()
                
                if not result:
                    raise Exception(f"Product {product_id} not found")
                
                stock, price = result
                if stock < quantity:
                    raise Exception(f"Insufficient stock: {stock} < {quantity}")
                
                # 2. 创建订单
                order_number = f"TXN-{int(time.time())}-{random.randint(1000, 9999)}"
                total_amount = price * quantity
                cursor.execute("""
                    INSERT INTO business_orders (user_id, order_number, amount, status)
                    VALUES (%s, %s, %s, 'confirmed')
                    RETURNING id
                """, (user_id, order_number, total_amount))
                order_id = cursor.fetchone()[0]
                
                # 3. 更新库存
                cursor.execute("""
                    UPDATE business_products 
                    SET stock = stock - %s
                    WHERE id = %s
                """, (quantity, product_id))
                
                # 4. 记录日志
                cursor.execute("""
                    INSERT INTO business_logs (user_id, action, details)
                    VALUES (%s, 'purchase', %s)
                """, (user_id, f"Purchased {quantity} units of product {product_id}, order {order_id}"))
                
                operation.affected_rows = 3  # 插入订单、更新库存、插入日志
            
            conn.commit()
            operation.response_time = time.time() - start_time
            operation.success = True
            
        except Exception as e:
            conn.rollback()
            operation.error_message = str(e)
            operation.response_time = time.time() - start_time if 'start_time' in locals() else 0
        
        operation.end_time = datetime.now(timezone.utc)
        return operation
    
    def _choose_operation_type(self) -> str:
        """根据权重选择操作类型"""
        rand = random.randint(1, 100)
        if rand <= self.read_weight:
            return 'read'
        elif rand <= self.read_weight + self.write_weight:
            return 'write'
        else:
            return 'transaction'
    
    def _execute_business_operation(self) -> BusinessOperation:
        """执行一个业务操作"""
        operation_type = self._choose_operation_type()
        
        if operation_type == 'read':
            return self._execute_read_operation()
        elif operation_type == 'write':
            return self._execute_write_operation()
        else:
            return self._execute_transaction_operation()
    def _detect_downtime(self, result: TestResult):
        """检测停机时间"""
        if not result.operations:
            return
        
        # 按时间排序操作
        operations = sorted(result.operations, key=lambda x: x.start_time)
        
        downtime_start = None
        consecutive_failures = 0
        failure_threshold = 3  # 连续3次失败认为是停机
        
        for op in operations:
            if not op.success:
                consecutive_failures += 1
                if consecutive_failures >= failure_threshold and downtime_start is None:
                    downtime_start = op.start_time
            else:
                if downtime_start is not None:
                    # 停机结束
                    downtime_duration = (op.start_time - downtime_start).total_seconds()
                    result.downtime_periods.append({
                        'start': downtime_start,
                        'end': op.start_time,
                        'duration': downtime_duration
                    })
                    downtime_start = None
                consecutive_failures = 0
        
        # 如果测试结束时仍在停机状态
        if downtime_start is not None:
            downtime_duration = (result.end_time - downtime_start).total_seconds()
            result.downtime_periods.append({
                'start': downtime_start,
                'end': result.end_time,
                'duration': downtime_duration
            })
    
    def run_test(self, duration: int, concurrent_workers: int = 3) -> TestResult:
        """
        运行业务场景测试
        
        Args:
            duration: 测试持续时间（秒）
            concurrent_workers: 并发工作线程数
        """
        print(f"开始 {self.connection_type} 业务场景测试")
        print(f"  持续时间: {duration}秒")
        print(f"  并发线程: {concurrent_workers}")
        print(f"  操作权重: 读{self.read_weight}% 写{self.write_weight}% 事务{self.transaction_weight}%")
        
        result = TestResult(
            connection_type=self.connection_type,
            start_time=datetime.now(timezone.utc)
        )
        
        self.is_running = True
        
        try:
            # 初始化连接池和测试表
            self._initialize_connection_pool()
            self._setup_test_tables()
            
            start_time = time.time()
            
            # 使用线程池执行并发操作
            with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
                futures = []
                
                while self.is_running and (time.time() - start_time) < duration:
                    # 提交新的操作任务
                    if len(futures) < concurrent_workers * 2:  # 保持任务队列
                        future = executor.submit(self._execute_business_operation)
                        futures.append(future)
                    
                    # 收集完成的任务
                    completed_futures = []
                    for future in futures:
                        if future.done():
                            try:
                                operation = future.result(timeout=0.1)
                                result.operations.append(operation)
                                result.total_attempts += 1
                                
                                # 更新统计信息
                                if operation.success:
                                    result.successful_attempts += 1
                                    if operation.operation_type == 'read':
                                        result.successful_reads += 1
                                    elif operation.operation_type == 'write':
                                        result.successful_writes += 1
                                    else:
                                        result.successful_transactions += 1
                                else:
                                    result.failed_attempts += 1
                                
                                # 按类型统计
                                if operation.operation_type == 'read':
                                    result.read_operations += 1
                                elif operation.operation_type == 'write':
                                    result.write_operations += 1
                                else:
                                    result.transaction_operations += 1
                                
                            except Exception as e:
                                print(f"[{self.connection_type}] 获取操作结果失败: {e}")
                            
                            completed_futures.append(future)
                    
                    # 移除已完成的任务
                    for future in completed_futures:
                        futures.remove(future)
                    
                    # 控制操作频率
                    time.sleep(random.uniform(self.min_interval, self.max_interval))
                    
                    # 每100个操作打印一次状态
                    if result.total_attempts > 0 and result.total_attempts % 100 == 0:
                        print(f"[{self.connection_type}] 已执行 {result.total_attempts} 个操作，成功率: {result.success_rate:.1f}%")
                
                # 等待剩余任务完成
                for future in futures:
                    try:
                        operation = future.result(timeout=5)
                        result.operations.append(operation)
                        result.total_attempts += 1
                        if operation.success:
                            result.successful_attempts += 1
                        else:
                            result.failed_attempts += 1
                    except Exception as e:
                        print(f"[{self.connection_type}] 等待任务完成失败: {e}")
        
        except KeyboardInterrupt:
            print(f"\n[{self.connection_type}] 测试被用户中断")
        
        except Exception as e:
            print(f"[{self.connection_type}] 测试过程中发生错误: {e}")
        
        finally:
            self.is_running = False
            result.end_time = datetime.now(timezone.utc)
            
            # 清理连接池
            for conn in self.connection_pool:
                if conn and not conn.closed:
                    try:
                        conn.close()
                    except:
                        pass
        
        # 检测停机时间
        self._detect_downtime(result)
        
        # 打印测试结果摘要
        print(f"\n[{self.connection_type}] 业务场景测试完成！")
        print(f"  总操作数: {result.total_attempts}")
        print(f"  成功操作: {result.successful_attempts}")
        print(f"  失败操作: {result.failed_attempts}")
        print(f"  总体成功率: {result.success_rate:.2f}%")
        print(f"  读操作: {result.read_operations} (成功率: {result.read_success_rate:.1f}%)")
        print(f"  写操作: {result.write_operations} (成功率: {result.write_success_rate:.1f}%)")
        print(f"  事务操作: {result.transaction_operations} (成功率: {result.transaction_success_rate:.1f}%)")
        print(f"  平均响应时间: {result.average_response_time:.3f}秒")
        print(f"  检测到的停机时间: {result.total_downtime:.3f}秒")
        
        return result
