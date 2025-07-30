"""
连接测试器核心模块
"""

import time
import psycopg2
import threading
from datetime import datetime, timezone
from typing import List, Dict, Optional
from dataclasses import dataclass, field

from .config import TestConfig


@dataclass
class TestEvent:
    """测试事件记录"""
    timestamp: datetime
    event_type: str  # 'success', 'failure', 'recovery'
    message: str = ""
    response_time: Optional[float] = None


@dataclass
class TestResult:
    """测试结果"""
    connection_type: str
    start_time: datetime
    end_time: datetime = None
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    events: List[TestEvent] = field(default_factory=list)
    downtime_periods: List[Dict] = field(default_factory=list)
    
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


class ConnectionTester:
    """连接测试器"""
    
    def __init__(self, config: TestConfig, connection_type: str):
        self.config = config
        self.connection_type = connection_type
        self.db_config = config.get_config(connection_type, 'writer')
        self.is_running = False
        self.current_connection = None
        
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
            conn.autocommit = True
            return conn
        except Exception as e:
            raise ConnectionError(f"无法创建数据库连接: {e}")
    
    def _execute_test_query(self, conn: psycopg2.extensions.connection) -> float:
        """执行测试查询并返回响应时间"""
        start_time = time.time()
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        
        return time.time() - start_time
    
    def _test_connection_once(self) -> tuple[bool, Optional[float], str]:
        """
        执行一次连接测试
        
        Returns:
            (是否成功, 响应时间, 错误信息)
        """
        try:
            # 如果没有连接或连接已关闭，创建新连接
            if self.current_connection is None or self.current_connection.closed:
                self.current_connection = self._create_connection()
            
            # 执行测试查询
            response_time = self._execute_test_query(self.current_connection)
            return True, response_time, ""
            
        except Exception as e:
            # 连接失败，清理当前连接
            if self.current_connection:
                try:
                    self.current_connection.close()
                except:
                    pass
                self.current_connection = None
            
            return False, None, str(e)
    
    def _wait_for_recovery(self, result: TestResult) -> datetime:
        """等待连接恢复"""
        print(f"[{self.connection_type}] 检测到连接失败，等待恢复...")
        
        while self.is_running:
            time.sleep(0.1)  # 100ms 间隔检查恢复
            
            success, response_time, error_msg = self._test_connection_once()
            if success:
                recovery_time = datetime.now(timezone.utc)
                result.events.append(TestEvent(
                    timestamp=recovery_time,
                    event_type='recovery',
                    message="连接已恢复",
                    response_time=response_time
                ))
                print(f"[{self.connection_type}] 连接已恢复！响应时间: {response_time:.3f}s")
                return recovery_time
        
        return datetime.now(timezone.utc)
    
    def run_test(self, duration: int, interval: float) -> TestResult:
        """
        运行故障转移测试
        
        Args:
            duration: 测试持续时间（秒）
            interval: 查询间隔（秒）
        """
        print(f"开始 {self.connection_type} 连接测试，持续时间: {duration}秒，查询间隔: {interval}秒")
        
        result = TestResult(
            connection_type=self.connection_type,
            start_time=datetime.now(timezone.utc)
        )
        
        self.is_running = True
        start_time = time.time()
        last_success_time = None
        in_downtime = False
        downtime_start = None
        
        try:
            while self.is_running and (time.time() - start_time) < duration:
                current_time = datetime.now(timezone.utc)
                
                # 执行连接测试
                success, response_time, error_msg = self._test_connection_once()
                result.total_attempts += 1
                
                if success:
                    result.successful_attempts += 1
                    last_success_time = current_time
                    
                    # 如果之前在停机状态，记录恢复
                    if in_downtime:
                        downtime_duration = (current_time - downtime_start).total_seconds()
                        result.downtime_periods.append({
                            'start': downtime_start,
                            'end': current_time,
                            'duration': downtime_duration
                        })
                        print(f"[{self.connection_type}] 停机时间: {downtime_duration:.3f}秒")
                        in_downtime = False
                    
                    result.events.append(TestEvent(
                        timestamp=current_time,
                        event_type='success',
                        response_time=response_time
                    ))
                    
                    if result.total_attempts % 50 == 0:  # 每50次打印一次状态
                        print(f"[{self.connection_type}] 测试进行中... 成功: {result.successful_attempts}, 失败: {result.failed_attempts}")
                
                else:
                    result.failed_attempts += 1
                    
                    # 如果不在停机状态，开始记录停机
                    if not in_downtime:
                        in_downtime = True
                        downtime_start = current_time
                        print(f"[{self.connection_type}] 检测到连接失败: {error_msg}")
                    
                    result.events.append(TestEvent(
                        timestamp=current_time,
                        event_type='failure',
                        message=error_msg
                    ))
                    
                    # 等待恢复
                    recovery_time = self._wait_for_recovery(result)
                    if recovery_time and in_downtime:
                        downtime_duration = (recovery_time - downtime_start).total_seconds()
                        result.downtime_periods.append({
                            'start': downtime_start,
                            'end': recovery_time,
                            'duration': downtime_duration
                        })
                        in_downtime = False
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print(f"\n[{self.connection_type}] 测试被用户中断")
        
        finally:
            self.is_running = False
            result.end_time = datetime.now(timezone.utc)
            
            # 清理连接
            if self.current_connection:
                try:
                    self.current_connection.close()
                except:
                    pass
        
        # 如果测试结束时仍在停机状态，记录最后的停机时间
        if in_downtime and downtime_start:
            downtime_duration = (result.end_time - downtime_start).total_seconds()
            result.downtime_periods.append({
                'start': downtime_start,
                'end': result.end_time,
                'duration': downtime_duration
            })
        
        print(f"[{self.connection_type}] 测试完成！")
        print(f"  总尝试次数: {result.total_attempts}")
        print(f"  成功次数: {result.successful_attempts}")
        print(f"  失败次数: {result.failed_attempts}")
        print(f"  成功率: {result.success_rate:.2f}%")
        print(f"  总停机时间: {result.total_downtime:.3f}秒")
        
        return result
