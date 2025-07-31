import threading
import time
import os
import psycopg2
from datetime import datetime, timezone
from .connection_tester import ConnectionTester, TestResult
from .pgbench_load_generator import PgbenchLoadGenerator, PgbenchConfig
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class DowntimeRecord:
    """停机时间记录"""
    connection_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    
    def finalize(self, end_time: datetime):
        """完成停机记录"""
        self.end_time = end_time
        self.duration = (end_time - self.start_time).total_seconds()

class FailoverTester:
    """故障转移测试器，能够精确监控每种连接类型的downtime"""
    
    def __init__(self, config):
        self.config = config
        self.connection_testers = {}
        self.downtime_monitors = {}
        self.downtime_records = {'direct': [], 'proxy': []}
        
        # 根据测试模式创建相应的连接测试器
        if config.mode in ['direct', 'both']:
            self.connection_testers['direct'] = ConnectionTester(config, 'direct')
        if config.mode in ['proxy', 'both']:
            self.connection_testers['proxy'] = ConnectionTester(config, 'proxy')
            
        self.load_generator = PgbenchLoadGenerator(config.pgbench_config)
        self.results = {}
        self.test_running = False
        self.monitor_threads = {}
    
    def run_test(self):
        """运行完整测试"""
        print("🎯 Aurora 故障转移 + pgbench 负载测试")
        print("=" * 60)
        
        try:
            # 1. 准备阶段
            self._prepare_phase()
            
            # 2. 预热阶段
            self._warmup_phase()
            
            # 3. 正式测试阶段
            self._main_test_phase()
            
            # 4. 结果分析
            self._analyze_results()
            
        except KeyboardInterrupt:
            print("\n⚠️ 测试被用户中断")
        except Exception as e:
            print(f"\n❌ 测试过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理资源
            self._cleanup()
    
    def _prepare_phase(self):
        """准备阶段"""
        print("\n📋 准备阶段")
        print("-" * 20)
        
        # 确保结果目录存在
        os.makedirs('results', exist_ok=True)
        
        # 准备 pgbench 测试数据
        self.load_generator.prepare_database()
        
        # 验证连接
        print("🔍 验证数据库连接...")
        self._verify_connections()
        
        print("✅ 准备阶段完成")
    
    def _verify_connections(self):
        """验证数据库连接"""
        for conn_type, conn_config in self.config.pgbench_config.connections.items():
            try:
                conn = psycopg2.connect(
                    host=conn_config['host'],
                    port=conn_config['port'],
                    user=conn_config['user'],
                    password=conn_config.get('password', ''),
                    database=conn_config['database'],
                    connect_timeout=2
                )
                conn.close()
                print(f"   ✅ {conn_type} 连接验证成功")
            except Exception as e:
                print(f"   ❌ {conn_type} 连接验证失败: {e}")
                raise
    
    def _warmup_phase(self):
        """预热阶段"""
        print(f"\n🔥 预热阶段 ({self.config.pgbench_config.warmup_time}秒)")
        print("-" * 20)
        
        # 启动负载生成
        self.load_generator.start_load_generation()
        
        # 等待预热完成
        warmup_start = time.time()
        last_report_time = warmup_start
        
        while time.time() - warmup_start < self.config.pgbench_config.warmup_time:
            current_time = time.time()
            
            # 每10秒报告一次预热状态
            if current_time - last_report_time >= 10:
                elapsed = int(current_time - warmup_start)
                remaining = self.config.pgbench_config.warmup_time - elapsed
                metrics = self.load_generator.get_current_metrics()
                
                print(f"   预热中... {elapsed}s/{self.config.pgbench_config.warmup_time}s (剩余 {remaining}s)")
                self._print_current_metrics(metrics, indent="     ")
                last_report_time = current_time
            
            time.sleep(1)
        
        print("✅ 预热阶段完成，开始正式测试")
    
    def _main_test_phase(self):
        """主测试阶段"""
        print(f"\n🚀 主测试阶段 ({self.config.duration}秒)")
        print("-" * 20)
        print("💡 请在另一个终端手动触发故障转移:")
        print("   aws rds failover-db-cluster --db-cluster-identifier ards-with-rdsproxy --region ap-southeast-1")
        print()
        
        self.test_running = True
        
        # 启动每种连接类型的独立downtime监控线程
        for conn_type in self.config.pgbench_config.connections.keys():
            monitor_thread = threading.Thread(
                target=self._monitor_connection_downtime,
                args=(conn_type,)
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            self.monitor_threads[conn_type] = monitor_thread
        
        # 主循环：监控负载性能
        start_time = time.time()
        last_report_time = start_time
        
        while time.time() - start_time < self.config.duration:
            current_time = time.time()
            
            # 每5秒报告一次性能
            if current_time - last_report_time >= 5:
                metrics = self.load_generator.get_current_metrics()
                elapsed = int(current_time - start_time)
                remaining = self.config.duration - elapsed
                
                print(f"\n⏱️  测试进行中... ({elapsed}s/{self.config.duration}s, 剩余 {remaining}s)")
                self._print_current_metrics(metrics)
                
                # 显示当前的downtime状态
                self._print_downtime_status()
                
                last_report_time = current_time
            
            time.sleep(1)
        
        self.test_running = False
        print("\n✅ 主测试阶段完成")
    
    def _monitor_connection_downtime(self, conn_type: str):
        """监控特定连接类型的downtime"""
        print(f"🔍 开始监控 {conn_type} 连接的downtime...")
        
        conn_config = self.config.pgbench_config.connections[conn_type]
        current_downtime = None
        check_interval = 0.1  # 100ms检查间隔
        
        while self.test_running:
            try:
                # 尝试连接
                conn = psycopg2.connect(
                    host=conn_config['host'],
                    port=conn_config['port'],
                    user=conn_config['user'],
                    password=conn_config.get('password', ''),
                    database=conn_config['database'],
                    connect_timeout=1
                )
                
                # 执行简单查询
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                
                conn.close()
                
                # 连接成功
                if current_downtime is not None:
                    # 结束当前的downtime记录
                    current_downtime.finalize(datetime.now(timezone.utc))
                    self.downtime_records[conn_type].append(current_downtime)
                    print(f"   ✅ {conn_type} 连接恢复，downtime: {current_downtime.duration:.3f}秒")
                    current_downtime = None
                
            except Exception as e:
                # 连接失败
                if current_downtime is None:
                    # 开始新的downtime记录
                    current_downtime = DowntimeRecord(
                        connection_type=conn_type,
                        start_time=datetime.now(timezone.utc)
                    )
                    print(f"   🚨 {conn_type} 连接失败，开始记录downtime: {e}")
            
            time.sleep(check_interval)
        
        # 测试结束时，如果还有未完成的downtime记录，完成它
        if current_downtime is not None:
            current_downtime.finalize(datetime.now(timezone.utc))
            self.downtime_records[conn_type].append(current_downtime)
            print(f"   ⚠️ 测试结束时 {conn_type} 仍在downtime，总时长: {current_downtime.duration:.3f}秒")
    
    def _print_current_metrics(self, metrics: dict, indent: str = "   "):
        """打印当前性能指标"""
        for conn_type, data in metrics.items():
            if data['sample_count'] > 0:
                print(f"{indent}{conn_type:>6}: TPS={data['avg_tps']:>7.1f} "
                      f"(max:{data['max_tps']:>7.1f}), "
                      f"延迟={data['avg_latency_ms']:>6.2f}ms "
                      f"(max:{data['max_latency_ms']:>6.2f}ms), "
                      f"错误={data['error_count']:>3d}")
            else:
                print(f"{indent}{conn_type:>6}: 等待数据...")
    
    def _print_downtime_status(self):
        """打印当前downtime状态"""
        for conn_type, records in self.downtime_records.items():
            if records:
                total_downtime = sum(record.duration for record in records if record.duration)
                active_downtime = len([r for r in records if r.end_time is None])
                print(f"   📊 {conn_type} downtime: 总计 {total_downtime:.3f}秒 "
                      f"({len(records)}次中断, {active_downtime}次进行中)")
    
    def _analyze_results(self):
        """分析结果"""
        print("\n📊 结果分析")
        print("-" * 20)
        
        # 获取最终的负载指标
        final_metrics = self.load_generator.get_current_metrics()
        self.results['load_metrics'] = final_metrics
        
        # 获取详细指标
        detailed_metrics = self.load_generator.get_detailed_metrics()
        self.results['detailed_metrics'] = detailed_metrics
        
        # 整理downtime信息
        self.results['downtime_analysis'] = self._analyze_downtime()
        
        # 生成报告
        self._generate_report()
    
    def _analyze_downtime(self) -> Dict:
        """分析downtime数据"""
        analysis = {}
        
        for conn_type, records in self.downtime_records.items():
            if records:
                durations = [r.duration for r in records if r.duration is not None]
                analysis[conn_type] = {
                    'total_downtime': sum(durations),
                    'downtime_count': len(records),
                    'avg_downtime': sum(durations) / len(durations) if durations else 0,
                    'max_downtime': max(durations) if durations else 0,
                    'min_downtime': min(durations) if durations else 0,
                    'records': [
                        {
                            'start': r.start_time.strftime('%H:%M:%S.%f')[:-3],
                            'end': r.end_time.strftime('%H:%M:%S.%f')[:-3] if r.end_time else 'N/A',
                            'duration': r.duration
                        }
                        for r in records
                    ]
                }
            else:
                analysis[conn_type] = {
                    'total_downtime': 0,
                    'downtime_count': 0,
                    'avg_downtime': 0,
                    'max_downtime': 0,
                    'min_downtime': 0,
                    'records': []
                }
        
        return analysis
    
    def _generate_report(self):
        """生成测试报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"results/pgbench_failover_report_{timestamp}.txt"
        
        print(f"📄 生成测试报告: {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("Aurora PostgreSQL 故障转移 + pgbench 负载测试报告\n")
            f.write("=" * 60 + "\n\n")
            
            # 测试时间
            f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 测试配置
            f.write("测试配置:\n")
            f.write(f"  测试时长: {self.config.duration}秒\n")
            f.write(f"  pgbench 客户端数: {self.config.pgbench_config.clients}\n")
            f.write(f"  pgbench 作业数: {self.config.pgbench_config.jobs}\n")
            f.write(f"  数据规模因子: {self.config.pgbench_config.scale_factor}\n")
            f.write(f"  测试模式: {self.config.pgbench_config.mode}\n")
            f.write(f"  预热时间: {self.config.pgbench_config.warmup_time}秒\n\n")
            
            # 负载性能结果
            f.write("负载性能结果:\n")
            for conn_type, metrics in self.results['load_metrics'].items():
                f.write(f"  {conn_type} 连接:\n")
                f.write(f"    平均 TPS: {metrics['avg_tps']:.2f}\n")
                f.write(f"    最大 TPS: {metrics['max_tps']:.2f}\n")
                f.write(f"    最小 TPS: {metrics['min_tps']:.2f}\n")
                f.write(f"    平均延迟: {metrics['avg_latency_ms']:.2f}ms\n")
                f.write(f"    最大延迟: {metrics['max_latency_ms']:.2f}ms\n")
                f.write(f"    最小延迟: {metrics['min_latency_ms']:.2f}ms\n")
                f.write(f"    错误数量: {metrics['error_count']}\n")
                f.write(f"    采样数量: {metrics['sample_count']}\n\n")
            
            # 详细的downtime分析
            f.write("故障转移 Downtime 分析:\n")
            downtime_analysis = self.results['downtime_analysis']
            
            for conn_type, analysis in downtime_analysis.items():
                f.write(f"  {conn_type} 连接:\n")
                f.write(f"    总 downtime: {analysis['total_downtime']:.3f}秒\n")
                f.write(f"    中断次数: {analysis['downtime_count']}\n")
                if analysis['downtime_count'] > 0:
                    f.write(f"    平均 downtime: {analysis['avg_downtime']:.3f}秒\n")
                    f.write(f"    最长 downtime: {analysis['max_downtime']:.3f}秒\n")
                    f.write(f"    最短 downtime: {analysis['min_downtime']:.3f}秒\n")
                    f.write("    详细记录:\n")
                    for i, record in enumerate(analysis['records'], 1):
                        f.write(f"      #{i}: {record['start']} - {record['end']} "
                               f"({record['duration']:.3f}秒)\n")
                else:
                    f.write("    无中断记录\n")
                f.write("\n")
            
            # 对比分析
            if len(downtime_analysis) == 2:
                f.write("Downtime 对比分析:\n")
                direct_downtime = downtime_analysis.get('direct', {}).get('total_downtime', 0)
                proxy_downtime = downtime_analysis.get('proxy', {}).get('total_downtime', 0)
                
                f.write(f"  Direct 连接总 downtime: {direct_downtime:.3f}秒\n")
                f.write(f"  Proxy 连接总 downtime: {proxy_downtime:.3f}秒\n")
                
                if direct_downtime > 0 and proxy_downtime > 0:
                    improvement = ((direct_downtime - proxy_downtime) / direct_downtime) * 100
                    f.write(f"  Proxy 相对 Direct 的改善: {improvement:+.1f}%\n")
                    
                    if improvement > 0:
                        f.write("  结论: RDS Proxy 在故障转移时表现更好，downtime 更短\n")
                    else:
                        f.write("  结论: Direct 连接在故障转移时表现更好，downtime 更短\n")
                elif direct_downtime == 0 and proxy_downtime == 0:
                    f.write("  结论: 两种连接方式都没有检测到 downtime\n")
                elif direct_downtime == 0:
                    f.write("  结论: Direct 连接没有 downtime，Proxy 连接有 downtime\n")
                elif proxy_downtime == 0:
                    f.write("  结论: Proxy 连接没有 downtime，Direct 连接有 downtime\n")
            
            # 性能对比（如果有两种连接类型）
            if len(self.results['load_metrics']) == 2:
                f.write("\n负载性能对比分析:\n")
                direct_metrics = self.results['load_metrics'].get('direct', {})
                proxy_metrics = self.results['load_metrics'].get('proxy', {})
                
                if direct_metrics and proxy_metrics:
                    tps_improvement = ((proxy_metrics['avg_tps'] - direct_metrics['avg_tps']) / direct_metrics['avg_tps']) * 100
                    latency_change = ((proxy_metrics['avg_latency_ms'] - direct_metrics['avg_latency_ms']) / direct_metrics['avg_latency_ms']) * 100
                    
                    f.write(f"  TPS 变化 (Proxy vs Direct): {tps_improvement:+.2f}%\n")
                    f.write(f"  延迟变化 (Proxy vs Direct): {latency_change:+.2f}%\n")
        
        print(f"✅ 增强版测试报告已保存: {filename}")
        
        # 在控制台显示简要结果
        self._print_enhanced_summary()
    
    def _print_enhanced_summary(self):
        """打印增强版测试摘要"""
        print("\n📋 测试摘要")
        print("-" * 30)
        
        # 负载性能摘要
        for conn_type, metrics in self.results['load_metrics'].items():
            print(f"{conn_type} 连接:")
            print(f"  平均 TPS: {metrics['avg_tps']:.2f}")
            print(f"  平均延迟: {metrics['avg_latency_ms']:.2f}ms")
            print(f"  错误数量: {metrics['error_count']}")
        
        # Downtime摘要
        print("\nDowntime 摘要:")
        downtime_analysis = self.results['downtime_analysis']
        for conn_type, analysis in downtime_analysis.items():
            print(f"  {conn_type}: {analysis['total_downtime']:.3f}秒 "
                  f"({analysis['downtime_count']}次中断)")
    
    def _cleanup(self):
        """清理资源"""
        print("\n🧹 清理资源...")
        self.test_running = False
        self.load_generator.stop_load_generation()
        print("✅ 清理完成")
