import threading
import time
import os
from datetime import datetime
from .connection_tester import ConnectionTester
from .pgbench_load_generator import PgbenchLoadGenerator, PgbenchConfig

class FailoverWithPgbenchTester:
    """集成的故障转移和 pgbench 负载测试器"""
    
    def __init__(self, config):
        self.config = config
        self.connection_tester = ConnectionTester(config)
        self.load_generator = PgbenchLoadGenerator(config.pgbench_config)
        self.results = {}
        self.failover_detected = False
        self.failover_start_time = None
        self.failover_end_time = None
    
    def run_test(self):
        """运行完整测试"""
        print("🎯 Aurora 故障转移 + pgbench 负载测试")
        print("=" * 50)
        
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
                import psycopg2
                conn = psycopg2.connect(
                    host=conn_config['host'],
                    port=conn_config['port'],
                    user=conn_config['user'],
                    password=conn_config.get('password', ''),
                    database=conn_config['database']
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
        
        # 启动故障转移监控线程
        failover_thread = threading.Thread(target=self._run_failover_monitoring)
        failover_thread.daemon = True
        failover_thread.start()
        
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
                
                # 如果检测到故障转移，显示额外信息
                if self.failover_detected:
                    if self.failover_end_time:
                        failover_duration = self.failover_end_time - self.failover_start_time
                        print(f"   🔄 故障转移已完成，耗时: {failover_duration:.2f}秒")
                    else:
                        failover_duration = current_time - self.failover_start_time
                        print(f"   🔄 故障转移进行中，已耗时: {failover_duration:.2f}秒")
                
                last_report_time = current_time
            
            time.sleep(1)
        
        print("\n✅ 主测试阶段完成")
    
    def _run_failover_monitoring(self):
        """运行故障转移监控"""
        try:
            # 使用现有的连接测试逻辑，但需要适配
            print("🔍 开始故障转移监控...")
            
            # 这里可以集成现有的 connection_tester 逻辑
            # 或者实现简化的故障转移检测
            self._simple_failover_detection()
            
        except Exception as e:
            print(f"❌ 故障转移监控出错: {e}")
    
    def _simple_failover_detection(self):
        """简化的故障转移检测"""
        import psycopg2
        
        # 监控连接状态变化
        last_status = {}
        check_interval = 0.5  # 500ms 检查间隔
        
        while True:
            current_status = {}
            
            for conn_type, conn_config in self.config.pgbench_config.connections.items():
                try:
                    conn = psycopg2.connect(
                        host=conn_config['host'],
                        port=conn_config['port'],
                        user=conn_config['user'],
                        password=conn_config.get('password', ''),
                        database=conn_config['database'],
                        connect_timeout=1
                    )
                    conn.close()
                    current_status[conn_type] = True
                except:
                    current_status[conn_type] = False
            
            # 检测状态变化
            if last_status:
                for conn_type in current_status:
                    if last_status.get(conn_type, True) and not current_status[conn_type]:
                        # 连接失败，可能是故障转移开始
                        if not self.failover_detected:
                            self.failover_detected = True
                            self.failover_start_time = time.time()
                            print(f"\n🚨 检测到 {conn_type} 连接失败，故障转移可能开始")
                    
                    elif not last_status.get(conn_type, False) and current_status[conn_type]:
                        # 连接恢复
                        if self.failover_detected and not self.failover_end_time:
                            self.failover_end_time = time.time()
                            duration = self.failover_end_time - self.failover_start_time
                            print(f"\n✅ 检测到 {conn_type} 连接恢复，故障转移完成，耗时: {duration:.2f}秒")
            
            last_status = current_status.copy()
            time.sleep(check_interval)
    
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
        
        # 故障转移信息
        if self.failover_detected:
            self.results['failover_info'] = {
                'detected': True,
                'start_time': self.failover_start_time,
                'end_time': self.failover_end_time,
                'duration': self.failover_end_time - self.failover_start_time if self.failover_end_time else None
            }
        else:
            self.results['failover_info'] = {'detected': False}
        
        # 分析故障转移对负载的影响
        self._analyze_failover_impact()
        
        # 生成报告
        self._generate_report()
    
    def _analyze_failover_impact(self):
        """分析故障转移对负载的影响"""
        if not self.failover_detected:
            print("   ℹ️ 未检测到故障转移事件")
            return
        
        print("   🔍 分析故障转移对负载的影响...")
        
        # 这里可以添加更详细的影响分析
        # 比如分析故障转移期间的 TPS 下降、延迟增加等
        
        impact_analysis = {
            'failover_detected': True,
            'impact_duration': self.results['failover_info'].get('duration', 0)
        }
        
        self.results['impact_analysis'] = impact_analysis
    
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
            
            # 故障转移信息
            f.write("故障转移信息:\n")
            failover_info = self.results['failover_info']
            if failover_info['detected']:
                f.write("  故障转移状态: 已检测到\n")
                if failover_info['start_time']:
                    start_time_str = datetime.fromtimestamp(failover_info['start_time']).strftime('%H:%M:%S.%f')[:-3]
                    f.write(f"  开始时间: {start_time_str}\n")
                if failover_info['end_time']:
                    end_time_str = datetime.fromtimestamp(failover_info['end_time']).strftime('%H:%M:%S.%f')[:-3]
                    f.write(f"  结束时间: {end_time_str}\n")
                    f.write(f"  持续时间: {failover_info['duration']:.3f}秒\n")
                else:
                    f.write("  结束时间: 未检测到恢复\n")
            else:
                f.write("  故障转移状态: 未检测到\n")
            f.write("\n")
            
            # 性能对比（如果有两种连接类型）
            if len(self.results['load_metrics']) == 2:
                f.write("性能对比分析:\n")
                direct_metrics = self.results['load_metrics'].get('direct', {})
                proxy_metrics = self.results['load_metrics'].get('proxy', {})
                
                if direct_metrics and proxy_metrics:
                    tps_improvement = ((proxy_metrics['avg_tps'] - direct_metrics['avg_tps']) / direct_metrics['avg_tps']) * 100
                    latency_change = ((proxy_metrics['avg_latency_ms'] - direct_metrics['avg_latency_ms']) / direct_metrics['avg_latency_ms']) * 100
                    
                    f.write(f"  TPS 变化: {tps_improvement:+.2f}%\n")
                    f.write(f"  延迟变化: {latency_change:+.2f}%\n")
                    
                    if tps_improvement > 0:
                        f.write("  结论: RDS 代理在负载测试中表现更好\n")
                    else:
                        f.write("  结论: 直接连接在负载测试中表现更好\n")
        
        print(f"✅ 测试报告已保存: {filename}")
        
        # 在控制台显示简要结果
        self._print_summary()
    
    def _print_summary(self):
        """打印测试摘要"""
        print("\n📋 测试摘要")
        print("-" * 20)
        
        for conn_type, metrics in self.results['load_metrics'].items():
            print(f"{conn_type} 连接:")
            print(f"  平均 TPS: {metrics['avg_tps']:.2f}")
            print(f"  平均延迟: {metrics['avg_latency_ms']:.2f}ms")
            print(f"  错误数量: {metrics['error_count']}")
        
        if self.results['failover_info']['detected']:
            duration = self.results['failover_info'].get('duration')
            if duration:
                print(f"\n故障转移耗时: {duration:.3f}秒")
            else:
                print("\n故障转移: 检测到开始，但未检测到完成")
        else:
            print("\n故障转移: 未检测到")
    
    def _cleanup(self):
        """清理资源"""
        print("\n🧹 清理资源...")
        self.load_generator.stop_load_generation()
        print("✅ 清理完成")
