"""
结果报告模块
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from .connection_tester import TestResult


class Reporter:
    """测试结果报告器"""
    
    def __init__(self, results_dir: str = "results"):
        self.results_dir = results_dir
        self._ensure_results_dir()
        self.results: Dict[str, TestResult] = {}
    
    def _ensure_results_dir(self):
        """确保结果目录存在"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
    
    def save_result(self, test_type: str, result: TestResult):
        """保存测试结果"""
        self.results[test_type] = result
        
        # 保存详细结果到JSON文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.results_dir}/{test_type}_result_{timestamp}.json"
        
        result_data = {
            'connection_type': result.connection_type,
            'start_time': result.start_time.isoformat(),
            'end_time': result.end_time.isoformat(),
            'total_attempts': result.total_attempts,
            'successful_attempts': result.successful_attempts,
            'failed_attempts': result.failed_attempts,
            'success_rate': result.success_rate,
            'total_downtime': result.total_downtime,
            'downtime_periods': [
                {
                    'start': period['start'].isoformat(),
                    'end': period['end'].isoformat(),
                    'duration': period['duration']
                }
                for period in result.downtime_periods
            ],
            # 业务操作统计
            'read_operations': result.read_operations,
            'write_operations': result.write_operations,
            'transaction_operations': result.transaction_operations,
            'successful_reads': result.successful_reads,
            'successful_writes': result.successful_writes,
            'successful_transactions': result.successful_transactions,
            'read_success_rate': result.read_success_rate,
            'write_success_rate': result.write_success_rate,
            'transaction_success_rate': result.transaction_success_rate,
            'average_response_time': result.average_response_time,
            # 详细操作记录
            'operations': [
                {
                    'operation_id': op.operation_id,
                    'operation_type': op.operation_type,
                    'start_time': op.start_time.isoformat(),
                    'end_time': op.end_time.isoformat() if op.end_time else None,
                    'success': op.success,
                    'error_message': op.error_message,
                    'response_time': op.response_time,
                    'affected_rows': op.affected_rows
                }
                for op in result.operations
            ]
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False)
        
        print(f"详细结果已保存到: {filename}")
    
    def generate_comparison_report(self):
        """生成对比报告"""
        if 'direct' not in self.results or 'proxy' not in self.results:
            print("警告: 需要同时有直接连接和代理连接的测试结果才能生成对比报告")
            return
        
        direct_result = self.results['direct']
        proxy_result = self.results['proxy']
        
        # 生成对比报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"{self.results_dir}/business_comparison_report_{timestamp}.txt"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write("Aurora PostgreSQL 业务场景故障转移测试对比报告\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 测试配置信息
            f.write("测试配置\n")
            f.write("-" * 20 + "\n")
            test_duration = (direct_result.end_time - direct_result.start_time).total_seconds()
            f.write(f"测试持续时间: {test_duration:.0f}秒\n")
            f.write(f"测试开始时间: {direct_result.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"测试结束时间: {direct_result.end_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 基本统计信息
            f.write("基本统计信息\n")
            f.write("-" * 20 + "\n")
            f.write(f"{'指标':<20} {'直接连接':<15} {'代理连接':<15} {'差异':<15}\n")
            f.write("-" * 70 + "\n")
            
            f.write(f"{'总操作数':<20} {direct_result.total_attempts:<15} {proxy_result.total_attempts:<15} {proxy_result.total_attempts - direct_result.total_attempts:<15}\n")
            f.write(f"{'成功操作':<20} {direct_result.successful_attempts:<15} {proxy_result.successful_attempts:<15} {proxy_result.successful_attempts - direct_result.successful_attempts:<15}\n")
            f.write(f"{'失败操作':<20} {direct_result.failed_attempts:<15} {proxy_result.failed_attempts:<15} {proxy_result.failed_attempts - direct_result.failed_attempts:<15}\n")
            f.write(f"{'总体成功率(%)':<20} {direct_result.success_rate:<15.2f} {proxy_result.success_rate:<15.2f} {proxy_result.success_rate - direct_result.success_rate:<15.2f}\n\n")
            
            # 按操作类型统计
            f.write("按操作类型统计\n")
            f.write("-" * 20 + "\n")
            f.write(f"{'操作类型':<15} {'直接连接':<25} {'代理连接':<25} {'成功率差异':<15}\n")
            f.write("-" * 80 + "\n")
            
            f.write(f"{'读操作':<15} {direct_result.read_operations}({direct_result.read_success_rate:.1f}%)<{'':<10} {proxy_result.read_operations}({proxy_result.read_success_rate:.1f}%)<{'':<10} {proxy_result.read_success_rate - direct_result.read_success_rate:<15.1f}\n")
            f.write(f"{'写操作':<15} {direct_result.write_operations}({direct_result.write_success_rate:.1f}%)<{'':<10} {proxy_result.write_operations}({proxy_result.write_success_rate:.1f}%)<{'':<10} {proxy_result.write_success_rate - direct_result.write_success_rate:<15.1f}\n")
            f.write(f"{'事务操作':<15} {direct_result.transaction_operations}({direct_result.transaction_success_rate:.1f}%)<{'':<10} {proxy_result.transaction_operations}({proxy_result.transaction_success_rate:.1f}%)<{'':<10} {proxy_result.transaction_success_rate - direct_result.transaction_success_rate:<15.1f}\n\n")
            
            # 性能指标
            f.write("性能指标\n")
            f.write("-" * 20 + "\n")
            f.write(f"{'指标':<20} {'直接连接':<15} {'代理连接':<15} {'差异':<15}\n")
            f.write("-" * 70 + "\n")
            
            f.write(f"{'平均响应时间(秒)':<20} {direct_result.average_response_time:<15.3f} {proxy_result.average_response_time:<15.3f} {proxy_result.average_response_time - direct_result.average_response_time:<15.3f}\n")
            f.write(f"{'总停机时间(秒)':<20} {direct_result.total_downtime:<15.3f} {proxy_result.total_downtime:<15.3f} {direct_result.total_downtime - proxy_result.total_downtime:<15.3f}\n")
            f.write(f"{'停机次数':<20} {len(direct_result.downtime_periods):<15} {len(proxy_result.downtime_periods):<15} {len(direct_result.downtime_periods) - len(proxy_result.downtime_periods):<15}\n\n")
            
            # 性能改善分析
            if direct_result.total_downtime > 0:
                downtime_improvement = ((direct_result.total_downtime - proxy_result.total_downtime) / direct_result.total_downtime) * 100
                f.write(f"性能改善分析\n")
                f.write("-" * 20 + "\n")
                f.write(f"RDS 代理停机时间减少: {downtime_improvement:.2f}%\n")
                
                if proxy_result.average_response_time > 0 and direct_result.average_response_time > 0:
                    response_time_change = ((proxy_result.average_response_time - direct_result.average_response_time) / direct_result.average_response_time) * 100
                    f.write(f"RDS 代理响应时间变化: {response_time_change:+.2f}%\n")
                
                success_rate_improvement = proxy_result.success_rate - direct_result.success_rate
                f.write(f"RDS 代理成功率提升: {success_rate_improvement:+.2f}%\n\n")
                
                f.write("结论:\n")
                if downtime_improvement > 5:
                    f.write("✅ RDS 代理显著减少了故障转移停机时间\n")
                elif downtime_improvement > 0:
                    f.write("✅ RDS 代理减少了故障转移停机时间\n")
                elif downtime_improvement < -5:
                    f.write("❌ RDS 代理显著增加了故障转移停机时间\n")
                else:
                    f.write("➖ RDS 代理对故障转移停机时间影响较小\n")
            
            # 详细停机时间记录
            f.write(f"\n详细停机时间记录\n")
            f.write("-" * 30 + "\n")
            
            f.write("直接连接停机记录:\n")
            if direct_result.downtime_periods:
                for i, period in enumerate(direct_result.downtime_periods, 1):
                    f.write(f"  {i}. {period['start'].strftime('%H:%M:%S')} - {period['end'].strftime('%H:%M:%S')} (持续 {period['duration']:.3f}秒)\n")
            else:
                f.write("  无停机记录\n")
            
            f.write("\n代理连接停机记录:\n")
            if proxy_result.downtime_periods:
                for i, period in enumerate(proxy_result.downtime_periods, 1):
                    f.write(f"  {i}. {period['start'].strftime('%H:%M:%S')} - {period['end'].strftime('%H:%M:%S')} (持续 {period['duration']:.3f}秒)\n")
            else:
                f.write("  无停机记录\n")
        
        print(f"业务场景对比报告已保存到: {report_filename}")
        
        # 在控制台也显示简要对比
        print("\n" + "=" * 60)
        print("业务场景测试结果对比")
        print("=" * 60)
        print(f"直接连接:")
        print(f"  总操作数: {direct_result.total_attempts}")
        print(f"  总体成功率: {direct_result.success_rate:.2f}%")
        print(f"  读操作: {direct_result.read_operations} (成功率: {direct_result.read_success_rate:.1f}%)")
        print(f"  写操作: {direct_result.write_operations} (成功率: {direct_result.write_success_rate:.1f}%)")
        print(f"  事务操作: {direct_result.transaction_operations} (成功率: {direct_result.transaction_success_rate:.1f}%)")
        print(f"  平均响应时间: {direct_result.average_response_time:.3f}秒")
        print(f"  总停机时间: {direct_result.total_downtime:.3f}秒")
        
        print(f"\n代理连接:")
        print(f"  总操作数: {proxy_result.total_attempts}")
        print(f"  总体成功率: {proxy_result.success_rate:.2f}%")
        print(f"  读操作: {proxy_result.read_operations} (成功率: {proxy_result.read_success_rate:.1f}%)")
        print(f"  写操作: {proxy_result.write_operations} (成功率: {proxy_result.write_success_rate:.1f}%)")
        print(f"  事务操作: {proxy_result.transaction_operations} (成功率: {proxy_result.transaction_success_rate:.1f}%)")
        print(f"  平均响应时间: {proxy_result.average_response_time:.3f}秒")
        print(f"  总停机时间: {proxy_result.total_downtime:.3f}秒")
        
        print(f"\n性能对比:")
        if direct_result.total_downtime > 0:
            downtime_improvement = ((direct_result.total_downtime - proxy_result.total_downtime) / direct_result.total_downtime) * 100
            print(f"  停机时间减少: {downtime_improvement:.2f}%")
        
        success_rate_improvement = proxy_result.success_rate - direct_result.success_rate
        print(f"  成功率提升: {success_rate_improvement:+.2f}%")
        
        if proxy_result.average_response_time > 0 and direct_result.average_response_time > 0:
            response_time_change = ((proxy_result.average_response_time - direct_result.average_response_time) / direct_result.average_response_time) * 100
            print(f"  响应时间变化: {response_time_change:+.2f}%")
        
        print("=" * 60)
