#!/bin/bash

# Aurora PostgreSQL 故障转移测试脚本
# 使用增强版测试器，能够精确监控每种连接类型的downtime

echo "🎯 Aurora PostgreSQL 故障转移测试"
echo "=================================="

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "❌ 虚拟环境不存在，请先运行 python -m venv .venv"
    exit 1
fi

# 激活虚拟环境
source .venv/bin/activate

# 检查依赖
echo "🔍 检查依赖..."
pip install -r requirements.txt > /dev/null 2>&1

# 确保结果目录存在
mkdir -p results

# 显示测试配置
echo ""
echo "📋 测试配置:"
echo "  - 测试时长: 300秒 (5分钟)"
echo "  - pgbench 客户端数: 10"
echo "  - pgbench 作业数: 2"
echo "  - 数据规模因子: 10"
echo "  - 预热时间: 60秒"
echo "  - 测试模式: both (direct + proxy)"
echo "  - 详细日志: 启用"
echo ""

echo "💡 请在测试开始后手动触发故障转移:"
echo "   aws rds failover-db-cluster --db-cluster-identifier ards-with-rdsproxy --region ap-southeast-1"
echo ""

echo "🚀 启动故障转移测试（详细日志模式）..."
echo "========================================="

# 运行测试，启用详细日志
python main.py \
    --enable-pgbench \
    --mode both \
    --duration 300 \
    --pgbench-clients 10 \
    --pgbench-jobs 2 \
    --pgbench-scale 10 \
    --warmup-time 60 \
    --verbose

echo ""
echo "✅ 测试完成！"
echo ""
echo "📄 查看结果文件:"
echo "   - pgbench 报告: results/pgbench_failover_report_*.txt"
echo "   - 详细日志: results/test_log_*.log"
echo ""

# 显示最新的日志文件
latest_log=$(ls -t results/test_log_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
    echo "📋 最新日志文件: $latest_log"
    echo "   查看日志: tail -f $latest_log"
fi

latest_report=$(ls -t results/pgbench_failover_report_*.txt 2>/dev/null | head -1)
if [ -n "$latest_report" ]; then
    echo "📊 最新报告文件: $latest_report"
    echo "   查看报告: cat $latest_report"
fi
