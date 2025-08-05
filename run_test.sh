#!/bin/bash

# Aurora PostgreSQL 故障转移测试脚本
# 使用增强版测试器，能够精确监控每种连接类型的downtime

echo "🎯 Aurora PostgreSQL 故障转移测试"
echo "=================================="

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --mode MODE                测试模式 (direct|proxy|both，默认: both)"
    echo "  --duration SECONDS         测试时长，秒 (默认: 300)"
    echo "  --concurrent-workers NUM   业务场景并发工作线程数 (默认: 3)"
    echo "  --enable-pgbench          启用 pgbench 负载测试"
    echo "  --pgbench-clients NUM     pgbench 客户端数 (默认: 10)"
    echo "  --pgbench-jobs NUM        pgbench 作业数 (默认: 2)"
    echo "  --pgbench-scale NUM       pgbench 数据规模因子 (默认: 10)"
    echo "  --warmup-time SECONDS     预热时间，秒 (默认: 60)"
    echo "  --verbose                 启用详细日志"
    echo "  --help, -h                显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                                           # 使用默认配置"
    echo "  $0 --enable-pgbench                         # 启用 pgbench 负载测试"
    echo "  $0 --concurrent-workers 5 --enable-pgbench  # 业务场景 + pgbench 组合测试"
    echo "  $0 --mode direct --duration 180             # 仅测试直接连接 3 分钟"
    echo ""
}

# 默认参数
MODE="both"
DURATION="300"
CONCURRENT_WORKERS=""
ENABLE_PGBENCH=""
PGBENCH_CLIENTS="10"
PGBENCH_JOBS="2"
PGBENCH_SCALE="10"
WARMUP_TIME="60"
VERBOSE="--verbose"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --concurrent-workers)
            CONCURRENT_WORKERS="--concurrent-workers $2"
            shift 2
            ;;
        --enable-pgbench)
            ENABLE_PGBENCH="--enable-pgbench"
            shift
            ;;
        --pgbench-clients)
            PGBENCH_CLIENTS="$2"
            shift 2
            ;;
        --pgbench-jobs)
            PGBENCH_JOBS="$2"
            shift 2
            ;;
        --pgbench-scale)
            PGBENCH_SCALE="$2"
            shift 2
            ;;
        --warmup-time)
            WARMUP_TIME="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "❌ 未知参数: $1"
            echo "使用 --help 查看帮助信息"
            exit 1
            ;;
    esac
done

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
echo "  - 测试模式: $MODE"
echo "  - 测试时长: ${DURATION}秒 ($((DURATION/60))分钟)"
if [[ -n "$CONCURRENT_WORKERS" ]]; then
    echo "  - 业务场景并发线程: $(echo $CONCURRENT_WORKERS | cut -d' ' -f2)"
fi
if [[ -n "$ENABLE_PGBENCH" ]]; then
    echo "  - pgbench 负载测试: 启用"
    echo "  - pgbench 客户端数: $PGBENCH_CLIENTS"
    echo "  - pgbench 作业数: $PGBENCH_JOBS"
    echo "  - 数据规模因子: $PGBENCH_SCALE"
    echo "  - 预热时间: ${WARMUP_TIME}秒"
else
    echo "  - pgbench 负载测试: 禁用"
fi
echo "  - 详细日志: 启用"
echo ""

echo "💡 请在测试开始后手动触发故障转移:"
echo "   aws rds failover-db-cluster --db-cluster-identifier ards-with-rdsproxy --region ap-southeast-1"
echo ""

echo "🚀 启动故障转移测试（详细日志模式）..."
echo "========================================="

# 构建命令参数
CMD_ARGS="--mode $MODE --duration $DURATION $VERBOSE"

if [[ -n "$CONCURRENT_WORKERS" ]]; then
    CMD_ARGS="$CMD_ARGS $CONCURRENT_WORKERS"
fi

if [[ -n "$ENABLE_PGBENCH" ]]; then
    CMD_ARGS="$CMD_ARGS $ENABLE_PGBENCH --pgbench-clients $PGBENCH_CLIENTS --pgbench-jobs $PGBENCH_JOBS --pgbench-scale $PGBENCH_SCALE --warmup-time $WARMUP_TIME"
fi

# 运行测试
python main.py $CMD_ARGS

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

echo ""
echo "✅ 测试完成！"
echo ""
echo "📄 查看结果文件:"
if [[ -n "$ENABLE_PGBENCH" ]]; then
    echo "   - pgbench 报告: results/pgbench_failover_report_*.txt"
else
    echo "   - 业务场景对比报告: results/business_comparison_report_*.txt"
fi
echo "   - 详细日志: results/test_log_*.log"
echo ""

# 显示最新的日志文件
latest_log=$(ls -t results/test_log_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
    echo "📋 最新日志文件: $latest_log"
    echo "   查看日志: tail -f $latest_log"
fi

if [[ -n "$ENABLE_PGBENCH" ]]; then
    latest_report=$(ls -t results/pgbench_failover_report_*.txt 2>/dev/null | head -1)
    if [ -n "$latest_report" ]; then
        echo "📊 最新报告文件: $latest_report"
        echo "   查看报告: cat $latest_report"
    fi
else
    latest_report=$(ls -t results/business_comparison_report_*.txt 2>/dev/null | head -1)
    if [ -n "$latest_report" ]; then
        echo "📊 最新报告文件: $latest_report"
        echo "   查看报告: cat $latest_report"
    fi
fi
