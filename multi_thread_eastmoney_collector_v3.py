"""
东方财富选股定时数据收集器 - 调度器版

使用 APScheduler 定时收集 stock 和 etf 数据
特点：
1. 只创建一次浏览器实例，调度中复用
2. 在交易时间内每10分钟自动运行
3. 使用 APScheduler 进行精确的时间调度
4. 统一的日志管理
5. 可配置的运行间隔和参数
6. 自动错误恢复和重试机制
7. 支持动态子域名匹配
8. 实时状态监控和报告
"""

import time
import signal
import sys
import os
from datetime import datetime
from typing import Dict, Optional
import json
import traceback
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# 导入日志系统
try:
    from financial_framework.logger_config import FinancialLogger, get_logger
    from eastmoney_xuangu_api_interceptor_scheduled import EastmoneySearchCodeInterceptor
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from eastmoney_xuangu_api_interceptor_scheduled import EastmoneySearchCodeInterceptor


class ScheduledDataCollector:
    """定时数据收集器（复用浏览器实例，调度执行）"""

    def __init__(self, task_id: str, data_type: str, config: Dict, logger):
        self.task_id = task_id
        self.data_type = data_type  # "stock", "etf", "fund", "bond"
        self.config = config
        self.logger = logger
        self.is_running = False

        # 统计信息
        self.run_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.last_success_time = None
        self.last_error = None

        # 拦截器实例（单例复用）
        self.interceptor = None

    def create_interceptor(self) -> bool:
        """创建数据拦截器实例（只创建一次，调度中复用）"""
        try:
            if self.interceptor is not None:
                self.logger.debug(f"[{self.task_id}] 拦截器已存在，跳过创建")
                return True

            self.logger.info(f"[{self.task_id}] 正在创建拦截器实例...")

            # 创建拦截器实例
            interceptor = EastmoneySearchCodeInterceptor(
                headless=self.config.get("headless", True),
                log_full_data=self.config.get("log_full_data", False),
                log_summary_only=self.config.get("log_summary_only", True),
                use_database=self.config.get("use_database", True)
            )

            if not interceptor.browser_manager.is_initialized():
                raise Exception("浏览器初始化失败")

            self.interceptor = interceptor
            self.logger.info(f"[{self.task_id}] ✓ 拦截器创建成功（将调度复用）")
            return True

        except Exception as e:
            self.logger.error(f"[{self.task_id}] ✗ 创建拦截器失败: {e}")
            return False

    def run_single_collection(self) -> bool:
        """执行单次数据收集（复用现有浏览器实例）"""
        try:
            self.run_count += 1
            self.logger.info(f"[{self.task_id}] ===== 第 {self.run_count} 次数据收集 =====")

            # 确保拦截器已创建
            if not self.interceptor and not self.create_interceptor():
                raise Exception("拦截器创建失败")

            # 生成带时间戳的CSV文件路径
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file_path = f"data/{self.data_type}_data_{timestamp}.csv"

            # 执行数据收集（这里会内部刷新页面，但浏览器实例复用）
            success = self.interceptor.scheduled_intercept_and_save(
                xuangu_id=self.config.get("xuangu_id", "xc0e3d869f99930168c0"),
                color=self.config.get("color", "w"),
                action=self.config.get("action", "edit_way"),
                type=self.data_type,
                check_interval=self.config.get("check_interval", 1),
                refresh_interval=self.config.get("refresh_interval", 15),
                max_refresh_attempts=self.config.get("max_refresh_attempts", 3),
                csv_file_path=csv_file_path
            )

            if success:
                self.success_count += 1
                self.last_success_time = datetime.now()
                self.last_error = None
                self.logger.info(f"[{self.task_id}] ✓ 第 {self.run_count} 次数据收集成功完成")
                self.logger.info(f"[{self.task_id}] 数据保存到: {csv_file_path}")
                return True
            else:
                raise Exception("数据收集失败")

        except Exception as e:
            self.failure_count += 1
            self.last_error = str(e)
            self.logger.error(f"[{self.task_id}] ✗ 第 {self.run_count} 次数据收集失败: {e}")

            # 如果是浏览器相关错误，尝试重新创建拦截器
            if any(keyword in str(e).lower() for keyword in ["browser", "driver", "connection", "session"]):
                self.logger.warning(f"[{self.task_id}] 检测到浏览器错误，尝试重新创建拦截器...")
                self._cleanup_interceptor()
                if self.create_interceptor():
                    self.logger.info(f"[{self.task_id}] ✓ 拦截器重新创建成功")

            return False

    def _cleanup_interceptor(self):
        """清理拦截器资源"""
        try:
            if self.interceptor:
                self.interceptor.close()
                self.interceptor = None
                self.logger.debug(f"[{self.task_id}] 拦截器资源已清理")
        except Exception as e:
            self.logger.warning(f"[{self.task_id}] 清理拦截器时出错: {e}")

    def _is_trading_time(self, current_time: datetime) -> bool:
        """
        判断当前时间是否在交易时间内

        交易时间：
        - 上午：9:30-11:30
        - 下午：13:00-15:00

        Args:
            current_time: 当前时间

        Returns:
            bool: 是否在交易时间内
        """
        # 检查是否为工作日（周一到周五）
        if current_time.weekday() >= 5:  # 5=周六, 6=周日
            return False

        current_hour = current_time.hour
        current_minute = current_time.minute
        time_in_minutes = current_hour * 60 + current_minute

        #延迟1分钟，不要9:30和13:00
        # 上午交易时间：9:30-11:30 (570-690分钟)
        morning_start = 9 * 60 + 31  # 9:30 = 570分钟
        morning_end = 11 * 60 + 30   # 11:30 = 690分钟

        # 下午交易时间：13:00-15:00 (780-900分钟)
        afternoon_start = 13 * 60+1     # 13:00 = 780分钟
        afternoon_end = 15 * 60       # 15:00 = 900分钟

        is_morning = morning_start <= time_in_minutes <= morning_end
        is_afternoon = afternoon_start <= time_in_minutes <= afternoon_end

        return is_morning or is_afternoon

    def scheduled_job(self):
        """调度器执行的任务函数"""
        current_time = datetime.now()
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")

        self.logger.info(f"[{self.task_id}] ===== 调度任务执行 - {current_time_str} =====")

        # 检查是否在交易时间内
        if not self._is_trading_time(current_time):
            self.logger.info(f"[{self.task_id}] 当前时间非交易时间，跳过数据收集")
            return

        # 执行单次数据收集（复用浏览器实例）
        success = self.run_single_collection()

        if not success:
            self.logger.warning(f"[{self.task_id}] 本次收集失败，等待下次调度执行")

    def initialize(self) -> bool:
        """初始化收集器（创建拦截器）"""
        try:
            self.is_running = True
            self.logger.info(f"[{self.task_id}] 初始化定时数据收集器 (数据类型: {self.data_type})")
            self.logger.info(f"[{self.task_id}] 模式: 浏览器实例复用 + 定时调度执行")

            # 创建拦截器
            if not self.create_interceptor():
                self.logger.error(f"[{self.task_id}] 无法创建拦截器，初始化失败")
                self.is_running = False
                return False

            self.logger.info(f"[{self.task_id}] ✓ 定时数据收集器初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"[{self.task_id}] 初始化发生异常: {e}", exc_info=True)
            self.is_running = False
            return False

    def cleanup(self):
        """清理收集器资源"""
        self.logger.info(f"[{self.task_id}] 正在清理定时数据收集器...")
        self._cleanup_interceptor()
        self.is_running = False
        self.logger.info(f"[{self.task_id}] 定时数据收集器清理完成")

    def get_status(self) -> Dict:
        """获取收集器状态"""
        return {
            "task_id": self.task_id,
            "data_type": self.data_type,
            "is_running": self.is_running,
            "browser_initialized": self.interceptor is not None and self.interceptor.browser_manager.is_initialized() if self.interceptor else False,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": self.success_count / max(self.run_count, 1) * 100,
            "last_success_time": self.last_success_time.isoformat() if self.last_success_time else None,
            "last_error": self.last_error
        }


class ScheduledDataCollectorManager:
    """定时数据收集器管理器"""

    def __init__(self):
        self.collectors: Dict[str, ScheduledDataCollector] = {}
        self.scheduler = None
        self.is_running = False
        self.logger = get_logger('scheduled_data_collector_manager')
        self.data_logger = FinancialLogger.get_data_logger()

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def add_collector(self, task_id: str, data_type: str, config: Dict) -> bool:
        """添加定时数据收集器"""
        try:
            if task_id in self.collectors:
                self.logger.warning(f"收集器 {task_id} 已存在，将被覆盖")

            collector = ScheduledDataCollector(task_id, data_type, config, self.logger)
            self.collectors[task_id] = collector

            self.logger.info(f"添加定时收集器: {task_id} (数据类型: {data_type})")
            return True

        except Exception as e:
            self.logger.error(f"添加收集器失败: {e}")
            return False

    def add_stock_etf_collectors(self, base_config: Dict) -> bool:
        """添加股票和ETF定时收集器"""
        try:
            # 添加股票收集器
            stock_config = base_config.copy()
            stock_config.update({
                "xuangu_id": stock_config.get("stock_xuangu_id", "xc0e3d869f9 9930168c0"),
                "type": "stock"
            })
            self.add_collector("stock_collector", "stock", stock_config)

            # 添加ETF收集器
            etf_config = base_config.copy()
            etf_config.update({
                "xuangu_id": etf_config.get("etf_xuangu_id", "xc0e70095ec893006c72"),
                "type": "etf"
            })
            self.add_collector("etf_collector", "etf", etf_config)

            return True

        except Exception as e:
            self.logger.error(f"添加股票和ETF收集器失败: {e}")
            return False

    def start_scheduler(self):
        """启动调度器"""
        if not self.collectors:
            self.logger.error("没有可启动的收集器")
            return False

        if self.scheduler and self.scheduler.running:
            self.logger.warning("调度器已在运行中")
            return False

        try:
            # 创建调度器
            self.scheduler = BlockingScheduler(timezone='Asia/Shanghai')

            # 初始化所有收集器
            for task_id, collector in self.collectors.items():
                if not collector.initialize():
                    self.logger.error(f"[{task_id}] 收集器初始化失败")
                    return False

            self.is_running = True
            self.logger.info("=" * 80)
            self.logger.info("启动定时数据收集系统 - 调度器版")
            self.logger.info(f"收集器数量: {len(self.collectors)}")
            self.logger.info("调度模式: 交易时间内每10分钟执行一次")
            self.logger.info("模式: 浏览器实例复用 + 定时调度执行")

            for task_id, collector in self.collectors.items():
                self.logger.info(f"  - {task_id}: {collector.data_type}")

            self.logger.info("=" * 80)

            # 为每个收集器添加定时任务
            for task_id, collector in self.collectors.items():
                # 简化的cron表达式: 周一到周五，每10分钟执行一次
                # 任务内部会自行判断是否在交易时间内
                cron_expression = '*/10 * * * *'  # 每10分钟执行，周一到周五
                # cron_expression = '* * * * *'  # 临时测试：每分钟执行，便于调试

                self.scheduler.add_job(
                    collector.scheduled_job,
                    CronTrigger.from_crontab(cron_expression),
                    id=f"{task_id}_data_collection",
                    name=f"{task_id} 定时数据收集",
                    replace_existing=True
                )

                self.logger.info(f"[{task_id}] ✓ 定时任务添加成功 (每10分钟执行，任务内部判断交易时间)")

            self.logger.info("=" * 80)
            self.logger.info("调度器配置完成，开始运行...")
            self.logger.info("下次执行时间将根据交易时间自动计算")
            self.logger.info("按 Ctrl+C 停止系统")
            self.logger.info("=" * 80)

            return True

        except Exception as e:
            self.logger.error(f"启动调度器失败: {e}", exc_info=True)
            self.is_running = False
            return False

    def stop_scheduler(self):
        """停止调度器"""
        if not self.scheduler:
            return

        try:
            self.logger.info("正在停止调度器...")

            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)

            # 清理所有收集器
            for collector in self.collectors.values():
                collector.cleanup()

            self.is_running = False
            self.logger.info("调度器已停止")

        except Exception as e:
            self.logger.error(f"停止调度器时出错: {e}")

    def run_scheduler(self):
        """运行调度器（阻塞）"""
        if not self.scheduler:
            self.logger.error("调度器未初始化")
            return

        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            self.logger.info("收到中断信号，正在停止...")
        finally:
            self.stop_scheduler()

    def print_status(self):
        """打印所有收集器的状态"""
        self.logger.info("=" * 80)
        self.logger.info("定时数据收集系统状态 - 调度器版")
        self.logger.info("=" * 80)

        total_runs = 0
        total_successes = 0
        total_failures = 0

        for task_id, collector in self.collectors.items():
            status = collector.get_status()
            total_runs += status["run_count"]
            total_successes += status["success_count"]
            total_failures += status["failure_count"]

            self.logger.info(f"收集器: {task_id}")
            self.logger.info(f"  数据类型: {status['data_type']}")
            self.logger.info(f"  运行状态: {'运行中' if status['is_running'] else '已停止'}")
            self.logger.info(f"  浏览器状态: {'已初始化' if status['browser_initialized'] else '未初始化'}")
            self.logger.info(f"  运行次数: {status['run_count']}")
            self.logger.info(f"  成功次数: {status['success_count']}")
            self.logger.info(f"  失败次数: {status['failure_count']}")
            self.logger.info(f"  成功率: {status['success_rate']:.1f}%")
            self.logger.info(f"  最后成功时间: {status['last_success_time'] or '从未成功'}")
            if status['last_error']:
                self.logger.info(f"  最后错误: {status['last_error']}")
            self.logger.info("-" * 60)

        self.logger.info("=" * 80)
        self.logger.info("总体统计:")
        self.logger.info(f"  总运行次数: {total_runs}")
        self.logger.info(f"  总成功次数: {total_successes}")
        self.logger.info(f"  总失败次数: {total_failures}")
        if total_runs > 0:
            overall_success_rate = total_successes / total_runs * 100
            self.logger.info(f"  总成功率: {overall_success_rate:.1f}%")
        self.logger.info("=" * 80)

    def get_status_report(self) -> Dict:
        """获取详细状态报告"""
        return {
            "system_running": self.is_running,
            "scheduler_running": self.scheduler.running if self.scheduler else False,
            "collectors": {
                task_id: collector.get_status()
                for task_id, collector in self.collectors.items()
            },
            "summary": {
                "total_collectors": len(self.collectors),
                "running_collectors": sum(1 for c in self.collectors.values() if c.is_running),
                "total_runs": sum(c.run_count for c in self.collectors.values()),
                "total_successes": sum(c.success_count for c in self.collectors.values()),
                "total_failures": sum(c.failure_count for c in self.collectors.values())
            }
        }

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        print(f"\n收到信号 {signum}，正在停止系统...")
        self.stop_scheduler()
        sys.exit(0)


def create_default_config() -> Dict:
    """创建默认配置"""
    return {
        # 浏览器配置
        "headless": False,  # 显示浏览器窗口
        "log_full_data": False,  # 不记录完整数据到日志
        "log_summary_only": True,  # 只显示摘要信息
        "use_database": True,  # 使用数据库保存数据

        # 选股配置
        "stock_xuangu_id": "xc0d89dd08430800eeee",
        "etf_xuangu_id": "xc0d38036a3d93000c67",  # ETF专用ID
        "color": "w",
        "action": "edit_way",

        # 运行配置
        "check_interval": 1,  # 检查网络日志间隔（秒）
        "refresh_interval": 15,  # 页面刷新间隔（秒）
        "max_refresh_attempts": 3,  # 最大刷新尝试次数
        "run_interval": 300  # 循环运行间隔（秒）- 5分钟
    }


def main():
    """主函数"""
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('scheduled_data_collector_main')
    logger.info("=" * 80)
    logger.info("东方财富定时数据收集系统启动 - 调度器版")
    logger.info("定时收集 stock 和 etf 数据")
    logger.info("调度特性: 交易时间内每10分钟自动执行")
    logger.info("优化特性: 浏览器实例复用 + 定时调度执行")
    logger.info("=" * 80)

    # 创建收集器管理器
    collector_manager = ScheduledDataCollectorManager()

    # 获取默认配置
    config = create_default_config()

    # 添加股票和ETF收集器
    if not collector_manager.add_stock_etf_collectors(config):
        logger.error("添加收集器失败，退出程序")
        return

    try:
        # 启动调度器
        if collector_manager.start_scheduler():
            logger.info("调度器启动成功，系统运行中...")
            logger.info("系统将在交易时间内每10分钟自动执行数据收集")
            logger.info("每个收集器复用浏览器实例，通过调度器定时执行")
            logger.info("按 Ctrl+C 停止系统")

            # 运行调度器（阻塞）
            collector_manager.run_scheduler()
        else:
            logger.error("启动调度器失败")

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    except Exception as e:
        logger.error(f"程序运行异常: {e}", exc_info=True)
    finally:
        collector_manager.stop_scheduler()

        # 打印最终报告
        logger.info("=" * 80)
        logger.info("最终运行报告")
        logger.info("=" * 80)
        final_report = collector_manager.get_status_report()
        logger.info(json.dumps(final_report, ensure_ascii=False, indent=2))
        logger.info("=" * 80)
        logger.info("程序结束")


if __name__ == "__main__":
    main()