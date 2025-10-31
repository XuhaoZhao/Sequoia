"""
多线程数据收集调度器

使用同一个 eastmoney_xuangu_api_interceptor_scheduled.py 文件
通过传入不同的 type 参数同时运行 stock 和 etf 数据收集
特点：
1. 多线程同时运行stock和etf数据收集
2. 统一的日志管理，避免日志冲突
3. 独立的浏览器实例，避免冲突
4. 可配置的运行间隔和参数
5. 自动错误恢复和重试机制
"""

import time
import threading
import signal
import sys
import os
from datetime import datetime
from typing import Dict, Optional
import json
import queue

# 导入日志系统
try:
    from financial_framework.logger_config import FinancialLogger, get_logger
    from eastmoney_xuangu_api_interceptor_scheduled import EastmoneySearchCodeInterceptor
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from eastmoney_xuangu_api_interceptor_scheduled import EastmoneySearchCodeInterceptor


class DataCollectionWorker:
    """数据收集工作线程"""

    def __init__(self, worker_name: str, data_type: str, config: Dict, logger):
        self.worker_name = worker_name
        self.data_type = data_type  # "stock" 或 "etf"
        self.config = config
        self.logger = logger
        self.interceptor = None
        self.is_running = False
        self.should_stop = False
        self.last_success_time = None
        self.last_error = None
        self.run_count = 0

    def create_interceptor(self):
        """创建数据拦截器实例"""
        try:
            # 为每个worker创建独立的拦截器实例
            interceptor = EastmoneySearchCodeInterceptor(
                headless=self.config.get("headless", True),
                log_full_data=self.config.get("log_full_data", False),
                log_summary_only=self.config.get("log_summary_only", True),
                use_database=self.config.get("use_database", True)
            )
            return interceptor
        except Exception as e:
            self.logger.error(f"[{self.worker_name}] 创建拦截器失败: {e}")
            return None

    def run_single_collection(self):
        """执行单次数据收集"""
        try:
            self.logger.info(f"[{self.worker_name}] 开始第 {self.run_count + 1} 次数据收集...")

            # 创建新的拦截器实例
            self.interceptor = self.create_interceptor()
            if not self.interceptor:
                raise Exception("拦截器创建失败")

            if not self.interceptor.browser_manager.is_initialized():
                raise Exception("浏览器初始化失败")

            # 生成带时间戳的CSV文件路径
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file_path = f"data/{self.data_type}_data_{timestamp}.csv"

            # 执行数据收集
            success = self.interceptor.scheduled_intercept_and_save(
                xuangu_id=self.config.get("xuangu_id", "xc0d647de27193013bb2"),
                color=self.config.get("color", "w"),
                action=self.config.get("action", "edit_way"),
                type=self.data_type,  # 关键参数：传入数据类型
                check_interval=self.config.get("check_interval", 1),
                refresh_interval=self.config.get("refresh_interval", 15),
                max_refresh_attempts=self.config.get("max_refresh_attempts", 3),
                csv_file_path=csv_file_path
            )

            if success:
                self.run_count += 1
                self.last_success_time = datetime.now()
                self.last_error = None
                self.logger.info(f"[{self.worker_name}] ✓ 第 {self.run_count} 次数据收集成功完成")
                self.logger.info(f"[{self.worker_name}] 数据保存到: {csv_file_path}")
                return True
            else:
                raise Exception("数据收集失败")

        except Exception as e:
            self.last_error = str(e)
            self.logger.error(f"[{self.worker_name}] ✗ 数据收集失败: {e}")
            return False
        finally:
            # 清理资源
            if self.interceptor:
                try:
                    self.interceptor.close()
                except Exception as e:
                    self.logger.warning(f"[{self.worker_name}] 关闭拦截器时出错: {e}")
                finally:
                    self.interceptor = None

    def worker_loop(self):
        """工作线程主循环"""
        self.is_running = True
        self.logger.info(f"[{self.worker_name}] 工作线程启动 (数据类型: {self.data_type})")

        try:
            while not self.should_stop:
                start_time = datetime.now()

                # 执行单次数据收集
                success = self.run_single_collection()

                if not success:
                    self.logger.warning(f"[{self.worker_name}] 本次收集失败，将在重试间隔后继续")

                # 计算下次运行时间
                interval = self.config.get("run_interval", 300)  # 默认5分钟
                next_run_time = start_time.timestamp() + interval
                next_run_str = datetime.fromtimestamp(next_run_time).strftime('%Y-%m-%d %H:%M:%S')

                self.logger.info(f"[{self.worker_name}] 下次运行时间: {next_run_str}")

                # 等待指定间隔或停止信号
                wait_start = time.time()
                while time.time() - wait_start < interval and not self.should_stop:
                    time.sleep(1)

        except Exception as e:
            self.logger.error(f"[{self.worker_name}] 工作线程异常: {e}", exc_info=True)
        finally:
            self.is_running = False
            self.logger.info(f"[{self.worker_name}] 工作线程结束")

    def stop(self):
        """停止工作线程"""
        self.logger.info(f"[{self.worker_name}] 正在停止工作线程...")
        self.should_stop = True

        # 清理资源
        if self.interceptor:
            try:
                self.interceptor.close()
            except:
                pass
            self.interceptor = None


class MultiThreadDataCollector:
    """多线程数据收集调度器"""

    def __init__(self):
        self.workers: Dict[str, DataCollectionWorker] = {}
        self.worker_threads: Dict[str, threading.Thread] = {}
        self.is_running = False
        self.logger = get_logger('multi_thread_collector')

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def add_worker(self, worker_name: str, data_type: str, config: Dict) -> bool:
        """添加数据收集工作线程"""
        try:
            if worker_name in self.workers:
                self.logger.warning(f"工作线程 {worker_name} 已存在，将被覆盖")

            worker = DataCollectionWorker(worker_name, data_type, config, self.logger)
            self.workers[worker_name] = worker

            self.logger.info(f"添加工作线程: {worker_name} (数据类型: {data_type})")
            return True
        except Exception as e:
            self.logger.error(f"添加工作线程失败: {e}")
            return False

    def start_all_workers(self):
        """启动所有工作线程"""
        if not self.workers:
            self.logger.error("没有可启动的工作线程")
            return False

        self.is_running = True
        self.logger.info("=" * 80)
        self.logger.info("启动多线程数据收集系统")
        self.logger.info(f"工作线程数量: {len(self.workers)}")

        for worker_name, worker in self.workers.items():
            self.logger.info(f"  - {worker_name}: {worker.data_type}")

        self.logger.info("=" * 80)

        # 启动所有工作线程
        for worker_name, worker in self.workers.items():
            try:
                thread = threading.Thread(
                    target=worker.worker_loop,
                    name=f"DataCollector-{worker_name}",
                    daemon=True
                )
                thread.start()
                self.worker_threads[worker_name] = thread

                # 等待一小段时间确保线程启动
                time.sleep(2)

                self.logger.info(f"[{worker_name}] ✓ 工作线程启动成功")

            except Exception as e:
                self.logger.error(f"[{worker_name}] ✗ 启动工作线程失败: {e}")

        return True

    def stop_all_workers(self):
        """停止所有工作线程"""
        self.logger.info("正在停止所有工作线程...")

        # 发送停止信号
        for worker in self.workers.values():
            worker.stop()

        # 等待所有线程结束
        for worker_name, thread in self.worker_threads.items():
            try:
                if thread.is_alive():
                    self.logger.info(f"等待工作线程 {worker_name} 结束...")
                    thread.join(timeout=30)  # 最多等待30秒

                    if thread.is_alive():
                        self.logger.warning(f"工作线程 {worker_name} 未能在超时时间内结束")
                    else:
                        self.logger.info(f"工作线程 {worker_name} 已结束")
            except Exception as e:
                self.logger.error(f"停止工作线程 {worker_name} 时出错: {e}")

        self.is_running = False
        self.logger.info("所有工作线程已停止")

    def get_status(self) -> Dict:
        """获取所有工作线程的状态"""
        status = {
            'system_running': self.is_running,
            'workers': {}
        }

        for worker_name, worker in self.workers.items():
            thread = self.worker_threads.get(worker_name)
            status['workers'][worker_name] = {
                'data_type': worker.data_type,
                'is_running': worker.is_running,
                'should_stop': worker.should_stop,
                'run_count': worker.run_count,
                'last_success_time': worker.last_success_time.isoformat() if worker.last_success_time else None,
                'last_error': worker.last_error,
                'thread_alive': thread.is_alive() if thread else False
            }

        return status

    def print_status(self):
        """打印状态信息"""
        status = self.get_status()

        print("\n" + "=" * 60)
        print(f"系统状态: {'运行中' if status['system_running'] else '已停止'}")
        print(f"工作线程数量: {len(status['workers'])}")
        print("-" * 60)

        for worker_name, worker_status in status['workers'].items():
            print(f"工作线程: {worker_name}")
            print(f"  数据类型: {worker_status['data_type']}")
            print(f"  运行状态: {'运行中' if worker_status['is_running'] else '已停止'}")
            print(f"  线程状态: {'存活' if worker_status['thread_alive'] else '已结束'}")
            print(f"  运行次数: {worker_status['run_count']}")
            print(f"  最后成功时间: {worker_status['last_success_time'] or '从未成功'}")
            if worker_status['last_error']:
                print(f"  最后错误: {worker_status['last_error']}")
            print("-" * 60)

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        print(f"\n收到信号 {signum}，正在停止系统...")
        self.stop_all_workers()
        sys.exit(0)

    def wait_for_completion(self):
        """等待所有工作线程完成（阻塞）"""
        try:
            while self.is_running:
                time.sleep(10)
                # 可选：定期打印状态
                # self.print_status()
        except KeyboardInterrupt:
            print("\n收到中断信号，正在停止...")
            self.stop_all_workers()


def create_default_configs() -> Dict:
    """创建默认配置"""
    return {
        'stock': {
            'headless': True,
            'log_full_data': False,
            'log_summary_only': True,
            'use_database': True,
            'xuangu_id': 'xc0d647de27193013bb2',
            'color': 'w',
            'action': 'edit_way',
            'check_interval': 1,
            'refresh_interval': 15,
            'max_refresh_attempts': 3,
            'run_interval': 300  # 5分钟运行一次
        },
        'etf': {
            'headless': True,
            'log_full_data': False,
            'log_summary_only': True,
            'use_database': True,
            'xuangu_id': 'xc0d647de27193013bb2',  # 可以使用相同的ID，或者使用ETF专用的ID
            'color': 'w',
            'action': 'edit_way',
            'check_interval': 1,
            'refresh_interval': 15,
            'max_refresh_attempts': 3,
            'run_interval': 300  # 5分钟运行一次
        }
    }


def main():
    """主函数"""
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('multi_collector_main')
    logger.info("=" * 80)
    logger.info("多线程数据收集系统启动")
    logger.info("使用 eastmoney_xuangu_api_interceptor_scheduled.py 同时收集 stock 和 etf 数据")
    logger.info("=" * 80)

    # 创建调度器
    collector = MultiThreadDataCollector()

    # 获取默认配置
    configs = create_default_configs()

    # 添加stock数据收集工作线程
    collector.add_worker(
        worker_name="stock_worker",
        data_type="stock",
        config=configs['stock']
    )

    # 添加etf数据收集工作线程
    collector.add_worker(
        worker_name="etf_worker",
        data_type="etf",
        config=configs['etf']
    )

    try:
        # 启动所有工作线程
        if collector.start_all_workers():
            logger.info("所有工作线程已启动，系统运行中...")
            logger.info("按 Ctrl+C 停止系统")

            # 等待程序结束
            collector.wait_for_completion()
        else:
            logger.error("启动工作线程失败")

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    except Exception as e:
        logger.error(f"程序运行异常: {e}", exc_info=True)
    finally:
        collector.stop_all_workers()
        logger.info("程序结束")


if __name__ == "__main__":
    main()