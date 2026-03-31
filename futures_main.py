# -*- encoding: UTF-8 -*-
"""
期货5分钟数据收集主程序

功能：
1. 启动期货5分钟数据收集定时任务
2. 每天凌晨5点自动执行数据收集
3. 持续运行，确保任务准时执行

使用方法：
    python futures_main.py

说明：
    - 程序会持续运行，每天凌晨5点自动收集期货5分钟数据
    - 按 Ctrl+C 可以停止程序
    - 日志会记录到 sequoia.log 文件中
"""

import logging
import settings
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from futures_5m_collector import Futures5MinCollector


# 设置日志 - 同时输出到控制台和文件
import sys
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('sequoia.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
settings.init()


def scheduled_futures_5m_collection():
    """
    定时期货5分钟数据收集任务
    每天凌晨5点执行
    """
    import datetime

    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("开始执行定时期货5分钟数据收集任务")
    logger.info(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 创建收集器实例
        collector = Futures5MinCollector()

        # 收集所有期货5分钟数据
        stats = collector.collect_all_futures_5m_data()

        logger.info("定时任务执行完成")
        logger.info(f"执行结果: 成功 {stats['success']}, 失败 {stats['failed']}, 总数据量 {stats['total']} 条")
        logger.info("=" * 50)

        return stats

    except Exception as e:
        logger.error(f"定时任务执行出错: {e}")
        import traceback
        traceback.print_exc()
        logger.info("=" * 50)
        return {'success': 0, 'failed': 0, 'total': 0}


def setup_futures_scheduled_jobs(test_mode=False):
    """
    设置期货5分钟数据收集定时任务

    Args:
        test_mode (bool): 是否为测试模式，测试模式下会立即执行一次任务

    Returns:
        BackgroundScheduler: 调度器实例
    """
    logger = logging.getLogger(__name__)
    logger.info("设置期货5分钟数据收集定时任务: 每天凌晨5:00执行")

    # 创建调度器
    scheduler = BackgroundScheduler()

    # 添加期货5分钟数据收集任务：每天凌晨5:00执行
    scheduler.add_job(
        func=scheduled_futures_5m_collection,
        trigger=CronTrigger(hour=5, minute=0),
        id='futures_5m_collection',
        name='期货5分钟数据收集任务',
        misfire_grace_time=300,  # 允许延迟5分钟内仍然执行
        replace_existing=True
    )

    # 启动调度器
    scheduler.start()
    logger.info(f"调度器已启动，状态: {'运行中' if scheduler.running else '已停止'}")

    # 测试模式下立即执行一次
    if test_mode:
        logger.info("测试模式：立即执行一次数据收集任务")
        scheduler.add_job(
            func=scheduled_futures_5m_collection,
            trigger='date',  # 立即执行一次
            id='test_futures_5m_collection',
            name='测试期货5分钟数据收集任务'
        )

    return scheduler


def stop_scheduled_jobs(scheduler):
    """
    停止定时任务

    Args:
        scheduler: BackgroundScheduler实例
    """
    logger = logging.getLogger(__name__)
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("调度器已停止")


def main():
    """
    主函数 - 启动期货5分钟数据收集定时任务
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("启动期货5分钟数据收集系统")
    logger.info("=" * 80)

    # 设置期货5分钟数据收集任务
    # 设置test_mode=True来立即执行一次测试
    data_scheduler = setup_futures_scheduled_jobs(test_mode=False)

    # 注册退出函数，确保程序退出时正确关闭调度器
    atexit.register(stop_scheduled_jobs, data_scheduler)

    try:
        # 保持程序运行，APScheduler会在后台自动执行定时任务
        logger.info("程序运行中，期货5分钟数据收集任务将于每天凌晨5:00执行")
        logger.info("按 Ctrl+C 停止程序")
        logger.info("=" * 80)

        while True:
            time.sleep(60)  # 每分钟检查一次，减少CPU占用
    except KeyboardInterrupt:
        logger.info("收到中断信号，程序退出")
        stop_scheduled_jobs(data_scheduler)
    except Exception as e:
        logger.error(f"程序运行时发生错误: {e}")
        stop_scheduled_jobs(data_scheduler)
        raise


if __name__ == "__main__":
    main()
