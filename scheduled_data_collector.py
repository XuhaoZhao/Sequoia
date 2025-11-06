# -*- encoding: UTF-8 -*-

import os
import logging
import time
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from financial_framework.eastmoney_xuangu_api_interceptor import EastmoneySearchCodeInterceptor
from financial_framework.file_path_generator import FilePathGenerator, generate_etf_data_path, generate_stock_data_path
from financial_framework.unified_financial_system import UnifiedDataCollector,UnifiedAnalyzer,TechnicalAnalyzer

# 设置日志
logger = logging.getLogger(__name__)


def cleanup_existing_file(file_path):
    """
    清理已存在的文件

    Args:
        file_path (str): 要清理的文件路径

    Returns:
        bool: 清理成功返回True，失败返回False
    """
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"已删除已存在的文件: {file_path}")
        except Exception as e:
            logger.error(f"删除文件失败 {file_path}: {e}")
            return False
    return True


def generate_etf_data():
    """
    生成ETF数据文件
    文件名格式: etf_data_YYYY-MM-DD.csv

    Returns:
        bool: 生成成功返回True，失败返回False
    """
    logger.info("开始生成ETF数据...")

    # 使用路径生成器生成文件路径
    csv_file_path = generate_etf_data_path()

    # 检查并清理已存在的文件
    if not cleanup_existing_file(csv_file_path):
        logger.error("清理ETF数据文件失败，跳过本次执行")
        return False

    # 确保目录存在
    FilePathGenerator.ensure_directory_exists(csv_file_path)

    try:
        # 创建API拦截器实例
        interceptor = EastmoneySearchCodeInterceptor()

        # 调用拦截函数获取ETF数据
        success = interceptor.intercept_and_save_to_csv(
            xuangu_id="xc0d38036a3d93000c67",
            color="w",
            action="edit_way",
            type="etf",                      # ETF数据类型
            check_interval=1,                # 每1秒检查一次网络日志
            refresh_interval=10,             # 每10秒刷新一次页面
            csv_file_path=csv_file_path,     # 使用基于日期的文件名
            max_refresh_attempts=5           # 最大刷新尝试次数
        )

        if success:
            logger.info(f"✓ ETF数据获取和保存成功: {csv_file_path}")
            return True
        else:
            logger.warning("✗ ETF数据获取和保存失败或未获取到足够数据")
            return False

    except Exception as e:
        logger.error(f"生成ETF数据时发生错误: {e}")
        return False


def generate_stock_data():
    """
    生成股票数据文件
    文件名格式: stock_data_YYYY-MM-DD.csv

    Returns:
        bool: 生成成功返回True，失败返回False
    """
    logger.info("开始生成股票数据...")

    # 使用路径生成器生成文件路径
    csv_file_path = generate_stock_data_path()

    # 检查并清理已存在的文件
    if not cleanup_existing_file(csv_file_path):
        logger.error("清理股票数据文件失败，跳过本次执行")
        return False

    # 确保目录存在
    FilePathGenerator.ensure_directory_exists(csv_file_path)

    try:
        # 创建API拦截器实例
        interceptor = EastmoneySearchCodeInterceptor()

        # 调用拦截函数获取股票数据
        success = interceptor.intercept_and_save_to_csv(
            xuangu_id="xc0d89dd08430800eeee",
            color="w",
            action="edit_way",
            type="stock",                     # 股票数据类型
            check_interval=1,                 # 每1秒检查一次网络日志
            refresh_interval=10,              # 每10秒刷新一次页面
            csv_file_path=csv_file_path,      # 使用基于日期的文件名
            max_refresh_attempts=5            # 最大刷新尝试次数
        )

        if success:
            logger.info(f"✓ 股票数据获取和保存成功: {csv_file_path}")
            return True
        else:
            logger.warning("✗ 股票数据获取和保存失败或未获取到足够数据")
            return False

    except Exception as e:
        logger.error(f"生成股票数据时发生错误: {e}")
        return False


def is_weekday():
    """
    检查今天是否是工作日（周一到周五）

    Returns:
        bool: 如果是工作日返回True，否则返回False
    """
    today = datetime.datetime.now().weekday()  # 0=周一, 6=周日
    return today < 5  # 0-4是周一到周五


def scheduled_data_collection():
    """
    定时数据收集任务
    每天15:05执行，只在工作日运行
    """
    logger.info("=" * 50)
    logger.info("开始执行定时数据收集任务")
    logger.info(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 检查是否是工作日
    # if not is_weekday():
    #     logger.info("今天不是工作日，跳过数据收集")
    #     return

    logger.info("今天是工作日，开始数据收集...")

    # 生成ETF数据
    etf_success = generate_etf_data()

    # 等待一段时间再生成股票数据，避免频繁请求
    time.sleep(5)

    # 生成股票数据
    stock_success = generate_stock_data()

    # 总结执行结果
    logger.info("=" * 50)
    logger.info("定时数据收集任务完成")
    logger.info(f"ETF数据收集: {'成功' if etf_success else '失败'}")
    logger.info(f"股票数据收集: {'成功' if stock_success else '失败'}")
    logger.info("=" * 50)
    time.sleep(50)
    logger.info("开始获取etf 30m数据")
    unifiedDataCollector = UnifiedDataCollector()
    unifiedDataCollector.collect_all_historical_min_data(instrument_type='etf', period="30")
    logger.info("结束获取etf 30m数据")
    time.sleep(5)
    logger.info("开始获取stock 30m数据")
    unifiedDataCollector.collect_all_historical_min_data(instrument_type='stock', period="30")
    logger.info("结束获取stock 30m数据")
    # time.sleep(5)
    # unifiedAnalyzer = UnifiedAnalyzer()
    # logger.info("开始分析stock macd")
    # unifiedAnalyzer.analyze_all_instruments('stock')
    # logger.info("结束分析stock macd")
    # time.sleep(5)
    # logger.info("开始分析etf macd")
    # unifiedAnalyzer.analyze_all_instruments('etf')
    # logger.info("结束分析etf macd")
    # time.sleep(5)
    # logger.info("开始获取etf 1d数据")
    # unifiedDataCollector.collect_all_daily_data('etf')
    # logger.info("结束获取etf 1d数据")
    # time.sleep(5)
    # logger.info("开始获取stock 1d数据")
    # unifiedDataCollector.collect_all_daily_data('stock')
    # logger.info("结束获取stock 1d数据")
    # technicalAnalyzer = TechnicalAnalyzer()
    # logger.info("开始分析etf dayK")
    # technicalAnalyzer.analyze_instruments_from_macd_data('etf')
    # logger.info("结束分析etf dayK")
    # logger.info("开始分析stock dayK")
    # technicalAnalyzer.analyze_instruments_from_macd_data('stock')
    # logger.info("结束分析stock dayK")


def setup_scheduled_jobs(test_mode=False):
    """
    设置定时任务，使用APScheduler

    Args:
        test_mode (bool): 是否为测试模式，测试模式下会立即执行一次任务

    Returns:
        BackgroundScheduler: 调度器实例
    """
    logger.info("设置定时任务: 每天15:05执行数据收集")

    # 创建调度器
    scheduler = BackgroundScheduler()

    # 添加定时任务：每天15:05执行数据收集
    scheduler.add_job(
        func=scheduled_data_collection,
        trigger=CronTrigger(hour=15, minute=54),
        id='daily_data_collection',
        name='每日数据收集任务',
        replace_existing=True
    )

    # 启动调度器
    scheduler.start()
    logger.info(f"调度器已启动，状态: {'运行中' if scheduler.running else '已停止'}")

    # 测试模式下立即执行一次
    if test_mode:
        logger.info("测试模式：立即执行一次数据收集任务")
        scheduler.add_job(
            func=scheduled_data_collection,
            trigger='date',  # 立即执行一次
            id='test_data_collection',
            name='测试数据收集任务'
        )

    return scheduler


def run_test():
    """
    测试运行定时任务
    """
    logger.info("=" * 50)
    logger.info("开始测试数据收集功能")
    logger.info("=" * 50)

    # 测试ETF数据生成
    etf_success = generate_etf_data()

    # 等待一段时间
    time.sleep(2)

    # 测试股票数据生成
    stock_success = generate_stock_data()

    # 输出测试结果
    logger.info("=" * 50)
    logger.info("测试结果:")
    logger.info(f"ETF数据生成: {'成功' if etf_success else '失败'}")
    logger.info(f"股票数据生成: {'成功' if stock_success else '失败'}")
    logger.info("=" * 50)

    return etf_success and stock_success


def stop_scheduled_jobs(scheduler):
    """
    停止定时任务

    Args:
        scheduler: BackgroundScheduler实例
    """
    if scheduler and scheduler.running:
        scheduler.shutdown()
        logger.info("调度器已停止")