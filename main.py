# -*- encoding: UTF-8 -*-

import utils
import logging
import work_flow
import settings
import time
import atexit
from scheduled_data_collector import setup_scheduled_jobs, stop_scheduled_jobs


def job():
    if utils.is_weekday():
        work_flow.prepare()


logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log')
logging.getLogger().setLevel(logging.INFO)
settings.init()

if settings.config['cron']:
    # 添加数据收集任务，使用APScheduler
    # 设置test_mode=True来立即执行一次测试
    data_scheduler = setup_scheduled_jobs(test_mode=True)

    # 添加原有的work_flow任务，也使用APScheduler，每天15:15执行
    # from apscheduler.triggers.cron import CronTrigger
    # data_scheduler.add_job(
    #     func=job,
    #     trigger=CronTrigger(hour=15, minute=15),
    #     id='work_flow_job',
    #     name='工作流任务',
    #     replace_existing=True
    # )

    # 注册退出函数，确保程序退出时正确关闭调度器
    atexit.register(stop_scheduled_jobs, data_scheduler)

    try:
        # 保持程序运行，APScheduler会在后台自动执行定时任务
        while True:
            time.sleep(60)  # 每分钟检查一次，减少CPU占用
    except KeyboardInterrupt:
        logging.info("收到中断信号，程序退出")
        stop_scheduled_jobs(data_scheduler)
    except Exception as e:
        logging.error(f"程序运行时发生错误: {e}")
        stop_scheduled_jobs(data_scheduler)
        raise
else:
    work_flow.prepare()