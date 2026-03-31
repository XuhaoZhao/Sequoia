# -*- encoding: UTF-8 -*-
"""
期货5分钟数据收集模块

使用 akshare 获取期货5分钟数据，并存储到数据库中。
支持按月分表存储，自动去重。
"""

import logging
import time
import datetime
import re
import akshare as ak
from typing import List, Dict, Optional
from db_manager import IndustryDataDB

# 设置日志
logger = logging.getLogger(__name__)


class Futures5MinCollector:
    """
    期货5分钟数据收集器

    功能：
    1. 从数据库获取活跃主力合约列表
    2. 转换合约代码格式（3位补全为4位）
    3. 调用 akshare 获取5分钟数据
    4. 存储到数据库（按月分表，自动去重）
    """

    def __init__(self, db: IndustryDataDB = None):
        """
        初始化收集器

        Args:
            db: 数据库管理器实例，为None时自动创建
        """
        self.db = db or IndustryDataDB()
        self.contracts_cache = {}  # 缓存合约信息

    def get_all_contracts(self) -> List[Dict]:
        """
        从数据库获取所有期货合约列表

        Returns:
            合约列表，每个合约包含 contract_code 和 variety_name_cn
        """
        try:
            # 获取所有合约
            df = self.db.query_futures_contracts()

            if df.empty:
                logger.warning("数据库中没有找到期货合约")
                return []

            # 构建结果列表
            result = []
            for _, row in df.iterrows():
                result.append({
                    'contract_code': row['contract_code'],
                    'variety_name_cn': row['variety_name_cn']
                })

            logger.info(f"获取到 {len(result)} 个期货合约")
            return result

        except Exception as e:
            logger.error(f"获取期货合约失败: {e}")
            return []

    def fix_contract_code(self, contract_code: str) -> str:
        """
        修正合约代码格式（将3位数字补全为4位）

        Args:
            contract_code: 原始合约代码

        Returns:
            修正后的合约代码
        """
        # 检查数字部分是否已经是4位
        digits_match = re.search(r'(\d+)$', contract_code)
        if not digits_match:
            return contract_code

        digits = digits_match.group(1)

        # 如果已经是4位数字，不需要修正
        if len(digits) == 4:
            return contract_code

        # 如果是3位数字，需要补全为4位
        if len(digits) == 3:
            # 提取品种代码和3位数字
            match = re.match(r'^([A-Za-z]+)(\d{3})$', contract_code)
            if not match:
                return contract_code

            variety = match.group(1)  # 品种代码，如 AP
            month_3digit = match.group(2)  # 3位数字，如 605

            # 根据当前年份判断完整年份
            first_digit = int(month_3digit[0])  # 第一位，如 6
            current_year_last_digit = datetime.datetime.now().year % 10  # 当前年份最后一位
            current_decade = datetime.datetime.now().year // 10 * 10  # 当前年代

            # 判断合约年份
            if first_digit > current_year_last_digit:
                contract_year = current_decade + first_digit
            elif first_digit < current_year_last_digit - 2:
                contract_year = current_decade + 10 + first_digit
            else:
                contract_year = current_decade + first_digit

            # 补全为4位数字：年份后2位 + 月份后2位
            year_2digit = str(contract_year)[2:]  # 如 "26"
            month_2digit = month_3digit[1:]  # 如 "05"
            month_4digit = f"{year_2digit}{month_2digit}"  # "2605"

            # 构造新的合约代码
            new_code = f"{variety}{month_4digit}"

            logger.debug(f"合约代码修正: {contract_code} -> {new_code}")
            return new_code

        return contract_code

    def collect_futures_5m_data(self, contract_code: str, variety_name: str,
                                period: str = "5", retry: int = 3) -> Optional[List[Dict]]:
        """
        获取单个期货合约的5分钟数据

        Args:
            contract_code: 合约代码（如 RB2605）
            variety_name: 品种名称（如 螺纹钢）
            period: 数据周期，默认"5"表示5分钟
            retry: 重试次数，默认3次

        Returns:
            数据列表，失败返回None
        """
        # 修正合约代码
        fixed_code = self.fix_contract_code(contract_code)

        for attempt in range(retry):
            try:
                logger.debug(f"正在获取 {variety_name} ({fixed_code}) 的5分钟数据... (尝试 {attempt + 1}/{retry})")

                # 调用 akshare 获取数据
                # 注意：新浪期货接口需要使用品种代码，而不是合约代码
                # 例如：RB0 表示螺纹钢连续，RB2605 表示具体合约
                df = ak.futures_zh_minute_sina(symbol=fixed_code, period=period)

                if df is None or df.empty:
                    logger.warning(f"{variety_name} ({fixed_code}) 没有返回数据")
                    return None

                # 转换数据格式
                data_list = []
                for idx, row in df.iterrows():
                    # 处理时间格式：可能是字符串、datetime对象或数字索引
                    datetime_str = ""
                    if isinstance(idx, str):
                        # 如果是字符串，直接使用
                        datetime_str = idx
                    elif hasattr(idx, 'strftime'):
                        # 如果是datetime对象，格式化
                        datetime_str = idx.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # 如果是数字索引，尝试从行数据获取时间
                        # 检查df是否有时间列
                        if 'datetime' in df.columns:
                            datetime_str = str(row['datetime'])
                        elif '时间' in df.columns:
                            datetime_str = str(row['时间'])
                        elif 'date' in df.columns:
                            datetime_str = str(row['date'])
                        else:
                            # 使用当前时间
                            datetime_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    data_list.append({
                        'contract_code': fixed_code,
                        'variety_name': variety_name,
                        'datetime': datetime_str,
                        'open': float(row['open']) if 'open' in row else 0.0,
                        'high': float(row['high']) if 'high' in row else 0.0,
                        'low': float(row['low']) if 'low' in row else 0.0,
                        'close': float(row['close']) if 'close' in row else 0.0,
                        'volume': int(row['volume']) if 'volume' in row else 0,
                        'amount': float(row['amount']) if 'amount' in row else 0.0
                    })

                logger.info(f"✓ {variety_name} ({fixed_code}) 获取到 {len(data_list)} 条5分钟数据")
                return data_list

            except Exception as e:
                logger.error(f"获取 {variety_name} ({fixed_code}) 数据失败 (尝试 {attempt + 1}/{retry}): {e}")
                if attempt < retry - 1:
                    time.sleep(2)  # 等待2秒后重试
                else:
                    logger.error(f"✗ {variety_name} ({fixed_code}) 数据获取失败，已重试 {retry} 次")
                    return None

        return None

    def collect_all_futures_5m_data(self) -> Dict[str, int]:
        """
        收集所有期货合约的5分钟数据

        Returns:
            统计信息字典，包含成功数量、失败数量、总数据量
        """
        logger.info("=" * 50)
        logger.info("开始收集期货5分钟数据")
        logger.info(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 获取所有合约列表
        contracts = self.get_all_contracts()

        if not contracts:
            logger.error("没有找到期货合约，无法收集数据")
            return {'success': 0, 'failed': 0, 'total': 0}

        # 统计信息
        success_count = 0
        failed_count = 0
        total_data_count = 0
        failed_contracts = []

        # 遍历收集数据
        for i, contract in enumerate(contracts, 1):
            contract_code = contract['contract_code']
            variety_name = contract['variety_name_cn']

            logger.info(f"[{i}/{len(contracts)}] 正在处理 {variety_name} ({contract_code})...")

            # 获取数据
            data_list = self.collect_futures_5m_data(contract_code, variety_name)

            if data_list:
                try:
                    # 存储到数据库
                    inserted_count = self.db.insert_futures_5m_data(data_list)
                    total_data_count += inserted_count
                    success_count += 1
                    logger.info(f"  ✓ 成功存储 {inserted_count} 条数据")
                except Exception as e:
                    logger.error(f"  ✗ 存储数据失败: {e}")
                    failed_count += 1
                    failed_contracts.append(f"{variety_name} ({contract_code})")
            else:
                failed_count += 1
                failed_contracts.append(f"{variety_name} ({contract_code})")

            # 避免请求过于频繁
            time.sleep(60)

        # 打印统计信息
        logger.info("=" * 50)
        logger.info("期货5分钟数据收集完成")
        logger.info(f"总合约数: {len(contracts)}")
        logger.info(f"成功: {success_count}")
        logger.info(f"失败: {failed_count}")
        logger.info(f"总数据量: {total_data_count} 条")

        if failed_contracts:
            logger.warning(f"失败的合约: {', '.join(failed_contracts)}")

        logger.info("=" * 50)

        return {
            'success': success_count,
            'failed': failed_count,
            'total': total_data_count
        }


def scheduled_futures_5m_collection():
    """
    定时期货5分钟数据收集任务
    每天凌晨5点执行
    """
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


def setup_futures_5m_scheduled_job(scheduler):
    """
    设置期货5分钟数据收集的定时任务

    Args:
        scheduler: APScheduler 调度器实例
    """
    from apscheduler.triggers.cron import CronTrigger

    scheduler.add_job(
        func=scheduled_futures_5m_collection,
        trigger=CronTrigger(hour=5, minute=0),  # 每天凌晨5点执行
        id='futures_5m_collection',
        name='期货5分钟数据收集任务',
        misfire_grace_time=300,  # 允许延迟5分钟内仍然执行
        replace_existing=True
    )

    logger.info("✓ 已注册期货5分钟数据收集定时任务: 每天凌晨5:00执行")


if __name__ == "__main__":
    # 测试运行 - 同时输出到控制台和文件
    import sys
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('sequoia.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger.info("测试期货5分钟数据收集功能")

    collector = Futures5MinCollector()
    stats = collector.collect_all_futures_5m_data()

    logger.info(f"测试完成: {stats}")
