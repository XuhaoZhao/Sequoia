"""
新浪期货K线数据采集器

专门用于采集分钟级K线数据
目标API: https://stock2.finance.sina.com.cn/futures/api/jsonp.php/.../InnerFuturesNewService.getFewMinLine?symbol=期货代码&type=类型

使用说明：
- type=0: 1分钟K线
- type=1: 5分钟K线
- type=2: 15分钟K线
- type=3: 30分钟K线
- type=4: 60分钟K线
- type=5: 日K线
- type=6: 周K线
- type=7: 月K线

数据格式：
[{"d":"2026-02-27 10:05:00","o":"5220.000","h":"5232.000","l":"5214.000","c":"5226.000","v":"15937","p":"1458815"}, ...]

字段说明：
- d: 日期时间
- o: 开盘价 (Open)
- h: 最高价 (High)
- l: 最低价 (Low)
- c: 收盘价 (Close)
- v: 成交量 (Volume)
- p: 持仓量 (Position)
"""

import time
import json
import re
from datetime import datetime
import requests
import csv
import os

# 导入日志系统
try:
    from .logger_config import FinancialLogger, get_logger
except ImportError:
    from logger_config import FinancialLogger, get_logger


class SinaFuturesKlineCollector:
    """
    新浪期货K线数据采集器
    """
    def __init__(self, log_full_data=False):
        """
        初始化采集器
        :param log_full_data: 是否记录完整数据
        """
        self.logger = get_logger('financial_framework.sina_futures_kline')
        self.data_logger = FinancialLogger.get_data_logger()
        self.log_full_data = log_full_data

        # 存储K线数据
        self.kline_data = []

    def fetch_kline_data(self, symbol="TA2605", kline_type=5):
        """
        获取K线数据
        :param symbol: 期货代码，如 "TA2605"
        :param kline_type: K线类型（0:1分, 1:5分, 2:15分, 3:30分, 4:60分, 5:日K, 6:周K, 7:月K）
        :return: K线数据列表
        """
        try:
            # 构造URL
            # URL格式: https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_TA2605_5_1773934511848=/InnerFuturesNewService.getFewMinLine?symbol=TA2605&type=5
            timestamp = int(time.time() * 1000)
            callback_name = f"var%20_{symbol}_{kline_type}_{timestamp}="
            url = f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/{callback_name}/InnerFuturesNewService.getFewMinLine?symbol={symbol}&type={kline_type}"

            self.logger.info(f"正在获取K线数据: {symbol}, 类型: {kline_type}")
            self.logger.info(f"请求URL: {url}")

            # 发送请求
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                body = response.text
                self.logger.info(f"响应大小: {len(body)} 字符")

                # 解析K线数据
                kline_list = self._parse_kline_data(body)

                if kline_list:
                    self.kline_data.extend(kline_list)
                    self.logger.info(f"[OK] 成功获取 {len(kline_list)} 条K线数据")

                    # 显示前几条数据
                    self._display_kline_data(kline_list[:5])

                    # 记录到日志
                    self._log_kline_data(symbol, kline_type, kline_list)

                    return kline_list
                else:
                    self.logger.warning("未获取到K线数据")
                    return None
            else:
                self.logger.error(f"请求失败，状态码: {response.status_code}")
                return None

        except Exception as e:
            self.logger.error(f"获取K线数据失败: {e}", exc_info=True)
            return None

    def _parse_kline_data(self, body: str) -> list:
        """
        解析K线数据
        :param body: 响应体
        :return: K线数据列表
        """
        try:
            # 提取JSON数组
            # 响应格式: /*<script>location.href='//sina.com';</script>*/
            #          var _TA2605_5_1773934511848=([{"d":"2026-02-27 10:05:00",...}]);

            # 步骤1: 去除script标签前缀（如果存在）
            # 找到第一个 "var" 的位置
            var_pos = body.find('var ')
            if var_pos == -1:
                self.logger.warning(f"未找到 'var' 关键字，响应前200字符: {body[:200]}")
                return None

            # 从 var 开始截取
            body = body[var_pos:]

            # 步骤2: 找到 "=([" 模式
            start_pattern = '=(['
            start_pos = body.find(start_pattern)
            if start_pos == -1:
                self.logger.warning(f"未找到 '=([ 模式，响应前200字符: {body[:200]}")
                return None

            # 步骤3: 从数组开始位置 "[{" 开始，找到匹配的结束位置
            # 我们需要找到最后的 "]);" 的位置
            array_start = start_pos + len(start_pattern) - 1  # 指向 [

            # 从后向前查找，找到 "]);"
            # 注意：最后是 "..."}]);" 而不是 "..."}]);
            end_pattern = ']);'
            end_pos = body.rfind(end_pattern)
            if end_pos == -1:
                self.logger.warning(f"未找到 ']);' 模式，响应后200字符: {body[-200:]}")
                return None

            # 提取JSON数组字符串 (从 [ 到 ])
            # end_pos指向]的位置，所以需要+1来包含]
            json_str = body[array_start:end_pos + 1]

            # 步骤4: 解析JSON
            kline_list = json.loads(json_str)

            self.logger.info(f"成功解析JSON数组，包含 {len(kline_list)} 条K线数据")

            parsed_list = []
            for item in kline_list:
                parsed_item = {
                    'datetime': item.get('d', ''),  # 日期时间
                    'open': float(item.get('o', 0)),  # 开盘价
                    'high': float(item.get('h', 0)),  # 最高价
                    'low': float(item.get('l', 0)),  # 最低价
                    'close': float(item.get('c', 0)),  # 收盘价
                    'volume': int(item.get('v', 0)),  # 成交量
                    'open_interest': int(item.get('p', 0))  # 持仓量
                }
                parsed_list.append(parsed_item)

            return parsed_list
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
            self.logger.error(f"JSON字符串前200字符: {json_str[:200] if 'json_str' in locals() else 'N/A'}")
            self.logger.error(f"JSON字符串后200字符: {json_str[-200:] if 'json_str' in locals() else 'N/A'}")
            return None
        except Exception as e:
            self.logger.error(f"解析K线数据失败: {e}", exc_info=True)
            return None

    def _display_kline_data(self, kline_list):
        """显示K线数据"""
        self.logger.info("=" * 80)
        self.logger.info("K线数据预览 (前5条):")
        self.logger.info("=" * 80)

        for i, item in enumerate(kline_list, 1):
            self.logger.info(f"第{i}条: {item['datetime']}")
            self.logger.info(f"  开盘:{item['open']} 最高:{item['high']} 最低:{item['low']} 收盘:{item['close']}")
            self.logger.info(f"  成交量:{item['volume']} 持仓量:{item['open_interest']}")
            self.logger.info("-" * 80)

    def _log_kline_data(self, symbol, kline_type, kline_list):
        """记录K线数据到日志"""
        api_info = {
            'symbol': symbol,
            'kline_type': kline_type,
            'data_count': len(kline_list),
            'fetch_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'fetch_timestamp': datetime.now().isoformat(),
            'data': kline_list if self.log_full_data else f"{len(kline_list)}条数据(仅摘要模式)"
        }

        self.data_logger.info("=" * 80)
        self.data_logger.info(f"K线数据采集: {symbol}, 类型: {kline_type}")
        self.data_logger.info("=" * 80)
        self.data_logger.info(f"数据:\n{json.dumps(api_info, ensure_ascii=False, indent=2)}")
        self.data_logger.info("=" * 80)

    def save_to_csv(self, csv_file_path="futures_kline_data.csv"):
        """
        保存K线数据到CSV文件
        :param csv_file_path: CSV文件路径
        :return: 是否成功
        """
        try:
            if not self.kline_data:
                self.logger.warning("没有K线数据可保存")
                return False

            # 确保目录存在
            os.makedirs(os.path.dirname(csv_file_path) if os.path.dirname(csv_file_path) else '.', exist_ok=True)

            # 准备数据
            fieldnames = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'open_interest']

            with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # 写入表头
                writer.writeheader()
                self.logger.info(f"创建K线数据CSV文件: {csv_file_path}")

                # 写入数据
                writer.writerows(self.kline_data)

                self.logger.info(f"[OK] 成功写入 {len(self.kline_data)} 条K线数据到CSV文件")
                return True

        except Exception as e:
            self.logger.error(f"保存K线数据到CSV失败: {e}", exc_info=True)
            return False


def main():
    """
    主函数 - 演示如何使用K线数据采集器
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.sina_futures_kline_main')
    logger.info("=" * 80)
    logger.info("新浪期货K线数据采集器")
    logger.info("=" * 80)

    # 初始化采集器
    collector = SinaFuturesKlineCollector(log_full_data=False)

    try:
        # 示例：获取TA2605的5分钟K线数据
        logger.info("开始获取K线数据...")
        kline_data = collector.fetch_kline_data(
            symbol="TA2605",  # 期货代码
            kline_type=1      # 1: 5分钟K线, 0:1分钟, 5:日K
        )

        if kline_data:
            # 保存到CSV
            logger.info("保存K线数据到CSV文件")
            collector.save_to_csv("data/futures_kline_5min.csv")
        else:
            logger.warning("未获取到K线数据")

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
