"""
新浪期货行情 API 拦截器

使用 Selenium/Playwright 拦截新浪期货页面的实时行情数据
目标API: http://hq.sinajs.cn/list=期货代码
数据格式: JSONP回调，数据存储在 window.hq_str_nf_期货代码 变量中

特点：
1. 拦截新浪期货实时行情接口
2. 解析JSONP返回的数据
3. 支持多个期货合约的监听
4. 实时打印拦截到的数据
5. 使用统一日志系统记录所有操作
6. 支持数据导出到CSV文件
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
    from .selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager


class SinaFuturesAPIInterceptor:
    """
    使用 Selenium 拦截新浪期货页面的实时行情数据
    """
    def __init__(self, headless=False, log_full_data=False, log_summary_only=False):
        """
        初始化拦截器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param log_full_data: 是否记录完整的请求和响应数据到数据日志（默认False）
        :param log_summary_only: 是否只记录关键摘要信息到控制台（默认False，True时仅显示关键统计信息）
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.sina_futures_interceptor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.sina_futures_interceptor.browser'
        )

        # API URL模式
        self.api_patterns = {
            'realtime': "hq.sinajs.cn",  # 实时行情
            'kline': "InnerFuturesNewService.getFewMinLine"  # K线数据
        }

        # 用于跟踪已处理的request_id，避免重复处理
        self.processed_request_ids = set()

        # 存储拦截到的数据
        self.intercepted_data = []
        self.kline_data = []  # K线数据

        # 日志控制参数
        self.log_full_data = log_full_data
        self.log_summary_only = log_summary_only

        self.logger.info("初始化新浪期货API拦截器")
        self.logger.info(f"日志配置: 完整数据={log_full_data}, 仅摘要={log_summary_only}")
        self.browser_manager.init_driver()

    def _matches_api_pattern(self, url: str) -> str:
        """
        检查URL是否匹配API模式
        :param url: 要检查的URL
        :return: API类型 ('realtime', 'kline') 或 None
        """
        if not url:
            return None

        for api_type, pattern in self.api_patterns.items():
            if pattern in url:
                return api_type
        return None

    def _parse_futures_data(self, data_string: str, symbol: str) -> dict:
        """
        解析期货数据字符串
        :param data_string: 数据字符串，如 "PTA2605,225959,6790.000,6858.000,..."
        :param symbol: 期货代码，如 "TA2605"
        :return: 解析后的数据字典
        """
        try:
            # 分割字符串
            parts = data_string.split(',')

            if len(parts) < 17:
                return None

            # 解析数据
            data = {
                'symbol': parts[0],  # 期货代码
                'time': parts[1],  # 时间 HHMMSS
                'open': float(parts[2]) if parts[2] else 0,  # 开盘价
                'high': float(parts[3]) if parts[3] else 0,  # 最高价
                'low': float(parts[4]) if parts[4] else 0,  # 最低价
                'settlement': float(parts[5]) if parts[5] else 0,  # 结算价
                'close': float(parts[6]) if parts[6] else 0,  # 最新价
                'ask1': float(parts[7]) if parts[7] else 0,  # 卖一价
                'bid1': float(parts[8]) if parts[8] else 0,  # 买一价
                'ask2': float(parts[9]) if parts[9] else 0,  # 卖二价
                'prev_settlement': float(parts[10]) if parts[10] else 0,  # 昨结算
                'bid2': float(parts[11]) if parts[11] else 0,  # 买二价
                'bid3': float(parts[12]) if parts[12] else 0,  # 买三量
                'open_interest': float(parts[13]) if parts[13] else 0,  # 持仓量
                'volume': int(parts[14]) if parts[14] else 0,  # 成交量
                'exchange': parts[15],  # 交易所
                'name': parts[16],  # 品种名称
                'date': parts[17] if len(parts) > 17 else '',  # 日期
                'raw_data': data_string  # 原始数据
            }

            # 计算涨跌幅
            if data['prev_settlement'] > 0:
                data['change'] = data['close'] - data['prev_settlement']
                data['change_percent'] = (data['change'] / data['prev_settlement']) * 100
            else:
                data['change'] = 0
                data['change_percent'] = 0

            return data
        except Exception as e:
            self.logger.error(f"解析期货数据失败: {e}, 数据: {data_string[:100]}")
            return None

    def _parse_kline_data(self, body: str) -> list:
        """
        解析K线数据
        :param body: 响应体，如 "var _TA2605_5_1773934511848=([...]);"
        :return: K线数据列表
        """
        try:
            # 提取JSON数组
            # 响应格式: /*<script>location.href='//sina.com';</script>*/
            #          var _TA2605_5_1773934511848=([{"d":"2026-02-27 10:05:00",...}]);

            # 步骤1: 去除script标签前缀（如果存在）
            var_pos = body.find('var ')
            if var_pos == -1:
                self.logger.warning(f"未找到 'var' 关键字，响应前200字符: {body[:200]}")
                return None

            body = body[var_pos:]

            # 步骤2: 找到 "=([" 模式
            start_pattern = '=(['
            start_pos = body.find(start_pattern)
            if start_pos == -1:
                self.logger.warning(f"未找到 '=([ 模式，响应前200字符: {body[:200]}")
                return None

            # 步骤3: 从数组开始位置 "[{" 开始，找到匹配的结束位置
            array_start = start_pos + len(start_pattern) - 1  # 指向 [

            # 从后向前查找，找到 "]);"
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
            self.logger.error(f"解析K线数据失败: {e}, 响应: {body[:200]}")
            return None

    def start_interception(self, symbol="TA2605", duration=60, check_interval=1):
        """
        持续拦截期货实时数据
        :param symbol: 期货代码（如 "TA2605"）
        :param duration: 监听时长（秒）
        :param check_interval: 检查网络日志的时间间隔（秒）
        """
        try:
            # 构造目标URL
            url = f"https://finance.sina.com.cn/futures/quotes/{symbol}.shtml"

            self.logger.info("=" * 80)
            self.logger.info(f"访问页面: {url}")
            self.logger.info(f"目标API: {self.api_pattern}")
            self.logger.info(f"监听时长: {duration}秒")
            self.logger.info(f"检查间隔: {check_interval}秒")
            self.logger.info("=" * 80)

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            self.logger.info("=" * 80)
            self.logger.info("开始持续监听期货实时行情API...")
            self.logger.info(f"按 Ctrl+C 停止监听")
            self.logger.info("=" * 80)

            intercept_count = 0
            start_time = time.time()

            # 持续监听
            while time.time() - start_time < duration:
                try:
                    # 获取性能日志
                    logs = self.browser_manager.get_performance_logs()

                    for log in logs:
                        message = json.loads(log['message'])
                        method = message.get('message', {}).get('method', '')

                        # 查找网络响应
                        if method == 'Network.responseReceived':
                            params = message.get('message', {}).get('params', {})
                            response = params.get('response', {})
                            request_id = params.get('requestId', '')
                            url_received = response.get('url', '')
                            status = response.get('status', 0)

                            # 只处理期货行情API
                            if self._matches_api_pattern(url_received) and request_id not in self.processed_request_ids:
                                self.processed_request_ids.add(request_id)

                                # 尝试获取响应内容
                                try:
                                    body = self.browser_manager.get_response_body(request_id)

                                    if body and len(body) > 10:
                                        intercept_count += 1

                                        # 控制台日志：只显示关键信息
                                        if not self.log_summary_only:
                                            self.logger.info("=" * 80)
                                            self.logger.info(f"[OK] 拦截到第 {intercept_count} 个响应")
                                            self.logger.info(f"URL: {url_received}")
                                            self.logger.info(f"状态码: {status}")
                                        else:
                                            self.logger.info(f"[OK] 拦截响应 #{intercept_count} | URL:{url_received}")

                                        # 解析期货代码
                                        # URL格式: http://hq.sinajs.cn/list=TA2605
                                        symbol_match = re.search(r'list=([^&]+)', url_received)
                                        if symbol_match:
                                            symbol_code = symbol_match.group(1)

                                            # 尝试从响应体中提取数据
                                            # 响应格式: var hq_str_nf_TA2605="PTA2605,225959,...";
                                            data_match = re.search(r'var hq_str_[^=]+="([^"]+)"', body)
                                            if data_match:
                                                data_string = data_match.group(1)
                                                parsed_data = self._parse_futures_data(data_string, symbol_code)

                                                if parsed_data:
                                                    self.intercepted_data.append(parsed_data)

                                                    if not self.log_summary_only:
                                                        self.logger.info(f"期货代码: {parsed_data['symbol']}")
                                                        self.logger.info(f"品种: {parsed_data['name']}")
                                                        self.logger.info(f"最新价: {parsed_data['close']}")
                                                        self.logger.info(f"涨跌: {parsed_data['change']:+.2f} ({parsed_data['change_percent']:+.2f}%)")
                                                        self.logger.info(f"开盘: {parsed_data['open']}")
                                                        self.logger.info(f"最高: {parsed_data['high']}")
                                                        self.logger.info(f"最低: {parsed_data['low']}")
                                                        self.logger.info(f"成交量: {parsed_data['volume']}")
                                                        self.logger.info(f"持仓量: {parsed_data['open_interest']}")
                                                        self.logger.info(f"时间: {parsed_data['date']} {parsed_data['time']}")
                                                        self.logger.info("=" * 80)
                                                    else:
                                                        self.logger.info(f"  {parsed_data['symbol']} | 价格:{parsed_data['close']} | "
                                                                       f"涨跌:{parsed_data['change']:+.2f} ({parsed_data['change_percent']:+.2f}%) | "
                                                                       f"成交量:{parsed_data['volume']} | 持仓:{parsed_data['open_interest']}")

                                                    # 记录到数据日志
                                                    api_info = {
                                                        'url': url_received,
                                                        'status': status,
                                                        'symbol': symbol_code,
                                                        'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                        'intercept_timestamp': datetime.now().isoformat(),
                                                        'intercept_index': intercept_count,
                                                        'data': parsed_data
                                                    }

                                                    self.data_logger.info("=" * 80)
                                                    self.data_logger.info(f"拦截期货数据 #{intercept_count}")
                                                    self.data_logger.info("=" * 80)
                                                    self.data_logger.info(f"数据:\\n{json.dumps(api_info, ensure_ascii=False, indent=2)}")
                                                    self.data_logger.info("=" * 80)

                                except Exception as e:
                                    self.logger.debug(f"获取响应体失败: {e}")

                    # 等待一段时间再检查
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    self.logger.info("=" * 80)
                    self.logger.info("用户停止监听")
                    self.logger.info(f"总共拦截到 {intercept_count} 个响应")
                    self.logger.info("=" * 80)
                    break

        except Exception as e:
            self.logger.error(f"API拦截失败: {e}", exc_info=True)

    def get_intercepted_data(self):
        """
        获取所有拦截到的数据
        :return: 拦截数据列表
        """
        return self.intercepted_data

    def save_to_csv(self, csv_file_path="futures_data.csv"):
        """
        将拦截到的数据保存到CSV文件
        :param csv_file_path: CSV文件保存路径
        :return: 保存是否成功
        """
        try:
            if not self.intercepted_data:
                self.logger.warning("没有数据可保存到CSV文件")
                return False

            # 确保目录存在
            os.makedirs(os.path.dirname(csv_file_path) if os.path.dirname(csv_file_path) else '.', exist_ok=True)

            # 准备数据
            fieldnames = ['symbol', 'name', 'date', 'time', 'close', 'change', 'change_percent',
                         'open', 'high', 'low', 'settlement', 'prev_settlement',
                         'bid1', 'ask1', 'volume', 'open_interest', 'exchange',
                         'intercept_time']

            with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # 写入表头
                writer.writeheader()
                self.logger.info(f"创建CSV文件并写入表头: {csv_file_path}")

                # 写入数据
                for data in self.intercepted_data:
                    row = {
                        'symbol': data['symbol'],
                        'name': data['name'],
                        'date': data['date'],
                        'time': data['time'],
                        'close': data['close'],
                        'change': data['change'],
                        'change_percent': data['change_percent'],
                        'open': data['open'],
                        'high': data['high'],
                        'low': data['low'],
                        'settlement': data['settlement'],
                        'prev_settlement': data['prev_settlement'],
                        'bid1': data['bid1'],
                        'ask1': data['ask1'],
                        'volume': data['volume'],
                        'open_interest': data['open_interest'],
                        'exchange': data['exchange'],
                        'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    writer.writerow(row)

                self.logger.info(f"[OK] 成功写入 {len(self.intercepted_data)} 条数据到CSV文件")
                self.logger.info(f"文件路径: {csv_file_path}")
                return True

        except Exception as e:
            self.logger.error(f"保存数据到CSV文件失败: {e}", exc_info=True)
            return False

    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 启动新浪期货API拦截器
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.sina_futures_interceptor_main')
    logger.info("=" * 80)
    logger.info("新浪期货行情 API 拦截器")
    logger.info("=" * 80)

    # 初始化拦截器
    interceptor = SinaFuturesAPIInterceptor(
        headless=False,
        log_full_data=False,      # 设为 True 则记录完整数据
        log_summary_only=True     # 设为 False 则显示详细日志
    )

    if not interceptor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    try:
        # 示例：拦截TA2605期货合约的实时数据，监听60秒
        logger.info("开始拦截新浪期货实时数据")
        interceptor.start_interception(
            symbol="TA2605",        # 期货代码
            duration=60,            # 监听60秒
            check_interval=1        # 每1秒检查一次网络日志
        )

        # 保存数据到CSV
        if interceptor.get_intercepted_data():
            logger.info("保存拦截到的数据到CSV文件")
            interceptor.save_to_csv("data/sina_futures_data.csv")
        else:
            logger.warning("没有拦截到任何数据")

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    finally:
        interceptor.close()

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
