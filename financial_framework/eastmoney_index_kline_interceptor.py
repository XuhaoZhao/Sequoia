"""
东方财富指数K线数据拦截器

使用 Selenium 拦截东方财富指数页面的 K线数据 API
目标页面: https://quote.eastmoney.com/zz/1.000001.html
目标API: eastmoney.com/api/qt/stock/kline/get

特点：
1. 拦截指定指数页面的K线数据API
2. 支持多种指数类型（上证指数、深证成指等）
3. 自动解析K线数据为DataFrame格式
4. 使用统一日志系统记录所有操作
5. 支持自定义日期范围
"""

import time
import json
import pandas as pd
from datetime import datetime

# 导入日志系统
try:
    from .logger_config import FinancialLogger, get_logger
    from .selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager


class EastmoneyIndexKlineInterceptor:
    """
    使用 Selenium 拦截东方财富指数页面的 K线数据 API
    """
    def __init__(self, headless=False, log_full_data=False, log_summary_only=False):
        """
        初始化拦截器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param log_full_data: 是否记录完整的请求和响应数据到数据日志（默认False）
        :param log_summary_only: 是否只记录关键摘要信息到控制台（默认False，True时仅显示关键统计信息）
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.eastmoney_kline_interceptor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.eastmoney_kline_interceptor.browser'
        )

        # K线API URL模板
        self.kline_api_pattern = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

        # 指数代码映射表
        self.index_code_map = {
            "000001": {"market": "1", "name": "上证指数", "url_suffix": "1.000001"},
            "399001": {"market": "0", "name": "深证成指", "url_suffix": "0.399001"},
            "399006": {"market": "0", "name": "创业板指", "url_suffix": "0.399006"},
            "000300": {"market": "1", "name": "沪深300", "url_suffix": "1.000300"},
            "000016": {"market": "1", "name": "上证50", "url_suffix": "1.000016"},
            "000905": {"market": "1", "name": "中证500", "url_suffix": "1.000905"},
            "000852": {"market": "1", "name": "中证1000", "url_suffix": "1.000852"}
        }

        # 日志控制参数
        self.log_full_data = log_full_data
        self.log_summary_only = log_summary_only

        self.logger.info("初始化东方财富指数K线数据拦截器")
        self.logger.info(f"日志配置: 完整数据={log_full_data}, 仅摘要={log_summary_only}")
        self.browser_manager.init_driver()

    def intercept_kline_data(self, symbol="000001", period="daily", check_interval=1, wait_time=10):
        """
        拦截指定指数的K线数据
        :param symbol: 指数代码（默认："000001" 上证指数）
        :param period: 周期（"daily", "weekly", "monthly"）
        :param check_interval: 检查网络日志的时间间隔（秒）
        :param wait_time: 等待页面加载的时间（秒）
        """
        try:
            # 验证指数代码
            if symbol not in self.index_code_map:
                self.logger.error(f"不支持的指数代码: {symbol}")
                self.logger.info(f"支持的指数代码: {', '.join(self.index_code_map.keys())}")
                return None

            index_info = self.index_code_map[symbol]

            # 构造目标页面URL
            page_url = f"https://quote.eastmoney.com/zz/{index_info['url_suffix']}.html"

            self.logger.info("=" * 80)
            self.logger.info(f"访问页面: {page_url}")
            self.logger.info(f"指数代码: {symbol} ({index_info['name']})")
            self.logger.info(f"市场代码: {index_info['market']}")
            self.logger.info(f"周期: {period}")
            self.logger.info(f"目标API特征: {self.kline_api_pattern}")
            self.logger.info("=" * 80)

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(page_url)

            self.logger.info("=" * 80)
            self.logger.info("开始监听K线数据API...")
            self.logger.info(f"等待时间: {wait_time}秒")
            self.logger.info("按 Ctrl+C 停止监听")
            self.logger.info("=" * 80)

            # 用于跟踪已处理的request_id，避免重复处理
            processed_request_ids = set()
            # 存储请求信息，key为request_id
            pending_requests = {}
            intercept_count = 0
            start_time = time.time()

            # 持续监听指定时间
            while time.time() - start_time < wait_time:
                try:
                    # 获取性能日志
                    logs = self.browser_manager.get_performance_logs()

                    for log in logs:
                        message = json.loads(log['message'])
                        method = message.get('message', {}).get('method', '')

                        # 拦截请求发送，保存请求数据
                        if method == 'Network.requestWillBeSent':
                            params = message.get('message', {}).get('params', {})
                            request = params.get('request', {})
                            request_id = params.get('requestId', '')
                            url_sent = request.get('url', '')

                            # 只处理K线API的请求
                            if self.kline_api_pattern in url_sent:
                                pending_requests[request_id] = {
                                    'request_url': url_sent,
                                    'request_method': request.get('method', ''),
                                    'request_headers': request.get('headers', {}),
                                    'request_time': datetime.now().isoformat()
                                }

                        # 查找网络响应
                        if method == 'Network.responseReceived':
                            params = message.get('message', {}).get('params', {})
                            response = params.get('response', {})
                            request_id = params.get('requestId', '')
                            url_received = response.get('url', '')
                            status = response.get('status', 0)
                            mime_type = response.get('mimeType', '')

                            # 只处理K线API
                            if self.kline_api_pattern in url_received and request_id not in processed_request_ids:
                                processed_request_ids.add(request_id)

                                # 尝试获取响应内容
                                try:
                                    body = self.browser_manager.get_response_body(request_id)

                                    if body and len(body) > 10:  # 至少要有一些内容
                                        intercept_count += 1

                                        # 控制台日志：只显示关键信息
                                        if not self.log_summary_only:
                                            self.logger.info("=" * 80)
                                            self.logger.info(f"✓ 拦截到第 {intercept_count} 个K线数据响应")
                                            self.logger.info(f"URL: {url_received}")
                                            self.logger.info(f"状态码: {status}")
                                            self.logger.info(f"响应大小: {len(body)} 字符")
                                        else:
                                            self.logger.info(f"✓ 拦截K线响应 #{intercept_count} | 状态:{status} | 大小:{len(body)}字符")

                                        # 解析K线数据
                                        kline_data = self._parse_kline_response(body, symbol, period)

                                        if kline_data is not None:
                                            self.logger.info(f"✓ 成功解析K线数据: {len(kline_data)} 条记录")

                                            if not self.log_summary_only:
                                                self.logger.info("数据预览:")
                                                self.logger.info(kline_data.head().to_string())

                                            # 记录API信息到数据日志
                                            api_info = {
                                                'url': url_received,
                                                'status': status,
                                                'mime_type': mime_type,
                                                'request_id': request_id,
                                                'method': response.get('requestMethod', 'GET'),
                                                'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                'intercept_timestamp': datetime.now().isoformat(),
                                                'intercept_index': intercept_count,
                                                'symbol': symbol,
                                                'symbol_name': index_info['name'],
                                                'period': period,
                                                'data_count': len(kline_data),
                                                'date_range': {
                                                    'start': kline_data['日期'].min() if len(kline_data) > 0 else None,
                                                    'end': kline_data['日期'].max() if len(kline_data) > 0 else None
                                                }
                                            }

                                            # 只在需要时添加完整响应体
                                            if self.log_full_data:
                                                api_info['response_body'] = body
                                                api_info['data_preview'] = kline_data.head(10).to_dict('records')

                                            # 添加请求数据
                                            if request_id in pending_requests:
                                                request_info = pending_requests[request_id]
                                                api_info['request_url'] = request_info['request_url']
                                                api_info['request_method'] = request_info['request_method']
                                                api_info['request_time'] = request_info['request_time']

                                                # 只在需要完整数据时记录请求头
                                                if self.log_full_data:
                                                    api_info['request_headers'] = request_info['request_headers']

                                                # 清理已使用的请求信息
                                                del pending_requests[request_id]

                                            # 将API信息记录到数据日志
                                            self.data_logger.info("=" * 80)
                                            self.data_logger.info(f"拦截K线API数据 #{intercept_count}")
                                            self.data_logger.info("=" * 80)
                                            self.data_logger.info(f"API信息:\n{json.dumps(api_info, ensure_ascii=False, indent=2)}")
                                            self.data_logger.info("=" * 80)

                                            if not self.log_summary_only:
                                                self.logger.info(f"✓ K线数据已记录到日志")
                                                self.logger.info("=" * 80)

                                            # 返回解析的数据
                                            return kline_data

                                except Exception as e:
                                    # 获取响应体失败，可能是流式响应还没有数据
                                    self.logger.debug(f"获取响应体失败: {e}")

                    # 等待一段时间再检查
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    self.logger.info("用户停止监听")
                    break

            self.logger.info("=" * 80)
            self.logger.info(f"监听结束，总共拦截到 {intercept_count} 个响应")
            self.logger.info("=" * 80)
            return None

        except Exception as e:
            self.logger.error(f"K线数据拦截失败: {e}", exc_info=True)
            return None

    def _parse_kline_response(self, response_body, symbol, period):
        """
        解析K线数据响应，参考 rewrite_index_zh_em.py 的解析逻辑
        处理JSONP格式的响应
        :param response_body: API响应内容
        :param symbol: 指数代码
        :param period: 周期
        :return: 解析后的DataFrame
        """
        try:
            response_json = None

            # 检查是否是JSONP格式（jQuery_callback(...)）
            if response_body.startswith('jQuery') and '(' in response_body:
                # 移除JSONP包装
                json_start = response_body.find('(') + 1
                json_end = response_body.rfind(')')

                if json_end > json_start:
                    json_content = response_body[json_start:json_end]
                else:
                    # 如果找不到结束括号，从第一个括号开始到最后
                    json_content = response_body[json_start:]

                self.logger.info(f"检测到JSONP格式，提取JSON内容长度: {len(json_content)}")
                self.logger.debug(f"JSON内容前100字符: {json_content[:100]}")

                try:
                    response_json = json.loads(json_content)
                    self.logger.info("✓ JSONP格式解析成功")
                except json.JSONDecodeError as json_e:
                    self.logger.error(f"JSONP内容解析失败: {json_e}")
                    self.logger.debug(f"JSON内容前200字符: {json_content[:200]}")
                    return None
            else:
                # 标准JSON格式
                try:
                    response_json = json.loads(response_body)
                    self.logger.info("✓ 响应为标准JSON格式")
                except json.JSONDecodeError as e:
                    self.logger.error(f"响应JSON解析失败: {e}")
                    self.logger.debug(f"原始响应前500字符: {response_body[:500]}")
                    return None

            # 检查响应数据结构
            if not response_json.get('data') or not response_json['data'].get('klines'):
                self.logger.warning("响应数据中没有K线数据")
                self.logger.debug(f"响应结构: {list(response_json.keys()) if response_json else 'None'}")
                if response_json and 'data' in response_json:
                    data_keys = list(response_json['data'].keys()) if response_json['data'] else []
                    self.logger.debug(f"data字段包含: {data_keys}")
                return None

            klines = response_json['data']['klines']
            self.logger.info(f"获取到K线数据: {len(klines)} 条记录")

            # 解析K线数据，仿照 rewrite_index_zh_em.py 的逻辑
            data_list = []
            for kline_str in klines:
                # 按逗号分割K线数据
                fields = kline_str.split(",")
                if len(fields) >= 11:
                    data_list.append(fields)

            if not data_list:
                self.logger.warning("没有有效的K线数据")
                return None

            # 创建DataFrame
            temp_df = pd.DataFrame(data_list)

            # 设置列名，与 rewrite_index_zh_em.py 保持一致
            temp_df.columns = [
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
            ]

            # 转换数据类型
            temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce")
            temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
            temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
            temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
            temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
            temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
            temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
            temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
            temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
            temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
            temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")

            # 过滤掉日期转换失败的行
            valid_rows = temp_df['日期'].notna()
            temp_df = temp_df[valid_rows].reset_index(drop=True)

            if len(temp_df) == 0:
                self.logger.warning("所有行的日期转换都失败")
                return None

            # 按日期排序
            temp_df = temp_df.sort_values('日期').reset_index(drop=True)

            self.logger.info(f"成功解析K线数据: {len(temp_df)} 条记录")
            self.logger.info(f"日期范围: {temp_df['日期'].min()} 到 {temp_df['日期'].max()}")

            # 显示数据样本
            if not self.log_summary_only:
                self.logger.info("数据样本（前3行）:")
                for i, row in temp_df.head(3).iterrows():
                    self.logger.info(f"  {i+1}. {row['日期'].strftime('%Y-%m-%d')} 开:{row['开盘']} 高:{row['最高']} 低:{row['最低']} 收:{row['收盘']}")

            return temp_df

        except Exception as e:
            self.logger.error(f"解析K线数据时发生错误: {e}", exc_info=True)
            return None

    def get_kline_data_by_date_range(self, symbol="000001", period="daily", start_date="20240101", end_date=None, max_attempts=3):
        """
        获取指定日期范围的K线数据
        :param symbol: 指数代码
        :param period: 周期
        :param start_date: 开始日期 (YYYYMMDD格式)
        :param end_date: 结束日期 (YYYYMMDD格式，默认为当前日期)
        :param max_attempts: 最大尝试次数
        :return: K线数据DataFrame或None
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')

            self.logger.info("=" * 80)
            self.logger.info(f"获取K线数据")
            self.logger.info(f"指数: {symbol} ({self.index_code_map.get(symbol, {}).get('name', '未知')})")
            self.logger.info(f"日期范围: {start_date} 到 {end_date}")
            self.logger.info(f"周期: {period}")
            self.logger.info("=" * 80)

            # 多次尝试获取数据
            for attempt in range(1, max_attempts + 1):
                self.logger.info(f"第 {attempt} 次尝试获取数据...")

                kline_data = self.intercept_kline_data(
                    symbol=symbol,
                    period=period,
                    check_interval=1,
                    wait_time=15  # 每次等待15秒
                )

                if kline_data is not None:
                    # 按日期范围过滤数据
                    start_date_dt = pd.to_datetime(start_date, format='%Y%m%d')
                    end_date_dt = pd.to_datetime(end_date, format='%Y%m%d')

                    filtered_data = kline_data[
                        (kline_data['日期'] >= start_date_dt) &
                        (kline_data['日期'] <= end_date_dt)
                    ].reset_index(drop=True)

                    self.logger.info("=" * 80)
                    self.logger.info(f"✓ 成功获取K线数据")
                    self.logger.info(f"原始数据: {len(kline_data)} 条记录")
                    self.logger.info(f"过滤后数据: {len(filtered_data)} 条记录")
                    self.logger.info(f"实际日期范围: {filtered_data['日期'].min() if len(filtered_data) > 0 else '无'} 到 {filtered_data['日期'].max() if len(filtered_data) > 0 else '无'}")
                    self.logger.info("=" * 80)

                    return filtered_data
                else:
                    self.logger.warning(f"第 {attempt} 次尝试失败")
                    if attempt < max_attempts:
                        self.logger.info("等待5秒后重试...")
                        time.sleep(5)

            self.logger.error(f"经过 {max_attempts} 次尝试，仍无法获取K线数据")
            return None

        except Exception as e:
            self.logger.error(f"获取K线数据失败: {e}", exc_info=True)
            return None

    def add_index_code(self, symbol, market, name, url_suffix):
        """
        添加新的指数代码映射
        :param symbol: 指数代码
        :param market: 市场代码
        :param name: 指数名称
        :param url_suffix: URL后缀
        """
        self.index_code_map[symbol] = {
            "market": market,
            "name": name,
            "url_suffix": url_suffix
        }
        self.logger.info(f"添加新的指数代码: {symbol} ({name})")

    def get_supported_indices(self):
        """
        获取支持的指数列表
        :return: 支持的指数代码列表
        """
        return list(self.index_code_map.keys())

    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 启动K线数据拦截器
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.eastmoney_kline_interceptor_main')
    logger.info("=" * 80)
    logger.info("东方财富指数K线数据拦截器")
    logger.info("=" * 80)

    # 初始化拦截器
    logger.info("=" * 80)
    interceptor = EastmoneyIndexKlineInterceptor(
        headless=False,
        log_full_data=False,      # 设为 True 则记录完整数据
        log_summary_only=False    # 设为 True 则只显示摘要信息
    )

    if not interceptor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    # 显示支持的指数
    supported_indices = interceptor.get_supported_indices()
    logger.info(f"支持的指数代码: {', '.join(supported_indices)}")

    try:
        # 获取上证指数K线数据
        logger.info("开始获取上证指数K线数据...")
        kline_data = interceptor.get_kline_data_by_date_range(
            symbol="000001",           # 上证指数
            period="daily",            # 日线
            start_date="20240101",     # 2024年1月1日开始
            end_date="20241231",       # 2024年12月31日结束
            max_attempts=3
        )

        if kline_data is not None:
            logger.info("✓ K线数据获取成功")
            logger.info(f"数据形状: {kline_data.shape}")
            logger.info("前5行数据:")
            logger.info(kline_data.head().to_string())

            # 保存数据到CSV
            csv_filename = f"data/index_kline_000001_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            kline_data.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            logger.info(f"✓ 数据已保存到: {csv_filename}")
        else:
            logger.warning("✗ K线数据获取失败")

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    finally:
        interceptor.close()

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()