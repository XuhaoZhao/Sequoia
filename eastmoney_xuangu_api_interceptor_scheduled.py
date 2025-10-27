"""
东方财富选股 search-code API 拦截器

使用 Selenium 拦截东方财富选股页面的 search-code API
目标API: https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code

特点：
1. 只拦截 search-code API
2. 过滤掉流式响应中无数据的响应
3. 每5分钟自动刷新页面
4. 使用统一日志系统记录所有操作
"""

import time
import json
from datetime import datetime
import requests
import random
import string
import csv
import os
from db_manager import IndustryDataDB
# 导入日志系统
try:
    from financial_framework.logger_config import FinancialLogger, get_logger
    from financial_framework.selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager

class EastmoneySearchCodeInterceptor:
    """
    使用 Selenium 拦截东方财富选股页面的 search-code API
    """
    def __init__(self, headless=False, log_full_data=False, log_summary_only=False, use_database=True):
        """
        初始化拦截器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param log_full_data: 是否记录完整的请求和响应数据到数据日志（默认False）
        :param log_summary_only: 是否只记录关键摘要信息到控制台（默认False，True时仅显示关键统计信息）
        :param use_database: 是否使用数据库保存数据（默认True）
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.eastmoney_interceptor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.eastmoney_interceptor.browser'
        )

        # 初始化数据库连接
        self.use_database = use_database
        try:
            # 延迟导入数据库管理器
            self.db = IndustryDataDB("industry_data.db")
            self.logger.info("✓ 数据库连接已建立，将使用数据库保存数据")
        except Exception as e:
            self.logger.warning(f"数据库初始化失败: {e}，将使用CSV保存方式")
            self.use_database = False
            self.db = None

              # API URL映射表
        self.api_url_map = {
            "stock": "https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code",
            "etf": "https://np-tjxg-g.eastmoney.com/api/smart-tag/etf/v3/pw/search-code",
            "fund": "https://np-tjxg-g.eastmoney.com/api/smart-tag/fund/v3/pw/search-code",
            "bond": "https://np-tjxg-g.eastmoney.com/api/smart-tag/bond/v3/pw/search-code"
        }
        self.target_api_url = self.api_url_map.get("stock", self.api_url_map["stock"])  # 默认使用stock类型
        self.requested_pages = set()  # 用于跟踪已请求过的页码,避免重复请求

        # 日志控制参数
        self.log_full_data = log_full_data
        self.log_summary_only = log_summary_only

        self.logger.info("初始化东方财富选股API拦截器")
        self.logger.info(f"日志配置: 完整数据={log_full_data}, 仅摘要={log_summary_only}")
        self.browser_manager.init_driver()


    def _randomize_request_id(self, original_id):
        """
        随机替换requestId中的几个字符,避免反爬
        :param original_id: 原始的requestId
        :return: 修改后的requestId
        """
        if not original_id or not isinstance(original_id, str):
            return original_id

        # 转换为列表以便修改
        id_chars = list(original_id)
        id_length = len(id_chars)

        if id_length == 0:
            return original_id

        # 随机替换3-5个字符(根据长度调整)
        num_chars_to_replace = min(random.randint(3, 5), id_length // 3)

        # 随机选择要替换的位置
        positions_to_replace = random.sample(range(id_length), num_chars_to_replace)

        for pos in positions_to_replace:
            original_char = id_chars[pos]

            # 根据原字符类型选择替换字符
            if original_char.isdigit():
                # 数字替换为数字
                id_chars[pos] = random.choice(string.digits)
            elif original_char.islower():
                # 小写字母替换为小写字母
                id_chars[pos] = random.choice(string.ascii_lowercase)
            elif original_char.isupper():
                # 大写字母替换为大写字母
                id_chars[pos] = random.choice(string.ascii_uppercase)
            else:
                # 其他字符保持不变
                pass

        new_id = ''.join(id_chars)
        self.logger.debug(f"requestId随机化: {original_id[:20]}... → {new_id[:20]}...")
        return new_id

    def _request_next_page(self, request_info, request_json, first_page_response=None, type="stock"):
        """
        根据第一页的响应数据，自动请求所有分页并收集关键数据
        :param request_info: 原始请求信息
        :param request_json: 原始请求的JSON数据
        :param first_page_response: 第一页的响应数据（JSON格式）
        :param type: 数据类型（默认："stock"）
        """
        try:
            # 避免重复请求
            if hasattr(self, '_is_requesting_all_pages') and self._is_requesting_all_pages:
                return

            self._is_requesting_all_pages = True

            # 解析第一页数据，获取total和dataList
            if not first_page_response:
                self.logger.warning("未提供第一页响应数据，无法请求后续分页")
                return

            # 提取total和dataList
            try:
                data = first_page_response.get('data', {})
                result = data.get('result', {})
                total = result.get('total', 0)
                first_page_data_list = result.get('dataList', [])

                if not self.log_summary_only:
                    self.logger.info("=" * 80)
                    self.logger.info(f"第一页数据解析:")
                    self.logger.info(f"  总记录数(total): {total}")
                    self.logger.info(f"  第一页数据条数: {len(first_page_data_list)}")
                    self.logger.info("=" * 80)
                else:
                    self.logger.info(f"📊 第一页: 总数{total} | 返回{len(first_page_data_list)}条")

                if total == 0:
                    self.logger.warning("总记录数为0，无需请求后续分页")
                    return

                # 收集所有数据
                all_data_list = first_page_data_list.copy()

            except Exception as e:
                self.logger.error(f"解析第一页响应数据失败: {e}", exc_info=True)
                return

            # 计算总页数
            page_size = request_json.get('pageSize', 50)
            total_pages = (total + page_size - 1) // page_size  # 向上取整

            if not self.log_summary_only:
                self.logger.info(f"每页大小: {page_size}")
                self.logger.info(f"计算总页数: {total_pages}")
                self.logger.info("=" * 80)
            else:
                self.logger.info(f"📄 分页: {total_pages}页 × {page_size}条/页")

            # 如果只有一页，直接返回
            if total_pages <= 1:
                self.logger.info("只有1页数据，无需请求后续分页")
                self._log_all_key_data(total, all_data_list)
                return

            # 请求后续页面 (从第2页开始)
            url = request_info['request_url']
            headers = request_info['request_headers'].copy()
            headers['Content-Type'] = 'application/json'

            for page_no in range(2, total_pages + 1):
                # 检查是否已请求过
                if page_no in self.requested_pages:
                    continue

                self.requested_pages.add(page_no)

                # 延时1秒，避免请求过快
                if not self.log_summary_only:
                    self.logger.info(f"延时1秒后请求第{page_no}页...")
                time.sleep(1)

                # 复制请求数据
                next_page_json = request_json.copy()

                # 修改页码
                if 'pageNo' in next_page_json:
                    next_page_json['pageNo'] = page_no
                elif 'pageNum' in next_page_json:
                    next_page_json['pageNum'] = page_no
                elif 'page' in next_page_json:
                    next_page_json['page'] = page_no

                # 随机化requestId，避免反爬
                if 'requestId' in next_page_json:
                    next_page_json['requestId'] = self._randomize_request_id(next_page_json['requestId'])

                # 发起请求
                if not self.log_summary_only:
                    self.logger.info(f"正在请求第{page_no}/{total_pages}页...")
                    self.logger.info(f"请求URL: {url}")
                else:
                    self.logger.info(f"🔄 请求第{page_no}/{total_pages}页...")

                try:
                    response = requests.post(
                        url,
                        json=next_page_json,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        if not self.log_summary_only:
                            self.logger.info(f"✓ 第{page_no}页请求成功! 状态码: {response.status_code}")
                            self.logger.info(f"✓ 响应大小: {len(response.text)} 字符")
                        else:
                            self.logger.info(f"  ✓ 第{page_no}页成功 ({len(response.text)}字符)")

                        # 解析响应数据
                        try:
                            page_response_json = response.json()
                            page_data = page_response_json.get('data', {})
                            page_result = page_data.get('result', {})
                            page_data_list = page_result.get('dataList', [])

                            if not self.log_summary_only:
                                self.logger.info(f"✓ 第{page_no}页数据条数: {len(page_data_list)}")
                            else:
                                self.logger.info(f"    返回 {len(page_data_list)} 条数据")

                            # 收集数据
                            all_data_list.extend(page_data_list)

                            # 构造响应数据信息并记录到数据日志
                            api_info = {
                                'url': url,
                                'status': response.status_code,
                                'request_id': f'manual_page_{page_no}',
                                'method': 'POST',
                                'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'intercept_timestamp': datetime.now().isoformat(),
                                'intercept_index': f'page_{page_no}_manual',
                                'request_url': url,
                                'request_method': 'POST',
                                'request_params': {
                                    'pageNo': page_no,
                                    'pageSize': page_size,
                                    'keyWord': next_page_json.get('keyWord', '')
                                },
                                'page_info': {
                                    'pageNo': page_no,
                                    'pageSize': page_size,
                                    'keyWord': next_page_json.get('keyWord', '')
                                },
                                'response_summary': {
                                    'data_count': len(page_data_list)
                                },
                                'is_manual_request': True
                            }

                            # 只在需要完整数据时添加详细信息
                            if self.log_full_data:
                                api_info['headers'] = dict(response.headers)
                                api_info['request_headers'] = headers
                                api_info['request_post_data'] = json.dumps(next_page_json, ensure_ascii=False)
                                api_info['request_json'] = next_page_json
                                api_info['response_body'] = response.text
                                api_info['response_json'] = page_response_json

                            # 记录到数据日志
                            self.data_logger.info("=" * 80)
                            self.data_logger.info(f"拦截API数据 #page_{page_no}_manual (主动请求)")
                            self.data_logger.info("=" * 80)
                            self.data_logger.info(f"API信息:\n{json.dumps(api_info, ensure_ascii=False, indent=2)}")
                            self.data_logger.info("=" * 80)

                        except Exception as e:
                            self.logger.error(f"解析第{page_no}页响应数据失败: {e}", exc_info=True)
                    else:
                        self.logger.error(f"第{page_no}页请求失败! 状态码: {response.status_code}")
                        self.logger.error(f"响应内容: {response.text[:200]}")

                except Exception as e:
                    self.logger.error(f"请求第{page_no}页时发生错误: {e}", exc_info=True)

            # 所有页面请求完成，记录汇总数据
            self._log_all_key_data(total, all_data_list)

        except Exception as e:
            self.logger.error(f"请求所有分页时发生错误: {e}", exc_info=True)
        finally:
            self._is_requesting_all_pages = False

    def _log_all_key_data(self, total, all_data_list):
        """
        将所有关键数据打印到日志
        :param total: 总记录数
        :param all_data_list: 所有数据列表
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info("所有分页数据收集完成!")
            self.logger.info("=" * 80)
            self.logger.info(f"总记录数(total): {total}")
            self.logger.info(f"实际收集到的数据条数: {len(all_data_list)}")
            self.logger.info("=" * 80)

            # 根据配置决定是否打印详细数据到控制台
            if not self.log_summary_only:
                self.logger.info("关键数据详情:")
                self.logger.info("-" * 80)

                for idx, data_item in enumerate(all_data_list, 1):
                    self.logger.info(f"第{idx}条数据:")
                    self.logger.info(json.dumps(data_item, ensure_ascii=False, indent=2))
                    self.logger.info("-" * 80)
            else:
                # 仅摘要模式：显示前几条数据的关键字段
                self.logger.info("前5条数据预览:")
                for idx, data_item in enumerate(all_data_list[:5], 1):
                    # 提取关键字段（根据实际数据结构调整）
                    key_fields = {k: v for k, v in list(data_item.items())[:3]}  # 只显示前3个字段
                    self.logger.info(f"  {idx}. {json.dumps(key_fields, ensure_ascii=False)}")
                if len(all_data_list) > 5:
                    self.logger.info(f"  ... 还有 {len(all_data_list) - 5} 条数据")

            # 同时记录到数据日志（总是记录完整数据到数据日志文件）
            summary_info = {
                'summary_type': 'all_pages_data',
                'total_records': total,
                'collected_records': len(all_data_list),
                'timestamp': datetime.now().isoformat(),
            }

            # 根据配置决定是否记录完整数据列表
            if self.log_full_data:
                summary_info['all_data'] = all_data_list
            else:
                # 只记录数据摘要统计
                summary_info['data_summary'] = {
                    'count': len(all_data_list),
                    'sample': all_data_list[:5] if len(all_data_list) > 0 else []
                }

            self.data_logger.info("=" * 80)
            self.data_logger.info("所有分页数据汇总")
            self.data_logger.info("=" * 80)
            self.data_logger.info(json.dumps(summary_info, ensure_ascii=False, indent=2))
            self.data_logger.info("=" * 80)

            self.logger.info("=" * 80)
            self.logger.info("✓ 所有关键数据已记录到日志")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error(f"记录关键数据时发生错误: {e}", exc_info=True)

    def scheduled_intercept_and_save(self, xuangu_id="xc0d27d74884930004d1", color="w", action="edit_way",
                                   type="stock", check_interval=1, refresh_interval=15, csv_file_path="scheduled_xuangu_data.csv",
                                   max_refresh_attempts=3):
        """
        定时运行模式：访问页面并间隔刷新获取选股数据，保存到数据库或CSV文件
        与原版本不同的是：这个版本每次获取到有效数据都会立即保存，不跳过第一次

        :param xuangu_id: 选股方案ID
        :param color: 颜色参数
        :param action: 动作参数
        :param type: 数据类型（默认："stock"）
        :param check_interval: 检查网络日志的时间间隔（秒）
        :param refresh_interval: 刷新页面的时间间隔（秒）
        :param csv_file_path: CSV文件保存路径（仅在数据库不可用时使用）
        :param max_refresh_attempts: 最大刷新尝试次数（包含首次访问）
        """
        try:
            # 根据type设置正确的API URL
            if type in self.api_url_map:
                self.target_api_url = self.api_url_map[type]
                self.logger.info(f"根据type={type}设置API URL: {self.target_api_url}")
            else:
                self.logger.warning(f"未知的type: {type}，使用默认的stock API URL")
                self.target_api_url = self.api_url_map["stock"]

            # 构造目标URL
            url = f"https://xuangu.eastmoney.com/Result?type={type}&color={color}&id={xuangu_id}&a={action}"

            self.logger.info("=" * 80)
            self.logger.info(f"访问页面: {url}")
            self.logger.info(f"目标API: {self.target_api_url}")
            if self.use_database:
                self.logger.info("数据保存方式: 数据库")
            else:
                self.logger.info(f"数据保存方式: CSV文件 ({csv_file_path})")
            self.logger.info(f"最大刷新尝试次数: {max_refresh_attempts}")
            self.logger.info(f"检查间隔: {check_interval}秒")
            self.logger.info(f"刷新间隔: {refresh_interval}秒")
            self.logger.info("=" * 80)

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            # 用于跟踪已处理的request_id，避免重复处理
            processed_request_ids = set()
            # 存储请求信息，key为request_id
            pending_requests = {}
            intercept_count = 0
            refresh_count = 0
            last_refresh_time = time.time()  # 设置初始时间，确保首次刷新计时正确

            self.logger.info("=" * 80)
            self.logger.info("开始监听 search-code API，等待有效数据...")
            self.logger.info("定时运行模式：获取到任何有效数据都会保存")
            self.logger.info("=" * 80)

            # 重置分页跟踪，确保每次运行都能获取完整数据
            self.requested_pages.clear()

            # 持续监听，直到获取到有效数据或达到最大刷新次数
            while refresh_count < max_refresh_attempts:
                try:
                    # 检查是否需要刷新页面
                    current_time = time.time()
                    if current_time - last_refresh_time >= refresh_interval:
                        if refresh_count == 0:
                            self.logger.info("首次访问完成，等待下次刷新...")
                        else:
                            self.logger.info("=" * 60)
                            self.logger.info(f"第 {refresh_count + 1} 次刷新页面...")
                            self.logger.info("=" * 60)

                        self.browser_manager.refresh_page()
                        last_refresh_time = current_time
                        refresh_count += 1

                        # 刷新后清空已处理的request_id，以便重新拦截
                        processed_request_ids.clear()
                        pending_requests.clear()
                        self.requested_pages.clear()

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

                            # 只处理 search-code API 的请求
                            if self.target_api_url in url_sent:
                                pending_requests[request_id] = {
                                    'request_url': url_sent,
                                    'request_method': request.get('method', ''),
                                    'request_headers': request.get('headers', {}),
                                    'request_post_data': request.get('postData', None),
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

                            # 只处理 search-code API
                            if self.target_api_url in url_received and request_id not in processed_request_ids:
                                processed_request_ids.add(request_id)

                                # 尝试获取响应内容
                                try:
                                    body = self.browser_manager.get_response_body(request_id)

                                    # 只处理有数据的响应（过滤流式响应中的空数据）
                                    if body and len(body) > 10:  # 至少要有一些内容
                                        intercept_count += 1

                                        self.logger.info("=" * 80)
                                        self.logger.info(f"✓ 拦截到第 {intercept_count} 个有效响应")
                                        self.logger.info(f"URL: {url_received}")
                                        self.logger.info(f"状态码: {status}")
                                        self.logger.info(f"响应大小: {len(body)} 字符")
                                        self.logger.info("=" * 80)

                                        # 解析响应JSON
                                        response_json = None
                                        try:
                                            response_json = json.loads(body)
                                            if response_json and 'data' in response_json:
                                                data = response_json.get('data', {})
                                                result = data.get('result', {})
                                                data_list = result.get('dataList', [])

                                                self.logger.info(f"✓ 响应包含 {len(data_list)} 条选股数据")

                                                # 检查数据有效性
                                                if data_list and len(data_list) > 0:
                                                    # 根据配置选择保存方式
                                                    if self.use_database:
                                                        self.logger.info("✓ 获取到有效数据，开始保存到数据库")
                                                    else:
                                                        self.logger.info("✓ 获取到有效数据，开始保存到CSV文件")

                                                    # 获取请求信息用于分页请求
                                                    if request_id in pending_requests:
                                                        request_info = pending_requests[request_id]

                                                        # 解析请求的postData
                                                        request_json = None
                                                        if request_info['request_post_data']:
                                                            try:
                                                                request_json = json.loads(request_info['request_post_data'])
                                                            except Exception as e:
                                                                self.logger.warning(f"解析请求数据失败: {e}")

                                                        # 如果有请求数据，触发自动分页请求
                                                        if request_json:
                                                            # 重置分页跟踪，确保每次运行都能获取完整数据
                                                            self.requested_pages.clear()
                                                            self.logger.info("开始自动请求所有分页数据...")
                                                            success = self._request_next_page_and_save_to_database(
                                                                request_info, request_json, response_json, type
                                                            )

                                                            if success:
                                                                save_method = "数据库" if self.use_database else "CSV文件"
                                                                self.logger.info("=" * 80)
                                                                self.logger.info(f"✓ 所有分页数据已成功保存到{save_method}")
                                                                self.logger.info("=" * 80)
                                                                return True
                                                            else:
                                                                self.logger.error("分页请求或保存失败，尝试保存当前页数据")
                                                                # 分页失败，尝试保存当前页数据
                                                                if self.use_database:
                                                                    success = self._save_data_to_database(data_list, type)
                                                                else:
                                                                    success = self._save_data_to_csv(data_list, csv_file_path)

                                                                save_method = "数据库" if self.use_database else "CSV文件"
                                                                if success:
                                                                    self.logger.info(f"✓ 当前页数据已保存到{save_method}")
                                                                    return True
                                                        else:
                                                            # 如果没有请求数据，直接保存当前页面数据
                                                            self.logger.info("无请求数据，直接保存当前页面数据")
                                                            if self.use_database:
                                                                success = self._save_data_to_database(data_list, type)
                                                            else:
                                                                success = self._save_data_to_csv(data_list, csv_file_path)

                                                            save_method = "数据库" if self.use_database else "CSV文件"
                                                            if success:
                                                                self.logger.info("=" * 80)
                                                                self.logger.info(f"✓ 数据已成功保存到{save_method}")
                                                                self.logger.info("=" * 80)
                                                                return True
                                                            else:
                                                                save_method = "数据库" if self.use_database else "CSV文件"
                                                                self.logger.error(f"保存{save_method}失败")
                                                    else:
                                                        self.logger.warning("无法找到请求信息，直接保存当前页数据")
                                                        if self.use_database:
                                                            success = self._save_data_to_database(data_list, type)
                                                        else:
                                                            success = self._save_data_to_csv(data_list, csv_file_path)

                                                        save_method = "数据库" if self.use_database else "CSV文件"
                                                        if success:
                                                            self.logger.info(f"✓ 当前页数据已保存到{save_method}")
                                                            return True
                                                else:
                                                    self.logger.warning("响应数据为空或无效，继续等待...")

                                        except Exception as e:
                                            self.logger.warning(f"解析响应JSON失败: {e}")

                                        # 清理已使用的请求信息
                                        if request_id in pending_requests:
                                            del pending_requests[request_id]

                                except Exception as e:
                                    # 获取响应体失败，可能是流式响应还没有数据
                                    self.logger.debug(f"获取响应体失败: {e}")

                    # 如果还没有获取到有效数据，等待一段时间再检查
                    time_until_refresh = max(0, refresh_interval - (time.time() - last_refresh_time))
                    sleep_time = min(check_interval, time_until_refresh)
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                except KeyboardInterrupt:
                    self.logger.info("用户手动停止程序")
                    break

            # 达到最大刷新次数仍未获取到有效数据
            self.logger.info("=" * 80)
            self.logger.info(f"已达到最大刷新次数 {max_refresh_attempts}，未获取到有效数据")
            self.logger.info(f"总共拦截到 {intercept_count} 个响应")
            self.logger.info("=" * 80)
            return False

        except Exception as e:
            self.logger.error(f"定时拦截和保存失败: {e}", exc_info=True)
            return False

    def _convert_eastmoney_to_db_format(self, data_list, data_type="stock"):
        """
        将东方财富数据转换为数据库格式
        :param data_list: 东方财富API返回的数据列表
        :param data_type: 数据类型 (stock, etf, fund, bond)
        :return: 转换后的数据库格式数据列表
        """
        try:
            db_data = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:00')

            for item in data_list:
                # 根据实际的东方财富API数据结构提取字段
                # 从CSV文件头可以看出实际字段名
                code = item.get('SECURITY_CODE', '')
                name = item.get('SECURITY_SHORT_NAME', '')

                # 从NEWEST_PRICE获取最新价格作为收盘价
                close_price = self._parse_number(item.get('NEWEST_PRICE', 0))

                # 如果没有最新价格，尝试其他字段
                if close_price == 0:
                    close_price = self._parse_number(item.get('CLOSE', item.get('f2', 0)))

                # 开盘价，如果没有现成字段，用收盘价代替
                # 东方财富选股API通常不提供开盘价
                open_price = close_price

                # 最高价和最低价
                high_price = self._parse_number(item.get('PEAK_PRICE<140>', item.get('PEAK_PRICE', close_price)))
                low_price = self._parse_number(item.get('BOTTOM_PRICE<140>', item.get('BOTTOM_PRICE', close_price)))

                # 成交量处理
                volume_str = item.get('VOLUME', '0')
                volume = self._parse_number(volume_str)

                # 成交额处理
                amount_str = item.get('TRADING_VOLUMES', '0')
                amount = self._parse_number(amount_str)

                db_record = {
                    'code': code,
                    'name': name,
                    'datetime': current_time,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': int(volume),
                    'amount': amount
                }

                # 只有当代码和名称都不为空时才添加
                if code and name:
                    db_data.append(db_record)
                    self.logger.debug(f"转换数据: {name}({code}) - 价格:{close_price}, 成交量:{volume}")

            self.logger.info(f"✓ 成功转换 {len(db_data)} 条数据到数据库格式")
            return db_data

        except Exception as e:
            self.logger.error(f"数据格式转换失败: {e}", exc_info=True)
            return []

    def _parse_number(self, value_str):
        """
        解析数字字符串，处理中文单位（亿、万等）
        :param value_str: 数字字符串，可能包含中文单位
        :return: 浮点数
        """
        try:
            if not value_str or value_str == '0' or value_str == '-':
                return 0.0

            value_str = str(value_str).strip()

            # 处理中文单位
            if '亿' in value_str:
                number_part = value_str.replace('亿', '').strip()
                return float(number_part) * 100000000
            elif '万' in value_str:
                number_part = value_str.replace('万', '').strip()
                return float(number_part) * 10000
            elif '千' in value_str:
                number_part = value_str.replace('千', '').strip()
                return float(number_part) * 1000
            else:
                # 纯数字
                return float(value_str)

        except (ValueError, TypeError) as e:
            self.logger.debug(f"解析数字失败: {value_str} - {e}")
            return 0.0

    def _save_data_to_csv(self, data_list, csv_file_path):
        """
        将选股数据保存到CSV文件（保留作为备用方案）
        :param data_list: 选股数据列表
        :param csv_file_path: CSV文件保存路径
        :return: 保存是否成功
        """
        try:
            if not data_list:
                self.logger.warning("数据列表为空，无法保存到CSV文件")
                return False

            # 确保目录存在
            os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

            # 检查文件是否存在，如果不存在则创建并写入表头
            file_exists = os.path.exists(csv_file_path)

            with open(csv_file_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                if data_list:
                    # 获取第一条数据作为表头
                    fieldnames = list(data_list[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                    # 如果文件不存在，写入表头
                    if not file_exists:
                        writer.writeheader()
                        self.logger.info(f"创建新的CSV文件并写入表头: {csv_file_path}")

                    # 写入数据
                    writer.writerows(data_list)

                    self.logger.info(f"✓ 成功写入 {len(data_list)} 条数据到CSV文件")
                    self.logger.info(f"文件路径: {csv_file_path}")
                    return True
                else:
                    self.logger.warning("没有数据可以写入CSV文件")
                    return False

        except Exception as e:
            self.logger.error(f"保存数据到CSV文件失败: {e}", exc_info=True)
            return False

    def _save_data_to_database(self, data_list, data_type="stock"):
        """
        将选股数据保存到数据库
        :param data_list: 选股数据列表
        :param data_type: 数据类型 (stock, etf, fund, bond)
        :return: 保存是否成功
        """
        try:
            if not self.use_database or not self.db:
                self.logger.warning("数据库未可用，无法保存到数据库")
                return False

            if not data_list:
                self.logger.warning("数据列表为空，无法保存到数据库")
                return False

            # 转换数据格式
            db_data = self._convert_eastmoney_to_db_format(data_list, data_type)

            if not db_data:
                self.logger.warning("转换后的数据为空，无法保存到数据库")
                return False

            # 使用分钟数据周期保存
            period = "1m"  # 选股数据通常是日线数据

            # 保存到数据库
            inserted_count = self.db.insert_kline_data(period, db_data)

            if inserted_count > 0:
                self.logger.info(f"✓ 成功保存 {inserted_count} 条数据到数据库")
                self.logger.info(f"数据类型: {data_type}, 周期: {period}")
                return True
            else:
                self.logger.warning("数据库插入返回0条记录")
                return False

        except Exception as e:
            self.logger.error(f"保存数据到数据库失败: {e}", exc_info=True)
            return False

    def _request_next_page_and_save_to_database(self, request_info, request_json, first_page_response=None, type="stock"):
        """
        根据第一页的响应数据，自动请求所有分页并保存到数据库
        :param request_info: 原始请求信息
        :param request_json: 原始请求的JSON数据
        :param first_page_response: 第一页的响应数据（JSON格式）
        :param type: 数据类型（默认："stock"）
        :return: 保存是否成功
        """
        try:
            # 避免重复请求
            if hasattr(self, '_is_requesting_all_pages_db') and self._is_requesting_all_pages_db:
                return False

            self._is_requesting_all_pages_db = True

            # 重置分页跟踪，确保每次都能获取完整数据
            self.requested_pages.clear()

            # 解析第一页数据，获取total和dataList
            if not first_page_response:
                self.logger.warning("未提供第一页响应数据，无法请求后续分页")
                return False

            # 提取total和dataList
            try:
                data = first_page_response.get('data', {})
                result = data.get('result', {})
                total = result.get('total', 0)
                first_page_data_list = result.get('dataList', [])

                self.logger.info("=" * 80)
                self.logger.info(f"第一页数据解析:")
                self.logger.info(f"  总记录数(total): {total}")
                self.logger.info(f"  第一页数据条数: {len(first_page_data_list)}")
                self.logger.info("=" * 80)

                if total == 0:
                    self.logger.warning("总记录数为0，无需请求后续分页")
                    return False

                # 收集所有数据
                all_data_list = first_page_data_list.copy()

            except Exception as e:
                self.logger.error(f"解析第一页响应数据失败: {e}", exc_info=True)
                return False

            # 计算总页数
            page_size = request_json.get('pageSize', 50)
            total_pages = (total + page_size - 1) // page_size  # 向上取整

            self.logger.info(f"每页大小: {page_size}")
            self.logger.info(f"计算总页数: {total_pages}")
            self.logger.info("=" * 80)

            # 如果只有一页，直接保存第一页数据
            if total_pages <= 1:
                self.logger.info("只有1页数据，直接保存第一页数据")
                success = self._save_data_to_database(all_data_list, type)
                return success

            # 请求后续页面 (从第2页开始)
            url = request_info['request_url']
            headers = request_info['request_headers'].copy()
            headers['Content-Type'] = 'application/json'

            for page_no in range(2, total_pages + 1):
                # 检查是否已请求过
                if page_no in self.requested_pages:
                    continue

                self.requested_pages.add(page_no)

                # 延时1秒，避免请求过快
                self.logger.info(f"延时1秒后请求第{page_no}页...")
                time.sleep(1)

                # 复制请求数据
                next_page_json = request_json.copy()

                # 修改页码
                if 'pageNo' in next_page_json:
                    next_page_json['pageNo'] = page_no
                elif 'pageNum' in next_page_json:
                    next_page_json['pageNum'] = page_no
                elif 'page' in next_page_json:
                    next_page_json['page'] = page_no

                # 随机化requestId，避免反爬
                if 'requestId' in next_page_json:
                    next_page_json['requestId'] = self._randomize_request_id(next_page_json['requestId'])

                # 发起请求
                self.logger.info(f"正在请求第{page_no}/{total_pages}页...")
                self.logger.info(f"请求URL: {url}")

                try:
                    response = requests.post(
                        url,
                        json=next_page_json,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        self.logger.info(f"✓ 第{page_no}页请求成功! 状态码: {response.status_code}")
                        self.logger.info(f"✓ 响应大小: {len(response.text)} 字符")

                        # 解析响应数据
                        try:
                            page_response_json = response.json()
                            page_data = page_response_json.get('data', {})
                            page_result = page_data.get('result', {})
                            page_data_list = page_result.get('dataList', [])

                            self.logger.info(f"✓ 第{page_no}页数据条数: {len(page_data_list)}")

                            # 收集数据
                            all_data_list.extend(page_data_list)

                        except Exception as e:
                            self.logger.error(f"解析第{page_no}页响应数据失败: {e}", exc_info=True)
                    else:
                        self.logger.error(f"第{page_no}页请求失败! 状态码: {response.status_code}")
                        self.logger.error(f"响应内容: {response.text[:200]}")

                except Exception as e:
                    self.logger.error(f"请求第{page_no}页时发生错误: {e}", exc_info=True)

            # 所有页面请求完成，保存数据到数据库
            self.logger.info("=" * 80)
            self.logger.info("所有分页数据收集完成!")
            self.logger.info(f"总记录数(total): {total}")
            self.logger.info(f"实际收集到的数据条数: {len(all_data_list)}")
            self.logger.info("=" * 80)

            success = self._save_data_to_database(all_data_list, type)
            if success:
                self.logger.info("✓ 所有分页数据已成功保存到数据库")
            return success

        except Exception as e:
            self.logger.error(f"请求所有分页时发生错误: {e}", exc_info=True)
            return False
        finally:
            self._is_requesting_all_pages_db = False

    def _request_next_page_and_save_to_csv(self, request_info, request_json, first_page_response=None, type="stock", csv_file_path="xuangu_data.csv"):
        """
        根据第一页的响应数据，自动请求所有分页并保存到CSV文件
        :param request_info: 原始请求信息
        :param request_json: 原始请求的JSON数据
        :param first_page_response: 第一页的响应数据（JSON格式）
        :param type: 数据类型（默认："stock"）
        :param csv_file_path: CSV文件保存路径
        :return: 保存是否成功
        """
        try:
            # 避免重复请求
            if hasattr(self, '_is_requesting_all_pages_csv') and self._is_requesting_all_pages_csv:
                return False

            self._is_requesting_all_pages_csv = True

            # 重置分页跟踪，确保每次都能获取完整数据
            self.requested_pages.clear()

            # 解析第一页数据，获取total和dataList
            if not first_page_response:
                self.logger.warning("未提供第一页响应数据，无法请求后续分页")
                return False

            # 提取total和dataList
            try:
                data = first_page_response.get('data', {})
                result = data.get('result', {})
                total = result.get('total', 0)
                first_page_data_list = result.get('dataList', [])

                self.logger.info("=" * 80)
                self.logger.info(f"第一页数据解析:")
                self.logger.info(f"  总记录数(total): {total}")
                self.logger.info(f"  第一页数据条数: {len(first_page_data_list)}")
                self.logger.info("=" * 80)

                if total == 0:
                    self.logger.warning("总记录数为0，无需请求后续分页")
                    return False

                # 收集所有数据
                all_data_list = first_page_data_list.copy()

            except Exception as e:
                self.logger.error(f"解析第一页响应数据失败: {e}", exc_info=True)
                return False

            # 计算总页数
            page_size = request_json.get('pageSize', 50)
            total_pages = (total + page_size - 1) // page_size  # 向上取整

            self.logger.info(f"每页大小: {page_size}")
            self.logger.info(f"计算总页数: {total_pages}")
            self.logger.info("=" * 80)

            # 如果只有一页，直接保存第一页数据
            if total_pages <= 1:
                self.logger.info("只有1页数据，直接保存第一页数据")
                success = self._save_data_to_csv(all_data_list, csv_file_path)
                return success

            # 请求后续页面 (从第2页开始)
            url = request_info['request_url']
            headers = request_info['request_headers'].copy()
            headers['Content-Type'] = 'application/json'

            for page_no in range(2, total_pages + 1):
                # 检查是否已请求过
                if page_no in self.requested_pages:
                    continue

                self.requested_pages.add(page_no)

                # 延时1秒，避免请求过快
                self.logger.info(f"延时1秒后请求第{page_no}页...")
                time.sleep(1)

                # 复制请求数据
                next_page_json = request_json.copy()

                # 修改页码
                if 'pageNo' in next_page_json:
                    next_page_json['pageNo'] = page_no
                elif 'pageNum' in next_page_json:
                    next_page_json['pageNum'] = page_no
                elif 'page' in next_page_json:
                    next_page_json['page'] = page_no

                # 随机化requestId，避免反爬
                if 'requestId' in next_page_json:
                    next_page_json['requestId'] = self._randomize_request_id(next_page_json['requestId'])

                # 发起请求
                self.logger.info(f"正在请求第{page_no}/{total_pages}页...")
                self.logger.info(f"请求URL: {url}")

                try:
                    response = requests.post(
                        url,
                        json=next_page_json,
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        self.logger.info(f"✓ 第{page_no}页请求成功! 状态码: {response.status_code}")
                        self.logger.info(f"✓ 响应大小: {len(response.text)} 字符")

                        # 解析响应数据
                        try:
                            page_response_json = response.json()
                            page_data = page_response_json.get('data', {})
                            page_result = page_data.get('result', {})
                            page_data_list = page_result.get('dataList', [])

                            self.logger.info(f"✓ 第{page_no}页数据条数: {len(page_data_list)}")

                            # 收集数据
                            all_data_list.extend(page_data_list)

                        except Exception as e:
                            self.logger.error(f"解析第{page_no}页响应数据失败: {e}", exc_info=True)
                    else:
                        self.logger.error(f"第{page_no}页请求失败! 状态码: {response.status_code}")
                        self.logger.error(f"响应内容: {response.text[:200]}")

                except Exception as e:
                    self.logger.error(f"请求第{page_no}页时发生错误: {e}", exc_info=True)

            # 所有页面请求完成，保存数据到CSV
            self.logger.info("=" * 80)
            self.logger.info("所有分页数据收集完成!")
            self.logger.info(f"总记录数(total): {total}")
            self.logger.info(f"实际收集到的数据条数: {len(all_data_list)}")
            self.logger.info("=" * 80)

            success = self._save_data_to_csv(all_data_list, csv_file_path)
            if success:
                self.logger.info("✓ 所有分页数据已成功保存到CSV文件")
            return success

        except Exception as e:
            self.logger.error(f"请求所有分页时发生错误: {e}", exc_info=True)
            return False
        finally:
            self._is_requesting_all_pages_csv = False

    def add_api_type(self, type_name, api_url):
        """
        动态添加新的API类型
        :param type_name: 类型名称
        :param api_url: 对应的API URL
        """
        self.api_url_map[type_name] = api_url
        self.logger.info(f"添加新的API类型: {type_name} -> {api_url}")

    def get_available_types(self):
        """
        获取所有可用的API类型
        :return: 可用类型列表
        """
        return list(self.api_url_map.keys())

    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 启动定时运行的 search-code API 拦截器
    每5分钟自动刷新页面并保存数据到CSV文件
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.eastmoney_interceptor_main')
    logger.info("=" * 80)
    logger.info("东方财富选股 search-code API 定时拦截器")
    logger.info("模式：每5分钟自动刷新并保存数据")
    logger.info("=" * 80)

    # 初始化拦截器
    logger.info("=" * 80)
    # 日志配置参数说明：
    # - log_full_data=False: 不记录完整的请求和响应数据到数据日志（节省空间）
    # - log_summary_only=True: 控制台只显示关键摘要信息（简洁模式）
    # - use_database=True: 使用数据库保存数据（设为False则使用CSV文件）
    interceptor = EastmoneySearchCodeInterceptor(
        headless=False,
        log_full_data=False,      # 设为 True 则记录完整数据
        log_summary_only=True,    # 设为 False 则显示详细日志
        use_database=True         # 设为 False 则使用CSV文件保存
    )

    if not interceptor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    # 显示可用的API类型
    available_types = interceptor.get_available_types()
    logger.info(f"可用的API类型: {', '.join(available_types)}")

    # 定时运行参数
    SCHEDULE_INTERVAL = 120  # 5分钟 = 300秒
    CSV_BASE_PATH = "data/scheduled_xuangu_data.csv"

    # 配置参数
    config = {
        "xuangu_id": "xc0d3858a90493012efd",
        "color": "w",
        "action": "edit_way",
        "type": "stock",               # 数据类型
        "check_interval": 1,           # 每1秒检查一次网络日志
        "refresh_interval": 15,        # 每15秒刷新一次页面（用于单次数据获取）
        "max_refresh_attempts": 3      # 最大刷新尝试次数（用于单次数据获取）
    }

    run_count = 0
    logger.info("=" * 80)
    logger.info(f"定时运行配置:")
    logger.info(f"运行间隔: {SCHEDULE_INTERVAL}秒 ({SCHEDULE_INTERVAL/60:.1f}分钟)")
    logger.info(f"数据保存路径: {CSV_BASE_PATH}")
    logger.info(f"选股方案ID: {config['xuangu_id']}")
    logger.info(f"数据类型: {config['type']}")
    logger.info("=" * 80)
    logger.info("开始定时运行，按 Ctrl+C 停止程序")
    logger.info("=" * 80)

    try:
        while True:
            run_count += 1
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            logger.info("=" * 80)
            logger.info(f"第 {run_count} 次运行 - {current_time}")
            logger.info("=" * 80)

            # 为每次运行生成带时间戳的CSV文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file_path = f"data/scheduled_xuangu_data_{timestamp}.csv"

            logger.info(f"本次运行数据将保存到: {csv_file_path}")
            logger.info("开始获取选股数据...")

            # 执行单次数据获取（使用定时运行专用的方法）
            success = interceptor.scheduled_intercept_and_save(
                csv_file_path=csv_file_path,
                **config
            )

            if success:
                logger.info("=" * 80)
                logger.info(f"✓ 第 {run_count} 次运行完成，数据已保存")
                logger.info(f"✓ 数据文件: {csv_file_path}")
                logger.info("=" * 80)
            else:
                logger.warning("=" * 80)
                logger.warning(f"✗ 第 {run_count} 次运行失败或未获取到数据")
                logger.warning("=" * 80)

            # 计算下次运行时间
            next_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"下次运行时间: {next_run_time} (等待 {SCHEDULE_INTERVAL} 秒)")
            logger.info("=" * 80)

            # 等待指定时间间隔
            try:
                time.sleep(SCHEDULE_INTERVAL)
            except KeyboardInterrupt:
                logger.info("用户在等待期间停止程序")
                break

    except KeyboardInterrupt:
        logger.info("=" * 80)
        logger.info("用户手动停止定时运行程序")
        logger.info(f"总共运行了 {run_count} 次")
        logger.info("=" * 80)
    except Exception as e:
        logger.error(f"定时运行过程中发生错误: {e}", exc_info=True)
    finally:
        interceptor.close()

    logger.info("=" * 80)
    logger.info("定时程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()