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

# 导入日志系统
try:
    from .logger_config import FinancialLogger, get_logger
    from .selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager


class EastmoneySearchCodeInterceptor:
    """
    使用 Selenium 拦截东方财富选股页面的 search-code API
    """
    def __init__(self, headless=False, log_full_data=False, log_summary_only=False):
        """
        初始化拦截器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param log_full_data: 是否记录完整的请求和响应数据到数据日志（默认False）
        :param log_summary_only: 是否只记录关键摘要信息到控制台（默认False，True时仅显示关键统计信息）
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.eastmoney_interceptor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.eastmoney_interceptor.browser'
        )

        self.target_api_url = "https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code"
        self.requested_pages = set()  # 用于跟踪已请求过的页码,避免重复请求

        # 日志控制参数
        self.log_full_data = log_full_data
        self.log_summary_only = log_summary_only

        self.logger.info("初始化东方财富选股API拦截器")
        self.logger.info(f"日志配置: 完整数据={log_full_data}, 仅摘要={log_summary_only}")
        self.browser_manager.init_driver()

    def start_interception(self, xuangu_id="xc0d27d74884930004d1", color="w", action="edit_way",
                          check_interval=1, refresh_interval=300):
        """
        持续拦截 search-code API，每5分钟刷新页面
        :param xuangu_id: 选股方案ID
        :param color: 颜色参数
        :param action: 动作参数
        :param check_interval: 检查网络日志的时间间隔（秒）
        :param refresh_interval: 刷新页面的时间间隔（秒），默认300秒（5分钟）
        """
        try:
            # 构造目标URL
            url = f"https://xuangu.eastmoney.com/Result?color={color}&id={xuangu_id}&a={action}"

            self.logger.info("=" * 80)
            self.logger.info(f"访问页面: {url}")
            self.logger.info(f"目标API: {self.target_api_url}")
            self.logger.info(f"检查间隔: {check_interval}秒")
            self.logger.info(f"刷新间隔: {refresh_interval}秒 ({refresh_interval/60:.1f}分钟)")
            self.logger.info("=" * 80)

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            self.logger.info("=" * 80)
            self.logger.info("开始持续监听 search-code API...")
            self.logger.info(f"每 {refresh_interval/60:.1f} 分钟自动刷新页面")
            self.logger.info("按 Ctrl+C 停止监听")
            self.logger.info("=" * 80)

            # 用于跟踪已处理的request_id，避免重复处理
            processed_request_ids = set()
            # 存储请求信息，key为request_id
            pending_requests = {}
            intercept_count = 0
            last_refresh_time = time.time()

            # 持续监听
            while True:
                try:
                    # 检查是否需要刷新页面
                    current_time = time.time()
                    if current_time - last_refresh_time >= refresh_interval:
                        self.logger.info("=" * 60)
                        self.logger.info("自动刷新页面...")
                        self.logger.info("=" * 60)

                        self.browser_manager.refresh_page()
                        last_refresh_time = current_time

                        # 刷新后清空已处理的request_id，以便重新拦截
                        processed_request_ids.clear()
                        pending_requests.clear()
                        self.requested_pages.clear()  # 清空已请求页码记录

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

                                        # 控制台日志：只显示关键信息
                                        if not self.log_summary_only:
                                            self.logger.info("=" * 80)
                                            self.logger.info(f"✓ 拦截到第 {intercept_count} 个有效响应")
                                            self.logger.info(f"URL: {url_received}")
                                            self.logger.info(f"状态码: {status}")
                                            self.logger.info(f"响应大小: {len(body)} 字符")
                                        else:
                                            self.logger.info(f"✓ 拦截响应 #{intercept_count} | 状态:{status} | 大小:{len(body)}字符")

                                        api_info = {
                                            'url': url_received,
                                            'status': status,
                                            'mime_type': mime_type,
                                            'request_id': request_id,
                                            'method': response.get('requestMethod', 'POST'),
                                            'headers': response.get('headers', {}),
                                            'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            'intercept_timestamp': datetime.now().isoformat(),
                                            'intercept_index': intercept_count,
                                        }

                                        # 只在需要时添加完整响应体
                                        if self.log_full_data:
                                            api_info['response_body'] = body

                                        # 先尝试解析响应JSON
                                        response_json = None
                                        try:
                                            response_json = json.loads(body)
                                            # 提取关键数据字段（不包含完整的响应）
                                            if response_json and 'data' in response_json:
                                                data = response_json.get('data', {})
                                                result = data.get('result', {})
                                                api_info['response_summary'] = {
                                                    'total': result.get('total', 0),
                                                    'data_count': len(result.get('dataList', []))
                                                }

                                            # 只在需要完整数据时记录
                                            if self.log_full_data:
                                                api_info['response_json'] = response_json

                                            if not self.log_summary_only:
                                                self.logger.info("✓ 响应数据为JSON格式")
                                                if 'response_summary' in api_info:
                                                    self.logger.info(f"  总记录数: {api_info['response_summary']['total']}")
                                                    self.logger.info(f"  本次返回: {api_info['response_summary']['data_count']} 条")
                                        except Exception as e:
                                            if not self.log_summary_only:
                                                self.logger.warning(f"响应数据不是有效的JSON格式: {e}")

                                        # 添加请求数据
                                        if request_id in pending_requests:
                                            request_info = pending_requests[request_id]
                                            api_info['request_url'] = request_info['request_url']
                                            api_info['request_method'] = request_info['request_method']

                                            # 只在需要完整数据时记录请求头和请求体
                                            if self.log_full_data:
                                                api_info['request_headers'] = request_info['request_headers']
                                                api_info['request_post_data'] = request_info['request_post_data']

                                            api_info['request_time'] = request_info['request_time']

                                            # 尝试解析请求的 postData (可能包含分页参数)
                                            if request_info['request_post_data']:
                                                try:
                                                    request_json = json.loads(request_info['request_post_data'])

                                                    # 提取关键参数（总是记录）
                                                    keyword = request_json.get('keyWord', '')
                                                    page_size = request_json.get('pageSize', 0)
                                                    page_no = request_json.get('pageNo', 0)

                                                    api_info['request_params'] = {
                                                        'keyWord': keyword,
                                                        'pageSize': page_size,
                                                        'pageNo': page_no
                                                    }

                                                    # 只在需要完整数据时记录完整的请求JSON
                                                    if self.log_full_data:
                                                        api_info['request_json'] = request_json

                                                    # 控制台输出关键参数
                                                    if not self.log_summary_only:
                                                        self.logger.info("=" * 60)
                                                        self.logger.info("关键请求参数:")
                                                        self.logger.info(f"  keyWord: {keyword}")
                                                        self.logger.info(f"  pageSize: {page_size}")
                                                        self.logger.info(f"  pageNo: {page_no}")
                                                        self.logger.info("=" * 60)

                                                    # 提取分页信息
                                                    if 'pageNo' in request_json or 'pageNum' in request_json or 'page' in request_json:
                                                        page_info = {
                                                            'pageNo': request_json.get('pageNo') or request_json.get('pageNum') or request_json.get('page'),
                                                            'pageSize': request_json.get('pageSize') or request_json.get('size'),
                                                            'keyWord': keyword  # 添加 keyWord 到 page_info
                                                        }
                                                        api_info['page_info'] = page_info

                                                        if not self.log_summary_only:
                                                            self.logger.info(f"分页信息: 第 {page_info['pageNo']} 页, 每页 {page_info['pageSize']} 条")

                                                        # 如果是第1页，且有响应数据，触发自动分页请求
                                                        if page_info['pageNo'] == 1 and response_json:
                                                            self._request_next_page(request_info, request_json, response_json)
                                                except Exception as e:
                                                    self.logger.debug(f"解析请求数据失败: {e}")

                                            # 清理已使用的请求信息
                                            del pending_requests[request_id]

                                        # 将API信息记录到数据日志
                                        self.data_logger.info("=" * 80)
                                        self.data_logger.info(f"拦截API数据 #{intercept_count}")
                                        self.data_logger.info("=" * 80)
                                        self.data_logger.info(f"API信息:\n{json.dumps(api_info, ensure_ascii=False, indent=2)}")
                                        self.data_logger.info("=" * 80)

                                        if not self.log_summary_only:
                                            self.logger.info(f"✓ API数据已记录到日志")
                                            self.logger.info("=" * 80)

                                except Exception as e:
                                    # 获取响应体失败，可能是流式响应还没有数据
                                    self.logger.debug(f"获取响应体失败: {e}")

                    # 等待一段时间再检查
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    self.logger.info("=" * 80)
                    self.logger.info("用户停止监听")
                    self.logger.info(f"总共拦截到 {intercept_count} 个有效响应")
                    self.logger.info("=" * 80)
                    break

        except Exception as e:
            self.logger.error(f"API拦截失败: {e}", exc_info=True)

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

    def _request_next_page(self, request_info, request_json, first_page_response=None):
        """
        根据第一页的响应数据，自动请求所有分页并收集关键数据
        :param request_info: 原始请求信息
        :param request_json: 原始请求的JSON数据
        :param first_page_response: 第一页的响应数据（JSON格式）
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

    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 启动 search-code API 拦截器
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.eastmoney_interceptor_main')
    logger.info("=" * 80)
    logger.info("东方财富选股 search-code API 拦截器")
    logger.info("=" * 80)

    # 初始化拦截器
    logger.info("=" * 80)
    # 日志配置参数说明：
    # - log_full_data=False: 不记录完整的请求和响应数据到数据日志（节省空间）
    # - log_summary_only=True: 控制台只显示关键摘要信息（简洁模式）
    interceptor = EastmoneySearchCodeInterceptor(
        headless=False,
        log_full_data=False,      # 设为 True 则记录完整数据
        log_summary_only=True     # 设为 False 则显示详细日志
    )

    if not interceptor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    try:
        # 开始拦截，每5分钟刷新一次页面
        interceptor.start_interception(
            xuangu_id="xc0d291f999b33002095",
            color="w",
            action="edit_way",
            check_interval=1,  # 每1秒检查一次网络日志
            refresh_interval=100  # 每100秒刷新一次页面 (可根据需要调整为300秒即5分钟)
        )

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    finally:
        interceptor.close()

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
