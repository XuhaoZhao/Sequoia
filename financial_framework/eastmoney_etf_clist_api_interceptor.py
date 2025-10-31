"""
东方财富ETF网格列表 API 拦截器

使用 Selenium 拦截东方财富ETF网格列表页面的 API
目标页面: https://quote.eastmoney.com/center/gridlist.html#fund_etf
目标API: https://push2.eastmoney.com/api/qt/clist/get

特点：
1. 只拦截 clist API，不主动发起请求
2. 过滤掉流式响应中无数据的响应
3. 每5分钟自动刷新页面
4. 使用统一日志系统记录所有操作
5. 纯监听模式：通过人工点击页面分页来获取数据，避免被识别为爬虫
"""

import time
import json
from datetime import datetime
import os
import sys

# 添加父目录到路径以便导入db_manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入日志系统
try:
    from .logger_config import FinancialLogger, get_logger
    from .selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager

# 导入数据库管理器
try:
    from db_manager import IndustryDataDB
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    from ..db_manager import IndustryDataDB


class EastmoneyEtfClistInterceptor:
    """
    使用 Selenium 拦截东方财富ETF网格列表页面的 clist API
    """
    def __init__(self, headless=False, log_full_data=False, log_summary_only=False, db_path="industry_data.db"):
        """
        初始化拦截器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param log_full_data: 是否记录完整的请求和响应数据到数据日志（默认False）
        :param log_summary_only: 是否只记录关键摘要信息到控制台（默认False，True时仅显示关键统计信息）
        :param db_path: 数据库文件路径
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.etf_clist_interceptor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.etf_clist_interceptor.browser'
        )

        # 目标API URL
        self.target_api_url = "https://push2.eastmoney.com/api/qt/clist/get"

        # 日志控制参数
        self.log_full_data = log_full_data
        self.log_summary_only = log_summary_only

        # 初始化数据库管理器
        self.db = IndustryDataDB(db_path)

        self.logger.info("初始化东方财富ETF网格列表API拦截器（纯监听模式）")
        self.logger.info(f"日志配置: 完整数据={log_full_data}, 仅摘要={log_summary_only}")
        self.logger.info(f"数据库路径: {db_path}")
        self.logger.info("模式说明：仅监听API响应，不主动请求分页数据，需手动点击页面分页")
        self.browser_manager.init_driver()

    def start_interception(self, check_interval=1, refresh_interval=300):
        """
        持续拦截 clist API，每5分钟刷新页面（纯监听模式）
        :param check_interval: 检查网络日志的时间间隔（秒）
        :param refresh_interval: 刷新页面的时间间隔（秒），默认300秒（5分钟）
        """
        try:
            # 构造目标URL
            url = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"

            self.logger.info("=" * 80)
            self.logger.info(f"访问页面: {url}")
            self.logger.info(f"目标API: {self.target_api_url}")
            self.logger.info(f"检查间隔: {check_interval}秒")
            self.logger.info(f"刷新间隔: {refresh_interval}秒 ({refresh_interval/60:.1f}分钟)")
            self.logger.info("工作模式：纯监听，通过页面操作触发分页请求")
            self.logger.info("=" * 80)

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            self.logger.info("=" * 80)
            self.logger.info("开始持续监听 clist API（纯监听模式）...")
            self.logger.info(f"每 {refresh_interval/60:.1f} 分钟自动刷新页面")
            self.logger.info("请手动点击页面分页按钮来获取更多数据")
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

                        # 在刷新后增加额外等待时间，让页面完全加载
                        time.sleep(2)

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

                            # 只处理 clist API 的请求
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

                            # 只处理 clist API
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
                                            'method': response.get('requestMethod', 'GET'),
                                            'headers': response.get('headers', {}),
                                            'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            'intercept_timestamp': datetime.now().isoformat(),
                                            'intercept_index': intercept_count,
                                        }

                                        # 只在需要时添加完整响应体
                                        if self.log_full_data:
                                            api_info['response_body'] = body

                                        # 先尝试解析响应JSON（处理JSONP格式）
                                        response_json = None
                                        try:
                                            # 检查是否是JSONP格式（jQuery_callback(...)）
                                            if body.startswith('jQuery') and '(' in body:
                                                # 移除JSONP包装
                                                json_start = body.find('(') + 1
                                                json_end = body.rfind(')')

                                                if json_end > json_start:
                                                    json_content = body[json_start:json_end]
                                                else:
                                                    # 如果找不到结束括号，从第一个括号开始到最后
                                                    json_content = body[json_start:]

                                                self.logger.info(f"检测到JSONP格式，提取JSON内容长度: {len(json_content)}")
                                                self.logger.info(f"JSON内容前100字符: {json_content[:100]}")

                                                try:
                                                    response_json = json.loads(json_content)
                                                    self.logger.info("✓ JSONP格式解析成功")
                                                except json.JSONDecodeError as json_e:
                                                    self.logger.error(f"JSONP内容解析失败: {json_e}")
                                                    self.logger.info(f"JSON内容前200字符: {json_content[:200]}")
                                                    # 继续尝试其他方法或让异常处理
                                                    raise
                                            else:
                                                response_json = json.loads(body)
                                            # 提取关键数据字段（不包含完整的响应）
                                            if response_json and 'data' in response_json:
                                                data = response_json.get('data', {})
                                                diff = data.get('diff', [])
                                                api_info['response_summary'] = {
                                                    'total': data.get('total', 0),
                                                    'data_count': len(diff)
                                                }

                                                # 如果有数据，立即保存到数据库
                                                if len(diff) > 0:
                                                    self.logger.info(f"✓ 开始保存 {len(diff)} 条ETF数据到数据库...")
                                                    self._save_data_to_db(diff)

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
                                                # 检查是否是JSONP格式
                                                if body.startswith('jQuery') and '(' in body:
                                                    self.logger.warning(f"JSONP格式解析失败: {e}")
                                                    self.logger.info(f"原始响应前500字符: {body[:500]}")
                                                else:
                                                    self.logger.warning(f"响应数据不是有效的JSON格式: {e}")
                                                    self.logger.info(f"原始响应前500字符: {body[:500]}")

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

                                            # 尝试解析请求的参数 (GET请求参数在URL中)
                                            if '?' in request_info['request_url']:
                                                from urllib.parse import urlparse, parse_qs
                                                parsed_url = urlparse(request_info['request_url'])
                                                query_params = parse_qs(parsed_url.query)

                                                # 提取关键参数（总是记录）
                                                pn = query_params.get('pn', ['0'])[0]
                                                pz = query_params.get('pz', ['20'])[0]
                                                fs = query_params.get('fs', [''])[0]

                                                api_info['request_params'] = {
                                                    'pn': pn,  # 页码
                                                    'pz': pz,  # 每页数量
                                                    'fs': fs   # 过滤条件
                                                }

                                                # 控制台输出关键参数
                                                if not self.log_summary_only:
                                                    self.logger.info("=" * 60)
                                                    self.logger.info("关键请求参数:")
                                                    self.logger.info(f"  pn(页码): {pn}")
                                                    self.logger.info(f"  pz(每页数量): {pz}")
                                                    self.logger.info(f"  fs(过滤条件): {fs}")
                                                    self.logger.info("=" * 60)

                                                # 提取分页信息
                                                page_info = {
                                                    'pageNo': int(pn),
                                                    'pageSize': int(pz),
                                                    'fs': fs
                                                }
                                                api_info['page_info'] = page_info

                                                if not self.log_summary_only:
                                                    self.logger.info(f"分页信息: 第 {page_info['pageNo']} 页, 每页 {page_info['pageSize']} 条")

                                                # 注释：纯监听模式，不主动请求分页数据
                                                # 如需获取其他页数据，请手动点击页面分页按钮

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

    def test_api_response(self):
        """
        测试获取API响应数据（用于验证监听功能是否正常）
        """
        self.logger.info("=" * 80)
        self.logger.info("测试获取API响应数据（纯监听模式）")
        self.logger.info("=" * 80)

        try:
            # 构造目标URL
            url = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            # 等待页面加载完成后主动刷新一次（参考选股拦截器的做法）
            self.logger.info("等待页面加载完成...")
            time.sleep(10)
            self.logger.info("执行主动刷新以触发数据加载...")
            self.browser_manager.refresh_page()
            self.logger.info("等待刷新后页面加载...")
            time.sleep(20)

            self.logger.info("开始监听 API 响应...")

            # 用于跟踪已处理的request_id，避免重复处理
            processed_request_ids = set()
            pending_requests = {}
            intercept_count = 0
            start_time = time.time()
            timeout = 60  # 60秒超时

            while time.time() - start_time < timeout:
                try:
                    # 获取性能日志
                    logs = self.browser_manager.get_performance_logs()

                    for log in logs:
                        message = json.loads(log['message'])
                        method = message.get('message', {}).get('method', '')

                        # 拦截请求发送
                        if method == 'Network.requestWillBeSent':
                            params = message.get('message', {}).get('params', {})
                            request = params.get('request', {})
                            request_id = params.get('requestId', '')
                            url_sent = request.get('url', '')

                            if self.target_api_url in url_sent:
                                pending_requests[request_id] = {
                                    'request_url': url_sent,
                                    'request_method': request.get('method', ''),
                                    'request_headers': request.get('headers', {}),
                                    'request_post_data': request.get('postData', None),
                                    'request_time': datetime.now().isoformat()
                                }
                                self.logger.info(f"✓ 检测到API请求: {url_sent}")

                        # 查找网络响应
                        if method == 'Network.responseReceived':
                            params = message.get('message', {}).get('params', {})
                            response = params.get('response', {})
                            request_id = params.get('requestId', '')
                            url_received = response.get('url', '')
                            status = response.get('status', 0)

                            if self.target_api_url in url_received and request_id not in processed_request_ids:
                                processed_request_ids.add(request_id)
                                intercept_count += 1

                                self.logger.info("=" * 60)
                                self.logger.info(f"✓ 拦截到第 {intercept_count} 个响应")
                                self.logger.info(f"URL: {url_received}")
                                self.logger.info(f"状态码: {status}")

                                # 尝试获取响应内容
                                try:
                                    body = self.browser_manager.get_response_body(request_id)

                                    if body and len(body) > 10:
                                        self.logger.info(f"✓ 响应大小: {len(body)} 字符")

                                        # 尝试解析JSON（处理JSONP格式）
                                        try:
                                            response_json = None

                                            # 检查是否是JSONP格式（jQuery_callback(...)）
                                            if body.startswith('jQuery') and '(' in body:
                                                # 移除JSONP包装
                                                json_start = body.find('(') + 1
                                                json_end = body.rfind(')')

                                                if json_end > json_start:
                                                    json_content = body[json_start:json_end]
                                                else:
                                                    # 如果找不到结束括号，从第一个括号开始到最后
                                                    json_content = body[json_start:]

                                                self.logger.info(f"检测到JSONP格式，提取JSON内容长度: {len(json_content)}")
                                                self.logger.info(f"JSON内容前100字符: {json_content[:100]}")

                                                response_json = json.loads(json_content)
                                                self.logger.info("✓ 响应为JSONP格式，已解析为JSON")
                                            else:
                                                response_json = json.loads(body)
                                                self.logger.info("✓ 响应为标准JSON格式")

                                            # 分析数据结构
                                            if 'data' in response_json:
                                                data = response_json['data']
                                                self.logger.info(f"✓ 包含data字段")

                                                if 'diff' in data:
                                                    diff = data['diff']
                                                    self.logger.info(f"✓ 包含diff数组，长度: {len(diff)}")

                                                    if len(diff) > 0:
                                                        self.logger.info("✓ 数据数组非空，显示第一条数据的关键字段:")
                                                        first_item = diff[0]

                                                        # 显示关键字段
                                                        key_fields = ['f12', 'f13', 'f14', 'f2', 'f3', 'f4', 'f5', 'f6']
                                                        for field in key_fields:
                                                            if field in first_item:
                                                                value = first_item[field]
                                                                field_name = {
                                                                    'f12': '股票代码',
                                                                    'f13': '股票类型',
                                                                    'f14': '股票名称',
                                                                    'f2': '最新价',
                                                                    'f3': '涨跌额',
                                                                    'f4': '涨跌幅',
                                                                    'f5': '成交量',
                                                                    'f6': '成交额'
                                                                }.get(field, field)
                                                                self.logger.info(f"  {field_name}({field}): {value}")

                                                        self.logger.info("完整第一条数据结构:")
                                                        for key, value in first_item.items():
                                                            self.logger.info(f"  {key}: {type(value).__name__} = {str(value)[:50]}...")

                                                        # 记录完整响应到数据日志
                                                        self.data_logger.info("=" * 80)
                                                        self.data_logger.info("第一步测试：完整API响应数据")
                                                        self.data_logger.info("=" * 80)
                                                        self.data_logger.info(f"响应JSON:\n{json.dumps(response_json, ensure_ascii=False, indent=2)}")
                                                        self.data_logger.info("=" * 80)

                                                        self.logger.info("=" * 80)
                                                        self.logger.info("✓ 测试完成：成功获取并分析API响应数据")
                                                        self.logger.info("✓ 监听功能正常，可以开始持续监听")
                                                        self.logger.info("✓ 数据已记录到日志")
                                                        self.logger.info("=" * 80)

                                                        return response_json

                                                    else:
                                                        self.logger.warning("diff数组为空")
                                                else:
                                                    self.logger.warning("data字段中不包含diff")

                                                if 'total' in data:
                                                    self.logger.info(f"✓ 总记录数: {data['total']}")

                                            else:
                                                self.logger.warning("响应中不包含data字段")

                                        except json.JSONDecodeError as e:
                                            self.logger.error(f"响应不是有效的JSON格式: {e}")
                                            self.logger.info(f"原始响应前500字符: {body[:500]}")
                                    else:
                                        self.logger.warning("响应体为空或过短")

                                except Exception as e:
                                    self.logger.error(f"获取响应体失败: {e}")

                                self.logger.info("=" * 60)

                    time.sleep(10)

                except KeyboardInterrupt:
                    self.logger.info("用户手动停止测试")
                    break

            self.logger.warning(f"在{timeout}秒内未获取到有效响应")
            return None

        except Exception as e:
            self.logger.error(f"测试API响应失败: {e}", exc_info=True)
            return None

    def listen_for_data(self, timeout=60):
        """
        监听单个API响应数据（纯监听模式）
        :param timeout: 超时时间（秒）
        :return: 响应数据或None
        """
        self.logger.info("=" * 80)
        self.logger.info("监听API响应数据（纯监听模式）")
        self.logger.info("=" * 80)

        try:
            # 构造目标URL
            url = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"

            # 启用网络拦截
            self.browser_manager.enable_network_interception()

            # 访问页面
            self.browser_manager.navigate_to(url)

            # 等待页面加载
            self.logger.info("等待页面加载完成...")
            time.sleep(10)
            self.logger.info("开始监听 API 响应...")
            self.logger.info("请手动点击页面分页按钮来获取不同页的数据")

            # 用于跟踪已处理的request_id，避免重复处理
            processed_request_ids = set()
            pending_requests = {}
            intercept_count = 0
            start_time = time.time()

            while time.time() - start_time < timeout:
                try:
                    # 获取性能日志
                    logs = self.browser_manager.get_performance_logs()

                    for log in logs:
                        message = json.loads(log['message'])
                        method = message.get('message', {}).get('method', '')

                        # 拦截请求发送
                        if method == 'Network.requestWillBeSent':
                            params = message.get('message', {}).get('params', {})
                            request = params.get('request', {})
                            request_id = params.get('requestId', '')
                            url_sent = request.get('url', '')

                            if self.target_api_url in url_sent:
                                pending_requests[request_id] = {
                                    'request_url': url_sent,
                                    'request_method': request.get('method', ''),
                                    'request_headers': request.get('headers', {}),
                                    'request_post_data': request.get('postData', None),
                                    'request_time': datetime.now().isoformat()
                                }
                                self.logger.info(f"✓ 检测到API请求: {url_sent}")

                        # 查找网络响应
                        if method == 'Network.responseReceived':
                            params = message.get('message', {}).get('params', {})
                            response = params.get('response', {})
                            request_id = params.get('requestId', '')
                            url_received = response.get('url', '')
                            status = response.get('status', 0)

                            if self.target_api_url in url_received and request_id not in processed_request_ids:
                                processed_request_ids.add(request_id)
                                intercept_count += 1

                                self.logger.info("=" * 60)
                                self.logger.info(f"✓ 拦截到第 {intercept_count} 个响应")
                                self.logger.info(f"URL: {url_received}")
                                self.logger.info(f"状态码: {status}")

                                # 尝试获取响应内容
                                try:
                                    body = self.browser_manager.get_response_body(request_id)

                                    if body and len(body) > 10:
                                        self.logger.info(f"✓ 响应大小: {len(body)} 字符")

                                        # 尝试解析JSON（处理JSONP格式）
                                        try:
                                            response_json = None

                                            # 检查是否是JSONP格式
                                            if body.startswith('jQuery') and '(' in body:
                                                # 移除JSONP包装
                                                json_start = body.find('(') + 1
                                                json_end = body.rfind(')')

                                                if json_end > json_start:
                                                    json_content = body[json_start:json_end]
                                                else:
                                                    json_content = body[json_start:]

                                                response_json = json.loads(json_content)
                                                self.logger.info("✓ 响应为JSONP格式，已解析为JSON")
                                            else:
                                                response_json = json.loads(body)
                                                self.logger.info("✓ 响应为标准JSON格式")

                                            # 分析数据结构
                                            if 'data' in response_json:
                                                data = response_json['data']
                                                self.logger.info(f"✓ 包含data字段")

                                                if 'diff' in data:
                                                    diff = data['diff']
                                                    self.logger.info(f"✓ 包含diff数组，长度: {len(diff)}")

                                                    if len(diff) > 0:
                                                        self.logger.info("✓ 获取到有效数据")

                                                        # 提取分页信息
                                                        if request_id in pending_requests:
                                                            request_info = pending_requests[request_id]
                                                            if '?' in request_info['request_url']:
                                                                from urllib.parse import urlparse, parse_qs
                                                                parsed_url = urlparse(request_info['request_url'])
                                                                query_params = parse_qs(parsed_url.query)

                                                                pn = query_params.get('pn', ['0'])[0]
                                                                pz = query_params.get('pz', ['20'])[0]

                                                                self.logger.info(f"分页信息: 第 {pn} 页, 每页 {pz} 条")

                                                                # 保存数据到数据库
                                                                self._save_data_to_db(diff)

                                                                # 记录完整响应到数据日志
                                                                self.data_logger.info("=" * 80)
                                                                self.data_logger.info(f"监听到API数据 - 第{pn}页")
                                                                self.data_logger.info("=" * 80)
                                                                self.data_logger.info(f"响应JSON:\n{json.dumps(response_json, ensure_ascii=False, indent=2)}")
                                                                self.data_logger.info("=" * 80)

                                                        return response_json

                                        except json.JSONDecodeError as e:
                                            self.logger.error(f"响应JSON解析失败: {e}")

                                except Exception as e:
                                    self.logger.error(f"获取响应体失败: {e}")

                                self.logger.info("=" * 60)

                    time.sleep(5)

                except KeyboardInterrupt:
                    self.logger.info("用户手动停止监听")
                    break

            self.logger.warning(f"在{timeout}秒内未获取到有效响应")
            return None

        except Exception as e:
            self.logger.error(f"监听API响应失败: {e}", exc_info=True)
            return None

    def _save_data_to_db(self, data_list):
        """
        将ETF数据保存到数据库
        :param data_list: ETF数据列表
        :return: 保存是否成功
        """
        try:
            if not data_list:
                self.logger.warning("数据列表为空，无法保存到数据库")
                return False

            self.logger.info("=" * 80)
            self.logger.info("开始将ETF数据保存到数据库...")

            success_count = 0
            error_count = 0

            for item in data_list:
                try:
                    # 提取关键字段
                    etf_code = str(item.get('f12', ''))  # ETF代码
                    etf_type = str(item.get('f13', ''))  # ETF类型
                    etf_name = str(item.get('f14', ''))  # ETF名称

                    # 验证必要字段
                    if not etf_code or not etf_name:
                        self.logger.warning(f"跳过无效记录: code={etf_code}, name={etf_name}")
                        error_count += 1
                        continue

                    # 调用数据库管理器保存数据
                    self.db.add_or_update_etf_info(
                        etf_code=etf_code,
                        etf_type=etf_type,
                        etf_name=etf_name
                    )
                    success_count += 1

                except Exception as e:
                    self.logger.error(f"保存单条数据失败: {e}, 数据: {item}")
                    error_count += 1
                    continue

            self.logger.info("=" * 80)
            self.logger.info(f"✓ 成功保存 {success_count} 条ETF数据到数据库")
            if error_count > 0:
                self.logger.warning(f"失败 {error_count} 条数据")

            # 显示前几条数据预览
            self.logger.info("数据预览（前3条）:")
            for i, item in enumerate(data_list[:3], 1):
                code = item.get('f12', '')
                name = item.get('f14', '')
                etf_type = item.get('f13', '')
                price = item.get('f2', '')
                change_pct = item.get('f4', '')
                self.logger.info(f"  {i}. {name}({code}) - 类型:{etf_type} 价格:{price} 涨跌幅:{change_pct}%")

            self.logger.info("=" * 80)
            return True

        except Exception as e:
            self.logger.error(f"保存数据到数据库失败: {e}", exc_info=True)
            return False

    def _wait_for_response_after_refresh(self, timeout=60):
        """
        在刷新后等待API响应（不重新访问页面）
        :param timeout: 超时时间（秒）
        :return: 响应数据或None
        """
        self.logger.info("在刷新后等待API响应...")

        # 用于跟踪已处理的request_id，避免重复处理
        processed_request_ids = set()
        pending_requests = {}
        intercept_count = 0
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # 获取性能日志
                logs = self.browser_manager.get_performance_logs()

                for log in logs:
                    message = json.loads(log['message'])
                    method = message.get('message', {}).get('method', '')

                    # 拦截请求发送
                    if method == 'Network.requestWillBeSent':
                        params = message.get('message', {}).get('params', {})
                        request = params.get('request', {})
                        request_id = params.get('requestId', '')
                        url_sent = request.get('url', '')

                        if self.target_api_url in url_sent:
                            pending_requests[request_id] = {
                                'request_url': url_sent,
                                'request_method': request.get('method', ''),
                                'request_headers': request.get('headers', {}),
                                'request_post_data': request.get('postData', None),
                                'request_time': datetime.now().isoformat()
                            }
                            self.logger.info(f"✓ 检测到API请求: {url_sent}")

                    # 查找网络响应
                    if method == 'Network.responseReceived':
                        params = message.get('message', {}).get('params', {})
                        response = params.get('response', {})
                        request_id = params.get('requestId', '')
                        url_received = response.get('url', '')
                        status = response.get('status', 0)

                        if self.target_api_url in url_received and request_id not in processed_request_ids:
                            processed_request_ids.add(request_id)
                            intercept_count += 1

                            self.logger.info("=" * 60)
                            self.logger.info(f"✓ 拦截到第 {intercept_count} 个响应")
                            self.logger.info(f"URL: {url_received}")
                            self.logger.info(f"状态码: {status}")

                            # 尝试获取响应内容
                            try:
                                body = self.browser_manager.get_response_body(request_id)

                                if body and len(body) > 10:
                                    self.logger.info(f"✓ 响应大小: {len(body)} 字符")

                                    # 尝试解析JSON（处理JSONP格式）
                                    try:
                                        response_json = None

                                        # 检查是否是JSONP格式（jQuery_callback(...)）
                                        if body.startswith('jQuery') and '(' in body:
                                            # 移除JSONP包装
                                            json_start = body.find('(') + 1
                                            json_end = body.rfind(')')

                                            if json_end > json_start:
                                                json_content = body[json_start:json_end]
                                            else:
                                                # 如果找不到结束括号，从第一个括号开始到最后
                                                json_content = body[json_start:]

                                            self.logger.info(f"检测到JSONP格式，提取JSON内容长度: {len(json_content)}")
                                            response_json = json.loads(json_content)
                                            self.logger.info("✓ 响应为JSONP格式，已解析为JSON")
                                        else:
                                            response_json = json.loads(body)
                                            self.logger.info("✓ 响应为标准JSON格式")

                                        # 分析数据结构
                                        if 'data' in response_json:
                                            data = response_json['data']
                                            self.logger.info(f"✓ 包含data字段")

                                            if 'diff' in data:
                                                diff = data['diff']
                                                self.logger.info(f"✓ 包含diff数组，长度: {len(diff)}")

                                                if len(diff) > 0:
                                                    self.logger.info("✓ 获取到有效数据")
                                                    return response_json

                                    except json.JSONDecodeError as e:
                                        self.logger.warning(f"响应JSON解析失败: {e}")
                                else:
                                    self.logger.warning("响应体为空或过短")

                            except Exception as e:
                                self.logger.error(f"获取响应体失败: {e}")

                            self.logger.info("=" * 60)

                time.sleep(5)

            except KeyboardInterrupt:
                self.logger.info("用户手动停止等待")
                break

        self.logger.warning(f"在{timeout}秒内未获取到有效响应")
        return None

    
    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 纯监听模式
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.etf_clist_interceptor_main')
    logger.info("=" * 80)
    logger.info("东方财富ETF网格列表 API 拦截器（纯监听模式）")
    logger.info("=" * 80)

    # 初始化拦截器
    interceptor = EastmoneyEtfClistInterceptor(
        headless=False,
        log_full_data=True,      # 记录完整数据以便分析
        log_summary_only=False   # 显示详细日志
    )

    if not interceptor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    try:
        logger.info("开始持续监听 API 响应...")
        logger.info("请手动点击页面分页按钮来获取不同页的数据")

        # 启动持续监听
        interceptor.start_interception()

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    finally:
        interceptor.close()

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()