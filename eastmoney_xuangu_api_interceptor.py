from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
from datetime import datetime
import requests
import random
import string


class EastmoneySearchCodeInterceptor:
    """
    使用 Selenium 拦截东方财富选股页面的 search-code API
    目标API: https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code
    特点：
    1. 只拦截 search-code API
    2. 过滤掉流式响应中无数据的响应
    3. 每5分钟自动刷新页面
    """
    def __init__(self, headless=False):
        """
        初始化 Selenium WebDriver
        :param headless: 是否无头模式(不显示浏览器窗口)
        """
        self.driver = None
        self.headless = headless
        self.target_api_url = "https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code"
        self.requested_pages = set()  # 用于跟踪已请求过的页码,避免重复请求
        self.init_driver()

    def init_driver(self):
        """初始化 Chrome WebDriver"""
        print("初始化 Chrome 浏览器...")

        chrome_options = Options()

        # 无头模式
        if self.headless:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')

        # 禁用一些不必要的功能，提高性能
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        # 设置窗口大小
        chrome_options.add_argument('--window-size=1920,1080')

        # 启用性能日志，用于拦截网络请求
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

        # 去除自动化标识
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            # 使用 webdriver-manager 自动管理 ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 执行 CDP 命令，进一步隐藏 webdriver 特征
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })

            print("  ✓ Chrome 浏览器初始化成功")
            return True
        except Exception as e:
            print(f"  ✗ 浏览器初始化失败: {e}")
            print(f"  提示: 请确保已安装 Chrome 浏览器")
            return False

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

            print(f"  访问页面: {url}")
            print(f"  目标API: {self.target_api_url}")
            print(f"  检查间隔: {check_interval} 秒")
            print(f"  刷新间隔: {refresh_interval} 秒 ({refresh_interval/60:.1f} 分钟)")

            # 启用网络拦截
            self.driver.execute_cdp_cmd('Network.enable', {})

            # 访问页面
            self.driver.get(url)

            # 等待页面加载
            wait = WebDriverWait(self.driver, 20)

            try:
                # 等待页面关键元素加载
                wait.until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                print(f"  ✓ 页面加载成功")
            except TimeoutException:
                print(f"  ⚠️ 页面加载超时")

            print(f"\n{'='*80}")
            print(f"开始持续监听 search-code API...")
            print(f"每 {refresh_interval/60:.1f} 分钟自动刷新页面")
            print(f"按 Ctrl+C 停止监听")
            print(f"{'='*80}\n")

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
                        print(f"\n{'='*60}")
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 自动刷新页面...")
                        print(f"{'='*60}\n")

                        self.driver.refresh()
                        last_refresh_time = current_time

                        # 等待页面重新加载
                        try:
                            wait.until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            print(f"  ✓ 页面刷新成功\n")
                        except TimeoutException:
                            print(f"  ⚠️ 页面刷新超时\n")

                        # 刷新后清空已处理的request_id，以便重新拦截
                        processed_request_ids.clear()
                        pending_requests.clear()
                        self.requested_pages.clear()  # 清空已请求页码记录

                    # 获取性能日志
                    logs = self.driver.get_log('performance')

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

                                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                                # 尝试获取响应内容
                                try:
                                    response_body = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': request_id})
                                    body = response_body.get('body', '')

                                    # 只处理有数据的响应（过滤流式响应中的空数据）
                                    if body and len(body) > 10:  # 至少要有一些内容
                                        intercept_count += 1

                                        print(f"[{current_timestamp}] ✓ 拦截到第 {intercept_count} 个有效响应")
                                        print(f"  URL: {url_received}")
                                        print(f"  状态码: {status}")
                                        print(f"  响应大小: {len(body)} 字符")

                                        api_info = {
                                            'url': url_received,
                                            'status': status,
                                            'mime_type': mime_type,
                                            'request_id': request_id,
                                            'method': response.get('requestMethod', 'POST'),
                                            'headers': response.get('headers', {}),
                                            'intercept_time': current_timestamp,
                                            'intercept_timestamp': datetime.now().isoformat(),
                                            'intercept_index': intercept_count,
                                            'response_body': body
                                        }

                                        # 添加请求数据
                                        if request_id in pending_requests:
                                            request_info = pending_requests[request_id]
                                            api_info['request_url'] = request_info['request_url']
                                            api_info['request_method'] = request_info['request_method']
                                            api_info['request_headers'] = request_info['request_headers']
                                            api_info['request_post_data'] = request_info['request_post_data']
                                            api_info['request_time'] = request_info['request_time']

                                            # 尝试解析请求的 postData (可能包含分页参数)
                                            if request_info['request_post_data']:
                                                try:
                                                    request_json = json.loads(request_info['request_post_data'])
                                                    api_info['request_json'] = request_json

                                                    # 提取分页信息
                                                    if 'pageNo' in request_json or 'pageNum' in request_json or 'page' in request_json:
                                                        page_info = {
                                                            'pageNo': request_json.get('pageNo') or request_json.get('pageNum') or request_json.get('page'),
                                                            'pageSize': request_json.get('pageSize') or request_json.get('size')
                                                        }
                                                        api_info['page_info'] = page_info
                                                        print(f"  分页信息: 第 {page_info['pageNo']} 页, 每页 {page_info['pageSize']} 条")

                                                        # 如果是第1页，尝试请求第2页
                                                        if page_info['pageNo'] == 1:
                                                            self._request_next_page(request_info, request_json)
                                                except:
                                                    pass

                                            # 清理已使用的请求信息
                                            del pending_requests[request_id]

                                        # 尝试解析响应JSON
                                        try:
                                            api_info['response_json'] = json.loads(body)
                                            print(f"  ✓ 响应数据为JSON格式")
                                        except:
                                            print(f"  ⚠️ 响应数据不是有效的JSON格式")

                                        # 实时保存到文件
                                        self._save_single_request(api_info)
                                        print(f"  ✓ 已实时保存到文件\n")
                                    else:
                                        # 这是流式响应中的空数据，忽略
                                        pass

                                except Exception as e:
                                    # 获取响应体失败，可能是流式响应还没有数据
                                    pass

                    # 等待一段时间再检查
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    print(f"\n{'='*80}")
                    print("用户停止监听")
                    print(f"总共拦截到 {intercept_count} 个有效响应")
                    print(f"{'='*80}")
                    break

        except Exception as e:
            print(f"  ✗ API拦截失败: {e}")

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
        print(f"  → requestId 随机化: {original_id[:20]}... → {new_id[:20]}...")
        return new_id

    def _request_next_page(self, request_info, request_json):
        """
        复制原始请求参数，修改pageNo为2，主动请求第二页
        :param request_info: 原始请求信息
        :param request_json: 原始请求的JSON数据
        """
        try:
            # 避免重复请求第2页
            if 2 in self.requested_pages:
                return

            self.requested_pages.add(2)

            print(f"\n  → 检测到第1页请求，准备请求第2页...")

            # 复制请求数据
            next_page_json = request_json.copy()

            # 修改页码为2
            if 'pageNo' in next_page_json:
                next_page_json['pageNo'] = 2
            elif 'pageNum' in next_page_json:
                next_page_json['pageNum'] = 2
            elif 'page' in next_page_json:
                next_page_json['page'] = 2

            # 随机化requestId,避免反爬
            if 'requestId' in next_page_json:
                next_page_json['requestId'] = self._randomize_request_id(next_page_json['requestId'])

            # 复制headers
            headers = request_info['request_headers'].copy()

            # 确保Content-Type是JSON
            headers['Content-Type'] = 'application/json'

            # 发起请求
            url = request_info['request_url']
            print(f"  → 请求URL: {url}")
            print(f"  → 请求参数: pageNo=2")

            response = requests.post(
                url,
                json=next_page_json,
                headers=headers,
                timeout=30
            )

            if response.status_code == 200:
                print(f"  ✓ 第2页请求成功! 状态码: {response.status_code}")
                print(f"  ✓ 响应大小: {len(response.text)} 字符")

                # 保存第2页的响应
                api_info = {
                    'url': url,
                    'status': response.status_code,
                    'request_id': 'manual_page_2',
                    'method': 'POST',
                    'headers': dict(response.headers),
                    'intercept_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'intercept_timestamp': datetime.now().isoformat(),
                    'intercept_index': 'page_2_manual',
                    'request_url': url,
                    'request_method': 'POST',
                    'request_headers': headers,
                    'request_post_data': json.dumps(next_page_json, ensure_ascii=False),
                    'request_json': next_page_json,
                    'page_info': {
                        'pageNo': 2,
                        'pageSize': next_page_json.get('pageSize') or next_page_json.get('size')
                    },
                    'response_body': response.text,
                    'is_manual_request': True
                }

                # 尝试解析响应JSON
                try:
                    api_info['response_json'] = response.json()
                    print(f"  ✓ 响应数据为JSON格式")
                except:
                    print(f"  ⚠️ 响应数据不是有效的JSON格式")

                # 保存到文件
                self._save_single_request(api_info)
                print(f"  ✓ 第2页数据已保存到文件\n")
            else:
                print(f"  ✗ 第2页请求失败! 状态码: {response.status_code}")
                print(f"  ✗ 响应内容: {response.text[:200]}\n")

        except Exception as e:
            print(f"  ✗ 请求第2页时发生错误: {e}\n")

    def _save_single_request(self, api_info, output_file='eastmoney_search_code_api.jsonl'):
        """实时保存单个API请求数据到JSONL文件（每行一个JSON对象）"""
        try:
            import os
            cwd = os.getcwd()
            full_path = os.path.join(cwd, output_file)

            # 追加模式写入，每条记录一行
            with open(full_path, 'a', encoding='utf-8') as f:
                json.dump(api_info, f, ensure_ascii=False)
                f.write('\n')
            return True
        except Exception as e:
            print(f"    ✗ 实时保存失败: {e}")
            return False

    def close(self):
        """关闭浏览器"""
        if self.driver:
            print("\n关闭浏览器...")
            self.driver.quit()


def main():
    """
    主函数 - 启动 search-code API 拦截器
    """
    print("=" * 80)
    print("东方财富选股 search-code API 拦截器")
    print("=" * 80)

    # 初始化拦截器
    print("\n" + "=" * 80)
    interceptor = EastmoneySearchCodeInterceptor(headless=False)

    if not interceptor.driver:
        print("浏览器初始化失败，退出程序")
        return

    try:
        # 开始拦截，每5分钟刷新一次页面
        interceptor.start_interception(
            xuangu_id="xc0d27d74884930004d1",
            color="w",
            action="edit_way",
            check_interval=1,  # 每1秒检查一次网络日志
            refresh_interval=100  # 每300秒（5分钟）刷新一次页面
        )

    except KeyboardInterrupt:
        print("\n\n用户手动停止程序")
    finally:
        interceptor.close()

    print("\n" + "=" * 80)
    print("程序结束")
    print("=" * 80)


if __name__ == "__main__":
    main()
