import requests
import time
import random
import platform
import os
import stat
import json
from datetime import datetime
from data_collect.stock_chip_race import stock_large_cap_filter
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


class DynamicDelayStrategy:
    """
    动态延迟策略类
    按照设定的延迟序列循环返回延迟时间: 10s -> 20s -> 30s -> 40s -> 50s -> 40s -> 30s -> 20s -> 10s -> ...
    """
    def __init__(self, delays=None):
        if delays is None:
            delays = [10, 20, 30, 40, 50]

        # 创建完整的循环序列: 递增 + 递减(不包括最大值,避免重复)
        self.delays = delays + delays[-2::-1]  # [10, 20, 30, 40, 50, 40, 30, 20]
        self.current_index = 0

    def get_next_delay(self):
        """获取下一个延迟时间"""
        delay = self.delays[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.delays)
        return delay

    def get_current_delay(self):
        """获取当前延迟时间(不移动索引)"""
        return self.delays[self.current_index]


class RealBrowserParamsFetcher:
    """
    使用Selenium获取真实浏览器的请求参数
    支持跨平台: Windows, macOS, Linux
    """
    def __init__(self, headless=True):
        """
        初始化浏览器参数获取器
        :param headless: 是否无头模式
        """
        self.driver = None
        self.headless = headless
        self.platform = platform.system()
        print(f"\n{'='*80}")
        print(f"启动真实浏览器获取最新请求参数 (平台: {self.platform})")
        print(f"{'='*80}")

    def init_driver(self):
        """初始化Chrome WebDriver - 跨平台支持"""
        print("正在初始化Chrome浏览器...")

        chrome_options = Options()

        # macOS特定配置
        if self.platform == 'Darwin':
            chrome_binary = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
            if os.path.exists(chrome_binary):
                chrome_options.binary_location = chrome_binary
                print(f"  ✓ 检测到macOS Chrome: {chrome_binary}")

        # 无头模式
        if self.headless:
            if self.platform == 'Darwin':
                chrome_options.add_argument('--headless=new')  # macOS使用新版headless
            else:
                chrome_options.add_argument('--headless')  # Windows/Linux使用标准headless
            chrome_options.add_argument('--disable-gpu')

        # 通用配置
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')

        # 启用性能日志
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

        # 去除自动化标识
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            # 使用webdriver-manager自动管理ChromeDriver
            print("  正在下载/配置ChromeDriver...")
            driver_path = ChromeDriverManager().install()
            print(f"  ChromeDriver路径: {driver_path}")

            # macOS特定处理
            if self.platform == 'Darwin':
                try:
                    # 添加执行权限
                    current_permissions = os.stat(driver_path).st_mode
                    os.chmod(driver_path, current_permissions | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    # 移除隔离属性
                    os.system(f'xattr -d com.apple.quarantine "{driver_path}" 2>/dev/null')
                    print(f"  ✓ macOS权限配置完成")
                except Exception as e:
                    print(f"  ⚠️  macOS权限配置警告: {e}")

            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 隐藏webdriver特征
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })

            print(f"  ✓ Chrome浏览器初始化成功")
            return True

        except Exception as e:
            print(f"  ✗ 浏览器初始化失败: {e}")
            if self.platform == 'Darwin':
                print(f"\n  macOS解决方案:")
                print(f"  1. 确保Chrome已安装: /Applications/Google Chrome.app")
                print(f"  2. 移除ChromeDriver隔离属性: xattr -d com.apple.quarantine ~/.wdm/drivers/chromedriver/mac*/*/chromedriver")
            elif self.platform == 'Windows':
                print(f"\n  Windows解决方案:")
                print(f"  1. 确保Chrome已安装")
                print(f"  2. 以管理员身份运行Python")
            print(f"\n  通用解决方案: pip install --upgrade selenium webdriver-manager")
            return False

    def fetch_real_params(self, stock_code='000001', max_wait=15):
        """
        获取真实浏览器的请求参数
        :param stock_code: 测试用股票代码
        :param max_wait: 最大等待时间(秒)
        :return: 包含请求参数的字典
        """
        if not self.init_driver():
            return None

        try:
            # 构造secid
            if stock_code.startswith('6'):
                secid = f"1.{stock_code}"
            else:
                secid = f"0.{stock_code}"

            url = f"https://quote.eastmoney.com/f1.html?newcode={secid}"
            print(f"\n访问页面: {url}")

            # 启用Network domain来拦截请求
            self.driver.execute_cdp_cmd('Network.enable', {})

            # 访问页面
            self.driver.get(url)

            # 等待页面加载
            wait = WebDriverWait(self.driver, max_wait)

            try:
                stock_name_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".stockName, .name"))
                )
                stock_name = stock_name_element.text
                print(f"✓ 页面加载成功: {stock_name} ({stock_code})")
            except TimeoutException:
                print(f"⚠️  页面加载超时")

            # 等待网络请求完成
            print(f"正在拦截网络请求...")
            time.sleep(5)  # 等待API请求完成

            # 从性能日志中提取网络请求
            logs = self.driver.get_log('performance')
            detail_requests = []

            for log in logs:
                try:
                    message = json.loads(log['message'])
                    method = message.get('message', {}).get('method', '')

                    # 捕获请求发送事件
                    if method == 'Network.requestWillBeSent':
                        request_data = message['message']['params']
                        request_url = request_data.get('request', {}).get('url', '')

                        # 检查URL是否包含'detail'或其他关键API端点
                        if ('detail' in request_url.lower()):
                            request_info = {
                                'url': request_url,
                                'method': request_data.get('request', {}).get('method', 'GET'),
                                'headers': request_data.get('request', {}).get('headers', {}),
                                'timestamp': datetime.now().isoformat()
                            }
                            detail_requests.append(request_info)

                except Exception:
                    continue

            if detail_requests:
                # 使用第一个找到的请求
                real_request = detail_requests[0]
                print(f"✓ 成功拦截到 {len(detail_requests)} 个API请求")
                print(f"✓ 使用请求: {real_request['url'][:80]}...")

                # 解析URL参数
                params = self._parse_url_params(real_request['url'])

                # 提取关键信息
                result = {
                    'url': real_request['url'],
                    'params': params,
                    'headers': real_request['headers'],
                    'method': real_request['method'],
                    'timestamp': real_request['timestamp'],
                    'success': True
                }

                # 显示关键参数
                print(f"\n提取到的关键参数:")
                important_params = ['ut', 'fields1', 'fields2', 'mpi', 'fltt', 'wbp2u']
                for key in important_params:
                    if key in params:
                        print(f"  {key}: {params[key]}")

                return result
            else:
                print(f"⚠️  未能拦截到API请求")
                return None

        except Exception as e:
            print(f"✗ 获取真实参数失败: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            if self.driver:
                print(f"\n关闭浏览器...")
                self.driver.quit()

    def _parse_url_params(self, url):
        """解析URL参数"""
        if '?' not in url:
            return {}

        query_string = url.split('?')[1]
        params = {}

        for param in query_string.split('&'):
            if '=' in param:
                key, value = param.split('=', 1)
                params[key] = value

        return params


def get_stock_intraday_real_browser(stock_code, real_params, timeout=30):
    """
    使用真实浏览器参数模拟请求获取股票分时数据

    :param stock_code: 股票代码,如 '000001'
    :param real_params: 从真实浏览器获取的参数字典
    :param timeout: 请求超时时间(秒)
    :return: 响应数据或None
    """
    # 构造secid (市场ID.股票代码)
    if stock_code.startswith('6'):
        secid = f"1.{stock_code}"  # 上海证券交易所
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        secid = f"0.{stock_code}"  # 深圳证券交易所
    else:
        secid = f"0.{stock_code}"  # 默认深圳

    # 从真实参数中提取base URL
    real_url = real_params.get('url', '')
    if 'push2.eastmoney.com' in real_url:
        # 提取base URL
        base_parts = real_url.split('?')[0]
        # 随机选择负载均衡服务器
        server_numbers = [7, 54, 73, 23, 45, 67, 89, 12, 34, 56]
        server_num = random.choice(server_numbers)
        # 替换服务器编号
        if '.push2.eastmoney.com' in base_parts:
            base_url = base_parts.replace(base_parts.split('.')[0].split('/')[-1], str(server_num))
        else:
            base_url = f"https://{server_num}.push2.eastmoney.com/api/qt/stock/details/sse"
    else:
        # 默认URL
        server_num = random.choice([7, 54, 73, 23, 45, 67, 89, 12, 34, 56])
        base_url = f"https://{server_num}.push2.eastmoney.com/api/qt/stock/details/sse"

    # 使用真实参数,但替换secid
    params = real_params.get('params', {}).copy()
    params['secid'] = secid  # 替换为当前股票代码

    # 使用真实的请求头
    headers = real_params.get('headers', {}).copy()

    # 更新关键请求头
    headers.update({
        'Accept': 'text/event-stream',  # 确保是SSE请求
        'Cache-Control': 'no-cache',
        'Host': f'{server_num}.push2.eastmoney.com',
        'Referer': f'https://quote.eastmoney.com/f1.html?newcode={secid}',  # 更新Referer
    })

    try:
        # 发送请求 - 使用stream=True来处理SSE流
        response = requests.get(
            base_url,
            params=params,
            headers=headers,
            timeout=timeout,
            stream=True,
            verify=True
        )

        # 检查HTTP状态码
        if response.status_code != 200:
            print(f"  ✗ HTTP错误: {response.status_code}")
            return None

        # 处理SSE流式响应
        # 由于SSE是持续流,我们只读取前几秒的数据
        collected_data = []
        start_time = time.time()
        max_read_time = 5  # 最多读取5秒的数据

        try:
            for line in response.iter_lines(decode_unicode=True):
                # 检查是否超时
                if time.time() - start_time > max_read_time:
                    break

                if line:
                    # SSE格式通常是 "data: {...}"
                    if line.startswith('data:'):
                        json_str = line[5:].strip()  # 移除 "data:" 前缀
                        try:
                            data = json.loads(json_str)
                            collected_data.append(data)
                        except json.JSONDecodeError:
                            # 如果不是JSON,保存原始文本
                            collected_data.append({'raw': json_str})
        except Exception as e:
            print(f"  读取流数据时出错: {e}")
        finally:
            # 关闭连接
            response.close()

        return {
            'status_code': response.status_code,
            'data': collected_data,
            'headers': dict(response.headers),
            'url': response.url,
            'success': True
        }

    except requests.exceptions.Timeout:
        print(f"  ✗ 请求超时")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"  ✗ 连接错误: {e}")
        return None
    except Exception as e:
        print(f"  ✗ 请求异常: {e}")
        return None


def test_ip_blocking():
    """
    测试循环调用真实浏览器模拟请求是否会被封锁IP
    首次启动时使用Selenium获取真实浏览器参数,然后使用requests模拟
    """
    print("=" * 80)
    print("开始测试IP封锁情况（真实浏览器参数模拟请求）")
    print("=" * 80)
    print("策略: 启动时使用Selenium获取真实参数,然后使用requests高速请求")
    print("=" * 80)

    # 步骤1: 使用Selenium获取真实浏览器参数
    fetcher = RealBrowserParamsFetcher(headless=True)
    real_params = fetcher.fetch_real_params(stock_code='000001', max_wait=15)

    if not real_params or not real_params.get('success'):
        print("\n✗ 无法获取真实浏览器参数,将使用默认参数")
        # 使用默认参数
        real_params = {
            'url': 'https://7.push2.eastmoney.com/api/qt/stock/details/sse',
            'params': {
                'fields1': 'f1,f2,f3,f4',
                'fields2': 'f51,f52,f53,f54,f55',
                'mpi': '2000',
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fltt': '2',
                'pos': '-0',
                'secid': '0.000001',
                'wbp2u': '|0|0|0|web'
            },
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            },
            'success': True
        }
    else:
        print(f"\n✓ 成功获取真实浏览器参数")

    # 保存真实参数到文件
    try:
        with open('real_browser_params.json', 'w', encoding='utf-8') as f:
            json.dump(real_params, f, ensure_ascii=False, indent=2)
        print(f"✓ 真实参数已保存到: real_browser_params.json")
    except Exception as e:
        print(f"⚠️  保存参数文件失败: {e}")

    # 初始化动态延迟策略
    delay_strategy = DynamicDelayStrategy([10, 20, 30, 40, 50])
    print(f"\n延迟策略序列: {delay_strategy.delays} (循环)")

    # 获取所有股票代码
    print(f"\n{'='*80}")
    print("正在获取股票代码列表...")
    try:
        stock_df = stock_large_cap_filter()
        stock_codes = stock_df['代码'].tolist() if '代码' in stock_df.columns else stock_df.iloc[:, 0].tolist()
        print(f"成功获取 {len(stock_codes)} 只股票代码")
        print(f"前10个股票代码: {stock_codes[:10]}")
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return

    # 循环调用真实浏览器模拟请求
    success_count = 0
    fail_count = 0
    failed_stocks = []
    all_responses = []

    print(f"\n{'='*80}")
    print("开始循环调用 (使用真实浏览器参数)")
    print("按 Ctrl+C 可以停止测试")
    print(f"{'='*80}\n")

    try:
        idx = 0
        while idx < len(stock_codes):
            stock_code = stock_codes[idx]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            request_success = False

            try:
                print(f"[{current_time}] 第 {idx+1}/{len(stock_codes)} 次请求 - 股票代码: {stock_code}")

                # 使用真实参数调用请求
                response = get_stock_intraday_real_browser(stock_code, real_params, timeout=30)

                # 检查返回数据
                if response and response.get('success'):
                    success_count += 1
                    request_success = True

                    data_count = len(response.get('data', []))
                    print(f"  ✓ 成功获取数据，响应状态码: {response['status_code']}, 数据块数: {data_count}")

                    # 保存响应数据
                    all_responses.append({
                        'stock_code': stock_code,
                        'timestamp': current_time,
                        'response': response
                    })

                    # 显示部分数据示例
                    if data_count > 0:
                        print(f"  数据示例: {str(response['data'][0])[:100]}...")
                else:
                    fail_count += 1
                    print(f"  ✗ 返回数据为空或请求失败")

            except Exception as e:
                fail_count += 1
                print(f"  ✗ 请求失败: {str(e)}")

                # 如果是网络相关错误，可能是被封锁
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ['timeout', 'connection', 'network', '403', '429', 'forbidden']):
                    print(f"  ⚠️ 警告：可能已被封锁IP！错误: {e}")

                # 失败时额外等待5分钟
                print(f"  ⚠️ 请求失败，额外等待 5 分钟 (300秒)...")
                time.sleep(300)
                print(f"  5分钟等待完成，将重新请求该股票")

            # 只有成功时才移动到下一个股票
            if request_success:
                idx += 1
            else:
                # 记录失败的股票
                if stock_code not in failed_stocks:
                    failed_stocks.append(stock_code)
                print(f"  ↻ 将重新请求股票 {stock_code}")

            # 显示统计信息
            total_requests = success_count + fail_count
            print(f"  当前统计 - 成功: {success_count}, 失败: {fail_count}, 成功率: {success_count/total_requests*100:.2f}%")
            print(f"  进度: {success_count}/{len(stock_codes)} 只股票已完成")

            # 使用动态延迟策略获取下一个延迟时间
            next_delay = delay_strategy.get_next_delay()
            print(f"  等待 {next_delay} 秒...\n")
            time.sleep(next_delay)

    except KeyboardInterrupt:
        print("\n\n用户手动停止测试")

    # 打印最终统计
    print("\n" + "=" * 80)
    print("测试结束")
    print("=" * 80)
    print(f"总请求次数: {success_count + fail_count}")
    print(f"成功次数: {success_count}")
    print(f"失败次数: {fail_count}")
    if success_count + fail_count > 0:
        print(f"成功率: {success_count/(success_count+fail_count)*100:.2f}%")
    print(f"完成股票数: {success_count}/{len(stock_codes)}")
    if failed_stocks:
        print(f"曾经失败过的股票数: {len(failed_stocks)}")
        print(f"失败股票列表: {failed_stocks}")

    # 保存响应数据到文件
    if all_responses:
        try:
            with open('browser_simulation_responses.json', 'w', encoding='utf-8') as f:
                json.dump(all_responses, f, ensure_ascii=False, indent=2)
            print(f"\n✓ 响应数据已保存到: browser_simulation_responses.json")
        except Exception as e:
            print(f"\n✗ 保存响应数据失败: {e}")

    print("=" * 80)


if __name__ == "__main__":
    test_ip_blocking()
