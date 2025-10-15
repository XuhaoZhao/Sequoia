from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
from datetime import datetime
from data_collect.stock_chip_race import stock_large_cap_filter
import pandas as pd
import os
import stat
import platform
import json
import hashlib


class DynamicDelayStrategy:
    """
    动态延迟策略类
    按照设定的延迟序列循环返回延迟时间,并增加随机性
    """
    def __init__(self, delays=None):
        if delays is None:
            delays = [10, 20, 30, 40, 50]

        # 创建完整的循环序列
        self.delays = delays + delays[-2::-1]
        self.current_index = 0

    def get_next_delay(self):
        """获取下一个延迟时间,并增加随机波动"""
        base_delay = self.delays[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.delays)

        # 增加 ±20% 的随机波动
        random_factor = random.uniform(0.8, 1.2)
        actual_delay = base_delay * random_factor
        return round(actual_delay, 2)


class RequestComparator:
    """
    请求对比器,用于分析和对比多次请求的差异
    """
    def __init__(self):
        self.request_history = []

    def add_request(self, request_data):
        """添加请求记录"""
        self.request_history.append(request_data)

    def compare_requests(self, url_pattern='detail'):
        """
        对比包含特定关键字的请求
        返回差异分析结果
        """
        # 筛选包含关键字的请求
        filtered_requests = [
            req for req in self.request_history
            if url_pattern in req.get('url', '')
        ]

        if len(filtered_requests) < 2:
            return {
                'status': 'insufficient_data',
                'message': f'只找到 {len(filtered_requests)} 个包含 "{url_pattern}" 的请求,无法对比',
                'requests': filtered_requests
            }

        print(f"\n{'='*80}")
        print(f"请求对比分析 - 找到 {len(filtered_requests)} 个包含 '{url_pattern}' 的请求")
        print(f"{'='*80}\n")

        # 分析各个请求的差异
        comparison_results = []

        for i, req in enumerate(filtered_requests, 1):
            print(f"\n请求 #{i}:")
            print(f"  URL: {req.get('url', 'N/A')}")
            print(f"  时间戳: {req.get('timestamp', 'N/A')}")
            print(f"  方法: {req.get('method', 'N/A')}")

            # 分析URL参数
            url = req.get('url', '')
            if '?' in url:
                query_string = url.split('?')[1]
                params = dict(param.split('=') for param in query_string.split('&') if '=' in param)
                print(f"  URL参数 ({len(params)}个):")
                for key, value in params.items():
                    print(f"    - {key}: {value}")

            # 分析请求头
            headers = req.get('headers', {})
            print(f"  请求头 ({len(headers)}个):")
            interesting_headers = ['cookie', 'authorization', 'x-', 'sec-', 'referer', 'user-agent']
            for key, value in headers.items():
                if any(h in key.lower() for h in interesting_headers):
                    display_value = value[:100] + '...' if len(str(value)) > 100 else value
                    print(f"    - {key}: {display_value}")

            # 分析请求体
            request_body = req.get('postData', req.get('request_body', ''))
            if request_body:
                print(f"  请求体:")
                print(f"    {request_body[:500]}")

            comparison_results.append({
                'index': i,
                'url': req.get('url'),
                'timestamp': req.get('timestamp'),
                'headers': headers,
                'params': params if '?' in url else {},
                'body': request_body
            })

            print(f"  {'-'*70}")

        # 对比相同URL的请求差异
        if len(comparison_results) >= 2:
            print(f"\n{'='*80}")
            print("差异分析:")
            print(f"{'='*80}\n")

            # 对比第一个和最后一个请求
            first_req = comparison_results[0]
            last_req = comparison_results[-1]

            print("对比第一次请求和最后一次请求的差异:\n")

            # URL差异
            if first_req['url'] != last_req['url']:
                print("✓ URL有差异:")
                print(f"  第一次: {first_req['url']}")
                print(f"  最后一次: {last_req['url']}")
                self._analyze_url_diff(first_req['url'], last_req['url'])
            else:
                print("✗ URL完全相同")

            # 参数差异
            if first_req['params'] or last_req['params']:
                param_diff = self._compare_dicts(first_req['params'], last_req['params'])
                if param_diff['changed'] or param_diff['added'] or param_diff['removed']:
                    print("\n✓ URL参数有差异:")
                    for key in param_diff['changed']:
                        print(f"  参数 '{key}' 值不同:")
                        print(f"    第一次: {first_req['params'].get(key)}")
                        print(f"    最后一次: {last_req['params'].get(key)}")
                    for key in param_diff['added']:
                        print(f"  新增参数 '{key}': {last_req['params'].get(key)}")
                    for key in param_diff['removed']:
                        print(f"  移除参数 '{key}': {first_req['params'].get(key)}")
                else:
                    print("\n✗ URL参数完全相同")

            # 请求头差异
            header_diff = self._compare_dicts(first_req['headers'], last_req['headers'])
            if header_diff['changed']:
                print("\n✓ 请求头有差异:")
                for key in header_diff['changed']:
                    val1 = first_req['headers'].get(key, '')
                    val2 = last_req['headers'].get(key, '')
                    if len(str(val1)) > 100 or len(str(val2)) > 100:
                        print(f"  请求头 '{key}' 值不同 (长度: {len(str(val1))} vs {len(str(val2))})")
                    else:
                        print(f"  请求头 '{key}' 值不同:")
                        print(f"    第一次: {val1}")
                        print(f"    最后一次: {val2}")
            else:
                print("\n✗ 主要请求头完全相同")

            # 请求体差异
            if first_req['body'] or last_req['body']:
                if first_req['body'] != last_req['body']:
                    print("\n✓ 请求体有差异:")
                    print(f"  第一次: {first_req['body'][:200]}")
                    print(f"  最后一次: {last_req['body'][:200]}")
                else:
                    print("\n✗ 请求体完全相同")

            # 检测可能的防重放机制
            print(f"\n{'='*80}")
            print("防重放机制检测:")
            print(f"{'='*80}\n")

            replay_protection = self._detect_replay_protection(comparison_results)
            if replay_protection:
                print("✓ 检测到可能的防重放机制:")
                for item in replay_protection:
                    print(f"  - {item}")
            else:
                print("✗ 未检测到明显的防重放机制")

        return {
            'status': 'success',
            'total_requests': len(filtered_requests),
            'comparison_results': comparison_results,
            'replay_protection': replay_protection if len(comparison_results) >= 2 else []
        }

    def _compare_dicts(self, dict1, dict2):
        """对比两个字典的差异"""
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())

        changed = []
        for key in keys1 & keys2:
            if dict1[key] != dict2[key]:
                changed.append(key)

        return {
            'changed': changed,
            'added': list(keys2 - keys1),
            'removed': list(keys1 - keys2)
        }

    def _analyze_url_diff(self, url1, url2):
        """分析两个URL的差异"""
        print("\n  详细URL差异分析:")

        # 分离base和query
        if '?' in url1:
            base1, query1 = url1.split('?', 1)
            params1 = dict(p.split('=') for p in query1.split('&') if '=' in p)
        else:
            base1, params1 = url1, {}

        if '?' in url2:
            base2, query2 = url2.split('?', 1)
            params2 = dict(p.split('=') for p in query2.split('&') if '=' in p)
        else:
            base2, params2 = url2, {}

        if base1 != base2:
            print(f"  Base URL不同:")
            print(f"    第一次: {base1}")
            print(f"    最后一次: {base2}")

        # 对比参数
        diff = self._compare_dicts(params1, params2)
        if diff['changed']:
            print(f"  变化的参数:")
            for key in diff['changed']:
                print(f"    - {key}: '{params1[key]}' → '{params2[key]}'")

    def _detect_replay_protection(self, comparison_results):
        """检测防重放机制"""
        protection_mechanisms = []

        # 检测常见的防重放参数名
        time_related = ['timestamp', 'time', 't', 'ts', '_', 'datetime', 'nonce']
        signature_related = ['sign', 'signature', 'sig', 'token', 'hash', 'auth']

        # 收集所有出现过的参数
        all_params = set()
        for req in comparison_results:
            all_params.update(req['params'].keys())

        # 检测时间戳参数
        found_time_params = [p for p in all_params if any(t in p.lower() for t in time_related)]
        if found_time_params:
            protection_mechanisms.append(f"时间戳参数: {', '.join(found_time_params)}")

        # 检测签名参数
        found_sig_params = [p for p in all_params if any(s in p.lower() for s in signature_related)]
        if found_sig_params:
            protection_mechanisms.append(f"签名参数: {', '.join(found_sig_params)}")

        # 检测Cookie变化
        cookies = [req['headers'].get('Cookie', '') for req in comparison_results]
        if cookies and len(set(cookies)) > 1:
            protection_mechanisms.append("Cookie在请求之间发生变化")

        # 检测动态Token
        for header_name in ['Authorization', 'X-Token', 'X-Auth-Token']:
            values = [req['headers'].get(header_name, '') for req in comparison_results]
            if values and len(set(values)) > 1:
                protection_mechanisms.append(f"{header_name} 在请求之间发生变化")

        return protection_mechanisms

    def save_to_file(self, filename='request_comparison_log.json'):
        """保存请求对比日志到文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'total_requests': len(self.request_history),
                    'timestamp': datetime.now().isoformat(),
                    'requests': self.request_history
                }, f, ensure_ascii=False, indent=2)
            print(f"\n✓ 请求对比日志已保存到: {filename}")
            return True
        except Exception as e:
            print(f"\n✗ 保存请求日志失败: {e}")
            return False


class SeleniumStockFetcher:
    """
    使用 Selenium 获取股票分时数据,并拦截记录所有网络请求
    """
    def __init__(self, headless=False):
        """
        初始化 Selenium WebDriver
        :param headless: 是否无头模式(不显示浏览器窗口)
        """
        self.driver = None
        self.headless = headless
        self.request_comparator = RequestComparator()
        self.init_driver()

    def init_driver(self):
        """初始化 Chrome WebDriver (macOS 优化版本)"""
        print("初始化 Chrome 浏览器 (macOS)...")

        chrome_options = Options()

        # macOS 特定 - 明确指定 Chrome 浏览器路径
        if platform.system() == 'Darwin':
            chrome_options.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'

        # 无头模式
        if self.headless:
            chrome_options.add_argument('--headless=new')  # 使用新版 headless 模式
            chrome_options.add_argument('--disable-gpu')

        # 禁用一些不必要的功能,提高性能
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        # 设置窗口大小
        chrome_options.add_argument('--window-size=1920,1080')

        # 启用性能日志,用于拦截网络请求
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

        # 去除自动化标识
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            # 使用 webdriver-manager 自动管理 ChromeDriver
            print("  正在下载/配置 ChromeDriver...")
            driver_path = ChromeDriverManager().install()
            print(f"  ChromeDriver 路径: {driver_path}")

            # macOS 特定处理 - 确保 ChromeDriver 有执行权限并移除隔离属性
            if platform.system() == 'Darwin':
                # 添加执行权限
                current_permissions = os.stat(driver_path).st_mode
                os.chmod(driver_path, current_permissions | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                print(f"  ✓ 已设置 ChromeDriver 执行权限")

                # 移除 macOS 的隔离属性 (quarantine attribute)
                try:
                    os.system(f'xattr -d com.apple.quarantine "{driver_path}" 2>/dev/null')
                    print(f"  ✓ 已移除 ChromeDriver 隔离属性")
                except:
                    pass

            service = Service(driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 执行 CDP 命令,进一步隐藏 webdriver 特征
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

            # macOS 特定的错误提示
            if platform.system() == 'Darwin':
                print(f"\n  macOS 特定解决方案:")
                print(f"  1. 手动移除 ChromeDriver 隔离属性:")
                print(f"     打开终端运行: xattr -d com.apple.quarantine ~/.wdm/drivers/chromedriver/mac64/*/chromedriver")
                print(f"  2. 或者直接删除缓存重新下载:")
                print(f"     rm -rf ~/.wdm/drivers/chromedriver")
                print(f"  3. 确保 Chrome 浏览器已安装在: /Applications/Google Chrome.app")

            print(f"\n  通用解决方案: pip install --upgrade selenium webdriver-manager")
            return False

    def get_stock_intraday_data(self, stock_code):
        """
        获取股票分时数据 - 通过CDP拦截所有网络请求,特别关注包含'detail'的请求
        :param stock_code: 股票代码,如 '000001'
        :return: 包含完整分时数据的字典或 None
        """
        try:
            # 启用Network domain来拦截请求
            self.driver.execute_cdp_cmd('Network.enable', {})

            # 构造 URL
            if stock_code.startswith('6'):
                secid = f"1.{stock_code}"
            else:
                secid = f"0.{stock_code}"

            url = f"https://quote.eastmoney.com/f1.html?newcode={secid}"

            print(f"\n{'='*80}")
            print(f"开始拦截请求 - 股票代码: {stock_code}")
            print(f"{'='*80}")
            print(f"  访问页面: {url}")

            # 访问页面
            self.driver.get(url)

            # 等待页面加载
            wait = WebDriverWait(self.driver, 15)

            try:
                # 等待股票名称加载
                stock_name_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".stockName, .name"))
                )
                stock_name = stock_name_element.text
                print(f"  ✓ 页面加载成功,股票: {stock_name} ({stock_code})")
            except TimeoutException:
                print(f"  ⚠️ 页面加载超时")
                stock_name = None

            # 等待网络请求完成
            print(f"  等待网络请求完成...")
            time.sleep(8)  # 等待所有请求完成

            # 从性能日志中提取网络请求
            print(f"\n  正在分析网络请求日志...")
            logs = self.driver.get_log('performance')

            detail_requests = []
            all_requests_count = 0

            for log in logs:
                try:
                    message = json.loads(log['message'])
                    method = message.get('message', {}).get('method', '')

                    # 捕获请求发送事件
                    if method == 'Network.requestWillBeSent':
                        all_requests_count += 1
                        request_data = message['message']['params']
                        request_url = request_data.get('request', {}).get('url', '')

                        # 检查URL是否包含'detail'
                        if 'detail' in request_url.lower():
                            request_id = request_data.get('requestId')

                            request_info = {
                                'request_id': request_id,
                                'url': request_url,
                                'method': request_data.get('request', {}).get('method', 'GET'),
                                'headers': request_data.get('request', {}).get('headers', {}),
                                'postData': request_data.get('request', {}).get('postData', ''),
                                'timestamp': request_data.get('timestamp', ''),
                                'wallTime': datetime.fromtimestamp(request_data.get('wallTime', 0)).isoformat() if request_data.get('wallTime') else '',
                                'initiator': request_data.get('initiator', {}),
                                'stock_code': stock_code,
                                'stock_name': stock_name
                            }

                            # 尝试获取响应数据
                            try:
                                response = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': request_id})
                                request_info['response_body'] = response.get('body', '')
                                request_info['response_base64'] = response.get('base64Encoded', False)
                            except Exception as e:
                                request_info['response_body'] = f'无法获取响应体: {str(e)}'

                            detail_requests.append(request_info)

                            # 添加到对比器
                            self.request_comparator.add_request(request_info)

                            print(f"\n  ✓ 拦截到包含'detail'的请求:")
                            print(f"    URL: {request_url[:100]}...")
                            print(f"    方法: {request_info['method']}")
                            print(f"    时间: {request_info['wallTime']}")

                except Exception as e:
                    continue

            print(f"\n  总共分析了 {len(logs)} 条日志,发现 {all_requests_count} 个网络请求")
            print(f"  ✓ 找到 {len(detail_requests)} 个包含'detail'的请求\n")

            if detail_requests:
                # 详细打印每个detail请求
                print(f"{'='*80}")
                print(f"详细请求信息:")
                print(f"{'='*80}\n")

                for idx, req in enumerate(detail_requests, 1):
                    print(f"\n请求 #{idx}:")
                    print(f"  URL: {req['url']}")
                    print(f"  方法: {req['method']}")
                    print(f"  时间: {req['wallTime']}")

                    # 打印重要的请求头
                    print(f"  请求头:")
                    important_headers = ['cookie', 'authorization', 'user-agent', 'referer', 'origin', 'accept', 'content-type']
                    for key, value in req['headers'].items():
                        if any(h in key.lower() for h in important_headers):
                            display_value = str(value)[:100] + '...' if len(str(value)) > 100 else value
                            print(f"    {key}: {display_value}")

                    # 打印请求体
                    if req.get('postData'):
                        print(f"  请求体: {req['postData'][:500]}")

                    # 打印响应体(前500字符)
                    response_body = req.get('response_body', '')
                    if response_body and '无法获取' not in response_body:
                        print(f"  响应体 (前500字符): {response_body[:500]}")

                    print(f"  {'-'*70}")

                return {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'detail_requests': detail_requests,
                    'total_requests': all_requests_count,
                    'success': True,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                print(f"  ⚠️ 未找到包含'detail'的请求")
                return None

        except TimeoutException:
            print(f"  ✗ 页面加载超时")
            return None
        except Exception as e:
            print(f"  ✗ 请求失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def close(self):
        """关闭浏览器"""
        if self.driver:
            print("\n关闭浏览器...")
            self.driver.quit()


def test_ip_blocking_selenium():
    """
    使用 Selenium 测试 IP 封锁,并详细记录分析包含'detail'的请求
    """
    print("=" * 80)
    print("开始测试IP封锁情况(Selenium 模式 - 网络请求拦截分析)")
    print("=" * 80)

    # 初始化延迟策略
    delay_strategy = DynamicDelayStrategy([10, 20, 30, 40, 50])
    print(f"延迟策略序列: {delay_strategy.delays} (循环, 带随机波动)")

    # 获取所有股票代码
    print("\n正在获取股票代码列表...")
    try:
        stock_df = stock_large_cap_filter()
        stock_codes = stock_df['代码'].tolist() if '代码' in stock_df.columns else stock_df.iloc[:, 0].tolist()
        print(f"成功获取 {len(stock_codes)} 只股票代码")
        print(f"前10个股票代码: {stock_codes[:10]}")
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return

    # 初始化 Selenium
    print("\n" + "=" * 80)
    fetcher = SeleniumStockFetcher(headless=False)  # 设置 headless=True 可无头运行

    if not fetcher.driver:
        print("浏览器初始化失败,退出测试")
        return

    # 统计数据
    success_count = 0
    fail_count = 0
    failed_stocks = []
    all_data = []

    print("\n开始循环调用")
    print("按 Ctrl+C 可以停止测试\n")

    try:
        idx = 0
        while idx < len(stock_codes):
            stock_code = stock_codes[idx]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            request_success = False

            print(f"\n[{current_time}] 第 {idx+1}/{len(stock_codes)} 次请求 - 股票代码: {stock_code}")

            # 获取数据
            data = fetcher.get_stock_intraday_data(stock_code)

            if data and data.get('success'):
                success_count += 1
                request_success = True
                all_data.append(data)

                # 每5个请求进行一次对比分析
                if success_count % 5 == 0:
                    print(f"\n{'='*80}")
                    print(f"执行累积请求对比分析 (已完成 {success_count} 个请求)")
                    print(f"{'='*80}")
                    fetcher.request_comparator.compare_requests(url_pattern='detail')
            else:
                fail_count += 1
                print(f"  ✗ 获取数据失败")

            # 失败处理
            if not request_success:
                if stock_code not in failed_stocks:
                    failed_stocks.append(stock_code)

                print(f"  ⚠️ 请求失败,额外等待 5 分钟 (300秒)...")
                time.sleep(300)
                print(f"  5分钟等待完成,将重新请求该股票")
                print(f"  ↻ 将重新请求股票 {stock_code}")
            else:
                # 成功,移动到下一个
                idx += 1

            # 显示统计
            total_requests = success_count + fail_count
            print(f"\n  当前统计 - 成功: {success_count}, 失败: {fail_count}, 成功率: {success_count/total_requests*100:.2f}%")
            print(f"  进度: {success_count}/{len(stock_codes)} 只股票已完成")

            # 动态延迟
            next_delay = delay_strategy.get_next_delay()
            print(f"  等待 {next_delay} 秒...\n")
            time.sleep(next_delay)

    except KeyboardInterrupt:
        print("\n\n用户手动停止测试")
    finally:
        # 最终对比分析
        print(f"\n{'='*80}")
        print("最终请求对比分析")
        print(f"{'='*80}")
        fetcher.request_comparator.compare_requests(url_pattern='detail')

        # 保存请求对比日志
        fetcher.request_comparator.save_to_file('request_comparison_log.json')

        # 关闭浏览器
        fetcher.close()

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
        print(f"失败股票列表: {failed_stocks[:20]}")

    # 保存数据到文件
    if all_data:
        try:
            # 保存详细的请求数据
            with open('detailed_requests_log.json', 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            print(f"\n✓ 详细请求数据已保存到: detailed_requests_log.json")

            # 保存摘要数据到CSV
            summary_data = []
            for item in all_data:
                summary_data.append({
                    'stock_code': item.get('stock_code'),
                    'stock_name': item.get('stock_name'),
                    'total_requests': item.get('total_requests'),
                    'detail_requests_count': len(item.get('detail_requests', [])),
                    'timestamp': item.get('timestamp')
                })

            df = pd.DataFrame(summary_data)
            output_file = 'stock_data_selenium.csv'
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"✓ 摘要数据已保存到: {output_file}")
        except Exception as e:
            print(f"\n✗ 保存数据失败: {e}")

    print("=" * 80)


if __name__ == "__main__":
    test_ip_blocking_selenium()
