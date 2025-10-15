from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
from datetime import datetime
from data_collect.stock_chip_race import stock_large_cap_filter
import pandas as pd


class DynamicDelayStrategy:
    """
    动态延迟策略类
    按照设定的延迟序列循环返回延迟时间，并增加随机性
    """
    def __init__(self, delays=None):
        if delays is None:
            delays = [10, 20, 30, 40, 50]

        # 创建完整的循环序列
        self.delays = delays + delays[-2::-1]
        self.current_index = 0

    def get_next_delay(self):
        """获取下一个延迟时间，并增加随机波动"""
        base_delay = self.delays[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.delays)

        # 增加 ±20% 的随机波动
        random_factor = random.uniform(0.8, 1.2)
        actual_delay = base_delay * random_factor
        return round(actual_delay, 2)


class SeleniumStockFetcher:
    """
    使用 Selenium 获取股票分时数据
    """
    def __init__(self, headless=False):
        """
        初始化 Selenium WebDriver
        :param headless: 是否无头模式（不显示浏览器窗口）
        """
        self.driver = None
        self.headless = headless
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

        # 禁用图片加载（可选，提高速度）
        # prefs = {'profile.managed_default_content_settings.images': 2}
        # chrome_options.add_experimental_option('prefs', prefs)

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
            print(f"  如果问题依然存在，请尝试: pip install --upgrade selenium webdriver-manager")
            return False

    def get_stock_intraday_data(self, stock_code):
        """
        获取股票分时数据 - 通过拦截网络请求获取完整数据
        :param stock_code: 股票代码，如 '000001'
        :return: 包含完整分时数据的字典或 None
        """
        try:
            # 构造 URL
            if stock_code.startswith('6'):
                secid = f"1.{stock_code}"
            else:
                secid = f"0.{stock_code}"

            url = f"https://quote.eastmoney.com/f1.html?newcode={secid}"

            print(f"  访问页面: {url}")

            # 启用网络拦截
            self.driver.execute_cdp_cmd('Network.enable', {})

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
                print(f"  ✓ 页面加载成功，股票: {stock_name} ({stock_code})")
            except TimeoutException:
                print(f"  ⚠️ 页面加载超时")

            # 等待一下让数据加载完成
            time.sleep(random.uniform(2, 4))

            # 方法1: 从页面中提取 JavaScript 变量（分时数据通常存在全局变量中）
            try:
                trend_data = self.driver.execute_script("""
                    // 尝试获取分时数据
                    // 东方财富通常会把数据存在全局变量中

                    // 方式1: 从 window 对象获取
                    if (window.fsjson) return window.fsjson;
                    if (window.trendData) return window.trendData;
                    if (window.MinuteData) return window.MinuteData;

                    // 方式2: 从 localStorage 获取
                    try {
                        const localData = localStorage.getItem('trendData');
                        if (localData) return JSON.parse(localData);
                    } catch(e) {}

                    // 方式3: 查找页面中的 script 标签
                    const scripts = document.getElementsByTagName('script');
                    for (let script of scripts) {
                        const content = script.textContent;
                        if (content.includes('var fsjson') || content.includes('分时数据')) {
                            // 尝试解析
                            return content;
                        }
                    }

                    return null;
                """)

                if trend_data:
                    print(f"  ✓ 成功提取分时数据（方法1: JavaScript变量）")
                    return {
                        'stock_code': stock_code,
                        'stock_name': stock_name if 'stock_name' in locals() else None,
                        'trend_data': trend_data,
                        'method': 'javascript_var',
                        'success': True,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                print(f"  ⚠️ JavaScript提取失败: {e}")

            # 方法2: 获取网络请求日志，找到 API 请求
            try:
                logs = self.driver.get_log('performance')
                api_data = None

                for log in logs:
                    import json
                    message = json.loads(log['message'])
                    method = message.get('message', {}).get('method', '')

                    # 查找网络响应
                    if method == 'Network.responseReceived':
                        response = message.get('message', {}).get('params', {}).get('response', {})
                        url = response.get('url', '')

                        # 判断是否是分时数据的 API
                        if 'api/qt/stock' in url or 'fsjson' in url or 'min' in url:
                            print(f"  ✓ 找到数据API: {url[:100]}...")
                            # 这里可以继续处理
                            api_data = url
                            break

                if api_data:
                    print(f"  ✓ 成功找到数据API（方法2: 网络拦截）")
            except Exception as e:
                print(f"  ⚠️ 网络日志提取失败: {e}")

            # 方法3: 直接解析页面元素 - 适用于表格形式的数据
            try:
                # 查找分时数据表格或列表
                # 这里需要根据实际页面结构调整选择器

                # 尝试滚动页面，加载所有数据
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # 提取表格数据
                table_data = self.driver.execute_script("""
                    const data = [];

                    // 尝试找到分时数据的容器
                    const container = document.querySelector('#fschart, .trend-container, #minuteTab');
                    if (!container) return null;

                    // 提取所有数据点
                    const rows = container.querySelectorAll('tr, .data-row, .minute-item');
                    rows.forEach(row => {
                        const cells = row.querySelectorAll('td, span, div');
                        if (cells.length > 0) {
                            const rowData = Array.from(cells).map(cell => cell.textContent.trim());
                            data.push(rowData);
                        }
                    });

                    return data.length > 0 ? data : null;
                """)

                if table_data:
                    print(f"  ✓ 成功提取表格数据，共 {len(table_data)} 行")
                    return {
                        'stock_code': stock_code,
                        'stock_name': stock_name if 'stock_name' in locals() else None,
                        'table_data': table_data,
                        'method': 'table_parsing',
                        'success': True,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                print(f"  ⚠️ 表格解析失败: {e}")

            # 方法4: 提取页面上的基本行情数据（作为后备方案）
            try:
                basic_data = self.driver.execute_script("""
                    return {
                        current_price: document.querySelector('.zxj, .price, [class*="price"]')?.textContent,
                        change: document.querySelector('.zd, .change')?.textContent,
                        change_percent: document.querySelector('.zdf, .percent')?.textContent,
                        volume: document.querySelector('.cje, .volume')?.textContent,
                        turnover: document.querySelector('.hsl, .turnover')?.textContent,
                        high: document.querySelector('.zgj, .high')?.textContent,
                        low: document.querySelector('.zdj, .low')?.textContent,
                        open: document.querySelector('.jrk, .open')?.textContent,
                    };
                """)

                # 过滤掉空值
                basic_data = {k: v for k, v in basic_data.items() if v}

                if basic_data:
                    print(f"  ✓ 成功提取基本行情数据: {list(basic_data.keys())}")
                    return {
                        'stock_code': stock_code,
                        'stock_name': stock_name if 'stock_name' in locals() else None,
                        'basic_data': basic_data,
                        'method': 'basic_info',
                        'success': True,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                print(f"  ⚠️ 基本数据提取失败: {e}")

            print(f"  ✗ 所有数据提取方法均失败")
            return None

        except TimeoutException:
            print(f"  ✗ 页面加载超时")
            return None
        except Exception as e:
            print(f"  ✗ 请求失败: {e}")
            return None

    def close(self):
        """关闭浏览器"""
        if self.driver:
            print("\n关闭浏览器...")
            self.driver.quit()


def test_ip_blocking_selenium():
    """
    使用 Selenium 测试 IP 封锁
    """
    print("=" * 80)
    print("开始测试IP封锁情况（Selenium 模式 - 真实浏览器）")
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
        print("浏览器初始化失败，退出测试")
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

            print(f"[{current_time}] 第 {idx+1}/{len(stock_codes)} 次请求 - 股票代码: {stock_code}")

            # 获取数据
            data = fetcher.get_stock_intraday_data(stock_code)

            if data and data.get('success'):
                success_count += 1
                request_success = True
                all_data.append(data)
            else:
                fail_count += 1
                print(f"  ✗ 获取数据失败")

            # 失败处理
            if not request_success:
                if stock_code not in failed_stocks:
                    failed_stocks.append(stock_code)

                print(f"  ⚠️ 请求失败，额外等待 5 分钟 (300秒)...")
                time.sleep(300)
                print(f"  5分钟等待完成，将重新请求该股票")
                print(f"  ↻ 将重新请求股票 {stock_code}")
            else:
                # 成功，移动到下一个
                idx += 1

            # 显示统计
            total_requests = success_count + fail_count
            print(f"  当前统计 - 成功: {success_count}, 失败: {fail_count}, 成功率: {success_count/total_requests*100:.2f}%")
            print(f"  进度: {success_count}/{len(stock_codes)} 只股票已完成")

            # 动态延迟
            next_delay = delay_strategy.get_next_delay()
            print(f"  等待 {next_delay} 秒...\n")
            time.sleep(next_delay)

    except KeyboardInterrupt:
        print("\n\n用户手动停止测试")
    finally:
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
            df = pd.DataFrame(all_data)
            output_file = 'stock_data_selenium.csv'
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n✓ 数据已保存到: {output_file}")
        except Exception as e:
            print(f"\n✗ 保存数据失败: {e}")

    print("=" * 80)


if __name__ == "__main__":
    test_ip_blocking_selenium()
