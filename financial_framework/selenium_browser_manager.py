"""
Selenium 浏览器管理器

封装所有 Selenium 相关的操作，包括：
1. ChromeDriver 初始化和查找
2. 浏览器配置和启动
3. 网络拦截（CDP 命令）
4. 页面操作（访问、刷新、等待）
5. 性能日志获取
6. 浏览器关闭
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import os
import platform
import subprocess
import shutil
import json

# 导入日志系统
try:
    from .logger_config import get_logger
except ImportError:
    from logger_config import get_logger


class SeleniumBrowserManager:
    """
    Selenium 浏览器管理器
    负责所有与 Selenium WebDriver 相关的操作
    """

    def __init__(self, headless=False, logger_name='selenium_browser_manager'):
        """
        初始化浏览器管理器
        :param headless: 是否无头模式(不显示浏览器窗口)
        :param logger_name: 日志器名称
        """
        self.logger = get_logger(logger_name)
        self.driver = None
        self.headless = headless

        self.logger.info("初始化 Selenium 浏览器管理器")

    def _find_local_chromedriver(self):
        """
        查找本地已安装的 ChromeDriver
        优先级：
        1. webdriver-manager 缓存目录
        2. 系统 PATH
        3. 常见安装路径
        """
        system_name = platform.system()
        self.logger.debug(f"查找本地ChromeDriver - 系统: {system_name}")

        # 1. 检查 webdriver-manager 缓存
        home_dir = os.path.expanduser('~')
        wdm_cache_paths = []

        if system_name == 'Darwin':  # macOS
            wdm_cache_paths = [
                os.path.join(home_dir, '.wdm', 'drivers', 'chromedriver'),
            ]
        elif system_name == 'Windows':
            wdm_cache_paths = [
                os.path.join(home_dir, '.wdm', 'drivers', 'chromedriver'),
            ]
        else:  # Linux
            wdm_cache_paths = [
                os.path.join(home_dir, '.wdm', 'drivers', 'chromedriver'),
            ]

        # 搜索缓存目录中的 chromedriver
        for cache_path in wdm_cache_paths:
            if os.path.exists(cache_path):
                # 递归查找 chromedriver 可执行文件
                for root, _, files in os.walk(cache_path):
                    for file in files:
                        # Windows: 必须是 chromedriver.exe，跳过 .zip 文件
                        # macOS/Linux: chromedriver (无扩展名)
                        if system_name == 'Windows':
                            if file.lower() == 'chromedriver.exe':
                                driver_path = os.path.join(root, file)
                                if os.path.isfile(driver_path):
                                    self.logger.info(f"找到本地ChromeDriver: {driver_path}")
                                    return driver_path
                        else:
                            if file == 'chromedriver' and not file.endswith('.zip'):
                                driver_path = os.path.join(root, file)
                                # 验证是否可执行
                                if os.access(driver_path, os.X_OK):
                                    self.logger.info(f"找到本地ChromeDriver: {driver_path}")
                                    return driver_path

        # 2. 检查系统 PATH
        chromedriver_in_path = shutil.which('chromedriver')
        if chromedriver_in_path:
            self.logger.info(f"找到系统PATH中的ChromeDriver: {chromedriver_in_path}")
            return chromedriver_in_path

        # 3. 检查常见安装路径
        common_paths = []
        if system_name == 'Darwin':  # macOS
            common_paths = [
                '/usr/local/bin/chromedriver',
                '/opt/homebrew/bin/chromedriver',
            ]
        elif system_name == 'Windows':
            common_paths = [
                'C:\\Program Files\\chromedriver.exe',
                'C:\\chromedriver.exe',
            ]
        else:  # Linux
            common_paths = [
                '/usr/bin/chromedriver',
                '/usr/local/bin/chromedriver',
            ]

        for path in common_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                self.logger.info(f"找到常见路径中的ChromeDriver: {path}")
                return path

        self.logger.warning("未找到本地ChromeDriver，将联网下载")
        return None

    def _get_chrome_version(self):
        """获取本地 Chrome 浏览器版本"""
        system_name = platform.system()

        try:
            if system_name == 'Darwin':  # macOS
                chrome_path = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
                if os.path.exists(chrome_path):
                    result = subprocess.run([chrome_path, '--version'],
                                          capture_output=True, text=True, timeout=5)
                    version = result.stdout.strip().split()[-1]
                    return version
            elif system_name == 'Windows':
                chrome_paths = [
                    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
                    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
                ]
                for chrome_path in chrome_paths:
                    if os.path.exists(chrome_path):
                        result = subprocess.run([chrome_path, '--version'],
                                              capture_output=True, text=True, timeout=5)
                        version = result.stdout.strip().split()[-1]
                        return version
            else:  # Linux
                result = subprocess.run(['google-chrome', '--version'],
                                      capture_output=True, text=True, timeout=5)
                version = result.stdout.strip().split()[-1]
                return version
        except Exception as e:
            self.logger.debug(f"获取Chrome版本失败: {e}")

        return None

    def init_driver(self):
        """初始化 Chrome WebDriver - 优先使用本地已安装的 ChromeDriver"""
        self.logger.info("开始初始化Chrome浏览器...")

        chrome_options = Options()

        # 无头模式
        if self.headless:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            self.logger.debug("已启用无头模式")

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
            # 获取 Chrome 版本
            chrome_version = self._get_chrome_version()
            if chrome_version:
                self.logger.info(f"检测到Chrome版本: {chrome_version}")

            # 先尝试使用本地 ChromeDriver
            local_driver_path = self._find_local_chromedriver()
            service = None

            if local_driver_path:
                self.logger.info("使用本地ChromeDriver (跳过联网检查)")
                try:
                    service = Service(local_driver_path)
                    # 尝试创建driver验证路径是否有效
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                except Exception as e:
                    self.logger.warning(f"本地ChromeDriver无法使用: {e}")
                    self.logger.info("将尝试使用webdriver-manager重新下载")
                    service = None
                    self.driver = None

            # 如果本地ChromeDriver不可用，使用webdriver-manager下载
            if not service or not self.driver:
                self.logger.info("使用webdriver-manager下载ChromeDriver")
                driver_path = ChromeDriverManager().install()
                self.logger.info(f"ChromeDriver已下载: {driver_path}")
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 执行 CDP 命令，进一步隐藏 webdriver 特征
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })

            self.logger.info("Chrome浏览器初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"浏览器初始化失败: {e}", exc_info=True)
            self.logger.error("请确保已安装Chrome浏览器")
            return False

    def enable_network_interception(self):
        """启用网络拦截功能"""
        if not self.driver:
            self.logger.error("浏览器未初始化，无法启用网络拦截")
            return False

        try:
            self.driver.execute_cdp_cmd('Network.enable', {})
            self.logger.info("网络拦截已启用")
            return True
        except Exception as e:
            self.logger.error(f"启用网络拦截失败: {e}", exc_info=True)
            return False

    def navigate_to(self, url, wait_timeout=20):
        """
        访问指定URL并等待页面加载
        :param url: 目标URL
        :param wait_timeout: 等待超时时间（秒）
        :return: 是否成功加载
        """
        if not self.driver:
            self.logger.error("浏览器未初始化，无法访问页面")
            return False

        try:
            self.logger.info(f"访问页面: {url}")
            self.driver.get(url)

            # 等待页面加载
            wait = WebDriverWait(self.driver, wait_timeout)
            wait.until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            self.logger.info("页面加载成功")
            return True
        except TimeoutException:
            self.logger.warning("页面加载超时")
            return False
        except Exception as e:
            self.logger.error(f"访问页面失败: {e}", exc_info=True)
            return False

    def refresh_page(self, wait_timeout=20):
        """
        刷新当前页面并等待加载
        :param wait_timeout: 等待超时时间（秒）
        :return: 是否成功刷新
        """
        if not self.driver:
            self.logger.error("浏览器未初始化，无法刷新页面")
            return False

        try:
            self.logger.info("刷新页面...")
            self.driver.refresh()

            # 等待页面重新加载
            wait = WebDriverWait(self.driver, wait_timeout)
            wait.until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            self.logger.info("页面刷新成功")
            return True
        except TimeoutException:
            self.logger.warning("页面刷新超时")
            return False
        except Exception as e:
            self.logger.error(f"刷新页面失败: {e}", exc_info=True)
            return False

    def get_performance_logs(self):
        """
        获取浏览器性能日志（用于网络请求拦截）
        :return: 性能日志列表
        """
        if not self.driver:
            self.logger.error("浏览器未初始化，无法获取性能日志")
            return []

        try:
            logs = self.driver.get_log('performance')
            return logs
        except Exception as e:
            self.logger.error(f"获取性能日志失败: {e}", exc_info=True)
            return []

    def get_response_body(self, request_id):
        """
        通过 CDP 命令获取指定请求的响应体
        :param request_id: 请求ID
        :return: 响应体内容，失败返回 None
        """
        if not self.driver:
            self.logger.error("浏览器未初始化，无法获取响应体")
            return None

        try:
            response_body = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': request_id})
            body = response_body.get('body', '')
            return body
        except Exception as e:
            self.logger.debug(f"获取响应体失败 (request_id={request_id}): {e}")
            return None

    def execute_cdp_cmd(self, cmd, params):
        """
        执行 Chrome DevTools Protocol 命令
        :param cmd: CDP 命令名称
        :param params: 命令参数
        :return: 命令执行结果
        """
        if not self.driver:
            self.logger.error("浏览器未初始化，无法执行CDP命令")
            return None

        try:
            result = self.driver.execute_cdp_cmd(cmd, params)
            return result
        except Exception as e:
            self.logger.error(f"执行CDP命令失败 (cmd={cmd}): {e}", exc_info=True)
            return None

    def is_initialized(self):
        """检查浏览器是否已初始化"""
        return self.driver is not None

    def close(self):
        """关闭浏览器"""
        if self.driver:
            self.logger.info("关闭浏览器...")
            try:
                self.driver.quit()
                self.driver = None
                self.logger.info("浏览器已关闭")
            except Exception as e:
                self.logger.error(f"关闭浏览器失败: {e}", exc_info=True)
        else:
            self.logger.warning("浏览器未初始化，无需关闭")

    def __enter__(self):
        """上下文管理器入口"""
        self.init_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
