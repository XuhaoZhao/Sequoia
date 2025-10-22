"""
点击事件监听器

使用 Selenium 打开指定网站，监听并记录用户在页面上的所有点击事件
"""

import time
import json
from datetime import datetime
import os

# 导入日志系统
try:
    from .logger_config import FinancialLogger, get_logger
    from .selenium_browser_manager import SeleniumBrowserManager
except ImportError:
    # 如果相对导入失败（直接运行此文件），尝试绝对导入
    from logger_config import FinancialLogger, get_logger
    from selenium_browser_manager import SeleniumBrowserManager


class ClickEventMonitor:
    """
    使用 Selenium 监听网页上的用户点击事件
    """

    def __init__(self, headless=False):
        """
        初始化点击事件监听器
        :param headless: 是否无头模式(不显示浏览器窗口)
        """
        # 初始化日志器
        self.logger = get_logger('financial_framework.click_event_monitor')
        self.data_logger = FinancialLogger.get_data_logger()

        # 使用 SeleniumBrowserManager 管理浏览器
        self.browser_manager = SeleniumBrowserManager(
            headless=headless,
            logger_name='financial_framework.click_event_monitor.browser'
        )

        self.click_events = []  # 存储点击事件
        self.click_count = 0  # 点击计数

        self.logger.info("初始化点击事件监听器")
        self.browser_manager.init_driver()

    def _inject_click_listener(self):
        """
        向页面注入JavaScript代码来监听点击事件
        """
        try:
            # JavaScript代码：监听所有点击事件并存储到window对象
            js_code = """
            // 创建点击事件记录数组
            if (!window.clickEvents) {
                window.clickEvents = [];
            }

            // 添加点击事件监听器
            document.addEventListener('click', function(event) {
                // 获取点击元素的信息
                var target = event.target;
                var tagName = target.tagName;
                var id = target.id || '';
                var className = target.className || '';
                var text = target.innerText || target.textContent || '';

                // 限制文本长度
                if (text.length > 100) {
                    text = text.substring(0, 100) + '...';
                }

                // 获取元素的完整绝对 XPath（不使用 id 简写）
                function getAbsoluteXPath(element) {
                    if (element === document.documentElement) {
                        return '/html';
                    }
                    if (element === document.body) {
                        return '/html/body';
                    }

                    var ix = 0;
                    var siblings = element.parentNode.childNodes;
                    for (var i = 0; i < siblings.length; i++) {
                        var sibling = siblings[i];
                        if (sibling === element) {
                            return getAbsoluteXPath(element.parentNode) + '/' + element.tagName.toLowerCase() + '[' + (ix + 1) + ']';
                        }
                        if (sibling.nodeType === 1 && sibling.tagName === element.tagName) {
                            ix++;
                        }
                    }
                }

                // 获取相对 XPath（使用 id 和其他属性优化）
                function getRelativeXPath(element) {
                    if (element.id !== '') {
                        return '//*[@id="' + element.id + '"]';
                    }
                    if (element === document.body) {
                        return '//body';
                    }

                    var ix = 0;
                    var siblings = element.parentNode.childNodes;
                    for (var i = 0; i < siblings.length; i++) {
                        var sibling = siblings[i];
                        if (sibling === element) {
                            var tagName = element.tagName.toLowerCase();
                            var path = getRelativeXPath(element.parentNode) + '/' + tagName + '[' + (ix + 1) + ']';
                            return path;
                        }
                        if (sibling.nodeType === 1 && sibling.tagName === element.tagName) {
                            ix++;
                        }
                    }
                }

                // 获取优化的 XPath（优先使用 id、class、属性等）
                function getOptimizedXPath(element) {
                    // 如果有 id，使用 id
                    if (element.id !== '') {
                        return '//*[@id="' + element.id + '"]';
                    }

                    // 如果有唯一的 class
                    if (element.className) {
                        var classes = element.className.trim().split(/\s+/);
                        if (classes.length > 0 && classes[0] !== '') {
                            var classPath = '//' + element.tagName.toLowerCase() + '[@class="' + element.className + '"]';
                            // 检查是否唯一
                            try {
                                var elements = document.evaluate(classPath, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                                if (elements.snapshotLength === 1) {
                                    return classPath;
                                }
                            } catch(e) {}
                        }
                    }

                    // 如果有 name 属性
                    if (element.name) {
                        var namePath = '//' + element.tagName.toLowerCase() + '[@name="' + element.name + '"]';
                        try {
                            var elements = document.evaluate(namePath, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                            if (elements.snapshotLength === 1) {
                                return namePath;
                            }
                        } catch(e) {}
                    }

                    // 使用相对路径
                    return getRelativeXPath(element);
                }

                // 生成 CSS Selector
                function getCssSelector(element) {
                    if (element.id) {
                        return '#' + element.id;
                    }

                    var path = [];
                    while (element && element.nodeType === Node.ELEMENT_NODE) {
                        var selector = element.nodeName.toLowerCase();
                        if (element.id) {
                            selector += '#' + element.id;
                            path.unshift(selector);
                            break;
                        } else {
                            var sibling = element;
                            var nth = 1;
                            while (sibling.previousElementSibling) {
                                sibling = sibling.previousElementSibling;
                                if (sibling.nodeName.toLowerCase() === selector) {
                                    nth++;
                                }
                            }
                            if (nth !== 1) {
                                selector += ':nth-of-type(' + nth + ')';
                            }
                        }
                        path.unshift(selector);
                        element = element.parentNode;
                    }
                    return path.join(' > ');
                }

                var absoluteXPath = getAbsoluteXPath(target);
                var relativeXPath = getRelativeXPath(target);
                var optimizedXPath = getOptimizedXPath(target);
                var cssSelector = getCssSelector(target);

                // 获取点击位置
                var clickX = event.clientX;
                var clickY = event.clientY;
                var pageX = event.pageX;
                var pageY = event.pageY;

                // 获取元素位置和大小
                var rect = target.getBoundingClientRect();

                // 创建点击事件记录
                var clickEvent = {
                    timestamp: new Date().toISOString(),
                    tagName: tagName,
                    id: id,
                    className: className,
                    text: text,
                    // 提供多种定位策略供自动化测试使用
                    locators: {
                        xpath_absolute: absoluteXPath,      // 完整绝对路径
                        xpath_relative: relativeXPath,      // 相对路径
                        xpath_optimized: optimizedXPath,    // 优化路径（推荐）
                        css_selector: cssSelector           // CSS选择器
                    },
                    clickPosition: {
                        clientX: clickX,
                        clientY: clickY,
                        pageX: pageX,
                        pageY: pageY
                    },
                    elementPosition: {
                        top: rect.top,
                        left: rect.left,
                        width: rect.width,
                        height: rect.height
                    },
                    href: target.href || '',
                    attributes: {}
                };

                // 获取元素的所有属性
                if (target.attributes) {
                    for (var i = 0; i < target.attributes.length; i++) {
                        var attr = target.attributes[i];
                        clickEvent.attributes[attr.name] = attr.value;
                    }
                }

                // 添加到记录数组
                window.clickEvents.push(clickEvent);

                // 在控制台输出（可选）
                console.log('Click Event Captured:', clickEvent);
            }, true);  // 使用捕获阶段，可以捕获所有点击事件

            console.log('Click event listener injected successfully');
            """

            # 执行JavaScript代码
            self.browser_manager.driver.execute_script(js_code)
            self.logger.info("✓ 点击事件监听器已注入页面")

        except Exception as e:
            self.logger.error(f"注入点击事件监听器失败: {e}", exc_info=True)

    def _get_click_events_from_page(self):
        """
        从页面获取新的点击事件
        :return: 新的点击事件列表
        """
        try:
            # 从window.clickEvents获取点击事件
            js_code = """
            var events = window.clickEvents || [];
            var result = JSON.stringify(events);
            // 清空已获取的事件
            window.clickEvents = [];
            return result;
            """

            events_json = self.browser_manager.driver.execute_script(js_code)

            if events_json:
                events = json.loads(events_json)
                return events
            else:
                return []

        except Exception as e:
            self.logger.debug(f"获取点击事件失败: {e}")
            return []

    def start_monitoring(self, url, check_interval=0.5, save_to_file=True, output_file="click_events.json"):
        """
        打开网站并开始监听点击事件
        :param url: 要访问的网站URL
        :param check_interval: 检查点击事件的时间间隔（秒）
        :param save_to_file: 是否保存点击事件到文件
        :param output_file: 输出文件路径
        """
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"访问网站: {url}")
            self.logger.info(f"检查间隔: {check_interval}秒")
            self.logger.info(f"保存到文件: {save_to_file}")
            if save_to_file:
                self.logger.info(f"输出文件: {output_file}")
            self.logger.info("=" * 80)

            # 访问页面
            self.browser_manager.navigate_to(url)

            # 等待页面加载
            time.sleep(2)

            # 注入点击事件监听器
            self._inject_click_listener()

            self.logger.info("=" * 80)
            self.logger.info("开始监听用户点击事件...")
            self.logger.info("在浏览器中点击页面元素，点击事件将被记录")
            self.logger.info("按 Ctrl+C 停止监听")
            self.logger.info("=" * 80)

            # 持续监听
            while True:
                try:
                    # 获取新的点击事件
                    new_events = self._get_click_events_from_page()

                    if new_events:
                        for event in new_events:
                            self.click_count += 1
                            event['click_index'] = self.click_count
                            event['captured_at'] = datetime.now().isoformat()

                            self.click_events.append(event)

                            # 输出到控制台
                            self.logger.info("=" * 80)
                            self.logger.info(f"✓ 捕获到第 {self.click_count} 个点击事件")
                            self.logger.info(f"时间: {event['timestamp']}")
                            self.logger.info(f"元素: <{event['tagName']}>")
                            if event['id']:
                                self.logger.info(f"ID: {event['id']}")
                            if event['className']:
                                self.logger.info(f"Class: {event['className']}")
                            if event['text']:
                                self.logger.info(f"文本: {event['text']}")
                            if event['href']:
                                self.logger.info(f"链接: {event['href']}")
                            self.logger.info(f"点击位置: ({event['clickPosition']['pageX']}, {event['clickPosition']['pageY']})")

                            # 显示所有定位器信息
                            self.logger.info("-" * 60)
                            self.logger.info("定位器信息（用于自动化测试）:")
                            locators = event.get('locators', {})
                            if locators.get('xpath_optimized'):
                                self.logger.info(f"  推荐XPath: {locators['xpath_optimized']}")
                            if locators.get('css_selector'):
                                self.logger.info(f"  CSS选择器: {locators['css_selector']}")
                            if locators.get('xpath_absolute'):
                                self.logger.info(f"  绝对XPath: {locators['xpath_absolute']}")
                            if locators.get('xpath_relative'):
                                self.logger.info(f"  相对XPath: {locators['xpath_relative']}")
                            self.logger.info("-" * 60)
                            self.logger.info("=" * 80)

                            # 记录到数据日志
                            self.data_logger.info("=" * 80)
                            self.data_logger.info(f"点击事件 #{self.click_count}")
                            self.data_logger.info("=" * 80)
                            self.data_logger.info(json.dumps(event, ensure_ascii=False, indent=2))
                            self.data_logger.info("=" * 80)

                    # 等待一段时间再检查
                    time.sleep(check_interval)

                except KeyboardInterrupt:
                    self.logger.info("=" * 80)
                    self.logger.info("用户停止监听")
                    self.logger.info(f"总共捕获 {self.click_count} 个点击事件")
                    self.logger.info("=" * 80)
                    break

            # 保存到文件
            if save_to_file and self.click_events:
                self._save_events_to_file(output_file)

        except Exception as e:
            self.logger.error(f"监听点击事件失败: {e}", exc_info=True)

    def _save_events_to_file(self, output_file):
        """
        保存点击事件到JSON文件
        :param output_file: 输出文件路径
        """
        try:
            # 确保目录存在
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            # 添加时间戳到文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name, ext = os.path.splitext(output_file)
            output_file_with_timestamp = f"{base_name}_{timestamp}{ext}"

            # 保存到JSON文件
            with open(output_file_with_timestamp, 'w', encoding='utf-8') as f:
                json.dump({
                    'total_clicks': self.click_count,
                    'monitoring_started': datetime.now().isoformat(),
                    'events': self.click_events
                }, f, ensure_ascii=False, indent=2)

            self.logger.info("=" * 80)
            self.logger.info(f"✓ 点击事件已保存到文件")
            self.logger.info(f"文件路径: {output_file_with_timestamp}")
            self.logger.info(f"总事件数: {self.click_count}")
            self.logger.info("=" * 80)

            # 同时生成自动化测试代码
            self._generate_test_code(output_file_with_timestamp, timestamp)

        except Exception as e:
            self.logger.error(f"保存点击事件到文件失败: {e}", exc_info=True)

    def _generate_test_code(self, json_file_path, timestamp):
        """
        根据点击事件生成 Selenium 自动化测试代码
        :param json_file_path: JSON文件路径
        :param timestamp: 时间戳
        """
        try:
            base_name = os.path.splitext(json_file_path)[0]
            test_file_path = f"{base_name}_test.py"

            # 生成测试代码
            test_code = f'''"""
自动生成的 Selenium 自动化测试脚本
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
基于点击事件记录: {os.path.basename(json_file_path)}
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time


class GeneratedTest:
    """自动生成的测试类"""

    def __init__(self, headless=False):
        """初始化测试"""
        self.driver = None
        self.wait_timeout = 10
        self.headless = headless

    def setup(self):
        """设置浏览器"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()
        print("✓ 浏览器初始化成功")

    def teardown(self):
        """关闭浏览器"""
        if self.driver:
            self.driver.quit()
            print("✓ 浏览器已关闭")

    def wait_and_click(self, locator_type, locator_value, description=""):
        """
        等待元素可点击并点击
        :param locator_type: 定位方式 (By.XPATH, By.CSS_SELECTOR等)
        :param locator_value: 定位值
        :param description: 元素描述
        """
        try:
            element = WebDriverWait(self.driver, self.wait_timeout).until(
                EC.element_to_be_clickable((locator_type, locator_value))
            )
            element.click()
            print(f"✓ 点击: {{description}}")
            time.sleep(0.5)  # 等待页面响应
            return True
        except TimeoutException:
            print(f"✗ 超时: 无法找到元素 {{description}}")
            print(f"  定位器: {{locator_type}} = {{locator_value}}")
            return False
        except Exception as e:
            print(f"✗ 错误: 点击 {{description}} 失败")
            print(f"  异常: {{str(e)}}")
            return False

    def run_test(self):
        """执行测试"""
        print("=" * 80)
        print("开始执行自动化测试")
        print("=" * 80)

'''

            # 添加每个点击事件的测试代码
            for idx, event in enumerate(self.click_events, 1):
                locators = event.get('locators', {})
                text = event.get('text', '').replace("'", "\\'").replace('"', '\\"')
                tag_name = event.get('tagName', '')
                element_id = event.get('id', '')
                class_name = event.get('className', '')

                # 构建元素描述
                description_parts = [f"元素{idx}"]
                if text:
                    description_parts.append(f'"{text}"')
                if element_id:
                    description_parts.append(f'id={element_id}')
                elif class_name:
                    description_parts.append(f'class={class_name}')
                else:
                    description_parts.append(f'{tag_name}')

                description = ' - '.join(description_parts)

                # 优先使用优化的XPath，备选CSS选择器
                preferred_locator = locators.get('xpath_optimized') or locators.get('css_selector')
                locator_type = 'By.XPATH' if 'xpath' in str(preferred_locator).lower() or '//' in str(preferred_locator) else 'By.CSS_SELECTOR'

                test_code += f'''
        # 点击事件 {idx}: {description}
        # 时间戳: {event.get('timestamp')}
        self.wait_and_click(
            {locator_type},
            "{preferred_locator}",
            "{description}"
        )
'''

                # 如果有href，说明可能会跳转页面
                if event.get('href'):
                    test_code += f'''        # 注意: 此元素有链接 {event.get('href')}，可能会跳转页面
        time.sleep(1)  # 等待页面加载
'''

            # 添加测试结束代码
            test_code += '''
        print("=" * 80)
        print("✓ 测试执行完成")
        print("=" * 80)


def main():
    """主函数"""
    test = GeneratedTest(headless=False)

    try:
        # 初始化浏览器
        test.setup()

        # TODO: 请在此处添加要访问的URL
        # test.driver.get("https://www.example.com")

        # 执行测试
        test.run_test()

        # 保持浏览器打开以便查看结果
        input("\\n按回车键关闭浏览器...")

    except KeyboardInterrupt:
        print("\\n用户中断测试")
    except Exception as e:
        print(f"\\n测试执行失败: {e}")
    finally:
        test.teardown()


if __name__ == "__main__":
    main()
'''

            # 保存测试代码
            with open(test_file_path, 'w', encoding='utf-8') as f:
                f.write(test_code)

            self.logger.info("=" * 80)
            self.logger.info(f"✓ 自动化测试代码已生成")
            self.logger.info(f"测试文件路径: {test_file_path}")
            self.logger.info("=" * 80)
            self.logger.info("使用方法:")
            self.logger.info(f"  1. 编辑 {test_file_path}")
            self.logger.info(f"  2. 在 main() 函数中添加要访问的URL")
            self.logger.info(f"  3. 运行: python {test_file_path}")
            self.logger.info("=" * 80)

        except Exception as e:
            self.logger.error(f"生成测试代码失败: {e}", exc_info=True)

    def get_click_summary(self):
        """
        获取点击事件统计摘要
        :return: 统计摘要字典
        """
        if not self.click_events:
            return {
                'total_clicks': 0,
                'elements_clicked': {}
            }

        # 统计各类元素的点击次数
        elements_clicked = {}
        for event in self.click_events:
            tag_name = event['tagName']
            if tag_name not in elements_clicked:
                elements_clicked[tag_name] = 0
            elements_clicked[tag_name] += 1

        return {
            'total_clicks': self.click_count,
            'elements_clicked': elements_clicked,
            'first_click_time': self.click_events[0]['timestamp'] if self.click_events else None,
            'last_click_time': self.click_events[-1]['timestamp'] if self.click_events else None
        }

    def close(self):
        """关闭浏览器"""
        self.browser_manager.close()


def main():
    """
    主函数 - 启动点击事件监听器
    """
    # 初始化日志系统
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=20,  # INFO级别
        console_output=True
    )

    logger = get_logger('financial_framework.click_event_monitor_main')
    logger.info("=" * 80)
    logger.info("网页点击事件监听器")
    logger.info("=" * 80)

    # 初始化监听器
    monitor = ClickEventMonitor(headless=False)

    if not monitor.browser_manager.is_initialized():
        logger.error("浏览器初始化失败，退出程序")
        return

    try:
        # 要监听的网站URL（可以修改为任何网站）
        target_url = "https://www.baidu.com"

        # 开始监听点击事件
        monitor.start_monitoring(
            url=target_url,
            check_interval=0.5,  # 每0.5秒检查一次
            save_to_file=True,
            output_file="data/click_events.json"
        )

        # 显示统计摘要
        summary = monitor.get_click_summary()
        logger.info("=" * 80)
        logger.info("点击事件统计摘要:")
        logger.info(json.dumps(summary, ensure_ascii=False, indent=2))
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.info("用户手动停止程序")
    finally:
        monitor.close()

    logger.info("=" * 80)
    logger.info("程序结束")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
