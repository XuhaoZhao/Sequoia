from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import os
import stat
import platform


class DataSourceDiagnostic:
    """诊断页面数据来源"""

    def __init__(self):
        self.driver = None
        self.init_driver()

    def init_driver(self):
        """初始化浏览器"""
        print("初始化浏览器...")
        chrome_options = Options()

        if platform.system() == 'Darwin':
            chrome_options.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'

        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            # 方法1: 先尝试使用系统已安装的 ChromeDriver
            try:
                print("  尝试使用系统 ChromeDriver...")
                self.driver = webdriver.Chrome(options=chrome_options)
                print("  ✓ 使用系统 ChromeDriver 成功")
            except Exception as e1:
                print(f"  系统 ChromeDriver 不可用: {e1}")
                print("  尝试通过 webdriver-manager 下载...")

                # 方法2: 使用 webdriver-manager 下载
                driver_path = ChromeDriverManager().install()
                if platform.system() == 'Darwin':
                    current_permissions = os.stat(driver_path).st_mode
                    os.chmod(driver_path, current_permissions | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    try:
                        os.system(f'xattr -d com.apple.quarantine "{driver_path}" 2>/dev/null')
                    except:
                        pass

                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                print("  ✓ 使用下载的 ChromeDriver 成功")

            # 隐藏 webdriver 特征
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })

            print("✓ 浏览器初始化成功\n")
        except Exception as e:
            print(f"✗ 浏览器初始化失败: {e}")
            print("\n可能的解决方案:")
            print("1. 确保 Chrome 浏览器已安装")
            print("2. 手动安装 ChromeDriver:")
            print("   brew install chromedriver")
            print("3. 或访问 https://chromedriver.chromium.org/ 下载")
            self.driver = None

    def diagnose_data_sources(self, url):
        """诊断页面的数据来源"""
        print(f"诊断页面: {url}\n")
        print("="*80)

        # 检查浏览器是否初始化成功
        if not self.driver:
            print("❌ 浏览器未初始化，无法继续")
            return

        # 注入全面的监控脚本
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                window.diagnosticData = {
                    websockets: [],
                    xhrRequests: [],
                    fetchRequests: [],
                    eventSources: [],
                    localStorage: {},
                    sessionStorage: {},
                    windowVariables: [],
                    postMessages: []
                };

                // 1. 监控 WebSocket
                (function() {
                    const OriginalWebSocket = window.WebSocket;
                    window.WebSocket = function(...args) {
                        const ws = new OriginalWebSocket(...args);
                        const wsInfo = {
                            url: args[0],
                            protocol: args[1] || 'default',
                            readyState: ws.readyState,
                            messages: [],
                            opened: false,
                            closed: false
                        };

                        window.diagnosticData.websockets.push(wsInfo);
                        console.log('🔌 WebSocket 创建:', args[0]);

                        ws.addEventListener('open', () => {
                            wsInfo.opened = true;
                            wsInfo.readyState = ws.readyState;
                            console.log('✅ WebSocket 已连接:', args[0]);
                        });

                        ws.addEventListener('message', (event) => {
                            const msg = {
                                data: event.data,
                                timestamp: new Date().toISOString(),
                                length: event.data.length
                            };
                            wsInfo.messages.push(msg);
                            console.log('📨 WebSocket 收到消息:', event.data.substring(0, 200));
                        });

                        ws.addEventListener('close', () => {
                            wsInfo.closed = true;
                            console.log('🔴 WebSocket 已关闭:', args[0]);
                        });

                        ws.addEventListener('error', (error) => {
                            wsInfo.error = error.toString();
                            console.log('❌ WebSocket 错误:', error);
                        });

                        return ws;
                    };
                })();

                // 2. 监控 EventSource (Server-Sent Events)
                (function() {
                    const OriginalEventSource = window.EventSource;
                    if (OriginalEventSource) {
                        window.EventSource = function(...args) {
                            const es = new OriginalEventSource(...args);
                            const esInfo = {
                                url: args[0],
                                messages: []
                            };
                            window.diagnosticData.eventSources.push(esInfo);
                            console.log('🌊 EventSource 创建:', args[0]);

                            es.addEventListener('message', (event) => {
                                esInfo.messages.push({
                                    data: event.data,
                                    timestamp: new Date().toISOString()
                                });
                                console.log('📨 EventSource 消息:', event.data.substring(0, 200));
                            });

                            return es;
                        };
                    }
                })();

                // 3. 监控 postMessage
                window.addEventListener('message', (event) => {
                    window.diagnosticData.postMessages.push({
                        origin: event.origin,
                        data: event.data,
                        timestamp: new Date().toISOString()
                    });
                    console.log('💬 postMessage:', event.origin, event.data);
                });

                // 4. 监控 XHR
                (function() {
                    const origOpen = XMLHttpRequest.prototype.open;
                    const origSend = XMLHttpRequest.prototype.send;

                    XMLHttpRequest.prototype.open = function(method, url) {
                        this._url = url;
                        this._method = method;
                        return origOpen.apply(this, arguments);
                    };

                    XMLHttpRequest.prototype.send = function() {
                        const xhr = this;
                        window.diagnosticData.xhrRequests.push({
                            method: xhr._method,
                            url: xhr._url,
                            timestamp: new Date().toISOString()
                        });
                        console.log('🌐 XHR:', xhr._method, xhr._url);
                        return origSend.apply(this, arguments);
                    };
                })();

                // 5. 监控 Fetch
                (function() {
                    const origFetch = window.fetch;
                    window.fetch = function(...args) {
                        window.diagnosticData.fetchRequests.push({
                            url: args[0],
                            timestamp: new Date().toISOString()
                        });
                        console.log('🌐 Fetch:', args[0]);
                        return origFetch.apply(this, args);
                    };
                })();
            '''
        })

        # 访问页面
        self.driver.get(url)
        print("\n等待页面加载...")
        time.sleep(5)

        # 第一次数据采集
        print("\n" + "="*80)
        print("【第一次数据快照 - 页面初始加载】")
        print("="*80)
        snapshot1 = self.collect_snapshot()
        self.print_snapshot(snapshot1)

        # 等待用户交互（比如点击下一页）
        print("\n" + "="*80)
        print("⏸️  请在浏览器中进行操作（如点击下一页），然后回到终端按 Enter 继续...")
        print("="*80)
        input()

        # 第二次数据采集
        print("\n" + "="*80)
        print("【第二次数据快照 - 用户操作后】")
        print("="*80)
        snapshot2 = self.collect_snapshot()
        self.print_snapshot(snapshot2)

        # 对比分析
        print("\n" + "="*80)
        print("【对比分析】")
        print("="*80)
        self.compare_snapshots(snapshot1, snapshot2)

        # 保持浏览器打开
        print("\n浏览器将保持打开，按 Enter 关闭...")
        input()

    def collect_snapshot(self):
        """收集当前页面的数据快照"""
        try:
            snapshot = self.driver.execute_script("""
                const snapshot = {
                    diagnostic: window.diagnosticData || {},
                    windowKeys: [],
                    localStorage: {},
                    sessionStorage: {},
                    reactState: null,
                    vueState: null,
                    angularState: null,
                    globalData: {}
                };

                // 获取 window 上的所有变量（可能的数据存储）
                const windowKeys = Object.keys(window);
                const dataRelatedKeys = windowKeys.filter(key => {
                    const lower = key.toLowerCase();
                    return lower.includes('data') ||
                           lower.includes('stock') ||
                           lower.includes('quote') ||
                           lower.includes('list') ||
                           lower.includes('table') ||
                           lower.includes('page') ||
                           lower.includes('state') ||
                           lower.includes('store');
                });

                snapshot.windowKeys = dataRelatedKeys;

                // 尝试获取这些变量的值
                for (let key of dataRelatedKeys.slice(0, 50)) {  // 限制数量
                    try {
                        const value = window[key];
                        if (value && typeof value === 'object') {
                            snapshot.globalData[key] = JSON.stringify(value).substring(0, 1000);
                        } else if (typeof value === 'string' || typeof value === 'number') {
                            snapshot.globalData[key] = value;
                        }
                    } catch(e) {
                        snapshot.globalData[key] = 'Error: ' + e.message;
                    }
                }

                // localStorage
                try {
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        snapshot.localStorage[key] = localStorage.getItem(key).substring(0, 500);
                    }
                } catch(e) {}

                // sessionStorage
                try {
                    for (let i = 0; i < sessionStorage.length; i++) {
                        const key = sessionStorage.key(i);
                        snapshot.sessionStorage[key] = sessionStorage.getItem(key).substring(0, 500);
                    }
                } catch(e) {}

                // 检查常见框架的状态
                // React
                if (window.__REACT_DEVTOOLS_GLOBAL_HOOK__) {
                    snapshot.reactState = 'React detected';
                }

                // Vue
                if (window.__VUE__) {
                    snapshot.vueState = 'Vue detected';
                }

                // Angular
                if (window.ng) {
                    snapshot.angularState = 'Angular detected';
                }

                return snapshot;
            """)
            return snapshot
        except Exception as e:
            print(f"收集快照失败: {e}")
            return {}

    def print_snapshot(self, snapshot):
        """打印快照信息"""
        if not snapshot:
            print("❌ 快照为空")
            return

        diagnostic = snapshot.get('diagnostic', {})

        # WebSocket
        ws_list = diagnostic.get('websockets', [])
        print(f"\n🔌 WebSocket 连接: {len(ws_list)} 个")
        for i, ws in enumerate(ws_list, 1):
            print(f"  {i}. URL: {ws.get('url', 'N/A')}")
            print(f"     状态: {'已连接' if ws.get('opened') else '未连接'}")
            print(f"     消息数: {len(ws.get('messages', []))}")
            if ws.get('messages'):
                latest = ws['messages'][-1]
                print(f"     最新消息 ({latest['timestamp']}): {latest['data'][:200]}...")

        # EventSource
        es_list = diagnostic.get('eventSources', [])
        print(f"\n🌊 EventSource 连接: {len(es_list)} 个")
        for i, es in enumerate(es_list, 1):
            print(f"  {i}. URL: {es.get('url', 'N/A')}")
            print(f"     消息数: {len(es.get('messages', []))}")

        # postMessage
        pm_list = diagnostic.get('postMessages', [])
        print(f"\n💬 postMessage 消息: {len(pm_list)} 个")
        for i, pm in enumerate(pm_list[:5], 1):
            print(f"  {i}. Origin: {pm.get('origin', 'N/A')}")
            print(f"     Data: {str(pm.get('data', ''))[:200]}")

        # XHR/Fetch
        xhr_list = diagnostic.get('xhrRequests', [])
        fetch_list = diagnostic.get('fetchRequests', [])
        print(f"\n🌐 网络请求:")
        print(f"  XHR: {len(xhr_list)} 个")
        print(f"  Fetch: {len(fetch_list)} 个")

        # Window 变量
        window_keys = snapshot.get('windowKeys', [])
        print(f"\n🪟 Window 上的数据相关变量: {len(window_keys)} 个")
        for i, key in enumerate(window_keys[:20], 1):
            value = snapshot.get('globalData', {}).get(key, '')
            if isinstance(value, str):
                print(f"  {i}. {key}: {value[:100]}...")
            else:
                print(f"  {i}. {key}: {value}")

        # Storage
        ls = snapshot.get('localStorage', {})
        ss = snapshot.get('sessionStorage', {})
        print(f"\n💾 Storage:")
        print(f"  localStorage: {len(ls)} 项")
        print(f"  sessionStorage: {len(ss)} 项")

        if ls:
            print("  localStorage 内容:")
            for key, value in list(ls.items())[:5]:
                print(f"    {key}: {value[:100]}...")

        if ss:
            print("  sessionStorage 内容:")
            for key, value in list(ss.items())[:5]:
                print(f"    {key}: {value[:100]}...")

        # 框架检测
        frameworks = []
        if snapshot.get('reactState'):
            frameworks.append('React')
        if snapshot.get('vueState'):
            frameworks.append('Vue')
        if snapshot.get('angularState'):
            frameworks.append('Angular')

        if frameworks:
            print(f"\n⚛️ 检测到前端框架: {', '.join(frameworks)}")

    def compare_snapshots(self, snap1, snap2):
        """对比两个快照，找出变化"""
        print("\n📊 数据变化分析:")

        # 对比 WebSocket 消息
        ws1 = snap1.get('diagnostic', {}).get('websockets', [])
        ws2 = snap2.get('diagnostic', {}).get('websockets', [])

        if ws2:
            for i, ws in enumerate(ws2):
                msg_count_before = len(ws1[i]['messages']) if i < len(ws1) else 0
                msg_count_after = len(ws['messages'])
                new_messages = msg_count_after - msg_count_before

                if new_messages > 0:
                    print(f"\n✅ WebSocket #{i+1} 收到 {new_messages} 条新消息")
                    print(f"   URL: {ws.get('url', 'N/A')}")
                    print(f"   最新消息:")
                    for msg in ws['messages'][-new_messages:]:
                        print(f"     [{msg['timestamp']}] {msg['data'][:300]}...")

        # 对比 window 变量
        keys1 = set(snap1.get('windowKeys', []))
        keys2 = set(snap2.get('windowKeys', []))
        new_keys = keys2 - keys1

        if new_keys:
            print(f"\n✅ 新增了 {len(new_keys)} 个 window 变量:")
            for key in list(new_keys)[:10]:
                value = snap2.get('globalData', {}).get(key, '')
                print(f"   {key}: {str(value)[:100]}...")

        # 对比 localStorage/sessionStorage
        ls1 = snap1.get('localStorage', {})
        ls2 = snap2.get('localStorage', {})
        ss1 = snap1.get('sessionStorage', {})
        ss2 = snap2.get('sessionStorage', {})

        ls_changed = {k: v for k, v in ls2.items() if ls1.get(k) != v}
        ss_changed = {k: v for k, v in ss2.items() if ss1.get(k) != v}

        if ls_changed:
            print(f"\n✅ localStorage 有 {len(ls_changed)} 项变化:")
            for key, value in list(ls_changed.items())[:5]:
                print(f"   {key}: {value[:100]}...")

        if ss_changed:
            print(f"\n✅ sessionStorage 有 {len(ss_changed)} 项变化:")
            for key, value in list(ss_changed.items())[:5]:
                print(f"   {key}: {value[:100]}...")

        # 网络请求变化
        xhr1_count = len(snap1.get('diagnostic', {}).get('xhrRequests', []))
        xhr2_count = len(snap2.get('diagnostic', {}).get('xhrRequests', []))
        fetch1_count = len(snap1.get('diagnostic', {}).get('fetchRequests', []))
        fetch2_count = len(snap2.get('diagnostic', {}).get('fetchRequests', []))

        if xhr2_count > xhr1_count or fetch2_count > fetch1_count:
            print(f"\n✅ 检测到新的网络请求:")
            print(f"   新增 XHR: {xhr2_count - xhr1_count} 个")
            print(f"   新增 Fetch: {fetch2_count - fetch1_count} 个")

    def close(self):
        """关闭浏览器"""
        if self.driver:
            self.driver.quit()


if __name__ == "__main__":
    # 使用示例
    print("="*80)
    print("数据源诊断工具")
    print("="*80)
    print("这个工具会帮助您找到页面数据的藏身之处\n")

    # 请替换为您要诊断的实际URL
    test_url = "https://quote.eastmoney.com/f1.html?newcode=0.000001"

    diagnostic = DataSourceDiagnostic()
    try:
        diagnostic.diagnose_data_sources(test_url)
    finally:
        diagnostic.close()
