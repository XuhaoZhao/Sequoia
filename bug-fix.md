这个问题非常常见，尤其是在 Windows PowerShell 或 cmd 中运行包含 while True（或类似无限循环）的 Python 脚本时，现象表现为：

程序启动后正常打印，但过一段时间“卡住”，需要敲一下键盘（比如回车键）才能继续执行下去。

✅ 根本原因

PowerShell 的 控制台缓冲区机制 与 Python 的 print() 输出刷新策略、time.sleep() 和 schedule 库的内部轮询机制之间存在交互问题。
常见触发点如下：

输出缓冲区未及时刷新

Python 的 print() 默认是行缓冲模式（interactive TTY 会自动刷新，但并非总是如此）。

当 PowerShell 的窗口未检测到“活动”输入时，输出可能会被缓冲而不立即显示。

敲键盘会触发 PowerShell 重新刷新 stdout，从而程序“恢复”。

PowerShell 的空闲事件阻塞问题

PowerShell 控制台在无输入事件时，有时会导致 Python 子进程（尤其在 time.sleep() 循环中）挂起或延迟执行，具体与 PowerShell 的 I/O 事件模型有关。

Windows 控制台睡眠状态 / 电源策略

如果是笔记本电脑或服务器，Windows 可能会进入“低功耗”状态，导致 Python 的循环任务停顿。

敲击键盘重新唤醒控制台事件循环。

schedule 库在 Windows 环境下的间歇性 sleep 阻塞

schedule 本身依赖 Python 的 time.sleep() + 主线程轮询。

如果控制台或解释器线程阻塞（例如等待刷新），则任务不会继续调度。


``` python
import time
import schedule
import threading
from datetime import datetime
import sys
import functools
import builtins

# 让所有 print 自动刷新
print = functools.partial(builtins.print, flush=True)


class RealtimeCollectorService:
    def __init__(self, collector):
        self.collector = collector
        self.is_running = True

    def start_realtime_collection(self):
        """启动定时每分钟实时数据收集（稳定版）"""
        self._print_header()

        schedule.clear()
        schedule.every().minute.do(self._safe_collect_data)

        # 如果当前在交易时间，立即执行一次
        if self.collector._is_trading_time():
            print("当前在交易时间内，立即执行一次数据收集...")
            self._safe_collect_data()
        else:
            print("当前不在交易时间内，等待交易时间开始...")

        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n收到键盘中断信号，准备停止服务...")
            self.is_running = False
        except Exception as e:
            print(f"[错误] 主循环异常: {e}")
        finally:
            print("实时数据收集服务已停止。")

    def _safe_collect_data(self):
        """封装数据收集逻辑，避免异常中断调度"""
        try:
            if self.collector._is_trading_time():
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始采集数据...")
                self.collect_data()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 数据采集完成。")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 非交易时间，跳过采集。")
        except Exception as e:
            print(f"[错误] 数据采集失败: {e}")

    def collect_data(self):
        """实际的数据采集逻辑（由你实现）"""
        self.collector.collect_data()

    def _print_header(self):
        """打印启动信息"""
        print("=" * 50)
        print("启动定时实时数据收集服务")
        print("=" * 50)
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("收集频率: 每分钟")
        print("交易时间: 9:30-11:30, 13:00-15:00")
        print("按 Ctrl+C 停止服务")
        print("=" * 50)


```

python -u realtime_collector.py > log.txt 2>&1
python -u realtime_collector.py --mode analysis > ana_log.txt 2>&1
python -u realtime_collector.py --mode historical > his_log.txt 2>&1

python -u realtime_collector.py --mode realtime > log.txt 2>&1 