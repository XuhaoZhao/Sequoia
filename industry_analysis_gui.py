#!/usr/bin/env python3
"""
Industry Analysis Desktop Application
板块分析桌面应用程序

提供图形界面进行板块数据收集、实时监控和技术分析
"""
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import queue
import time
import schedule
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import os
import sys

# 导入现有的分析模块
from industry_analysis import IndustryDataCollector, IndustryAnalyzer, IndustryDataReader

class IndustryAnalysisGUI:
    """板块分析桌面应用程序主类"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("板块分析系统 - Industry Analysis System")
        self.root.geometry("1400x900")
        self.root.minsize(1200, 800)
        
        # 初始化数据组件
        self.data_collector = IndustryDataCollector()
        self.analyzer = IndustryAnalyzer(self.data_collector)
        self.data_reader = IndustryDataReader()
        
        # 控制变量
        self.monitoring_enabled = tk.BooleanVar(value=False)
        self.analysis_enabled = tk.BooleanVar(value=False)
        self.selected_board = tk.StringVar()
        
        # 线程控制
        self.monitoring_thread = None
        self.analysis_thread = None
        self.stop_monitoring = threading.Event()
        self.stop_analysis = threading.Event()
        
        # 消息队列用于线程通信
        self.message_queue = queue.Queue()
        
        # 状态信息
        self.status_info = {
            'last_data_collect': "未启动",
            'last_analysis': "未启动",
            'monitoring_status': "停止",
            'total_boards': 0,
            'available_boards': []
        }
        
        # 创建界面
        self.create_widgets()
        self.update_board_list()
        self.start_message_handler()
        
        # 设置关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def create_widgets(self):
        """创建主界面组件"""
        # 创建主容器
        main_container = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧控制面板
        left_frame = ttk.Frame(main_container)
        main_container.add(left_frame, weight=1, minsize=400)
        
        # 右侧图表区域
        right_frame = ttk.Frame(main_container)
        main_container.add(right_frame, weight=2, minsize=800)
        
        self.create_control_panel(left_frame)
        self.create_chart_area(right_frame)
    
    def create_control_panel(self, parent):
        """创建左侧控制面板"""
        # 标题
        title_label = ttk.Label(parent, text="板块分析控制面板", font=("Arial", 16, "bold"))
        title_label.pack(pady=(10, 20))
        
        # 数据收集控制区域
        data_collect_frame = ttk.LabelFrame(parent, text="数据收集控制", padding=10)
        data_collect_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # 监控开关
        monitor_frame = ttk.Frame(data_collect_frame)
        monitor_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(monitor_frame, text="实时监控:").pack(side=tk.LEFT)
        self.monitor_switch = ttk.Checkbutton(
            monitor_frame, 
            variable=self.monitoring_enabled,
            command=self.toggle_monitoring
        )
        self.monitor_switch.pack(side=tk.LEFT, padx=(10, 0))
        
        self.monitor_status_label = ttk.Label(monitor_frame, text="停止", foreground="red")
        self.monitor_status_label.pack(side=tk.RIGHT)
        
        # 手动获取历史数据按钮
        ttk.Button(
            data_collect_frame, 
            text="获取历史数据", 
            command=self.collect_historical_data
        ).pack(fill=tk.X, pady=5)
        
        # 分析控制区域
        analysis_frame = ttk.LabelFrame(parent, text="分析控制", padding=10)
        analysis_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # 分析开关
        analyze_frame = ttk.Frame(analysis_frame)
        analyze_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(analyze_frame, text="自动分析:").pack(side=tk.LEFT)
        self.analyze_switch = ttk.Checkbutton(
            analyze_frame, 
            variable=self.analysis_enabled,
            command=self.toggle_analysis
        )
        self.analyze_switch.pack(side=tk.LEFT, padx=(10, 0))
        
        self.analysis_status_label = ttk.Label(analyze_frame, text="停止", foreground="red")
        self.analysis_status_label.pack(side=tk.RIGHT)
        
        # 手动分析按钮
        ttk.Button(
            analysis_frame, 
            text="立即分析", 
            command=self.run_analysis
        ).pack(fill=tk.X, pady=5)
        
        # 板块选择区域
        board_frame = ttk.LabelFrame(parent, text="板块选择", padding=10)
        board_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # 板块下拉框
        ttk.Label(board_frame, text="选择板块:").pack(anchor=tk.W)
        self.board_combobox = ttk.Combobox(
            board_frame, 
            textvariable=self.selected_board,
            state="readonly"
        )
        self.board_combobox.pack(fill=tk.X, pady=5)
        self.board_combobox.bind("<<ComboboxSelected>>", self.on_board_selected)
        
        # 刷新板块列表按钮
        ttk.Button(
            board_frame, 
            text="刷新板块列表", 
            command=self.update_board_list
        ).pack(fill=tk.X, pady=5)
        
        # 状态信息区域
        status_frame = ttk.LabelFrame(parent, text="系统状态", padding=10)
        status_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # 状态显示
        self.status_text = scrolledtext.ScrolledText(
            status_frame, 
            height=8, 
            width=40,
            state=tk.DISABLED
        )
        self.status_text.pack(fill=tk.BOTH, expand=True)
        
        # 清除日志按钮
        ttk.Button(
            status_frame, 
            text="清除日志", 
            command=self.clear_log
        ).pack(fill=tk.X, pady=(5, 0))
    
    def create_chart_area(self, parent):
        """创建右侧图表区域"""
        # 标题
        chart_title = ttk.Label(parent, text="数据可视化", font=("Arial", 16, "bold"))
        chart_title.pack(pady=(10, 20))
        
        # 创建matplotlib图表
        self.fig = Figure(figsize=(12, 8), dpi=100)
        
        # 创建子图
        self.ax1 = self.fig.add_subplot(2, 1, 1)  # 价格图
        self.ax2 = self.fig.add_subplot(2, 1, 2)  # MACD图
        
        # 设置图表样式
        self.fig.patch.set_facecolor('white')
        self.ax1.set_title('板块价格走势', fontsize=14, fontweight='bold')
        self.ax2.set_title('MACD指标', fontsize=14, fontweight='bold')
        
        # 创建画布
        self.canvas = FigureCanvasTkAgg(self.fig, parent)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 添加工具栏
        toolbar_frame = ttk.Frame(parent)
        toolbar_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(
            toolbar_frame, 
            text="更新图表", 
            command=self.update_chart
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            toolbar_frame, 
            text="导出图表", 
            command=self.export_chart
        ).pack(side=tk.LEFT, padx=5)
        
        # 初始化图表
        self.initialize_chart()
    
    def initialize_chart(self):
        """初始化空图表"""
        self.ax1.clear()
        self.ax2.clear()
        
        self.ax1.set_title('板块价格走势 - 请选择板块', fontsize=14, fontweight='bold')
        self.ax2.set_title('MACD指标 - 请选择板块', fontsize=14, fontweight='bold')
        
        self.ax1.text(0.5, 0.5, '请选择板块并点击"更新图表"', 
                     horizontalalignment='center', verticalalignment='center',
                     transform=self.ax1.transAxes, fontsize=12)
        
        self.ax2.text(0.5, 0.5, '请选择板块并点击"更新图表"', 
                     horizontalalignment='center', verticalalignment='center',
                     transform=self.ax2.transAxes, fontsize=12)
        
        self.fig.tight_layout()
        self.canvas.draw()
    
    def update_board_list(self):
        """更新板块列表"""
        try:
            boards = self.data_reader.get_available_boards()
            if not boards:
                # 如果没有本地数据，尝试获取在线板块列表
                try:
                    boards = self.data_collector.get_all_boards()[:20]  # 限制数量避免界面卡顿
                except:
                    boards = ["保险", "银行", "证券", "房地产", "钢铁"]  # 默认一些常见板块
            
            self.board_combobox['values'] = boards
            if boards and not self.selected_board.get():
                self.selected_board.set(boards[0])
            
            self.status_info['available_boards'] = boards
            self.status_info['total_boards'] = len(boards)
            
            self.log_message(f"已更新板块列表，共{len(boards)}个板块")
            
        except Exception as e:
            self.log_message(f"更新板块列表失败: {str(e)}")
    
    def toggle_monitoring(self):
        """切换监控状态"""
        if self.monitoring_enabled.get():
            self.start_monitoring()
        else:
            self.stop_monitoring_func()
    
    def start_monitoring(self):
        """启动数据收集监控"""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            return
        
        self.stop_monitoring.clear()
        self.monitoring_thread = threading.Thread(target=self.monitoring_worker, daemon=True)
        self.monitoring_thread.start()
        
        self.monitor_status_label.config(text="运行中", foreground="green")
        self.log_message("数据收集监控已启动")
    
    def stop_monitoring_func(self):
        """停止数据收集监控"""
        self.stop_monitoring.set()
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=2)
        
        self.monitor_status_label.config(text="停止", foreground="red")
        self.log_message("数据收集监控已停止")
    
    def monitoring_worker(self):
        """监控工作线程"""
        schedule.clear()  # 清除之前的任务
        
        # 设置定时任务
        schedule.every().day.at("08:00").do(self.scheduled_historical_collection)
        schedule.every().minute.do(self.scheduled_realtime_collection)
        schedule.every(15).minutes.do(self.scheduled_data_save)
        
        self.message_queue.put(("log", "监控线程启动，设置定时任务完成"))
        
        while not self.stop_monitoring.is_set():
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.message_queue.put(("log", f"监控线程错误: {str(e)}"))
                time.sleep(5)
        
        schedule.clear()
        self.message_queue.put(("log", "监控线程已退出"))
    
    def scheduled_historical_collection(self):
        """定时历史数据收集"""
        try:
            self.message_queue.put(("log", "开始定时获取历史数据..."))
            self.data_collector.collect_all_historical_data(delay_seconds=2)
            self.message_queue.put(("log", "历史数据获取完成"))
            self.status_info['last_data_collect'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.message_queue.put(("log", f"历史数据获取失败: {str(e)}"))
    
    def scheduled_realtime_collection(self):
        """定时实时数据收集"""
        try:
            self.data_collector.collect_realtime_data()
            self.status_info['monitoring_status'] = "运行中"
        except Exception as e:
            self.message_queue.put(("log", f"实时数据收集失败: {str(e)}"))
    
    def scheduled_data_save(self):
        """定时数据保存"""
        try:
            self.data_collector.save_realtime_data_to_disk()
            self.message_queue.put(("log", "实时数据已保存到磁盘"))
        except Exception as e:
            self.message_queue.put(("log", f"数据保存失败: {str(e)}"))
    
    def collect_historical_data(self):
        """手动获取历史数据"""
        def worker():
            try:
                self.message_queue.put(("log", "开始手动获取历史数据..."))
                self.data_collector.collect_all_historical_data(delay_seconds=2)
                self.message_queue.put(("log", "历史数据获取完成"))
                self.status_info['last_data_collect'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.message_queue.put(("update_board_list", None))
            except Exception as e:
                self.message_queue.put(("log", f"历史数据获取失败: {str(e)}"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def toggle_analysis(self):
        """切换分析状态"""
        if self.analysis_enabled.get():
            self.start_analysis()
        else:
            self.stop_analysis_func()
    
    def start_analysis(self):
        """启动自动分析"""
        if self.analysis_thread and self.analysis_thread.is_alive():
            return
        
        self.stop_analysis.clear()
        self.analysis_thread = threading.Thread(target=self.analysis_worker, daemon=True)
        self.analysis_thread.start()
        
        self.analysis_status_label.config(text="运行中", foreground="green")
        self.log_message("自动分析已启动")
    
    def stop_analysis_func(self):
        """停止自动分析"""
        self.stop_analysis.set()
        if self.analysis_thread:
            self.analysis_thread.join(timeout=2)
        
        self.analysis_status_label.config(text="停止", foreground="red")
        self.log_message("自动分析已停止")
    
    def analysis_worker(self):
        """分析工作线程"""
        self.message_queue.put(("log", "分析线程启动"))
        
        while not self.stop_analysis.is_set():
            try:
                self.message_queue.put(("log", "开始分析所有板块..."))
                self.analyzer.analyze_all_boards()
                self.message_queue.put(("log", "板块分析完成"))
                self.status_info['last_analysis'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 等待5分钟后再次分析
                for _ in range(300):  # 5分钟 = 300秒
                    if self.stop_analysis.is_set():
                        break
                    time.sleep(1)
                    
            except Exception as e:
                self.message_queue.put(("log", f"分析错误: {str(e)}"))
                time.sleep(30)  # 出错后等待30秒再重试
        
        self.message_queue.put(("log", "分析线程已退出"))
    
    def run_analysis(self):
        """手动运行分析"""
        def worker():
            try:
                self.message_queue.put(("log", "开始手动分析..."))
                self.analyzer.analyze_all_boards()
                self.message_queue.put(("log", "手动分析完成"))
                self.status_info['last_analysis'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                self.message_queue.put(("log", f"手动分析失败: {str(e)}"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def on_board_selected(self, event=None):
        """板块选择事件处理"""
        board_name = self.selected_board.get()
        if board_name:
            self.log_message(f"已选择板块: {board_name}")
    
    def update_chart(self):
        """更新图表"""
        board_name = self.selected_board.get()
        if not board_name:
            messagebox.showwarning("警告", "请先选择一个板块")
            return
        
        def worker():
            try:
                self.message_queue.put(("log", f"正在加载{board_name}的数据..."))
                
                # 获取数据
                data = self.data_reader.get_latest_data(board_name, 200)  # 获取最新200个数据点
                
                if data is None or data.empty:
                    self.message_queue.put(("log", f"{board_name}暂无数据"))
                    return
                
                self.message_queue.put(("update_chart", (board_name, data)))
                
            except Exception as e:
                self.message_queue.put(("log", f"更新图表失败: {str(e)}"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def update_chart_display(self, board_name, data):
        """更新图表显示"""
        try:
            self.ax1.clear()
            self.ax2.clear()
            
            # 绘制价格图
            dates = pd.to_datetime(data['日期时间'])
            prices = data['收盘']
            
            self.ax1.plot(dates, prices, 'b-', linewidth=1.5, label='收盘价')
            self.ax1.set_title(f'{board_name} - 价格走势', fontsize=14, fontweight='bold')
            self.ax1.set_ylabel('价格')
            self.ax1.grid(True, alpha=0.3)
            self.ax1.legend()
            
            # 格式化日期轴
            self.ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            self.ax1.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            plt.setp(self.ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # 计算并绘制MACD
            if len(data) >= 26:
                close_prices = data['收盘']
                macd_line, signal_line, histogram = self.analyzer.calculate_macd(close_prices, 5, 13, 5)
                
                if macd_line is not None:
                    valid_dates = dates[~pd.isna(macd_line)]
                    valid_macd = macd_line.dropna()
                    valid_signal = signal_line.dropna()
                    valid_hist = histogram.dropna()
                    
                    if len(valid_macd) > 0:
                        self.ax2.plot(valid_dates, valid_macd, 'b-', linewidth=1.5, label='MACD')
                        self.ax2.plot(valid_dates, valid_signal, 'r-', linewidth=1.5, label='Signal')
                        self.ax2.bar(valid_dates, valid_hist, alpha=0.3, label='Histogram')
                        
                        self.ax2.axhline(y=0, color='k', linestyle='-', alpha=0.3)
                        self.ax2.set_title(f'{board_name} - MACD指标', fontsize=14, fontweight='bold')
                        self.ax2.set_ylabel('MACD')
                        self.ax2.grid(True, alpha=0.3)
                        self.ax2.legend()
                        
                        # 格式化日期轴
                        self.ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                        self.ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
                        plt.setp(self.ax2.xaxis.get_majorticklabels(), rotation=45)
            
            self.fig.tight_layout()
            self.canvas.draw()
            
            self.log_message(f"{board_name}图表更新完成，数据点数: {len(data)}")
            
        except Exception as e:
            self.log_message(f"图表绘制失败: {str(e)}")
    
    def export_chart(self):
        """导出图表"""
        if not self.selected_board.get():
            messagebox.showwarning("警告", "请先选择板块并更新图表")
            return
        
        try:
            filename = f"{self.selected_board.get()}_chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            self.fig.savefig(filename, dpi=300, bbox_inches='tight')
            self.log_message(f"图表已导出到: {filename}")
            messagebox.showinfo("成功", f"图表已导出到: {filename}")
        except Exception as e:
            self.log_message(f"导出图表失败: {str(e)}")
            messagebox.showerror("错误", f"导出图表失败: {str(e)}")
    
    def log_message(self, message):
        """添加日志消息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, formatted_message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)
        
        # 限制日志长度
        lines = self.status_text.get("1.0", tk.END).split("\n")
        if len(lines) > 1000:
            self.status_text.config(state=tk.NORMAL)
            self.status_text.delete("1.0", f"{len(lines)-500}.0")
            self.status_text.config(state=tk.DISABLED)
    
    def clear_log(self):
        """清除日志"""
        self.status_text.config(state=tk.NORMAL)
        self.status_text.delete("1.0", tk.END)
        self.status_text.config(state=tk.DISABLED)
    
    def start_message_handler(self):
        """启动消息处理器"""
        def process_messages():
            try:
                while True:
                    msg_type, data = self.message_queue.get_nowait()
                    
                    if msg_type == "log":
                        self.log_message(data)
                    elif msg_type == "update_chart":
                        board_name, chart_data = data
                        self.update_chart_display(board_name, chart_data)
                    elif msg_type == "update_board_list":
                        self.update_board_list()
                    
            except queue.Empty:
                pass
            
            # 继续处理消息
            self.root.after(100, process_messages)
        
        # 启动消息处理
        self.root.after(100, process_messages)
    
    def on_closing(self):
        """应用程序关闭事件处理"""
        # 停止所有线程
        self.monitoring_enabled.set(False)
        self.analysis_enabled.set(False)
        
        self.stop_monitoring_func()
        self.stop_analysis_func()
        
        # 保存最后的实时数据
        try:
            self.data_collector.save_realtime_data_to_disk()
        except:
            pass
        
        # 关闭窗口
        self.root.quit()
        self.root.destroy()
    
    def run(self):
        """运行应用程序"""
        self.log_message("板块分析系统启动完成")
        self.log_message("点击相应按钮开始数据收集和分析")
        self.root.mainloop()


if __name__ == "__main__":
    # 设置matplotlib中文字体支持
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    app = IndustryAnalysisGUI()
    app.run()