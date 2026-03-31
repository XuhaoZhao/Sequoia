"""
新浪财经期货数据拦截器

使用 Playwright 拦截新浪财经期货页面的实时数据API
目标页面: https://finance.sina.com.cn/futures/quotes/V2605.shtml
目标API: https://hq.sinajs.cn/?_=时间戳/&list=期货代码列表

特点：
1. 拦截期货页面的实时数据API
2. 支持自定义期货代码列表
3. 模拟真实浏览器行为，躲避反爬
4. 自动解析期货数据
5. 支持修改请求参数获取指定数据
6. 支持导出CSV格式
7. 从数据库自动加载活跃主力合约

使用方法：
    # 创建拦截器实例
    interceptor = SinaFuturesInterceptor(
        headless=False,  # 是否无头模式
        custom_symbols=['nf_V2605', 'nf_LC2703'],  # 自定义期货代码
        wait_time=15  # 等待时间（秒）
    )

    # 运行拦截
    data = asyncio.run(interceptor.intercept_futures_data())

    # 导出CSV（可选）
    if data:
        interceptor.export_to_csv(data, 'futures_data.csv')
"""

import asyncio
import re
import csv
import sys
import os
from datetime import datetime
from playwright.async_api import async_playwright

# 添加项目路径以导入 db_manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_manager import IndustryDataDB


class SinaFuturesInterceptor:
    """
    使用 Playwright 拦截新浪财经期货实时数据
    """
    def __init__(self, headless=False, custom_symbols=None, continuous=True):
        """
        初始化拦截器
        :param headless: 是否无头模式（True不显示浏览器，False显示浏览器）
        :param custom_symbols: 自定义期货代码列表，例如 ['nf_V2605', 'nf_LC2703']
        :param continuous: 是否持续监听（True=一直监听直到手动停止，False=指定时间后停止）
        """
        self.headless = headless
        self.continuous = continuous  # 持续监听模式
        self.custom_symbols = custom_symbols or ['nf_V2605']  # 默认获取V2605

        # 目标页面URL
        self.target_url = "https://finance.sina.com.cn/futures/quotes/V2605.shtml"

        # API URL模式
        self.api_pattern = "https://hq.sinajs.cn/"

        # 存储拦截到的数据
        self.captured_data = []

        # 统计信息
        self.intercept_count = 0
        self.start_time = None

        # CSV文件路径
        self.csv_file = None

        print(f"初始化新浪财经期货数据拦截器")
        print(f"目标页面: {self.target_url}")
        print(f"自定义期货代码: {', '.join(self.custom_symbols)}")
        print(f"无头模式: {headless}")
        print(f"监听模式: {'持续监听（按Ctrl+C停止）' if continuous else '定时监听'}")

    async def intercept_futures_data(self):
        """
        拦截期货数据 - 持续监听模式
        """
        # 初始化CSV文件
        self.csv_file = self._init_csv_file()

        async with async_playwright() as p:
            # 启动浏览器
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            print("\n" + "=" * 80)
            print("开始监听期货数据API...")
            print("=" * 80)

            # 请求拦截器 - 修改请求参数
            async def handle_route(route, request):
                url = request.url

                # 只拦截特定模式：
                # 1. 带时间戳参数 ?_=时间戳/
                # 2. 带list参数 /&list=...
                # 3. list中包含V2605
                # 4. list中有多个变量（包含逗号）
                if '?_=' in url and '/&list=' in url:
                    # 提取list参数值进行精确匹配
                    list_start = url.find('list=')
                    if list_start != -1:
                        list_part = url[list_start + 5:].split('&')[0]  # 获取list参数值

                        # 检查条件：
                        # 1. list中包含V2605
                        # 2. list中有多个变量（包含逗号）
                        if 'V2605' in list_part and ',' in list_part:
                            # 解析并替换list参数
                            if self.custom_symbols:
                                # 构造新的URL
                                base_url = url.split('?')[0]
                                timestamp = str(int(datetime.now().timestamp() * 1000))

                                # 构造新的list参数
                                new_list = ','.join(self.custom_symbols)
                                new_url = f"{base_url}?_={timestamp}/&list={new_list}"

                                # 继续修改后的请求
                                await route.continue_(url=new_url)
                                return

                # 其他请求正常处理
                await route.continue_()

            # 响应拦截器
            async def handle_response(response):
                url = response.url
                # 只处理特定模式：
                # 1. 带时间戳参数 ?_=时间戳/
                # 2. 带list参数 /&list=...
                # 3. list中包含V2605
                # 4. list中有多个变量（包含逗号）
                if '?_=' in url and '/&list=' in url:
                    # 提取list参数值进行精确匹配
                    list_start = url.find('list=')
                    if list_start != -1:
                        list_part = url[list_start + 5:].split('&')[0]  # 获取list参数值

                        # 检查条件：
                        # 1. list中包含V2605
                        # 2. list中有多个变量（包含逗号）
                        if 'V2605' in list_part and ',' in list_part:
                            try:
                                # 获取响应内容
                                text = await response.text()

                                if text and len(text) > 0:
                                    self.intercept_count += 1

                                    # 解析数据
                                    parsed_data = self._parse_sina_response(text)
                                    if parsed_data:
                                        # 实时追加到CSV
                                        self._append_to_csv(parsed_data)

                                        # 显示统计信息（每10次显示一次详情）
                                        if self.intercept_count % 10 == 1:
                                            print(f"\n✓ 第 {self.intercept_count} 次拦截 - {len(parsed_data)} 条数据")
                                            self._print_futures_data(parsed_data)
                                        else:
                                            print(f"✓ 第 {self.intercept_count} 次拦截 - {len(parsed_data)} 条数据", end='\r')

                            except Exception as e:
                                print(f"\n✗ 响应处理失败: {e}")

            # 设置路由拦截
            await page.route('**/*', handle_route)

            # 监听响应事件
            page.on('response', handle_response)

            # 访问目标页面
            print(f"\n访问页面: {self.target_url}")
            await page.goto(self.target_url, wait_until='networkidle')

            self.start_time = datetime.now()
            print("\n" + "=" * 80)
            print("页面加载完成，开始持续监听实时数据...")
            print("按 Ctrl+C 停止监听")
            print("=" * 80)
            print(f"\n数据实时保存到: {self.csv_file}\n")

            try:
                # 持续监听 - 直到手动停止
                while self.continuous:
                    await asyncio.sleep(1)

                    # 每隔60秒显示统计信息
                    if self.intercept_count > 0 and self.intercept_count % 60 == 0:
                        elapsed = (datetime.now() - self.start_time).total_seconds()
                        rate = self.intercept_count / elapsed
                        print(f"\n📊 运行统计: 已拦截 {self.intercept_count} 次 | 运行时长: {elapsed:.0f}秒 | 频率: {rate:.2f}次/秒")

            except KeyboardInterrupt:
                print("\n\n" + "=" * 80)
                print("用户手动停止监听")
                print("=" * 80)

            # 关闭浏览器
            await browser.close()

            # 显示最终统计
            elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            print(f"\n📊 最终统计:")
            print(f"  总拦截次数: {self.intercept_count}")
            print(f"  运行时长: {elapsed:.0f}秒 ({elapsed/60:.1f}分钟)")
            print(f"  平均频率: {self.intercept_count/elapsed:.2f}次/秒" if elapsed > 0 else "")
            print(f"  数据文件: {self.csv_file}")
            print("=" * 80)

            return self.captured_data

    def _parse_sina_response(self, response_text):
        """
        解析新浪财经的响应数据
        数据格式: var hq_str_nf_V2605="聚氯乙烯,V2605,5123,5140,...";
        :param response_text: 响应文本
        :return: 解析后的数据列表
        """
        try:
            data_list = []

            # 使用正则提取所有数据行
            pattern = r'var hq_str_(.+?)="(.+?)";'
            matches = re.findall(pattern, response_text)

            for symbol, data_str in matches:
                # 分割数据字段
                fields = data_str.split(',')

                if len(fields) < 5:
                    continue

                # 构造数据字典
                data = {
                    '期货代码': symbol.replace('nf_', ''),  # 移除nf_前缀
                    '名称': fields[0] if len(fields) > 0 else '',
                    '合约': fields[1] if len(fields) > 1 else '',
                    '最新价': fields[2] if len(fields) > 2 else '',
                    '昨收': fields[3] if len(fields) > 3 else '',
                    '今开': fields[4] if len(fields) > 4 else '',
                    '最高': fields[5] if len(fields) > 5 else '',
                    '最低': fields[6] if len(fields) > 6 else '',
                    '买一': fields[7] if len(fields) > 7 else '',
                    '卖一': fields[8] if len(fields) > 8 else '',
                    '成交量': fields[9] if len(fields) > 9 else '',
                    '持仓量': fields[10] if len(fields) > 10 else '',
                    '时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '序号': self.intercept_count  # 添加拦截序号
                }

                data_list.append(data)

            return data_list

        except Exception as e:
            print(f"✗ 解析响应数据失败: {e}")
            return []

    def _print_futures_data(self, data_list):
        """
        打印期货数据到控制台
        :param data_list: 数据列表
        """
        print("\n" + "-" * 80)
        print("期货数据详情:")
        print("-" * 80)

        for data in data_list:
            print(f"\n【{data['名称']}】 {data['合约']}")
            print(f"  代码: {data['期货代码']}")
            print(f"  最新价: {data['最新价']}")
            print(f"  昨收: {data['昨收']}")
            print(f"  今开: {data['今开']}")
            print(f"  最高: {data['最高']}")
            print(f"  最低: {data['最低']}")
            print(f"  买一: {data['买一']}")
            print(f"  卖一: {data['卖一']}")
            print(f"  成交量: {data['成交量']}")
            print(f"  持仓量: {data['持仓量']}")
            print(f"  更新时间: {data['时间']}")

        print("-" * 80)

    def _init_csv_file(self):
        """
        初始化CSV文件，写入表头
        """
        import os
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data/sina_futures_realtime_{timestamp}.csv"

        # 确保目录存在
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

        # 写入表头
        headers = ['期货代码', '名称', '合约', '最新价', '昨收', '今开', '最高', '最低',
                  '买一', '卖一', '成交量', '持仓量', '时间', '序号']

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

        return filename

    def _append_to_csv(self, data_list):
        """
        追加数据到CSV文件
        """
        if not self.csv_file or not data_list:
            return

        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8-sig') as f:
                fieldnames = list(data_list[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # 为每条数据添加序号（不修改原始数据）
                for data in data_list:
                    row = data.copy()
                    row['序号'] = self.intercept_count
                    writer.writerow(row)

        except Exception as e:
            print(f"\n✗ 追加CSV失败: {e}")

    def export_to_csv(self, data_list, filename=None):
        """
        将数据导出到CSV文件
        :param data_list: 数据列表
        :param filename: 文件名（默认自动生成）
        :return: 文件路径
        """
        if not data_list:
            print("⚠ 没有数据可导出")
            return None

        try:
            # 自动生成文件名
            if filename is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"data/sina_futures_{timestamp}.csv"

            # 确保目录存在
            import os
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

            # 写入CSV
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                if data_list:
                    writer = csv.DictWriter(f, fieldnames=data_list[0].keys())
                    writer.writeheader()
                    writer.writerows(data_list)

            print(f"\n✓ 数据已导出到: {filename}")
            print(f"  共导出 {len(data_list)} 条记录")
            return filename

        except Exception as e:
            print(f"✗ 导出CSV失败: {e}")
            return None


def main():
    """
    主函数 - 启动拦截器（持续监听模式）
    """
    print("=" * 80)
    print("新浪财经期货实时数据拦截器 - 持续监听模式")
    print("=" * 80)

    # 从数据库自动加载成交额>20亿的主力合约
    print("\n正在从数据库加载活跃主力合约...")
    db = IndustryDataDB()

    try:
        # 获取成交额>20亿的主力合约
        contracts = db.get_active_main_contracts_simple(min_amount=20.0)

        if not contracts:
            print("⚠ 警告: 数据库中没有找到符合条件的主力合约")
            print("使用默认合约列表...")
            # 使用默认合约列表
            custom_symbols = [
                'nf_V2605',   # 聚氯乙烯
                'nf_LC2703',  # 碳酸锂
                'nf_TA2605',  # PTA
                'nf_MA2605',  # 甲醇
                'nf_AG2606',  # 白银
            ]
        else:
            # 转换为新浪财经格式：添加 'nf_' 前缀
            custom_symbols = [f"nf_{contract['contract_code']}" for contract in contracts]
            print(f"✓ 成功加载 {len(custom_symbols)} 个活跃主力合约")
            print(f"合约列表: {', '.join(custom_symbols[:10])}{'...' if len(custom_symbols) > 10 else ''}")

    except Exception as e:
        print(f"⚠ 警告: 从数据库加载合约失败: {e}")
        print("使用默认合约列表...")
        custom_symbols = [
            'nf_V2605',   # 聚氯乙烯
            'nf_LC2703',  # 碳酸锂
            'nf_TA2605',  # PTA
            'nf_MA2605',  # 甲醇
            'nf_AG2606',  # 白银
        ]

    print(f"\n配置的期货代码数量: {len(custom_symbols)}")
    print(f"将持续监听并记录所有实时数据，不丢失任何更新")

    # 创建拦截器
    interceptor = SinaFuturesInterceptor(
        headless=False,           # 显示浏览器窗口，便于观察
        custom_symbols=custom_symbols,
        continuous=True           # 持续监听模式
    )

    # 运行拦截
    try:
        data = asyncio.run(interceptor.intercept_futures_data())

        if data:
            print(f"\n✓ 监听完成，共获取 {len(data)} 条期货数据")
        else:
            print("\n程序正常结束")

    except KeyboardInterrupt:
        print("\n用户手动停止程序")
    except Exception as e:
        print(f"\n✗ 程序运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
