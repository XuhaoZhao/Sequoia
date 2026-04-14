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
5. 数据保存到数据库（带去重）
6. 从数据库自动加载活跃主力合约

使用方法：
    # 创建拦截器实例
    interceptor = SinaFuturesInterceptor(
        headless=False,  # 是否无头模式
        custom_symbols=['nf_V2605', 'nf_LC2703'],  # 自定义期货代码
        continuous=True  # 持续监听模式
    )

    # 运行拦截
    data = asyncio.run(interceptor.intercept_futures_data())
"""

import asyncio
import re
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
    def __init__(self, headless=False, custom_symbols=None, symbol_name_map=None, continuous=True):
        """
        初始化拦截器
        :param headless: 是否无头模式（True不显示浏览器，False显示浏览器）
        :param custom_symbols: 自定义期货代码列表，例如 ['nf_V2605', 'nf_LC2703']
        :param symbol_name_map: 合约代码到品种名称的映射，例如 {'nf_V2605': '聚氯乙烯'}
        :param continuous: 是否持续监听（True=一直监听直到手动停止，False=指定时间后停止）
        """
        self.headless = headless
        self.continuous = continuous  # 持续监听模式
        self.custom_symbols = custom_symbols or ['nf_V2605']  # 默认获取V2605
        self.symbol_name_map = symbol_name_map or {}  # 合约名称映射

        # 目标页面URL
        self.target_url = "https://finance.sina.com.cn/futures/quotes/V2605.shtml"

        # API URL模式
        self.api_pattern = "https://hq.sinajs.cn/"

        # 存储拦截到的数据
        self.captured_data = []

        # 统计信息
        self.intercept_count = 0
        self.start_time = None

        # 请求拦截计数（用于日志）
        self.request_intercept_count = 0

        # 跟踪成功获取的合约（用于最后统计缺失合约）
        self.captured_symbols = set()

        # 合约代码修正映射（记录旧代码到新代码的转换）
        self.contract_code_corrections = {}

        # 数据库管理器
        self.db = IndustryDataDB()

        # 标记是否已打印第一次拦截详情
        self.first_intercept_printed = False

        print(f"初始化新浪财经期货数据拦截器")
        print(f"目标页面: {self.target_url}")
        print(f"自定义期货代码数量: {len(self.custom_symbols)}")
        print(f"合约列表: {', '.join(self.custom_symbols[:10])}{'...' if len(self.custom_symbols) > 10 else ''}")
        print(f"无头模式: {headless}")
        print(f"监听模式: {'持续监听（按Ctrl+C停止）' if continuous else '定时监听'}")
        print(f"数据存储: 数据库 (sina_futures_realtime表)")

        # 首次运行时，修正所有合约代码格式
        self._fix_all_contract_codes()

    def _fix_all_contract_codes(self):
        """
        首次运行时，一次性修正所有合约代码格式
        新浪财经格式：3位数字需要补全为4位（如 AP605 -> AP2605）
        """
        import copy

        corrected_symbols = []
        corrections_map = {}  # 记录修正映射

        for symbol in self.custom_symbols:
            # 移除 nf_ 前缀
            code = symbol.replace('nf_', '')

            # 检查数字部分是否已经是4位
            digits_match = re.search(r'(\d+)$', code)
            if not digits_match:
                corrected_symbols.append(symbol)
                continue

            digits = digits_match.group(1)

            # 如果已经是4位数字，不需要修正
            if len(digits) == 4:
                corrected_symbols.append(symbol)
                continue

            # 如果是3位数字，需要补全为4位
            if len(digits) == 3:
                # 提取品种代码和3位数字
                match = re.match(r'^([A-Za-z]+)(\d{3})$', code)
                if not match:
                    corrected_symbols.append(symbol)
                    continue

                variety = match.group(1)  # 品种代码，如 AP
                month_3digit = match.group(2)  # 3位数字，如 605

                # 根据当前年份判断完整年份
                first_digit = int(month_3digit[0])  # 第一位，如 6
                current_year_last_digit = datetime.now().year % 10  # 当前年份最后一位
                current_decade = datetime.now().year // 10 * 10  # 当前年代

                # 判断合约年份
                if first_digit > current_year_last_digit:
                    contract_year = current_decade + first_digit
                elif first_digit < current_year_last_digit - 2:
                    contract_year = current_decade + 10 + first_digit
                else:
                    contract_year = current_decade + first_digit

                # 补全为4位数字：年份后2位 + 月份后2位
                year_2digit = str(contract_year)[2:]  # 如 "26"
                month_2digit = month_3digit[1:]  # 如 "05"
                month_4digit = f"{year_2digit}{month_2digit}"  # "2605"

                # 构造新的合约代码
                new_code = f"{variety}{month_4digit}"
                new_symbol = f"nf_{new_code}"

                # 记录修正
                corrected_symbols.append(new_symbol)
                corrections_map[symbol] = new_symbol

                # 同时更新 symbol_name_map
                if symbol in self.symbol_name_map:
                    self.symbol_name_map[new_symbol] = self.symbol_name_map[symbol]
                    del self.symbol_name_map[symbol]
            else:
                corrected_symbols.append(symbol)

        # 如果有修正，更新custom_symbols并显示
        if corrections_map:
            print(f"\n🔧 合约代码自动修正:")
            print(f"  修正数量: {len(corrections_map)} 个")
            print(f"  修正列表:")
            for old_code, new_code in corrections_map.items():
                print(f"    {old_code} -> {new_code}")
            print()

            # 更新 custom_symbols
            self.custom_symbols = corrected_symbols

            # 保存修正记录（用于最终统计）
            self.contract_code_corrections = corrections_map
        else:
            self.contract_code_corrections = {}
            print("✓ 所有合约代码格式正确，无需修正\n")

    async def intercept_futures_data(self):
        """
        拦截期货数据 - 持续监听模式
        """
        async with async_playwright() as p:
            # 启动浏览器
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            print("\n" + "=" * 80)
            print("开始监听期货数据API...")
            print("=" * 80)
            print(f"\n准备拦截所有包含 V2605 的API请求")
            print(f"将替换为指定的 {len(self.custom_symbols)} 个合约")
            print()

            # 请求拦截器 - 修改请求参数
            async def handle_route(route, request):
                url = request.url

                # 只拦截特定模式：
                # 1. 带时间戳参数 ?_=时间戳/
                # 2. 带list参数 /&list=...
                # 3. list中包含V2605且有多个变量（包含逗号）
                if '?_=' in url and '/&list=' in url:
                    # 提取list参数值进行精确匹配
                    list_start = url.find('list=')
                    if list_start != -1:
                        list_part = url[list_start + 5:].split('&')[0]  # 获取list参数值

                        # 检查条件：list中包含V2605且有多个变量
                        if 'V2605' in list_part and ',' in list_part:
                            self.request_intercept_count += 1

                            # 构造新的URL
                            base_url = url.split('?')[0]
                            timestamp = str(int(datetime.now().timestamp() * 1000))

                            # 构造新的list参数
                            new_list = ','.join(self.custom_symbols)
                            new_url = f"{base_url}?_={timestamp}/&list={new_list}"

                            # 第一次或每10次打印详细信息
                            if self.request_intercept_count == 1 or self.request_intercept_count % 10 == 0:
                                print(f"\n[请求拦截 #{self.request_intercept_count}] 替换为 {len(self.custom_symbols)} 个合约")
                                print(f"  原URL包含: {list_part[:50]}...")

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
                # 3. list中包含V2605且有多个变量
                if '?_=' in url and '/&list=' in url:
                    # 提取list参数值进行精确匹配
                    list_start = url.find('list=')
                    if list_start != -1:
                        list_part = url[list_start + 5:].split('&')[0]  # 获取list参数值

                        # 检查条件：list中包含V2605且有多个变量
                        # 并且是我们修改后的请求（包含我们的合约）
                        if 'V2605' in list_part and ',' in list_part:
                            if any(symbol in list_part for symbol in self.custom_symbols):
                                try:
                                    # 获取响应内容
                                    text = await response.text()

                                    if text and len(text) > 0:
                                        self.intercept_count += 1

                                        # 解析数据
                                        parsed_data = self._parse_sina_response(text)
                                        if parsed_data:
                                            # 保存到数据库
                                            inserted = self.db.insert_sina_futures_realtime(parsed_data)

                                            # 检查缺失的合约
                                            missing_symbols = self._check_missing_symbols()

                                            # 只在第一次拦截时打印详情
                                            if not self.first_intercept_printed:
                                                print(f"\n✓ 第 {self.intercept_count} 次拦截 - {len(parsed_data)} 条数据 (已保存 {inserted} 条)")
                                                self._print_futures_data(parsed_data[:3])  # 只打印前3条
                                                self.first_intercept_printed = True
                                            # 每10次打印简要信息
                                            elif self.intercept_count % 10 == 1:
                                                print(f"\n✓ 第 {self.intercept_count} 次拦截 - {len(parsed_data)} 条数据 (已保存 {inserted} 条)")
                                            else:
                                                # 简洁打印
                                                missing_info = f" | 缺失: {len(missing_symbols)}个" if missing_symbols else ""
                                                print(f"✓ 第 {self.intercept_count} 次拦截 - {len(parsed_data)}条 (保存{inserted}条){missing_info}", end='\r')

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
            print(f"\n数据将实时保存到数据库 (sina_futures_realtime表)\n")

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
            print(f"  数据存储: 数据库 (sina_futures_realtime表)")

            # 显示合约代码修正统计
            if self.contract_code_corrections:
                print(f"\n🔧 合约代码修正统计:")
                print(f"  修正数量: {len(self.contract_code_corrections)} 个")
                print(f"  修正列表:")
                for old_code, new_code in self.contract_code_corrections.items():
                    print(f"    {old_code} -> {new_code}")

            # 统计合约获取情况
            print(f"\n📋 合约获取统计:")
            print(f"  期望获取: {len(self.custom_symbols)} 个合约")
            print(f"  成功获取: {len(self.captured_symbols)} 个合约")

            # 计算缺失的合约
            missing_symbols = set(self.custom_symbols) - self.captured_symbols
            if missing_symbols:
                print(f"  ❌ 缺失合约: {len(missing_symbols)} 个")
                print(f"     缺失列表: {', '.join(sorted(missing_symbols))}")
            else:
                print(f"  ✓ 全部获取成功！")

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

                # DEBUG: 第一次拦截时打印完整的API响应格式
                if not self.first_intercept_printed:
                    print(f"\n{'='*80}")
                    print(f"[DEBUG] API响应格式分析 - 合约: {symbol}")
                    print(f"字段总数: {len(fields)}")
                    print(f"完整响应字符串: {data_str[:200]}...")
                    print(f"\n字段详情 (前20个):")
                    for i in range(min(20, len(fields))):
                        print(f"  fields[{i}] = '{fields[i]}'")
                    print(f"\n时间字段 (fields[1]): '{fields[1] if len(fields) > 1 else 'N/A'}'")
                    print(f"{'='*80}\n")

                if len(fields) < 2:
                    continue

                # 记录成功获取的合约代码
                # 注意：代码已经在初始化时修正过了，这里直接使用
                self.captured_symbols.add(symbol)

                # 提取API返回的时间
                # 根据用户反馈，时间在 fields[1]
                # 新浪财经API格式：
                # fields[1] = 时间 (可能是 "15:00:00" 或 "2026-04-07 15:00:00")
                api_datetime_field = fields[1] if len(fields) > 1 else ''

                # 解析 fields[1]
                if api_datetime_field:
                    # 如果包含日期和时间
                    if ' ' in api_datetime_field and '-' in api_datetime_field:
                        formatted_datetime = api_datetime_field  # 已经是完整格式
                    # 如果只有时间 (HH:MM:SS)
                    elif ':' in api_datetime_field:
                        # 使用今天的日期 + API返回的时间
                        today = datetime.now().strftime('%Y-%m-%d')
                        formatted_datetime = f"{today} {api_datetime_field}"
                    # 如果是其他格式，直接使用
                    else:
                        formatted_datetime = api_datetime_field
                else:
                    # 如果API没有返回时间，使用当前时间
                    formatted_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 构造数据字典
                # 新浪财经API字段映射（根据实际测试结果）：
                # fields[0]=品种名称, fields[1]=时间, fields[2]=今开, fields[3]=最高,
                # fields[4]=最低, fields[5]=最新价, fields[6]=?, fields[7]=买一, fields[8]=卖一,
                # fields[9]=结算价, fields[10]=昨结算, fields[11-12]=?, fields[13]=持仓量, fields[14]=成交量
                data = {
                    '期货代码': symbol.replace('nf_', ''),  # 移除nf_前缀
                    '名称': fields[0] if len(fields) > 0 else '',
                    '最新价': fields[5] if len(fields) > 5 else '',        # 修正：fields[5]是最新价
                    '昨收': fields[10] if len(fields) > 10 else '',       # 修正：fields[10]是昨结算
                    '今开': fields[2] if len(fields) > 2 else '',         # 修正：fields[2]是今开
                    '最高': fields[3] if len(fields) > 3 else '',         # 修正：fields[3]是最高
                    '最低': fields[4] if len(fields) > 4 else '',         # 修正：fields[4]是最低
                    '买一': fields[7] if len(fields) > 7 else '',         # 修正：fields[7]是买一
                    '卖一': fields[8] if len(fields) > 8 else '',         # 修正：fields[8]是卖一
                    '成交量': fields[14] if len(fields) > 14 else '',     # 修正：fields[14]是成交量
                    '持仓量': fields[13] if len(fields) > 13 else '',     # 修正：fields[13]是持仓量
                    '时间': formatted_datetime,
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
            print(f"\n【{data['名称']}】 {data['期货代码']}")
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

    def _check_missing_symbols(self):
        """
        检查缺失的合约
        :return: 缺失的合约符号列表
        """
        if not self.custom_symbols:
            return []

        missing = set(self.custom_symbols) - self.captured_symbols
        return sorted(list(missing))

    def _print_missing_symbols(self, missing_symbols):
        """
        打印缺失的合约信息
        :param missing_symbols: 缺失的合约符号列表
        """
        if not missing_symbols:
            return

        print(f"\n{'─' * 80}")
        print(f"⚠ 缺失合约 ({len(missing_symbols)}个):")

        # 打印缺失的合约，格式：代码 - 名称
        for symbol in missing_symbols[:20]:  # 最多显示20个
            name = self.symbol_name_map.get(symbol, '未知')
            print(f"  • {symbol} - {name}")

        if len(missing_symbols) > 20:
            print(f"  ... 还有 {len(missing_symbols) - 20} 个合约未显示")

        print(f"{'─' * 80}")


def main():
    """
    主函数 - 启动拦截器（持续监听模式）
    """
    print("=" * 80)
    print("新浪财经期货实时数据拦截器 - 持续监听模式")
    print("=" * 80)

    # 从数据库自动加载主力合约（成交额>=20亿，保证金<5万，手续费<30元）
    print("\n正在从数据库加载活跃主力合约...")
    print("筛选条件: 成交额>=20亿, 保证金<5万, 手续费<30元")
    db = IndustryDataDB()

    try:
        # 获取主力合约（成交额>=20亿，保证金<5万，手续费<30元）
        contracts = db.get_active_main_contracts_simple(
            min_amount=20.0,          # 成交额>=20亿
            max_margin=50000.0,       # 保证金<5万
            max_fee=30.0,             # 手续费<30元
            use_volume_filter=True    # 启用成交额筛选
        )

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
            # 创建默认的合约名称映射
            symbol_name_map = {
                'nf_V2605': '聚氯乙烯',
                'nf_LC2703': '碳酸锂',
                'nf_TA2605': 'PTA',
                'nf_MA2605': '甲醇',
                'nf_AG2606': '白银'
            }
        else:
            # 转换为新浪财经格式：添加 'nf_' 前缀，并统一转为大写
            custom_symbols = [f"nf_{contract['contract_code'].upper()}" for contract in contracts]

            # 创建合约代码到品种名称的映射
            # 需要从数据库重新查询以获取详细信息
            df = db.query_futures_contracts(is_main_contract=True)
            symbol_name_map = {}
            for _, row in df.iterrows():
                code_upper = row['contract_code'].upper()
                symbol = f"nf_{code_upper}"
                if symbol in custom_symbols:
                    symbol_name_map[symbol] = row['variety_name_cn']

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
        symbol_name_map = {
            'nf_V2605': '聚氯乙烯',
            'nf_LC2703': '碳酸锂',
            'nf_TA2605': 'PTA',
            'nf_MA2605': '甲醇',
            'nf_AG2606': '白银'
        }

    print(f"\n配置的期货代码数量: {len(custom_symbols)}")
    print(f"将持续监听并记录所有实时数据，不丢失任何更新")

    # 创建拦截器
    interceptor = SinaFuturesInterceptor(
        headless=False,           # 显示浏览器窗口，便于观察
        custom_symbols=custom_symbols,
        symbol_name_map=symbol_name_map,  # 传递合约名称映射
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
