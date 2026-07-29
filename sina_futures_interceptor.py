"""
东方财富期货实时数据拦截器

使用 Playwright 拦截东方财富期货页面自身的实时数据API请求
目标页面: https://qhweb.eastmoney.com/quote
数据API: https://futsseapi.eastmoney.com/list/trans/block/risk/mk0830

特点：
1. 被动拦截页面自身发出的定时数据请求，不主动fetch
2. 通过浏览器访问页面，自动携带正确的Referer/Cookie，绕过反爬
3. 自动解析JSON响应数据
4. 数据保存到数据库（带去重）
5. 支持持续监听模式

东方财富API返回字段说明：
  name  - 名称        dm  - 合约代码      p   - 最新价
  zdf   - 涨跌幅%     vol - 成交量        ccl - 持仓量
  rz    - 日增仓       cje - 成交额        zde - 涨跌额
  o     - 今开        h   - 最高          l   - 最低
  zf    - 振幅%       zjsj - 昨结算       zt  - 涨停
  dt    - 跌停        tjd - 套保投机比    tag - 标签
  sc    - 品种编码    uid - 交易所|合约    zsjd - 市价单状态

使用方法：
    interceptor = EastMoneyFuturesInterceptor(
        headless=False,   # 是否无头模式
        continuous=True   # 持续监听模式
    )
    data = asyncio.run(interceptor.start())
"""

import asyncio
import json
import sys
import os
from datetime import datetime
from playwright.async_api import async_playwright

# 添加项目路径以导入 db_manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_manager import IndustryDataDB


class EastMoneyFuturesInterceptor:
    """
    使用 Playwright 拦截东方财富期货页面自身发出的实时数据API
    """

    # 目标页面
    TARGET_URL = "https://qhweb.eastmoney.com/quote"

    # 需要拦截的API路径关键词
    API_PATTERN = "futsseapi.eastmoney.com/list/trans/block/risk/mk0830"

    def __init__(self, headless=False, continuous=True):
        """
        初始化拦截器
        :param headless: 是否无头模式
        :param continuous: 是否持续监听
        """
        self.headless = headless
        self.continuous = continuous

        # 数据库管理器
        self.db = IndustryDataDB()

        # 统计信息
        self.intercept_count = 0
        self.total_records = 0
        self.filtered_count = 0
        self.start_time = None
        self.first_intercept_printed = False

        # 一次性加载交易时间到内存，避免重复查询数据库
        # 格式: {variety_name: {'day': [(start_min, end_min), ...], 'night': [(start_min, end_min, cross_day), ...]}}
        self.trading_hours_map = self._load_trading_hours_map()

        print(f"初始化东方财富期货数据拦截器")
        print(f"目标页面: {self.TARGET_URL}")
        print(f"拦截API: {self.API_PATTERN}")
        print(f"监听模式: {'持续监听（按Ctrl+C停止）' if continuous else '单次获取'}")
        print(f"数据存储: 数据库 (sina_futures_realtime表)")
        print(f"交易时间过滤: 已加载 {len(self.trading_hours_map)} 个品种的交易时间")

    def _load_trading_hours_map(self):
        """
        从数据库一次性加载所有品种的交易时间到内存字典
        返回格式: {variety_name: {'day': [(start_min, end_min), ...], 'night': [(start_min, end_min, cross_day), ...]}}
        时间统一转为"从0点开始的分钟数"方便比较，cross_day表示是否跨日（如夜盘21:00-02:30）
        """
        trading_hours_map = {}
        try:
            import sqlite3
            with self.db.get_connection() as conn:
                cursor = conn.execute("SELECT variety_name, day_session, night_session FROM futures_trading_hours")
                for row in cursor.fetchall():
                    variety_name = row['variety_name']
                    day_ranges = self._parse_time_ranges(row['day_session'])
                    night_ranges = self._parse_time_ranges(row['night_session']) if row['night_session'] else []
                    trading_hours_map[variety_name] = {
                        'day': day_ranges,
                        'night': night_ranges,
                    }
        except Exception as e:
            print(f"[警告] 加载交易时间数据失败: {e}，将不进行交易时间过滤")
        return trading_hours_map

    @staticmethod
    def _parse_time_ranges(session_str):
        """
        解析交易时间字符串为分钟数区间列表
        输入: "9:00-10:15,10:30-11:30,13:30-15:00" 或 "21:00-02:30" 或 None
        输出: [(start_min, end_min, cross_day), ...]
          - start_min, end_min: 从0:00开始的分钟数
          - cross_day: bool, True表示结束时间在第二天（如夜盘 21:00-02:30）
        """
        if not session_str:
            return []

        ranges = []
        for segment in session_str.split(','):
            segment = segment.strip()
            if '-' not in segment:
                continue
            parts = segment.split('-')
            if len(parts) != 2:
                continue

            start_h, start_m = map(int, parts[0].split(':'))
            end_h, end_m = map(int, parts[1].split(':'))

            start_min = start_h * 60 + start_m
            end_min = end_h * 60 + end_m
            cross_day = end_min < start_min  # 结束分钟 < 开始分钟 => 跨日

            ranges.append((start_min, end_min, cross_day))

        return ranges

    def _find_trading_hours(self, variety_name):
        """
        根据品种名称模糊匹配查找对应的交易时间
        匹配规则: trading_hours_map的key是variety_name，传入的variety_name可能带有尾部数字或前缀
        优先精确匹配，然后尝试品种名称是传入名称的前缀
        """
        if not variety_name:
            return None

        # 1. 精确匹配
        if variety_name in self.trading_hours_map:
            return self.trading_hours_map[variety_name]

        # 2. 传入名称以交易时间表中的品种名称开头
        # 例如: "聚丙烯月均" 匹配 "聚丙烯", "PVC月均" 匹配 "PVC"
        for db_name, hours in self.trading_hours_map.items():
            if variety_name.startswith(db_name):
                return hours

        return None

    def is_in_trading_hours(self, variety_name, check_time=None):
        """
        判断指定品种在给定时间是否处于交易时间段内

        :param variety_name: 品种名称（从API数据中提取的，如"聚氯乙烯"、"沪铜"等）
        :param check_time: 要检查的时间，datetime对象，默认为当前时间
        :return: True表示在交易时间内，False表示不在；若找不到品种的交易时间配置也返回True（放行）
        """
        if check_time is None:
            check_time = datetime.now()

        # 周末（周六=5, 周日=6）不交易
        if check_time.weekday() >= 5:
            return False

        hours = self._find_trading_hours(variety_name)
        if hours is None:
            # 找不到交易时间配置，放行不过滤
            return True

        current_min = check_time.hour * 60 + check_time.minute

        # 检查日盘
        for start_min, end_min, cross_day in hours['day']:
            if start_min <= current_min <= end_min:
                return True

        # 检查夜盘
        for start_min, end_min, cross_day in hours['night']:
            if cross_day:
                # 跨日夜盘，如 21:00-02:30
                # 当天部分: current_min >= 21:00
                # 次日部分: current_min <= 02:30
                if current_min >= start_min or current_min <= end_min:
                    return True
            else:
                # 不跨日夜盘，如 21:00-23:00
                if start_min <= current_min <= end_min:
                    return True

        return False

    def _parse_response(self, response_json):
        """
        解析东方财富API的JSON响应，转换为数据库记录格式

        东方财富字段 -> 数据库字段映射：
          dm -> contract_code    name -> name
          p  -> latest_price     zjsj -> prev_close (昨结算)
          o  -> open_price       h   -> high_price
          l  -> low_price        (无买一卖一，留空)
          vol -> volume          ccl -> open_interest
        """
        data_list = []
        # 取当前时间（精确到秒）作为价格时间戳
        # API不返回时间，用拦截到响应的时刻作为数据时间
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        items = response_json.get('list', [])
        for item in items:
            # 跳过没有成交数据的品种（如停牌、未开盘）
            # p 为 "-" 或 0 表示无有效价格
            p = item.get('p')
            if p == '-' or p == 0 or p is None:
                continue

            dm = item.get('dm', '').upper()   # 合约代码转大写，如 v2605 -> V2605
            name = item.get('name', '')       # 名称，如 PVC2605

            # 从 name 中提取品种中文名（去掉合约号后缀）
            variety_name = name
            for suffix_digit in ['605', '606', '607', '608', '609',
                                 '2605', '2606', '2607', '2608', '2609',
                                 '505', '506', '507', '508', '509',
                                 '705', '706', '707', '708', '709']:
                if name.endswith(suffix_digit):
                    variety_name = name[:-len(suffix_digit)]
                    break

            data = {
                '期货代码': dm,
                '名称': variety_name,
                '最新价': p,
                '昨收': item.get('zjsj'),
                '今开': item.get('o'),
                '最高': item.get('h'),
                '最低': item.get('l'),
                '买一': '',           # 东方财富此API不提供买一卖一
                '卖一': '',
                '成交量': item.get('vol'),
                '持仓量': item.get('ccl'),
                '涨跌幅': item.get('zdf'),
                '涨跌额': item.get('zde'),
                '成交额': item.get('cje'),
                '日增仓': item.get('rz'),
                '振幅': item.get('zf'),
                '涨停': item.get('zt'),
                '跌停': item.get('dt'),
                '昨结算': item.get('zjsj'),
                '标签': item.get('tag'),
                '交易所': item.get('uid', '').split('|')[0] if '|' in str(item.get('uid', '')) else '',
                '时间': now,
                '序号': self.intercept_count,
            }
            data_list.append(data)

        return data_list

    def _print_futures_data(self, data_list):
        """打印期货数据到控制台（首次拦截时展示前几条）"""
        print("\n" + "-" * 80)
        print("期货实时数据 (前5条):")
        print("-" * 80)

        for data in data_list[:5]:
            print(f"\n  {data['名称']:8s} {data['期货代码']:10s} | "
                  f"最新价: {data['最新价']:>10}  涨跌幅: {data.get('涨跌幅', '-'):>7}%  "
                  f"成交量: {data['成交量']:>10}  持仓量: {data['持仓量']:>10}")
            print(f"  {'':8s} {'':10s} | "
                  f"今开: {data['今开']:>10}  最高: {data['最高']:>10}  最低: {data['最低']:>10}  "
                  f"昨结算: {data.get('昨结算', '-'):>10}")

        if len(data_list) > 5:
            print(f"\n  ... 共 {len(data_list)} 条有效数据")
        print("-" * 80)

    async def start(self):
        """
        启动拦截器 - 访问页面，被动拦截页面自身发出的API响应
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()

            # 响应拦截器 - 监听页面自身发出的API请求
            async def handle_response(response):
                url = response.url
                # 只处理目标API的响应
                if self.API_PATTERN not in url:
                    return

                try:
                    text = await response.text()
                    if not text:
                        return

                    response_json = json.loads(text)
                    if 'list' not in response_json:
                        return

                    self.intercept_count += 1

                    # 解析数据
                    parsed_data = self._parse_response(response_json)

                    if parsed_data:
                        # 过滤非交易时间段的数据
                        now = datetime.now()
                        filtered_data = [
                            d for d in parsed_data
                            if self.is_in_trading_hours(d['名称'], now)
                        ]
                        self.filtered_count += len(parsed_data) - len(filtered_data)

                        if not filtered_data:
                            if not self.first_intercept_printed:
                                print(f"\n[{self.intercept_count}] 拦截 {len(parsed_data)} 条数据，"
                                      f"但当前时间 {now.strftime('%H:%M:%S')} 不在任何品种交易时段内，全部过滤")
                                self.first_intercept_printed = True
                            return

                        # 保存到数据库
                        inserted = self.db.insert_sina_futures_realtime(filtered_data)
                        self.total_records += inserted

                        # 首次拦截打印详情
                        if not self.first_intercept_printed:
                            total = response_json.get('total', 0)
                            filtered_out = len(parsed_data) - len(filtered_data)
                            print(f"\n[{self.intercept_count}] 拦截到API响应: {total} 条, "
                                  f"有效 {len(parsed_data)} 条, 交易时段内 {len(filtered_data)} 条 "
                                  f"(过滤非交易时段 {filtered_out} 条, 保存 {inserted} 条)")
                            self._print_futures_data(filtered_data)
                            self.first_intercept_printed = True
                        else:
                            print(f"[{self.intercept_count}] 拦截 {len(parsed_data)} 条, "
                                  f"交易时段 {len(filtered_data)} 条, "
                                  f"保存 {inserted} 条 | 累计: {self.total_records} 条 "
                                  f"| 过滤: {self.filtered_count}",
                                  end='\r')
                except Exception as e:
                    print(f"\n[错误] 响应处理失败: {e}")
                    import traceback
                    traceback.print_exc()

            # 注册响应监听
            page.on('response', handle_response)

            # 访问目标页面
            print(f"\n正在访问页面: {self.TARGET_URL}")
            try:
                await page.goto(self.TARGET_URL, wait_until='networkidle', timeout=30000)
            except Exception as e:
                print(f"页面加载超时（不影响数据拦截）: {e}")

            self.start_time = datetime.now()
            print("\n" + "=" * 80)
            print("页面加载完成，开始被动拦截页面实时数据...")
            print("按 Ctrl+C 停止监听")
            print("=" * 80)
            print(f"\n数据将实时保存到数据库 (sina_futures_realtime表)\n")

            try:
                # 持续等待，页面会自己定时请求API，我们被动拦截
                while self.continuous:
                    await asyncio.sleep(1)

                    # 每60秒显示统计
                    if self.intercept_count > 0 and self.intercept_count % 60 == 0:
                        elapsed = (datetime.now() - self.start_time).total_seconds()
                        print(f"\n--- 运行统计: 拦截 {self.intercept_count} 次 | "
                              f"时长 {elapsed:.0f}秒 | "
                              f"入库 {self.total_records} 条 ---\n")

            except KeyboardInterrupt:
                print("\n\n" + "=" * 80)
                print("用户手动停止监听")
                print("=" * 80)

            await browser.close()

            # 最终统计
            elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            print(f"\n最终统计:")
            print(f"  总拦截次数: {self.intercept_count}")
            print(f"  运行时长: {elapsed:.0f}秒 ({elapsed/60:.1f}分钟)")
            print(f"  入库总记录: {self.total_records}")
            print(f"  过滤非交易时段记录: {self.filtered_count}")
            if elapsed > 0 and self.intercept_count > 0:
                print(f"  拦截频率: {self.intercept_count/elapsed:.2f}次/秒")
            print("=" * 80)

            return self.total_records


def main():
    """
    主函数 - 启动东方财富期货实时数据拦截器
    """
    print("=" * 80)
    print("东方财富期货实时数据拦截器 - 持续监听模式")
    print("=" * 80)

    interceptor = EastMoneyFuturesInterceptor(
        headless=False,     # 显示浏览器窗口，便于观察
        continuous=True     # 持续监听模式
    )

    try:
        total = asyncio.run(interceptor.start())
        print(f"\n监听完成，共入库 {total} 条记录")
    except KeyboardInterrupt:
        print("\n用户手动停止程序")
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
