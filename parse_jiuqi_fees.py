#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
九期网期货合约手续费数据解析脚本
从网页获取实时数据，保存到数据库

数据来源: https://www.9qihuo.com/qihuoshouxufei?zhuli=true
"""

import asyncio
import logging
import sys
import os
import re
import json
import aiohttp
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("错误: 未安装playwright库")
    print("请运行: pip install playwright")
    print("然后运行: playwright install chromium")
    sys.exit(1)

# 添加项目路径以导入 db_manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_manager import IndustryDataDB

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jiuqi_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class JiuqiWebParser:
    """九期网手续费数据解析器 - 基于网页HTML解析"""

    def __init__(self, url="https://www.9qihuo.com/qihuoshouxufei?zhuli=true", db_path="industry_data.db"):
        self.url = url
        self.db = IndustryDataDB(db_path=db_path)

        # 交易所映射（根据数据源中的交易所名称映射到标准代码）
        self.exchange_mapping = {
            '上海期货交易所': 'SHFE',
            '大连商品交易所': 'DCE',
            '郑州商品交易所': 'CZCE',
            '广州期货交易所': 'GFE',
            '中国金融期货交易所': 'CFFEX',
            '上海国际能源交易中心': 'INE'
        }

    async def fetch_html_with_playwright(self) -> Optional[str]:
        """
        使用Playwright获取网页HTML内容

        Returns:
            str: 网页HTML内容，失败返回None
        """
        try:
            logging.info("启动浏览器...")
            async with async_playwright() as p:
                # 启动浏览器（使用chromium）
                browser = await p.chromium.launch(headless=True)

                # 创建新页面
                page = await browser.new_page()

                # 设置超时时间
                page.set_default_timeout(30000)

                # 访问网页
                logging.info(f"访问网页: {self.url}")
                await page.goto(self.url, wait_until="networkidle")

                # 等待页面完全加载
                await page.wait_for_load_state("networkidle")

                # 获取页面HTML
                html_content = await page.content()

                # 关闭浏览器
                await browser.close()

                logging.info("成功获取网页内容")
                return html_content

        except Exception as e:
            logging.error(f"使用Playwright获取网页失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def parse_html(self, html_content: str) -> List[Dict]:
        """
        解析HTML内容，提取期货合约数据

        Args:
            html_content: 网页HTML内容

        Returns:
            list: 包含合约数据的字典列表
        """
        if not html_content:
            logging.error("HTML内容为空")
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        data_list = []

        # 查找所有表格
        tables = soup.find_all('table')
        if not tables:
            logging.error("未找到表格")
            return []

        logging.info(f"找到 {len(tables)} 个表格")

        # 解析每个表格（可能有多个交易所的表格）
        for table_idx, table in enumerate(tables):
            try:
                # 获取所有行
                rows = table.find_all('tr')

                # 将表格按交易所分割成多个子表格
                # 交易所分隔行的特征：只有1列，且包含"交易所"
                sub_tables = []
                current_table_rows = []
                current_exchange = None

                for row_idx, row in enumerate(rows):
                    cells = row.find_all(['th', 'td'])

                    # 检查是否是交易所分隔行
                    if len(cells) == 1:
                        text = cells[0].get_text(strip=True)
                        if '交易所' in text:
                            # 保存上一个子表格
                            if current_table_rows and current_exchange:
                                sub_tables.append({
                                    'exchange_name': current_exchange,
                                    'rows': current_table_rows
                                })
                            # 开始新的子表格
                            current_exchange = text
                            current_table_rows = []
                            continue

                    # 添加到当前子表格
                    current_table_rows.append(row)

                # 保存最后一个子表格
                if current_table_rows and current_exchange:
                    sub_tables.append({
                        'exchange_name': current_exchange,
                        'rows': current_table_rows
                    })

                logging.info(f"表格 {table_idx + 1}: 分割成 {len(sub_tables)} 个子表格")

                # 解析每个子表格
                for sub_table_idx, sub_table in enumerate(sub_tables):
                    exchange_name = sub_table['exchange_name']
                    sub_rows = sub_table['rows']

                    # 映射到标准交易所代码
                    exchange_code = self.exchange_mapping.get(exchange_name, exchange_name)
                    logging.info(f"  子表格 {sub_table_idx + 1}: {exchange_name} ({exchange_code})")
                    logging.info(f"  子表格 {sub_table_idx + 1}: 总行数 {len(sub_rows)} (包括表头)")

                    # 跳过表头行（前两行都是表头）
                    if len(sub_rows) < 4:  # 至少需要交易所名称 + 主表头 + 子表头 + 数据行
                        logging.warning(f"  子表格 {sub_table_idx + 1} 行数不足，跳过")
                        continue

                    # 从第3行开始是数据（索引2），因为只有2行表头（主表头+子表头）
                    data_rows = sub_rows[2:]
                    logging.info(f"  子表格 {sub_table_idx + 1}: 去除表头后找到 {len(data_rows)} 条数据行")

                    # 解析每一行数据
                    for row_idx, row in enumerate(data_rows):
                        try:
                            cells = row.find_all('td')

                            if len(cells) < 13:
                                # 记录跳过的行内容，方便调试
                                row_text = row.get_text(strip=True)[:50]  # 只取前50个字符
                                logging.warning(f"  子表格 {sub_table_idx + 1} 第 {row_idx + 1} 行: 单元格数量({len(cells)})不足13个，内容: {row_text}")
                                continue

                            # 提取数据（新数据源的13列）
                            contract_info = cells[0].get_text(strip=True)  # 合约品种，如 "白银2606 (ag2606)"
                            current_price = self._safe_float(cells[1].get_text(strip=True))  # 现价
                            price_limit = cells[2].get_text(strip=True)  # 涨/跌停板
                            buy_margin_rate = self._safe_float(cells[3].get_text(strip=True).rstrip('%'))  # 买开%（去除%）
                            sell_margin_rate = self._safe_float(cells[4].get_text(strip=True).rstrip('%'))  # 卖开%（去除%）
                            margin_per_lot = self._safe_float(cells[5].get_text(strip=True).rstrip('元'))  # 保证金/每手（去除元）
                            open_fee = cells[6].get_text(strip=True)  # 开仓手续费
                            close_yesterday_fee = cells[7].get_text(strip=True)  # 平昨手续费
                            close_today_fee = cells[8].get_text(strip=True)  # 平今手续费
                            tick_profit = self._safe_float(cells[9].get_text(strip=True))  # 每跳毛利/元
                            total_fee = self._safe_float(cells[10].get_text(strip=True).rstrip('元'))  # 手续费(开+平)（去除元）
                            tick_net_profit = self._safe_float(cells[11].get_text(strip=True))  # 每跳净利/元
                            remark = cells[12].get_text(strip=True) if len(cells) > 12 else ""  # 备注

                            # 解析合约品种信息
                            # 格式: "白银2606 (ag2606)" 或 "螺纹钢2605 (rb2605)"
                            contract_code, variety_code, variety_name_cn = self._parse_contract_info(contract_info)

                            # 判断是否主力合约
                            is_main_contract = '是' if '主力合约' in remark else '否'

                            # 解析手续费（提取每手手续费金额，支持"40元"或"2/万分之(4.8元)"格式）
                            open_fee_per_lot = self._parse_fee(open_fee)
                            close_yesterday_per_lot = self._parse_fee(close_yesterday_fee)
                            close_today_per_lot = self._parse_fee(close_today_fee)

                            # 构建数据库记录（只存储每手手续费金额，不存储费率）
                            record = {
                                '合约代码': contract_code,
                                '交易所': exchange_code,
                                '品种代码': variety_code,
                                '品种名称': variety_name_cn,
                                '最新价': current_price,
                                '涨跌停板': price_limit,
                                '做多保证金率': buy_margin_rate,
                                '做空保证金率': sell_margin_rate,
                                '做多1手保证金': margin_per_lot,
                                '做空1手保证金': margin_per_lot,  # 做空和做多保证金相同
                                '开仓手续费原始文本': open_fee,
                                '1手开仓费用': open_fee_per_lot,
                                '平昨手续费原始文本': close_yesterday_fee,
                                '1手平仓费用': close_yesterday_per_lot,
                                '平今手续费原始文本': close_today_fee,
                                '1手平今费用': close_today_per_lot,
                                '1Tick平仓盈亏': tick_profit,
                                '手续费开平': total_fee,
                                '1Tick平仓净利': tick_net_profit,
                                '是否主力合约': is_main_contract,
                                '备注': remark
                            }

                            data_list.append(record)

                        except Exception as e:
                            logging.error(f"解析子表格 {sub_table_idx + 1} 第 {row_idx + 1} 行时出错: {e}")
                            continue

            except Exception as e:
                logging.error(f"解析表格 {table_idx + 1} 时出错: {e}")
                import traceback
                traceback.print_exc()
                continue

        logging.info(f"总共从HTML解析了 {len(data_list)} 条数据")
        return data_list

    def _parse_contract_info(self, contract_info: str) -> tuple:
        """
        解析合约信息

        Args:
            contract_info: 合约信息字符串，如 "白银2606 (ag2606)"

        Returns:
            tuple: (contract_code, variety_code, variety_name_cn)
        """
        # 使用正则表达式提取合约代码和品种信息
        # 格式: "白银2606 (ag2606)" 或 "螺纹钢2605 (rb2605)"
        match = re.search(r'(.+?)\s*\((\w+)\)', contract_info)
        if match:
            variety_name_cn = match.group(1)  # 品种中文名，如 "白银"
            contract_code = match.group(2)     # 合约代码，如 "ag2606"
            # 从合约代码中提取品种代码（去掉数字月份）
            variety_code = re.sub(r'\d+$', '', contract_code)  # 如 "ag2606" -> "ag"
            return contract_code, variety_code, variety_name_cn
        else:
            # 如果无法解析，尝试直接使用合约信息
            logging.warning(f"无法解析合约信息: {contract_info}")
            return contract_info, contract_info[:2].lower(), contract_info[:2]

    def _parse_fee(self, fee_str: str) -> float:
        """
        解析手续费字符串，提取每手手续费金额（元）

        支持两种格式：
        1. "40元" - 固定金额
        2. "2/万分之(4.8元)" - 按比例+金额

        Args:
            fee_str: 手续费字符串，如 "0.5/万分之(13.5元)" 或 "3元" 或 "0元"

        Returns:
            float: 每手手续费金额（元）
        """
        if not fee_str or fee_str == '0元' or fee_str == '0/万分之(0元)':
            return 0.0

        # 优先提取括号内的金额（如 "2/万分之(4.8元)" 中的 4.8）
        per_lot_match = re.search(r'\(([\d.]+)元\)', fee_str)
        if per_lot_match:
            return self._safe_float(per_lot_match.group(1)) or 0.0

        # 如果没有括号，直接提取金额（如 "40元" 中的 40）
        direct_match = re.search(r'([\d.]+)元', fee_str)
        if direct_match:
            return self._safe_float(direct_match.group(1)) or 0.0

        # 无法解析，返回0
        return 0.0

    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        if value is None or value == '' or value == '-':
            return None
        try:
            # 去除可能的逗号分隔符
            if isinstance(value, str):
                value = value.replace(',', '')
            return float(value)
        except (ValueError, TypeError):
            return None

    async def fetch_turnover_amount_from_eastmoney(self) -> Dict[str, float]:
        """
        从东方财富API获取期货合约成交额数据

        Returns:
            dict: 成交额字典，格式为 {contract_code: turnover_amount}
                  例如: {"ag2606": 1234567890.0, "au2606": 9876543210.0}
        """
        url = "https://futsseapi.eastmoney.com/list/trans/block/risk/mk0830"
        params = {
            "orderBy": "",
            "sort": "",
            "pageSize": 999,
            "pageIndex": 0,
            "specificContract": "true",
            "platform": "zbPC",
            "field": "name,p,zdf,vol,ccl,rz,tjd,cje,zde,o,h,l,zf,zjsj,zt,dt,dm,sc,tag,uid,zsjd"
        }

        try:
            logging.info("正在从东方财富API获取成交额数据...")
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logging.error(f"东方财富API返回状态码: {response.status}")
                        return {}

                    # 读取响应文本并手动解析JSON（因为API返回的是text/javascript而不是application/json）
                    text = await response.text()
                    data = json.loads(text)

                    if 'list' not in data:
                        logging.error("东方财富API返回数据格式错误")
                        return {}

                    turnover_dict = {}
                    for item in data['list']:
                        contract_code = item.get('dm', '')  # 合约代码
                        turnover_amount = item.get('cje', 0)  # 成交额（单位：元）

                        # 过滤无效数据（成交额为0、None或字符串"-"）
                        if contract_code and turnover_amount and turnover_amount != '-':
                            try:
                                turnover_dict[contract_code] = float(turnover_amount)
                            except (ValueError, TypeError):
                                # 转换失败，跳过此合约
                                continue

                    logging.info(f"✓ 成功获取 {len(turnover_dict)} 个合约的成交额数据")
                    return turnover_dict

        except Exception as e:
            logging.error(f"获取东方财富成交额数据失败: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def print_data_summary(self, data: List[Dict]):
        """
        打印数据摘要

        Args:
            data: 解析后的数据列表
        """
        if not data:
            print("没有数据需要打印")
            return

        print("\n" + "=" * 100)
        print(f"共解析到 {len(data)} 条合约数据")
        print("=" * 100)

        # 统计主力合约数量
        main_count = sum(1 for d in data if d.get('是否主力合约') == '是')
        print(f"主力合约: {main_count} 个")
        print(f"普通合约: {len(data) - main_count} 个")

        # 按交易所统计
        exchange_stats = {}
        for record in data:
            exchange = record.get('交易所', 'Unknown')
            exchange_stats[exchange] = exchange_stats.get(exchange, 0) + 1

        print("\n按交易所统计:")
        for exchange, count in sorted(exchange_stats.items()):
            print(f"  {exchange}: {count} 个合约")

        # 打印前3条数据预览
        print("\n" + "-" * 100)
        print("数据预览（前3条）:")
        print("-" * 100)

        preview_count = min(3, len(data))
        for i in range(preview_count):
            print(f"\n【第 {i+1} 条】")
            for key, value in data[i].items():
                print(f"  {key}: {value}")

        print("\n" + "=" * 100)

    async def run(self):
        """
        执行解析任务
        """
        logging.info("=" * 50)
        logging.info("开始执行九期网数据解析任务")
        logging.info("=" * 50)

        # 1. 使用Playwright获取网页内容
        html_content = await self.fetch_html_with_playwright()

        if not html_content:
            logging.error("获取网页内容失败，任务终止")
            return

        # 2. 解析HTML数据
        logging.info("开始解析HTML数据...")
        data = self.parse_html(html_content)

        if not data:
            logging.warning("未能解析到任何数据")
            return

        # 3. 打印数据摘要
        self.print_data_summary(data)

        # 4. 保存到数据库
        try:
            logging.info("开始保存数据到数据库...")
            inserted_count = self.db.insert_futures_contracts(data)
            logging.info(f"✓ 成功保存 {inserted_count} 条期货合约数据到数据库")

            # 统计主力合约数量
            main_count = sum(1 for d in data if d.get('是否主力合约') == '是')
            logging.info(f"  - 其中主力合约: {main_count} 个")
            logging.info(f"  - 普通合约: {inserted_count - main_count} 个")

        except Exception as e:
            logging.error(f"✗ 保存到数据库失败: {e}")
            import traceback
            traceback.print_exc()

        # 5. 从东方财富API获取成交额数据并更新
        try:
            logging.info("")
            turnover_data = await self.fetch_turnover_amount_from_eastmoney()

            if turnover_data:
                logging.info("开始更新成交额到数据库...")
                updated_count = self.db.update_turnover_amount(turnover_data)
                logging.info(f"✓ 成功更新 {updated_count} 条合约的成交额数据")

                # 显示一些成交额示例
                if updated_count > 0:
                    sample_contracts = list(turnover_data.items())[:5]
                    logging.info("  成交额示例（前5个）:")
                    for code, amount in sample_contracts:
                        amount_yi = amount / 100000000  # 转换为亿元
                        logging.info(f"    {code}: {amount_yi:.2f}亿元")
        except Exception as e:
            logging.error(f"✗ 更新成交额失败: {e}")
            import traceback
            traceback.print_exc()

        logging.info("=" * 50)
        logging.info("任务执行完成")
        logging.info("=" * 50)


async def scheduled_job():
    """定时任务函数"""
    parser = JiuqiWebParser()
    await parser.run()


def main():
    """主函数 - 默认单次执行"""
    logging.info("模式: 单次执行")
    # 运行异步任务
    asyncio.run(scheduled_job())


if __name__ == "__main__":
    main()
