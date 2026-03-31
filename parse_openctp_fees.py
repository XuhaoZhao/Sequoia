#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenCTP期货合约手续费数据解析脚本
每天凌晨1点自动运行，解析 http://openctp.cn/fees.html
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import sys
import os

# 添加项目路径以导入 db_manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_manager import IndustryDataDB

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('openctp_parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class OpenCTPParser:
    """OpenCTP手续费数据解析器"""

    def __init__(self, url="http://openctp.cn/fees.html", db_path="industry_data.db"):
        self.url = url
        self.db = IndustryDataDB(db_path=db_path)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def fetch_html(self):
        """获取网页HTML内容"""
        try:
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'  # 根据实际编码调整
            return response.text
        except Exception as e:
            logging.error(f"获取网页失败: {e}")
            return None

    def parse_html(self, html_content):
        """
        解析HTML内容，提取期货合约数据

        Args:
            html_content: 网页HTML内容

        Returns:
            list: 包含合约数据的字典列表
        """
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, 'html.parser')

        # 定义字段名称（共37列）
        columns = [
            '交易所', '合约代码', '合约名称', '品种代码', '品种名称',
            '合约乘数', '最小跳动', '开仓费率', '开仓费用/手', '平仓费率',
            '平仓费用/手', '平今费率', '平今费用/手', '做多保证金率',
            '做多保证金/手', '做空保证金率', '做空保证金/手', '上日结算价',
            '上日收盘价', '最新价', '成交量', '持仓量', '1手开仓费用',
            '1手平仓费用', '1手平今费用', '做多1手保证金', '做空1手保证金',
            '1手市值', '1Tick平仓盈亏', '1Tick平仓净利', '2Tick平仓净利',
            '1Tick平仓收益率%', '2Tick平仓收益率%', '1Tick平今净利',
            '2Tick平今净利', '1Tick平今收益率%', '2Tick平今收益率%'
        ]

        # 查找fees_table表格
        fees_table = soup.select_one('#fees_table')
        if not fees_table:
            logging.error("未找到 #fees_table 表格")
            return []

        logging.info("成功找到 #fees_table 表格")

        # 获取tbody中的数据行
        tbody = fees_table.select_one('tbody')
        if not tbody:
            logging.error("未找到 tbody")
            return []

        rows = tbody.find_all('tr')
        logging.info(f"找到 {len(rows)} 条数据行")

        data_list = []

        # 解析每一行数据
        for row_idx, row in enumerate(rows):
            try:
                cells = row.find_all('td')

                if len(cells) != len(columns):
                    logging.warning(f"第 {row_idx + 1} 行: 单元格数量({len(cells)})与列数({len(columns)})不匹配")
                    continue

                # 提取单元格文本数据
                row_data = {}
                for col_name, cell in zip(columns, cells):
                    text_value = cell.get_text(strip=True)
                    row_data[col_name] = text_value

                # 检测是否主力合约（检查合约代码列是否有黄色高亮）
                # 合约代码在第2列（索引1）
                contract_code_cell = cells[1]
                is_main_contract = self._is_main_contract(contract_code_cell)
                row_data['是否主力合约'] = '是' if is_main_contract else '否'

                data_list.append(row_data)

            except Exception as e:
                logging.error(f"解析第 {row_idx + 1} 行时出错: {e}")
                continue

        return data_list

    def _is_main_contract(self, cell):
        """
        判断是否为主力合约（通过检查是否有黄色高亮）

        Args:
            cell: BeautifulSoup的td元素

        Returns:
            bool: 是否为主力合约
        """
        if not cell:
            return False

        # 检查style属性
        style = cell.get('style', '')
        if 'background-color:yellow' in style.lower() or 'background-color: yellow' in style.lower():
            return True

        return False

    def save_to_csv(self, data, filename=None):
        """
        保存数据到CSV文件

        Args:
            data: 要保存的数据
            filename: 文件名（可选，默认使用时间戳）
        """
        if not data:
            logging.warning("没有数据需要保存")
            return

        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'openctp_fees_{timestamp}.csv'

        try:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logging.info(f"数据已保存到: {filename}")
        except Exception as e:
            logging.error(f"保存CSV失败: {e}")

    def print_data(self, data):
        """
        打印数据到控制台

        Args:
            data: 解析后的数据列表
        """
        if not data:
            print("没有数据需要打印")
            return

        print("\n" + "=" * 100)
        print(f"共解析到 {len(data)} 条合约数据")
        print("=" * 100)

        # 打印表头
        headers = list(data[0].keys())
        print("\n字段列表:")
        for idx, header in enumerate(headers, 1):
            print(f"  {idx}. {header}")

        # 打印前10条和后5条数据预览
        print("\n" + "-" * 100)
        print("数据预览（前10条）:")
        print("-" * 100)

        preview_count = min(10, len(data))
        for i in range(preview_count):
            print(f"\n【第 {i+1} 条】")
            for key, value in data[i].items():
                print(f"  {key}: {value}")

        if len(data) > 10:
            print(f"\n... 省略中间 {len(data) - 15} 条数据 ...\n")

            print("数据预览（后5条）:")
            print("-" * 100)
            for i in range(len(data) - 5, len(data)):
                print(f"\n【第 {i+1} 条】")
                for key, value in data[i].items():
                    print(f"  {key}: {value}")

        print("\n" + "=" * 100)

    def run(self):
        """执行解析任务"""
        logging.info("=" * 50)
        logging.info("开始执行解析任务")
        logging.info("=" * 50)

        # 1. 获取网页内容
        html_content = self.fetch_html()
        if not html_content:
            return

        # 2. 解析HTML
        data = self.parse_html(html_content)

        # 3. 打印到控制台
        if data:
            self.print_data(data)

            # 4. 保存到数据库
            try:
                inserted_count = self.db.insert_futures_contracts(data)
                logging.info(f"✓ 成功保存 {inserted_count} 条期货合约数据到数据库")

                # 统计主力合约数量
                main_count = sum(1 for d in data if d.get('是否主力合约') == '是')
                logging.info(f"  - 其中主力合约: {main_count} 个")
                logging.info(f"  - 普通合约: {inserted_count - main_count} 个")

            except Exception as e:
                logging.error(f"✗ 保存到数据库失败: {e}")

            logging.info(f"✓ 成功解析 {len(data)} 条数据")
        else:
            logging.warning("未能解析到数据")

        logging.info("=" * 50)


def scheduled_job():
    """定时任务函数"""
    parser = OpenCTPParser()
    parser.run()


def main():
    """主函数 - 默认单次执行"""
    logging.info("模式: 单次执行")
    scheduled_job()


if __name__ == "__main__":
    main()
