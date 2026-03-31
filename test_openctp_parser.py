#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenCTP解析器测试脚本
使用示例HTML测试解析功能，不需要访问网页
"""

from parse_openctp_fees import OpenCTPParser

# 示例HTML数据（根据实际网页结构简化）
sample_html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>OpenCTP手续费</title>
</head>
<body>
    <table id="fees_table">
        <thead>
            <tr>
                <th onclick="sortTable(0)">交易所</th>
                <th onclick="sortTable(1)">合约代码</th>
                <th onclick="sortTable(2)">合约名称</th>
                <th onclick="sortTable(3)">品种代码</th>
                <th onclick="sortTable(4)">品种名称</th>
                <th onclick="sortTable(5)">合约乘数</th>
                <th onclick="sortTable(6)">最小跳动</th>
                <th onclick="sortTable(7)">开仓费率</th>
                <th onclick="sortTable(8)">开仓费用/手</th>
                <th onclick="sortTable(9)">平仓费率</th>
                <th onclick="sortTable(10)">平仓费用/手</th>
                <th onclick="sortTable(11)">平今费率</th>
                <th onclick="sortTable(12)">平今费用/手</th>
                <th onclick="sortTable(13)">做多保证金率</th>
                <th onclick="sortTable(14)">做多保证金/手</th>
                <th onclick="sortTable(15)">做空保证金率</th>
                <th onclick="sortTable(16)">做空保证金/手</th>
                <th onclick="sortTable(17)">上日结算价</th>
                <th onclick="sortTable(18)">上日收盘价</th>
                <th onclick="sortTable(19)">最新价</th>
                <th onclick="sortTable(20)">成交量</th>
                <th onclick="sortTable(21)">持仓量</th>
                <th onclick="sortTable(22)">1手开仓费用</th>
                <th onclick="sortTable(23)">1手平仓费用</th>
                <th onclick="sortTable(24)">1手平今费用</th>
                <th onclick="sortTable(25)">做多1手保证金</th>
                <th onclick="sortTable(26)">做空1手保证金</th>
                <th onclick="sortTable(27)">1手市值</th>
                <th onclick="sortTable(28)">1Tick平仓盈亏</th>
                <th onclick="sortTable(29)">1Tick平仓净利</th>
                <th onclick="sortTable(30)">2Tick平仓净利</th>
                <th onclick="sortTable(31)">1Tick平仓收益率%</th>
                <th onclick="sortTable(32)">2Tick平仓收益率%</th>
                <th onclick="sortTable(33)">1Tick平今净利</th>
                <th onclick="sortTable(34)">2Tick平今净利</th>
                <th onclick="sortTable(35)">1Tick平今收益率%</th>
                <th onclick="sortTable(36)">2Tick平今收益率%</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>CZCE</td>
                <td>AP604</td>
                <td>AP604</td>
                <td>AP</td>
                <td>苹果</td>
                <td>10</td>
                <td>1</td>
                <td>0.000001</td>
                <td>5.01</td>
                <td>0.000001</td>
                <td>5.01</td>
                <td>0.000001</td>
                <td style="background-color:yellow;">20.01</td>
                <td style="background-color:yellow;">0.2</td>
                <td>0</td>
                <td style="background-color:yellow;">0.2</td>
                <td>0</td>
                <td>9545</td>
                <td>9540</td>
                <td>9540</td>
                <td>0</td>
                <td>284</td>
                <td>5.09</td>
                <td>5.09</td>
                <td>20.09</td>
                <td>19080.0</td>
                <td>19080.0</td>
                <td>95400.0</td>
                <td>10.0</td>
                <td>-0.17</td>
                <td>9.83</td>
                <td>-0.001</td>
                <td>0.052</td>
                <td>-15.17</td>
                <td>-5.17</td>
                <td>-0.08</td>
                <td>-0.027</td>
            </tr>
            <tr>
                <td>SHFE</td>
                <td style="background-color:yellow;">cu2505</td>
                <td>cu2505</td>
                <td>CU</td>
                <td>铜</td>
                <td>5</td>
                <td>10</td>
                <td>0.0001</td>
                <td>12.51</td>
                <td>0.0001</td>
                <td>12.51</td>
                <td>0.0001</td>
                <td>12.51</td>
                <td>0.08</td>
                <td>0</td>
                <td>0.08</td>
                <td>0</td>
                <td>75760</td>
                <td>75690</td>
                <td>75880</td>
                <td>0</td>
                <td>123456</td>
                <td>25.29</td>
                <td>25.29</td>
                <td>25.29</td>
                <td>30240.0</td>
                <td>30240.0</td>
                <td>379400.0</td>
                <td>50.0</td>
                <td>24.71</td>
                <td>74.71</td>
                <td>0.097</td>
                <td>0.247</td>
                <td>24.71</td>
                <td>74.71</td>
                <td>0.097</td>
                <td>0.247</td>
            </tr>
            <tr>
                <td>DCE</td>
                <td>m2505</td>
                <td>m2505</td>
                <td>M</td>
                <td>豆粕</td>
                <td>10</td>
                <td>1</td>
                <td>0.0001</td>
                <td>3.01</td>
                <td>0.0001</td>
                <td>3.01</td>
                <td>0.0001</td>
                <td>3.01</td>
                <td>0.07</td>
                <td>0</td>
                <td>0.07</td>
                <td>0</td>
                <td>2856</td>
                <td>2850</td>
                <td>2889</td>
                <td>0</td>
                <td>567890</td>
                <td>5.79</td>
                <td>5.79</td>
                <td>5.79</td>
                <td>2012.3</td>
                <td>2012.3</td>
                <td>28890.0</td>
                <td>10.0</td>
                <td>4.21</td>
                <td>14.21</td>
                <td>0.245</td>
                <td>0.706</td>
                <td>4.21</td>
                <td>14.21</td>
                <td>0.245</td>
                <td>0.706</td>
            </tr>
        </tbody>
    </table>
</body>
</html>
"""

def test_parser():
    """测试解析器"""
    print("=" * 100)
    print("OpenCTP解析器测试")
    print("=" * 100)
    print("\n使用示例HTML数据进行测试...\n")

    # 创建解析器实例
    parser = OpenCTPParser()

    # 解析HTML
    print("开始解析示例HTML...")
    data = parser.parse_html(sample_html)

    if data:
        print(f"\n✓ 成功解析 {len(data)} 条数据\n")

        # 打印数据
        parser.print_data(data)

        # 保存到测试文件
        parser.save_to_csv(data, "test_openctp_fees.csv")
        print(f"\n✓ 测试数据已保存到: test_openctp_fees.csv")

        # 验证主力合约识别
        main_contracts = [d for d in data if d['是否主力合约'] == '是']
        print(f"\n✓ 识别到 {len(main_contracts)} 个主力合约:")
        for contract in main_contracts:
            print(f"  - {contract['合约代码']} ({contract['品种名称']})")

    else:
        print("\n✗ 解析失败")

    print("\n" + "=" * 100)
    print("测试完成")
    print("=" * 100)

if __name__ == "__main__":
    test_parser()
