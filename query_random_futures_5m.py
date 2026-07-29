"""
随机选择20个期货合约，查询最后两天的5分钟K线数据

功能：
1. 从数据库中获取所有期货品种
2. 随机选择20个合约品种
3. 从 futures_5m_2026_03 表中查询最后两天的5分钟K线数据
4. 打印到控制台
"""

import sys
import os
from datetime import datetime
import random

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_manager import IndustryDataDB


def get_random_contracts(db, num_contracts=20):
    """
    从数据库中随机获取指定数量的合约

    Args:
        db: IndustryDataDB实例
        num_contracts: 需要获取的合约数量

    Returns:
        合约代码列表
    """
    # 查询所有期货合约
    df = db.query_futures_contracts()

    if df.empty:
        print("数据库中没有期货合约数据")
        return []

    # 获取所有合约代码
    all_contracts = df['contract_code'].unique().tolist()

    print(f"数据库中共有 {len(all_contracts)} 个合约")

    # 随机选择指定数量的合约
    if len(all_contracts) <= num_contracts:
        selected_contracts = all_contracts
    else:
        selected_contracts = random.sample(all_contracts, num_contracts)

    return selected_contracts


def query_and_print_last_2days_data(db, contract_codes):
    """
    查询并打印最后两天的5分钟K线数据

    Args:
        db: IndustryDataDB实例
        contract_codes: 合约代码列表
    """
    # 查询futures_5m_2026_03表的最后两天数据（2026年3月的最后两天）
    start_date_str = '2026-03-30'
    end_date_str = '2026-03-31'

    # 创建输出文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'data/futures_5m_random20_{timestamp}.txt'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    print(f"\n查询时间范围: {start_date_str} 至 {end_date_str}")
    print(f"数据来源表: futures_5m_2026_03")
    print(f"选择的合约数量: {len(contract_codes)}")
    print(f"输出文件: {output_file}")
    print("=" * 100)

    # 打开文件准备写入
    with open(output_file, 'w', encoding='utf-8') as f:
        # 写入文件头
        f.write("=" * 100 + "\n")
        f.write(f"随机选择20个期货合约查询5分钟K线数据\n")
        f.write(f"查询时间范围: {start_date_str} 至 {end_date_str}\n")
        f.write(f"数据来源表: futures_5m_2026_03\n")
        f.write(f"选择的合约数量: {len(contract_codes)}\n")
        f.write("=" * 100 + "\n\n")

        # 遍历每个合约
        for i, contract_code in enumerate(contract_codes, 1):
            print(f"\n【{i}/{len(contract_codes)}】正在处理合约: {contract_code}...")

            # 写入文件
            f.write("\n" + "=" * 100 + "\n")
            f.write(f"【{i}/{len(contract_codes)}】合约: {contract_code}\n")
            f.write("=" * 100 + "\n")

            # 查询该合约的5分钟数据
            df = db.query_futures_5m_data(
                contract_code=contract_code,
                start_date=start_date_str,
                end_date=end_date_str
            )

            if df.empty:
                msg = f"  ⚠ 该合约在指定时间范围内没有数据\n"
                print(msg, end='')
                f.write(msg + "\n")
                continue

            # 打印数据统计
            stats = f"  数据条数: {len(df)}\n  时间范围: {df['datetime'].min()} 至 {df['datetime'].max()}\n\n"
            print(stats, end='')
            f.write(stats)
            f.write("-" * 100 + "\n")

            # 打印所有数据（一条不落）
            f.write(f"{'序号':<6}{'时间':<20}{'开盘价':<12}{'最高价':<12}{'最低价':<12}{'收盘价':<12}{'成交量':<12}\n")
            f.write("-" * 100 + "\n")

            for idx, (_, row) in enumerate(df.iterrows(), 1):
                line = f"{idx:<6}{row['datetime']:<20}{row['open_price']:<12.2f}{row['high_price']:<12.2f}{row['low_price']:<12.2f}{row['close_price']:<12.2f}{int(row['volume']):<12}\n"
                f.write(line)

                # 每处理50条打印一次进度
                if idx % 50 == 0:
                    print(f"    已处理 {idx}/{len(df)} 条数据...")

            f.write("\n" + "-" * 100 + "\n")
            print(f"  ✓ 完成！共处理 {len(df)} 条数据")

        # 写入文件尾
        f.write("\n" + "=" * 100 + "\n")
        f.write("查询完成！\n")
        f.write("=" * 100 + "\n")

    print(f"\n✓ 所有数据已保存到: {output_file}")


def main():
    """主函数"""
    print("=" * 100)
    print("随机选择20个期货合约查询5分钟K线数据")
    print("=" * 100)

    # 初始化数据库连接
    db = IndustryDataDB()

    # 随机选择20个合约
    print("\n正在随机选择合约...")
    selected_contracts = get_random_contracts(db, num_contracts=20)

    if not selected_contracts:
        print("未找到可用的合约")
        return

    print(f"\n已选择的合约:")
    for i, contract in enumerate(selected_contracts, 1):
        print(f"  {i}. {contract}")

    # 查询并打印数据
    query_and_print_last_2days_data(db, selected_contracts)

    print("\n" + "=" * 100)
    print("查询完成！")
    print("=" * 100)


if __name__ == "__main__":
    main()
