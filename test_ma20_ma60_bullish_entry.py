import pandas as pd
from datetime import datetime
from db_manager import IndustryDataDB
import warnings
warnings.filterwarnings('ignore')


def load_stock_list(csv_path):
    """从CSV文件加载股票列表"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype={'SECURITY_CODE': str})

    # 确保股票代码是6位数字格式，补前导0
    def format_code(code):
        """将股票代码格式化为6位数字，补前导0"""
        code_str = str(code).strip()
        # 移除可能的前缀（如SH, SZ等）
        if code_str[:2].upper() in ['SH', 'SZ']:
            code_str = code_str[2:]
        # 补前导0到6位
        return code_str.zfill(6)

    df['SECURITY_CODE'] = df['SECURITY_CODE'].apply(format_code)

    # 提取股票代码和名称
    stocks = df[['SECURITY_CODE', 'SECURITY_SHORT_NAME']].drop_duplicates()
    return stocks


def calculate_ma(df, period):
    """计算移动平均线"""
    return df['close_price'].rolling(window=period).mean()


def calculate_slope(ma_series):
    """计算均线斜率（使用简单的差分）"""
    return ma_series.diff()


def count_entry_points(db, stock_code, stock_name):
    """
    统计单只股票的入场点数量

    入场条件：
    1. MA20 > MA60（多头排列）
    2. MA20斜率向上（MA20_Slope > 0）
    3. 前一日是阴线（收盘价 < 开盘价）
    4. 当日是阳线（收盘价 > 开盘价）
    5. 当日开盘价 < 前一日收盘价
    6. 前一日收盘价 < 当日收盘价 < 前一日开盘价（部分收复但未完全收复）

    Args:
        db: 数据库管理器
        stock_code: 股票代码
        stock_name: 股票名称

    Returns:
        包含入场点统计信息的字典，如果没有入场点返回None
    """
    # 从数据库读取日K数据
    df = db.query_kline_data(period='1d', code=stock_code)

    if df.empty or len(df) < 60:
        return None

    # 按日期排序
    df = df.sort_values('datetime').reset_index(drop=True)

    # 计算均线
    df['MA20'] = calculate_ma(df, 20)
    df['MA60'] = calculate_ma(df, 60)

    # 计算20日均线斜率
    df['MA20_Slope'] = calculate_slope(df['MA20'])

    # 计算K线类型（阴线/阳线）
    df['is_red'] = df['close_price'] > df['open_price']  # 阳线
    df['is_green'] = df['close_price'] < df['open_price']  # 阴线

    # 计算前一日的指标
    df['prev_close'] = df['close_price'].shift(1)
    df['prev_open'] = df['open_price'].shift(1)  # 前一日开盘价
    df['prev_is_green'] = df['is_green'].shift(1)  # 前一日是否为阴线

    # 初始化统计列表
    entry_points = []

    # 从第61天开始检查（确保均线有足够数据）
    for i in range(60, len(df)):
        current_row = df.iloc[i]

        # 检查数据有效性
        if pd.isna(current_row['MA20']) or pd.isna(current_row['MA60']) or pd.isna(current_row['MA20_Slope']):
            continue
        if pd.isna(current_row['prev_close']) or pd.isna(current_row['prev_open']) or pd.isna(current_row['prev_is_green']):
            continue

        # 入场条件检查：
        # 1. MA20 > MA60（多头排列）
        condition1 = current_row['MA20'] > current_row['MA60']

        # 2. MA20斜率向上
        condition2 = current_row['MA20_Slope'] > 0

        # 3. 前一日是阴线
        condition3 = current_row['prev_is_green']

        # 4. 当日是阳线
        condition4 = current_row['is_red']

        # 5. 当日开盘价 < 前一日收盘价
        condition5 = current_row['open_price'] < current_row['prev_close']

        # 6. 前一日收盘价 < 当日收盘价 < 前一日开盘价（部分收复但未完全收复）
        condition6 = (current_row['close_price'] > current_row['prev_close'] and
                     current_row['close_price'] < current_row['prev_open'])

        # 所有条件都满足
        if condition1 and condition2 and condition3 and condition4 and condition5 and condition6:
            entry_points.append({
                'date': current_row['datetime'],
                'open_price': current_row['open_price'],
                'close_price': current_row['close_price'],
                'prev_close': current_row['prev_close'],
                'prev_open': current_row['prev_open'],
                'ma20': current_row['MA20'],
                'ma60': current_row['MA60'],
                'ma20_slope': current_row['MA20_Slope'],
                'price_change_pct': (current_row['close_price'] - current_row['prev_close']) / current_row['prev_close'] * 100
            })

    if not entry_points:
        return None

    # 统计信息
    entry_count = len(entry_points)
    avg_price_change = sum([ep['price_change_pct'] for ep in entry_points]) / entry_count

    return {
        'code': stock_code,
        'name': stock_name,
        'entry_count': entry_count,
        'avg_price_change_pct': avg_price_change,
        'entry_points': entry_points
    }


def main():
    """主函数"""
    print("=" * 80)
    print("MA20>MA60 多头排列 + MA20斜率向上 + 阴后阳部分收复 入场点统计系统")
    print("=" * 80)
    print("\n入场条件：")
    print("  1. MA20 > MA60（多头排列）")
    print("  2. MA20斜率向上（上升趋势）")
    print("  3. 前一日是阴线（收盘价 < 开盘价）")
    print("  4. 当日是阳线（收盘价 > 开盘价）")
    print("  5. 当日开盘价 < 前一日收盘价（跳空低开或平开）")
    print("  6. 前一日收盘价 < 当日收盘价 < 前一日开盘价")
    print("     （收复前一日收盘价，但未突破前一日开盘价）")
    print("\n这种形态通常代表：")
    print("  - 多头上升趋势中，经过一日回调后")
    print("  - 次日低开但收阳线，收复了前一日收盘价")
    print("  - 但还未突破前一日开盘价，属于部分收复")
    print("  - 显示企稳迹象，可能是一个潜在的入场机会")
    print("=" * 80)

    # 初始化数据库连接
    db = IndustryDataDB()

    # 加载股票列表
    today = datetime.now().strftime('%Y-%m-%d')
    csv_path = f"data/stock_data_2026-02-25.csv"

    # 如果今天的文件不存在，使用默认文件
    try:
        print(f"\n正在从 {csv_path} 加载股票列表...")
        stocks = load_stock_list(csv_path)
    except FileNotFoundError:
        csv_path = "data/stock_data_2026-01-25.csv"
        print(f"今天的文件不存在，使用 {csv_path}...")
        stocks = load_stock_list(csv_path)

    print(f"共加载 {len(stocks)} 只股票")

    # 统计每只股票的入场点
    print("\n开始统计入场点...")
    print("-" * 80)

    all_results = []
    success_count = 0
    total_entry_points = 0

    for _, row in stocks.iterrows():
        stock_code = row['SECURITY_CODE']
        stock_name = row['SECURITY_SHORT_NAME']

        result = count_entry_points(db, stock_code, stock_name)

        if result:
            all_results.append(result)
            success_count += 1
            total_entry_points += result['entry_count']

            # 打印进度
            if success_count % 20 == 0:
                print(f"已完成 {success_count} 只股票的统计...")

    print(f"\n统计完成！成功统计 {success_count} 只股票")
    print(f"共找到 {total_entry_points} 个入场点")

    if not all_results:
        print("没有找到入场点，程序退出")
        return

    # 转换为DataFrame
    results_df = pd.DataFrame(all_results)

    # 统计分析
    print("\n" + "=" * 80)
    print("统计分析结果")
    print("=" * 80)

    # 总体统计
    total_stocks = len(results_df)
    total_entries = results_df['entry_count'].sum()
    avg_entries_per_stock = total_entries / total_stocks
    avg_price_change = results_df['avg_price_change_pct'].mean()

    print(f"\n总体统计:")
    print(f"  - 有入场点的股票数: {total_stocks}")
    print(f"  - 总入场点数量: {total_entries}")
    print(f"  - 平均每只股票入场点数: {avg_entries_per_stock:.2f}")
    print(f"  - 平均当日涨幅: {avg_price_change:.2f}%")

    # 入场点数量分布
    print(f"\n入场点数量分布:")
    print(f"  - 只有1个入场点的股票: {len(results_df[results_df['entry_count'] == 1])}")
    print(f"  - 有2-3个入场点的股票: {len(results_df[(results_df['entry_count'] >= 2) & (results_df['entry_count'] <= 3)])}")
    print(f"  - 有4-5个入场点的股票: {len(results_df[(results_df['entry_count'] >= 4) & (results_df['entry_count'] <= 5)])}")
    print(f"  - 有6个及以上入场点的股票: {len(results_df[results_df['entry_count'] >= 6])}")

    # 显示入场点最多的20只股票
    print("\n" + "-" * 80)
    print("入场点最多的20只股票:")
    print("-" * 80)
    top20 = results_df.nlargest(20, 'entry_count')[['code', 'name', 'entry_count', 'avg_price_change_pct']]
    print(top20.to_string(index=False))

    # 显示平均涨幅最大的20只股票
    print("\n" + "-" * 80)
    print("平均当日涨幅最大的20只股票:")
    print("-" * 80)
    top20_gain = results_df.nlargest(20, 'avg_price_change_pct')[['code', 'name', 'entry_count', 'avg_price_change_pct']]
    print(top20_gain.to_string(index=False))

    # 保存详细结果到CSV
    output_file = 'ma20_ma60_bullish_entry_results.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n统计结果已保存到 {output_file}")

    # 收集所有入场点详情并保存到Excel
    print("\n正在收集所有入场点详情...")
    all_entry_details = []

    for result in all_results:
        if 'entry_points' in result and result['entry_points']:
            for entry in result['entry_points']:
                all_entry_details.append({
                    '股票代码': result['code'],
                    '股票名称': result['name'],
                    '日期': entry['date'],
                    '开盘价': entry['open_price'],
                    '收盘价': entry['close_price'],
                    '前一日收盘价': entry['prev_close'],
                    '前一日开盘价': entry['prev_open'],
                    'MA20': entry['ma20'],
                    'MA60': entry['ma60'],
                    'MA20斜率': entry['ma20_slope'],
                    '当日涨幅(%)': entry['price_change_pct']
                })

    if all_entry_details:
        entry_details_df = pd.DataFrame(all_entry_details)
        excel_file = 'ma20_ma60_bullish_entry_details.xlsx'
        entry_details_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"入场点详情已保存到 {excel_file}")
        print(f"共 {len(all_entry_details)} 个入场点")
    else:
        print("没有入场点详情")

    print("\n" + "=" * 80)
    print("统计完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()


