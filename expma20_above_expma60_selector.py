import pandas as pd
from datetime import datetime
from db_manager import IndustryDataDB
import warnings
warnings.filterwarnings('ignore')


def load_instrument_list(csv_path, code_col, name_col):
    """
    从CSV文件加载股票/ETF列表

    Args:
        csv_path: CSV文件路径
        code_col: 代码列名（如 'SECURITY_CODE' 或 'ETF_CODE'）
        name_col: 名称列名（如 'SECURITY_SHORT_NAME' 或 'ETF_NAME'）

    Returns:
        包含代码和名称的DataFrame
    """
    try:
        # 读取CSV，将代码列作为字符串类型以保留前导0
        df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype={code_col: str})

        # 确保代码是6位数字格式，补前导0
        def format_code(code):
            """将代码格式化为6位数字，补前导0"""
            code_str = str(code).strip()
            # 移除可能的前缀（如SH, SZ等）
            if code_str[:2].upper() in ['SH', 'SZ']:
                code_str = code_str[2:]
            # 补前导0到6位
            return code_str.zfill(6)

        df[code_col] = df[code_col].apply(format_code)

        # 提取代码和名称
        instruments = df[[code_col, name_col]].drop_duplicates()

        # 重命名列为统一格式
        instruments = instruments.rename(columns={
            code_col: 'SECURITY_CODE',
            name_col: 'SECURITY_SHORT_NAME'
        })

        return instruments
    except Exception as e:
        print(f"加载数据失败 ({csv_path}): {e}")
        return pd.DataFrame()


def calculate_expma(df, period):
    """
    计算指数移动平均线（EXPMA）

    Args:
        df: 包含收盘价的DataFrame
        period: 周期

    Returns:
        EXPMA序列
    """
    return df['close_price'].ewm(span=period, adjust=False).mean()


def calculate_slope(expma_series):
    """
    计算EXPMA斜率（使用简单的差分）

    Args:
        expma_series: EXPMA序列

    Returns:
        斜率序列
    """
    return expma_series.diff()


def calculate_deviation(expma20, expma60):
    """
    计算EXPMA20相对EXPMA60的偏离程度（百分比）

    Args:
        expma20: 20日指数移动平均
        expma60: 60日指数移动平均

    Returns:
        偏离程度（百分比）
    """
    if pd.isna(expma20) or pd.isna(expma60) or expma60 == 0:
        return 0.0
    return (expma20 - expma60) / expma60 * 100


def calculate_trend_strength(deviation, slope):
    """
    计算趋势强度（结合偏离程度和斜率）

    Args:
        deviation: 偏离程度
        slope: EXPMA20斜率

    Returns:
        趋势强度
    """
    # 简单的加权计算：偏离程度占70%，斜率占30%
    # 斜率需要标准化，假设斜率范围一般在-1到1之间
    normalized_slope = slope * 100  # 将斜率放大到与偏离程度相似的量级
    trend_strength = deviation * 0.7 + normalized_slope * 0.3
    return trend_strength


def select_stocks(db, code, name, instrument_type, select_date=None):
    """
    选出符合条件的股票/ETF：
    1. EXPMA20 > EXPMA60（多头排列）
    2. EXPMA20斜率 > 0（上升趋势）

    Args:
        db: 数据库管理器
        code: 股票/ETF代码
        name: 股票/ETF名称
        instrument_type: 产品类型 ('stock' 或 'etf')
        select_date: 选股日期，为None时使用最新日期

    Returns:
        符合条件的股票信息字典，不符合条件返回None
    """
    # 从数据库读取日K数据
    df = db.query_kline_data(period='1d', code=code)

    if df.empty or len(df) < 60:
        return None

    # 按日期排序
    df = df.sort_values('datetime').reset_index(drop=True)

    # 计算EXPMA
    df['EXPMA20'] = calculate_expma(df, 20)
    df['EXPMA60'] = calculate_expma(df, 60)

    # 计算EXPMA20斜率
    df['EXPMA20_Slope'] = calculate_slope(df['EXPMA20'])

    # 如果指定了选股日期，使用该日期的数据
    if select_date:
        target_rows = df[df['datetime'] == select_date]
        if target_rows.empty:
            print(f"{instrument_type.upper()} {code} 在 {select_date} 没有数据")
            return None
        row = target_rows.iloc[0]
    else:
        # 使用最新的一行数据
        row = df.iloc[-1]
        select_date = row['datetime']

    # 检查条件
    expma20 = row['EXPMA20']
    expma60 = row['EXPMA60']
    expma20_slope = row['EXPMA20_Slope']

    # 条件1: EXPMA20 > EXPMA60（多头排列）
    # 条件2: EXPMA20斜率 > 0（上升趋势）
    if (pd.isna(expma20) or pd.isna(expma60) or pd.isna(expma20_slope)):
        return None

    if expma20 > expma60 and expma20_slope > 0:
        # 计算偏离程度
        deviation = calculate_deviation(expma20, expma60)

        # 计算趋势强度
        trend_strength = calculate_trend_strength(deviation, expma20_slope)

        return {
            'code': code,
            'name': name,
            'instrument_type': instrument_type,
            'select_date': select_date,
            'close_price': row['close_price'],
            'expma20': expma20,
            'expma60': expma60,
            'expma20_slope': expma20_slope,
            'deviation': deviation,
            'trend_strength': trend_strength
        }

    return None


def main():
    """主函数"""
    print("=" * 60)
    print("EXPMA20>EXPMA60 且 EXPMA20斜率>0 选股系统")
    print("=" * 60)
    print("\n选股条件：")
    print("  1. EXPMA20 > EXPMA60（多头排列）")
    print("  2. EXPMA20斜率 > 0（上升趋势）")
    print("\n计算指标：")
    print("  - 偏离程度 = (EXPMA20 - EXPMA60) / EXPMA60 × 100")
    print("  - 趋势强度 = 偏离程度 × 0.7 + EXPMA20斜率 × 100 × 0.3")
    print("\n支持产品类型：股票 (stock) 和 ETF (etf)")
    print("=" * 60)

    # 初始化数据库连接
    db = IndustryDataDB()

    selected_all = []

    # ========== 选择股票 ==========
    print("\n" + "=" * 60)
    print("开始选择股票")
    print("=" * 60)

    # 加载股票列表（按日期动态生成文件路径）
    today = datetime.now().strftime('%Y-%m-%d')
    stock_csv_path = f"data/stock_data_{today}.csv"
    print(f"\n正在从 {stock_csv_path} 加载股票列表...")
    stocks = load_instrument_list(stock_csv_path, 'SECURITY_CODE', 'SECURITY_SHORT_NAME')

    if stocks.empty:
        print("未找到股票数据，跳过股票选择")
    else:
        print(f"共加载 {len(stocks)} 只股票")

        # 选股
        print("\n开始筛选股票...")
        print("-" * 60)

        stock_count = 0
        for _, row in stocks.iterrows():
            stock_code = row['SECURITY_CODE']
            stock_name = row['SECURITY_SHORT_NAME']

            result = select_stocks(db, stock_code, stock_name, 'stock')

            if result:
                selected_all.append(result)
                stock_count += 1

                # 打印进度
                if stock_count % 20 == 0:
                    print(f"已筛选 {stock_count} 只符合条件的股票...")

        print(f"\n股票筛选完成！共选出 {stock_count} 只符合条件的股票")

    # ========== 选择ETF ==========
    print("\n" + "=" * 60)
    print("开始选择ETF")
    print("=" * 60)

    # 加载ETF列表（按日期动态生成文件路径）
    etf_csv_path = f"data/etf_data_{today}.csv"
    print(f"\n正在从 {etf_csv_path} 加载ETF列表...")
    etfs = load_instrument_list(etf_csv_path, 'ETF_CODE', 'ETF_NAME')

    if etfs.empty:
        print("未找到ETF数据，跳过ETF选择")
    else:
        print(f"共加载 {len(etfs)} 只ETF")

        # 选ETF
        print("\n开始筛选ETF...")
        print("-" * 60)

        etf_count = 0
        for _, row in etfs.iterrows():
            etf_code = row['SECURITY_CODE']
            etf_name = row['SECURITY_SHORT_NAME']

            result = select_stocks(db, etf_code, etf_name, 'etf')

            if result:
                selected_all.append(result)
                etf_count += 1

                # 打印进度
                if etf_count % 10 == 0:
                    print(f"已筛选 {etf_count} 只符合条件的ETF...")

        print(f"\nETF筛选完成！共选出 {etf_count} 只符合条件的ETF")

    # ========== 统计分析 ==========
    if not selected_all:
        print("\n没有找到符合条件的股票或ETF，程序退出")
        return

    print("\n" + "=" * 60)
    print("总体选股统计结果")
    print("=" * 60)

    # 转换为DataFrame
    results_df = pd.DataFrame(selected_all)

    # 分别统计股票和ETF
    stock_results = results_df[results_df['instrument_type'] == 'stock']
    etf_results = results_df[results_df['instrument_type'] == 'etf']

    print(f"\n选出的总数量: {len(selected_all)}")
    print(f"  - 股票: {len(stock_results)} 只")
    print(f"  - ETF: {len(etf_results)} 只")

    # 总体统计
    avg_deviation = results_df['deviation'].mean()
    avg_trend_strength = results_df['trend_strength'].mean()
    avg_slope = results_df['expma20_slope'].mean()

    print(f"\n总体平均指标:")
    print(f"  - 平均偏离程度: {avg_deviation:.2f}%")
    print(f"  - 平均趋势强度: {avg_trend_strength:.2f}")
    print(f"  - 平均EXPMA20斜率: {avg_slope:.6f}")

    # 股票统计
    if not stock_results.empty:
        print(f"\n股票平均指标:")
        print(f"  - 平均偏离程度: {stock_results['deviation'].mean():.2f}%")
        print(f"  - 平均趋势强度: {stock_results['trend_strength'].mean():.2f}")

        # 显示偏离程度最高的股票
        print("\n" + "-" * 60)
        print("偏离程度最高的20只股票:")
        print("-" * 60)
        top20_stocks = stock_results.nlargest(20, 'deviation')[
            ['code', 'name', 'close_price', 'expma20', 'expma60', 'expma20_slope', 'deviation', 'trend_strength']
        ]
        print(top20_stocks.to_string(index=False))

    # ETF统计
    if not etf_results.empty:
        print(f"\nETF平均指标:")
        print(f"  - 平均偏离程度: {etf_results['deviation'].mean():.2f}%")
        print(f"  - 平均趋势强度: {etf_results['trend_strength'].mean():.2f}")

        # 显示所有符合条件的ETF
        print("\n" + "-" * 60)
        print("所有符合条件的ETF:")
        print("-" * 60)
        etf_display = etf_results[['code', 'name', 'close_price', 'expma20', 'expma60', 'expma20_slope', 'deviation', 'trend_strength']]
        print(etf_display.to_string(index=False))

    # 显示趋势强度最高的20只（股票+ETF）
    print("\n" + "-" * 60)
    print("趋势强度最高的20只产品（股票+ETF）:")
    print("-" * 60)
    top20_trend = results_df.nlargest(20, 'trend_strength')[
        ['code', 'name', 'instrument_type', 'close_price', 'expma20', 'expma60', 'expma20_slope', 'deviation', 'trend_strength']
    ]
    print(top20_trend.to_string(index=False))

    # 存储到数据库
    print("\n正在存储到数据库...")
    inserted_count = db.insert_ma20_above_ma60_selection(selected_all)
    print(f"成功存储 {inserted_count} 条记录到数据库")

    print("\n" + "=" * 60)
    print("选股完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
