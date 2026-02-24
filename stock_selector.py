import pandas as pd
from datetime import datetime, timedelta
from db_manager import IndustryDataDB
import warnings
warnings.filterwarnings('ignore')

def load_stock_list(csv_path):
    """从CSV文件加载股票列表"""
    # 读取CSV，将SECURITY_CODE作为字符串类型以保留前导0
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

def screen_stock(db, stock_code, stock_name):
    """
    对单只股票进行筛选（实时版本）

    筛选条件：
    1. 5日均线斜率大于0
    2. 收盘价在20日均线以上
    3. 60日均线斜率小于0

    实时策略：
    - 读取历史日K数据（到昨天）
    - 读取当天的1分钟数据
    - 将1分钟数据汇总成当天的日K数据
    - 合并后计算均线并判断买点
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # 1. 读取历史日K数据（到昨天）
    df_daily = db.query_kline_data(period='1d', code=stock_code)

    if df_daily.empty or len(df_daily) < 60:
        return None

    # 2. 读取当天的1分钟数据
    df_1m = db.query_kline_data(
        period='1m',
        code=stock_code,
        start_date=today,
        end_date=today
    )

    # 3. 如果有当天1分钟数据，汇总成日K数据
    if not df_1m.empty:
        # 从1分钟数据合成当天的OHLC
        today_ohlc = {
            'datetime': today,
            'open_price': df_1m.iloc[0]['open_price'],      # 当天第一根1分钟的开盘价
            'close_price': df_1m.iloc[-1]['close_price'],    # 当天最后一根1分钟的收盘价
            'high_price': df_1m['high_price'].max(),         # 当天最高价
            'low_price': df_1m['low_price'].min(),           # 当天最低价
            'volume': df_1m['volume'].sum(),                 # 当天总成交量
            'amount': df_1m['amount'].sum()                  # 当天总成交额
        }

        # 创建当天数据的DataFrame
        df_today = pd.DataFrame([today_ohlc])

        # 4. 合并历史数据和当天数据
        # 先移除历史数据中可能存在的当天数据（如果有）
        df_daily = df_daily[df_daily['datetime'] != today]

        # 合并数据
        df = pd.concat([df_daily, df_today], ignore_index=True)
    else:
        # 没有当天1分钟数据，使用历史日K数据
        df = df_daily.copy()

    # 按日期排序
    df = df.sort_values('datetime').reset_index(drop=True)

    # 计算均线
    df['MA5'] = calculate_ma(df, 5)
    df['MA20'] = calculate_ma(df, 20)
    df['MA60'] = calculate_ma(df, 60)

    # 计算斜率
    df['MA5_Slope'] = calculate_slope(df['MA5'])
    df['MA60_Slope'] = calculate_slope(df['MA60'])

    # 获取最新一天的数据
    latest = df.iloc[-1]

    # 检查是否有足够的数据
    if pd.isna(latest['MA5']) or pd.isna(latest['MA20']) or pd.isna(latest['MA60']):
        return None

    # 检查筛选条件
    # 条件1: 5日均线斜率大于0
    ma5_slope_positive = latest['MA5_Slope'] > 0

    # 条件2: 收盘价在20日均线以上
    price_above_ma20 = latest['close_price'] > latest['MA20']

    # 条件3: 60日均线斜率小于0
    ma60_slope_negative = latest['MA60_Slope'] < 0

    # 所有条件都满足
    if ma5_slope_positive and price_above_ma20 and ma60_slope_negative:
        return {
            '股票代码': stock_code,
            '股票名称': stock_name,
            '最新收盘价': latest['close_price'],
            '最新5日均线': latest['MA5'],
            '最新20日均线': latest['MA20'],
            '最新60日均线': latest['MA60'],
            'MA5斜率': latest['MA5_Slope'],
            'MA60斜率': latest['MA60_Slope'],
            '数据日期': latest['datetime']
        }

    return None

def main():
    """主函数"""
    print("=" * 60)
    print("三均线选股系统")
    print("=" * 60)
    print("\n筛选条件：")
    print("  1. 5日均线斜率大于0（短期上升趋势）")
    print("  2. 收盘价在20日均线以上（中期支撑）")
    print("  3. 60日均线斜率小于0（长期下降趋势）")
    print("\n策略逻辑：")
    print("  短期强势，中期有支撑，长期处于下降趋势的股票")
    print("  可能是反弹或反转机会")
    print("=" * 60)

    # 初始化数据库连接
    db = IndustryDataDB()

    # 加载股票列表
    # 根据当前日期生成CSV文件路径
    today = datetime.now().strftime("%Y-%m-%d")
    csv_path = f"data/stock_data_{today}.csv"
    print(f"\n正在从 {csv_path} 加载股票列表...")
    stocks = load_stock_list(csv_path)
    print(f"共加载 {len(stocks)} 只股票")

    # 筛选股票
    print("\n开始筛选...")
    print("-" * 60)

    selected_stocks = []
    processed_count = 0

    for _, row in stocks.iterrows():
        stock_code = row['SECURITY_CODE']
        stock_name = row['SECURITY_SHORT_NAME']

        result = screen_stock(db, stock_code, stock_name)

        if result:
            selected_stocks.append(result)

        processed_count += 1

        # 打印进度
        if processed_count % 100 == 0:
            print(f"已处理 {processed_count} 只股票，找到 {len(selected_stocks)} 只符合条件的股票...")

    print(f"\n筛选完成！共处理 {processed_count} 只股票")
    print(f"找到 {len(selected_stocks)} 只符合条件的股票")

    if not selected_stocks:
        print("没有找到符合条件的股票，程序退出")
        return

    # 转换为DataFrame
    results_df = pd.DataFrame(selected_stocks)

    # 显示结果
    print("\n" + "=" * 60)
    print("筛选结果")
    print("=" * 60)

    # 按收盘价排序
    results_df = results_df.sort_values('最新收盘价', ascending=False)

    # 显示所有符合条件的股票
    display_columns = ['股票代码', '股票名称', '最新收盘价', '最新5日均线', '最新20日均线', '最新60日均线', '数据日期']
    print("\n符合条件的股票列表：")
    print("-" * 60)
    print(results_df[display_columns].to_string(index=False))

    # 统计信息
    print("\n" + "-" * 60)
    print("统计信息：")
    print("-" * 60)
    print(f"符合条件的股票数量: {len(selected_stocks)}")
    print(f"平均收盘价: {results_df['最新收盘价'].mean():.2f}")
    print(f"最高收盘价: {results_df['最新收盘价'].max():.2f}")
    print(f"最低收盘价: {results_df['最新收盘价'].min():.2f}")

    # 保存结果到CSV
    output_file = 'selected_stocks.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n筛选结果已保存到 {output_file}")

    print("\n" + "=" * 60)
    print("筛选完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
