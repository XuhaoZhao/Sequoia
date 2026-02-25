import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from db_manager import IndustryDataDB
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

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

def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标
    返回: MACD线、信号线、柱状图
    """
    # 计算EMA
    df['EMA_FAST'] = df['close_price'].ewm(span=fast, adjust=False).mean()
    df['EMA_SLOW'] = df['close_price'].ewm(span=slow, adjust=False).mean()

    # 计算MACD线
    df['MACD'] = df['EMA_FAST'] - df['EMA_SLOW']

    # 计算信号线
    df['MACD_SIGNAL'] = df['MACD'].ewm(span=signal, adjust=False).mean()

    # 计算MACD柱状图
    df['MACD_HIST'] = df['MACD'] - df['MACD_SIGNAL']

    return df['MACD'], df['MACD_SIGNAL'], df['MACD_HIST']

def backtest_strategy(db, stock_code, stock_name):
    """
    对单只股票进行回测
    策略：20日均线突破 + 止盈止损策略

    买入条件：
    股价站上20日均线 且 20日均线斜率大于0
    （收盘价 > MA20 且 MA20_Slope > 0）

    卖出条件（满足任一即卖出）：
    1. 5日均线大于20日均线5%（MA5 > MA20 * 1.05）
    2. 止损：股价跌幅达到2%（当前价格 <= 买入价 * 0.98）
    """
    # 从数据库读取日K数据
    df = db.query_kline_data(period='1d', code=stock_code)

    if df.empty or len(df) < 60:
        return None

    # 按日期排序
    df = df.sort_values('datetime').reset_index(drop=True)

    # 计算均线
    df['MA5'] = calculate_ma(df, 5)
    df['MA20'] = calculate_ma(df, 20)

    # 计算20日均线斜率
    df['MA20_Slope'] = calculate_slope(df['MA20'])

    # 回测交易
    trades = []
    holding = False
    buy_price = 0
    buy_date = None

    for i in range(len(df)):
        row = df.iloc[i]

        # 检测买入信号：股价站上20日均线 且 20日均线斜率大于0
        if not holding:
            # 条件：收盘价 > MA20 且 MA20_Slope > 0
            if (row['close_price'] > row['MA20'] and
                row['MA20_Slope'] > 0 and
                not pd.isna(row['MA20']) and
                not pd.isna(row['MA20_Slope'])):
                holding = True
                buy_price = row['close_price']
                buy_date = row['datetime']
                continue

        # 检测卖出信号：5日线大于20日线20% 或 止损
        if holding:
            # 检查止盈：5日均线大于20日均线20%
            take_profit = row['MA5'] > row['MA20'] * 1.05

            # 检查止损：跌幅达到2%
            stop_loss = row['close_price'] <= buy_price * 0.9

            # 满足任一卖出条件即卖出
            if take_profit or stop_loss:
                # 卖出
                sell_price = row['close_price']
                sell_date = row['datetime']
                profit = (sell_price - buy_price) * 100  # 买入100股
                profit_pct = (sell_price - buy_price) / buy_price * 100

                trades.append({
                    'buy_date': buy_date,
                    'sell_date': sell_date,
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'profit': profit,
                    'profit_pct': profit_pct
                })

                holding = False
                buy_price = 0
                buy_date = None

    if not trades:
        return None

    # 计算总收益
    total_profit = sum([t['profit'] for t in trades])
    total_profit_pct = sum([t['profit_pct'] for t in trades])
    trade_count = len(trades)
    win_count = len([t for t in trades if t['profit'] > 0])
    win_rate = win_count / trade_count * 100 if trade_count > 0 else 0

    # 分离亏损交易
    losing_trades = [t for t in trades if t['profit'] <= 0]

    return {
        'code': stock_code,
        'name': stock_name,
        'trade_count': trade_count,
        'total_profit': total_profit,
        'total_profit_pct': total_profit_pct,
        'avg_profit_pct': total_profit_pct / trade_count if trade_count > 0 else 0,
        'win_count': win_count,
        'win_rate': win_rate,
        'trades': trades,
        'losing_trades': losing_trades
    }

def plot_results(results_df):
    """绘制回测结果图表"""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('20日均线突破策略回测结果', fontsize=16, fontweight='bold')

    # 1. 总收益柱状图
    ax1 = axes[0, 0]
    colors = ['green' if x > 0 else 'red' for x in results_df['total_profit']]
    ax1.bar(range(len(results_df)), results_df['total_profit'], color=colors, alpha=0.6)
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax1.set_title('各股票总收益', fontsize=12)
    ax1.set_xlabel('股票编号')
    ax1.set_ylabel('总收益 (元)')
    ax1.grid(True, alpha=0.3)

    # 2. 收益率分布
    ax2 = axes[0, 1]
    ax2.hist(results_df['total_profit_pct'], bins=50, color='blue', alpha=0.6, edgecolor='black')
    ax2.axvline(x=0, color='red', linestyle='--', linewidth=2, label='盈亏平衡线')
    ax2.set_title('总收益率分布', fontsize=12)
    ax2.set_xlabel('总收益率 (%)')
    ax2.set_ylabel('股票数量')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 3. 胜率分布
    ax3 = axes[1, 0]
    ax3.hist(results_df['win_rate'], bins=30, color='purple', alpha=0.6, edgecolor='black')
    ax3.axvline(x=50, color='red', linestyle='--', linewidth=2, label='50%基准线')
    avg_win_rate = results_df['win_rate'].mean()
    ax3.axvline(x=avg_win_rate, color='green', linestyle='--', linewidth=2, label=f'平均值: {avg_win_rate:.1f}%')
    ax3.set_title('胜率分布', fontsize=12)
    ax3.set_xlabel('胜率 (%)')
    ax3.set_ylabel('股票数量')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # 4. 胜率 vs 总收益率
    ax4 = axes[1, 1]
    scatter = ax4.scatter(results_df['win_rate'], results_df['total_profit_pct'],
                         c=results_df['total_profit'], cmap='RdYlGn', alpha=0.6, s=50)
    ax4.axhline(y=0, color='black', linestyle='--', linewidth=1)
    ax4.axvline(x=50, color='red', linestyle='--', linewidth=1, alpha=0.5, label='50%胜率线')
    ax4.set_title('胜率 vs 总收益率', fontsize=12)
    ax4.set_xlabel('胜率 (%)')
    ax4.set_ylabel('总收益率 (%)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)

    # 添加颜色条
    cbar = plt.colorbar(scatter, ax=ax4)
    cbar.set_label('总收益 (元)')

    plt.tight_layout()
    plt.savefig('ma5_reverse_backtest_results.png', dpi=300, bbox_inches='tight')
    print("\n图表已保存为 ma5_reverse_backtest_results.png")
    plt.show()

def main():
    """主函数"""
    print("=" * 60)
    print("20日均线突破策略回测系统")
    print("=" * 60)
    print("\n策略说明：")
    print("买入条件：")
    print("  股价站上20日均线 且 20日均线斜率大于0")
    print("  （收盘价 > MA20 且 MA20_Slope > 0）")
    print("\n卖出条件（满足任一即卖出）：")
    print("  1. 止盈：5日均线大于20日均线5%")
    print("     （MA5 > MA20 * 1.05）")
    print("  2. 止损：股价跌幅达到2%")
    print("     （当前价格 <= 买入价 * 0.98）")
    print("=" * 60)

    # 初始化数据库连接
    db = IndustryDataDB()

    # 加载股票列表
    csv_path = "data/stock_data_2025-12-28.csv"
    print(f"\n正在从 {csv_path} 加载股票列表...")
    stocks = load_stock_list(csv_path)
    print(f"共加载 {len(stocks)} 只股票")

    # 回测每只股票
    print("\n开始回测...")
    print("-" * 60)

    all_results = []
    success_count = 0

    for _, row in stocks.iterrows():
        stock_code = row['SECURITY_CODE']
        stock_name = row['SECURITY_SHORT_NAME']

        result = backtest_strategy(db, stock_code, stock_name)

        if result:
            all_results.append(result)
            success_count += 1

            # 打印进度
            if success_count % 20 == 0:
                print(f"已完成 {success_count} 只股票的回测...")

    print(f"\n回测完成！成功回测 {success_count} 只股票")

    if not all_results:
        print("没有回测结果，程序退出")
        return

    # 转换为DataFrame
    results_df = pd.DataFrame(all_results)

    # 统计分析
    print("\n" + "=" * 60)
    print("回测统计结果")
    print("=" * 60)

    total_all_profit = results_df['total_profit'].sum()
    avg_profit_pct = results_df['avg_profit_pct'].mean()
    total_trades = results_df['trade_count'].sum()
    avg_win_rate = results_df['win_rate'].mean()
    profitable_stocks = len(results_df[results_df['total_profit'] > 0])
    loss_stocks = len(results_df[results_df['total_profit'] <= 0])

    print(f"总收益: {total_all_profit:.2f} 元")
    print(f"平均收益率: {avg_profit_pct:.2f}%")
    print(f"总交易次数: {total_trades}")
    print(f"平均胜率: {avg_win_rate:.2f}%")
    print(f"盈利股票数: {profitable_stocks}")
    print(f"亏损股票数: {loss_stocks}")
    print(f"盈利股票占比: {profitable_stocks/len(results_df)*100:.2f}%")

    # 显示收益最好的10只股票
    print("\n" + "-" * 60)
    print("收益最好的10只股票:")
    print("-" * 60)
    top10 = results_df.nlargest(10, 'total_profit')[['code', 'name', 'total_profit', 'total_profit_pct', 'trade_count', 'win_rate']]
    print(top10.to_string(index=False))

    # 显示收益最差的10只股票
    print("\n" + "-" * 60)
    print("收益最差的10只股票:")
    print("-" * 60)
    bottom10 = results_df.nsmallest(10, 'total_profit')[['code', 'name', 'total_profit', 'total_profit_pct', 'trade_count', 'win_rate']]
    print(bottom10.to_string(index=False))

    # 保存详细结果到CSV
    output_file = 'ma5_reverse_backtest_detailed.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n详细结果已保存到 {output_file}")

    # 收集所有亏损交易并保存到Excel
    print("\n正在收集亏损交易...")
    all_losing_trades = []

    for result in all_results:
        if 'losing_trades' in result and result['losing_trades']:
            for trade in result['losing_trades']:
                all_losing_trades.append({
                    '股票代码': result['code'],
                    '股票名称': result['name'],
                    '买入日期': trade['buy_date'],
                    '卖出日期': trade['sell_date'],
                    '买入价格': trade['buy_price'],
                    '卖出价格': trade['sell_price'],
                    '收益': trade['profit'],
                    '收益率(%)': trade['profit_pct']
                })

    if all_losing_trades:
        losing_trades_df = pd.DataFrame(all_losing_trades)
        excel_file = 'losing_trades.xlsx'
        losing_trades_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"亏损交易已保存到 {excel_file}")
        print(f"共 {len(all_losing_trades)} 笔亏损交易")
    else:
        print("没有亏损交易记录")

    # 绘制图表
    print("\n正在生成图表...")
    plot_results(results_df)

    print("\n" + "=" * 60)
    print("回测完成！")
    print("=" * 60)

if __name__ == "__main__":
    main()
