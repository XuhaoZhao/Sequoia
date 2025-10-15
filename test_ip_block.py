import akshare as ak
import time
from datetime import datetime
from data_collect.stock_chip_race import stock_large_cap_filter


class DynamicDelayStrategy:
    """
    动态延迟策略类
    按照设定的延迟序列循环返回延迟时间: 10s -> 20s -> 30s -> 40s -> 50s -> 40s -> 30s -> 20s -> 10s -> ...
    """
    def __init__(self, delays=None):
        if delays is None:
            delays = [10, 20, 30, 40, 50]

        # 创建完整的循环序列: 递增 + 递减(不包括最大值,避免重复)
        self.delays = delays + delays[-2::-1]  # [10, 20, 30, 40, 50, 40, 30, 20]
        self.current_index = 0

    def get_next_delay(self):
        """获取下一个延迟时间"""
        delay = self.delays[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.delays)
        return delay

    def get_current_delay(self):
        """获取当前延迟时间(不移动索引)"""
        return self.delays[self.current_index]


def test_ip_blocking():
    """
    测试循环调用 stock_intraday_em 是否会被封锁IP
    使用动态延迟策略: 10s -> 20s -> 30s -> 40s -> 50s -> 40s -> 30s -> 20s -> 10s (循环)
    """
    print("=" * 80)
    print("开始测试IP封锁情况（动态延迟策略）")
    print("=" * 80)

    # 初始化动态延迟策略
    delay_strategy = DynamicDelayStrategy([10, 10, 10, 10, 10])
    print(f"延迟策略序列: {delay_strategy.delays} (循环)")

    # 获取所有股票代码
    print("\n正在获取股票代码列表...")
    try:
        stock_df = stock_large_cap_filter()
        stock_codes = stock_df['代码'].tolist() if '代码' in stock_df.columns else stock_df.iloc[:, 0].tolist()
        print(f"成功获取 {len(stock_codes)} 只股票代码")
        print(f"前10个股票代码: {stock_codes[:10]}")
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return

    # 循环调用 stock_intraday_em
    success_count = 0
    fail_count = 0
    failed_stocks = []  # 记录失败的股票代码

    print(f"\n开始循环调用")
    print("按 Ctrl+C 可以停止测试\n")

    try:
        idx = 0
        while idx < len(stock_codes):
            stock_code = stock_codes[idx]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            request_success = False

            try:
                print(f"[{current_time}] 第 {idx+1}/{len(stock_codes)} 次请求 - 股票代码: {stock_code}")

                # 调用 akshare 接口
                stock_intraday_em_df = ak.stock_intraday_em(symbol=stock_code)

                # 检查返回数据
                if stock_intraday_em_df is not None and not stock_intraday_em_df.empty:
                    success_count += 1
                    request_success = True
                    print(f"  ✓ 成功获取数据，数据行数: {len(stock_intraday_em_df)}")
                else:
                    fail_count += 1
                    print(f"  ✗ 返回数据为空")

            except Exception as e:
                fail_count += 1
                print(f"  ✗ 请求失败: {str(e)}")

                # 如果是网络相关错误，可能是被封锁
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in ['timeout', 'connection', 'network', '403', '429']):
                    print(f"  ⚠️ 警告：可能已被封锁IP！错误: {e}")

                # 失败时额外等待5分钟
                print(f"  ⚠️ 请求失败，额外等待 5 分钟 (300秒)...")
                time.sleep(300)
                print(f"  5分钟等待完成，将重新请求该股票")

            # 只有成功时才移动到下一个股票
            if request_success:
                idx += 1
            else:
                # 记录失败的股票（用于统计）
                if stock_code not in failed_stocks:
                    failed_stocks.append(stock_code)
                print(f"  ↻ 将重新请求股票 {stock_code}")

            # 显示统计信息
            total_requests = success_count + fail_count
            print(f"  当前统计 - 成功: {success_count}, 失败: {fail_count}, 成功率: {success_count/total_requests*100:.2f}%")
            print(f"  进度: {success_count}/{len(stock_codes)} 只股票已完成")

            # 使用动态延迟策略获取下一个延迟时间
            next_delay = delay_strategy.get_next_delay()
            print(f"  等待 {next_delay} 秒...\n")
            time.sleep(next_delay)

    except KeyboardInterrupt:
        print("\n\n用户手动停止测试")

    # 打印最终统计
    print("\n" + "=" * 80)
    print("测试结束")
    print("=" * 80)
    print(f"总请求次数: {success_count + fail_count}")
    print(f"成功次数: {success_count}")
    print(f"失败次数: {fail_count}")
    if success_count + fail_count > 0:
        print(f"成功率: {success_count/(success_count+fail_count)*100:.2f}%")
    print(f"完成股票数: {success_count}/{len(stock_codes)}")
    if failed_stocks:
        print(f"曾经失败过的股票数: {len(failed_stocks)}")
        print(f"失败股票列表: {failed_stocks}")
    print("=" * 80)

if __name__ == "__main__":
    test_ip_blocking()
