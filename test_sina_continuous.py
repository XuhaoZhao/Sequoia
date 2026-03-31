"""
测试持续监听模式 - 运行15秒后自动停止
"""
import asyncio
from sina_futures_interceptor import SinaFuturesInterceptor

async def test_continuous_mode():
    """测试持续监听模式"""

    # 配置期货代码
    custom_symbols = [
        'nf_V2605',   # 聚氯乙烯
        'nf_LC2703',  # 碳酸锂
    ]

    print("=" * 80)
    print("测试持续监听模式（15秒后自动停止）")
    print("=" * 80)

    # 创建拦截器
    interceptor = SinaFuturesInterceptor(
        headless=True,
        custom_symbols=custom_symbols,
        continuous=True
    )

    # 启动拦截任务
    task = asyncio.create_task(interceptor.intercept_futures_data())

    # 等待15秒后停止
    try:
        await asyncio.sleep(15)
        print("\n\n15秒测试时间到，正在停止...")
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        print("\n✓ 测试完成")
        print(f"数据已保存到: {interceptor.csv_file}")
        print(f"共拦截 {interceptor.intercept_count} 次")

    except Exception as e:
        print(f"\n✗ 测试出错: {e}")

if __name__ == "__main__":
    asyncio.run(test_continuous_mode())
