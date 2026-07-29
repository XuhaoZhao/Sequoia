"""
清理新浪期货实时数据表中的错误数据
（可选）如果之前插入了错误映射的数据，可以运行此脚本清理
"""

from db_manager import IndustryDataDB
from datetime import datetime


def clean_old_wrong_data():
    """清理旧的错误数据"""
    db = IndustryDataDB()

    print("="*80)
    print("清理新浪期货实时数据表")
    print("="*80)

    # 查询当前表中的数据量
    df = db.query_sina_futures_realtime(limit=10)

    if df.empty:
        print("\n当前表中没有数据，无需清理")
        return

    print(f"\n当前表中有 {len(df)} 条数据（显示前10条）")
    print("\n数据样例:")
    print(df[['contract_code', 'latest_price', 'open_price', 'high_price', 'low_price', 'volume', 'open_interest']].head())

    print("\n" + "-"*80)
    print("选择清理方案:")
    print("-"*80)
    print("1. 删除所有数据（慎用）")
    print("2. 仅删除今天之前的数据")
    print("3. 取消操作")
    print("-"*80)

    choice = input("\n请选择 (1/2/3): ").strip()

    if choice == '1':
        # 删除所有数据
        confirm = input("确认删除所有数据？(yes/no): ").strip().lower()
        if confirm == 'yes':
            count = db.clear_sina_futures_realtime()
            print(f"\n✓ 已删除 {count} 条数据")
        else:
            print("\n已取消操作")

    elif choice == '2':
        # 删除今天之前的数据
        today = datetime.now().strftime('%Y-%m-%d')
        start_datetime = f"1970-01-01 00:00:00"
        end_datetime = f"{today} 00:00:00"

        confirm = input(f"确认删除 {end_datetime} 之前的数据？(yes/no): ").strip().lower()
        if confirm == 'yes':
            count = db.delete_sina_futures_realtime_by_time_range(
                start_datetime=start_datetime,
                end_datetime=end_datetime
            )
            print(f"\n✓ 已删除 {count} 条数据")
        else:
            print("\n已取消操作")

    else:
        print("\n已取消操作")

    print("\n" + "="*80)
    print("操作完成")
    print("="*80)


if __name__ == "__main__":
    clean_old_wrong_data()
