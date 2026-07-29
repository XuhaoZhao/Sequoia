"""
解析并导入期货品种交易时间数据到数据库
"""

import re
from db_manager import IndustryDataDB


def parse_trading_hours() -> list:
    """
    解析期货品种交易时间文本数据，返回结构化列表

    Returns:
        list of dict, 每个字典包含:
            - variety_name: 品种名称
            - exchange: 所属交易所（中文名）
            - exchange_code: 交易所代码
            - day_session: 日盘时间
            - night_session: 夜盘时间（无则为None）
    """
    data = []

    # 交易所代码映射
    exchange_map = {
        "大商所": "DCE",
        "郑商所": "CZCE",
        "广期所": "GFEX",
        "上期所": "SHFE",
        "上期能源": "INE",
        "中金所": "CFFEX",
    }

    # 各交易所品种及交易时间定义
    # 格式: (交易所中文名, 品种列表, 日盘时间, 夜盘时间(可选))
    definitions = [
        # ===== 大商所 =====
        ("大商所",
         ["鸡蛋", "生猪", "原木", "胶合板", "纤维板"],
         "9:00-10:15,10:30-11:30,13:30-15:00", None),

        ("大商所",
         ["豆一", "豆二", "豆油", "豆粕", "棕榈油", "玉米", "淀粉", "粳米",
          "焦煤", "焦炭", "铁矿石", "LPG", "塑料", "PVC", "乙二醇", "聚丙烯",
          "苯乙烯", "纯苯", "塑料月均", "PVC月均", "聚丙烯月均"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-23:00"),

        # ===== 郑商所 =====
        ("郑商所",
         ["尿素", "硅铁", "锰硅", "苹果", "红枣", "花生", "强麦", "普麦",
          "早籼稻", "晚籼稻", "粳稻", "菜籽"],
         "9:00-10:15,10:30-11:30,13:30-15:00", None),

        ("郑商所",
         ["玻璃", "PTA", "瓶片", "对二甲苯", "丙烯", "甲醇", "纯碱", "烧碱",
          "白糖", "棉花", "棉纱", "菜油", "菜粕", "短纤", "动力煤"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-23:00"),

        # ===== 广期所 =====
        ("广期所",
         ["工业硅", "多晶硅", "碳酸锂", "铂", "钯"],
         "9:00-10:15,10:30-11:30,13:30-15:00", None),

        # ===== 上期所 =====
        ("上期所",
         ["线材"],
         "9:00-10:15,10:30-11:30,13:30-15:00", None),

        ("上期所",
         ["螺纹钢", "热卷", "燃油", "沥青", "橡胶", "丁二烯胶", "纸浆", "胶版纸"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-23:00"),

        ("上期所",
         ["沪铜", "沪铝", "氧化铝", "铸造铝", "沪锌", "沪铅", "沪镍", "沪锡", "不锈钢"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-01:00"),

        ("上期所",
         ["沪金", "沪银"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-02:30"),

        # ===== 上期能源 =====
        ("上期能源",
         ["欧线集运"],
         "9:00-10:15,10:30-11:30,13:30-15:00", None),

        ("上期能源",
         ["20号胶", "低硫燃油"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-23:00"),

        ("上期能源",
         ["国际铜"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-01:00"),

        ("上期能源",
         ["原油"],
         "9:00-10:15,10:30-11:30,13:30-15:00",
         "21:00-02:30"),

        # ===== 中金所 =====
        ("中金所",
         ["上证", "沪深", "中证500", "中证1000"],
         "9:30-11:30,13:00-15:00", None),

        ("中金所",
         ["二债", "五债", "十债", "三十债"],
         "9:30-11:30,13:00-15:15", None),
    ]

    for exchange_cn, varieties, day_session, night_session in definitions:
        exchange_code = exchange_map.get(exchange_cn, "")
        for variety in varieties:
            data.append({
                "variety_name": variety,
                "exchange": exchange_cn,
                "exchange_code": exchange_code,
                "day_session": day_session,
                "night_session": night_session,
            })

    return data


def main():
    """主函数：解析数据并写入数据库"""
    db = IndustryDataDB()
    data = parse_trading_hours()

    print(f"共解析到 {len(data)} 条期货品种交易时间数据")

    # 插入数据库
    inserted_count = db.insert_futures_trading_hours(data)
    print(f"成功插入/更新 {inserted_count} 条记录")

    # 验证：查询并展示
    df = db.query_futures_trading_hours()
    print(f"\n数据库中共有 {len(df)} 条记录")

    # 按交易所统计
    print("\n=== 按交易所统计 ===")
    for exchange, group in df.groupby('exchange'):
        night_count = group['night_session'].notna().sum()
        no_night_count = group['night_session'].isna().sum()
        print(f"  {exchange}({group['exchange_code'].iloc[0]}): "
              f"共 {len(group)} 个品种, "
              f"有夜盘 {night_count} 个, 无夜盘 {no_night_count} 个")

    # 展示有夜盘的品种
    print("\n=== 有夜盘品种一览 ===")
    df_night = db.query_futures_trading_hours(has_night_session=True)
    for _, row in df_night.iterrows():
        print(f"  {row['variety_name']:8s} | {row['exchange']:6s} | "
              f"日盘: {row['day_session']} | 夜盘: {row['night_session']}")


if __name__ == "__main__":
    main()
