#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/2/26 12:18
Desc: 通达信抢筹
http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
"""

import pandas as pd
import requests
import time
from urllib.parse import urlencode

def stock_chip_race_open(date: str = "") -> pd.DataFrame:
    """
    通达信竞价抢筹_早盘抢筹
    http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
    :return: 早盘抢筹
    :rtype: pandas.DataFrame
    """
    url = "http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp"
    #sort:1抢筹委托金额, 2抢筹成交金额, 3开盘金额, 4抢筹幅度, 5抢筹占比
    if date=="":
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 0,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC"}]
    else:
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 0,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC", "date": date}]
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 TdxW",
    }

    r = requests.post(url, json=params,headers=headers)
    data_json = r.json()
    data = data_json["datas"]
    if not data:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data)
    temp_df.columns = [
        "代码",
        "名称",
        "昨收",
        "今开",
        "开盘金额",
        "抢筹幅度",
        "抢筹委托金额",
        "抢筹成交金额",
        "最新价",
        "_",
    ]

    temp_df["昨收"] = temp_df["昨收"]/10000
    temp_df["今开"] = temp_df["今开"] / 10000
    temp_df["抢筹幅度"] = round(temp_df["抢筹幅度"] * 100, 2)
    temp_df["最新价"] = round(temp_df["最新价"], 2)
    temp_df["涨跌幅"] = round((temp_df["最新价"] / temp_df["昨收"]-1) * 100, 2)
    temp_df["抢筹占比"] = round((temp_df["抢筹成交金额"] / temp_df["开盘金额"]) * 100, 2)

    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "昨收",
            "今开",
            "开盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "抢筹占比",
        ]
    ]

    return temp_df

def stock_chip_race_end(date: str = "") -> pd.DataFrame:
    """
    通达信竞价抢筹_尾盘抢筹
    http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp
    :return: 尾盘抢筹
    :rtype: pandas.DataFrame
    """
    url = "http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp"
    #sort:1抢筹委托金额, 2抢筹成交金额, 3开盘金额, 4抢筹幅度, 5抢筹占比
    if date=="":
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 1,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC"}]
    else:
        params = [{"funcId": 20, "offset": 0, "count": 100, "sort": 1, "period": 1,
                   "Token": "6679f5cadca97d68245a086793fc1bfc0a50b487487c812f", "modname": "JJQC", "date": date}]
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": "TdxW",
    }

    r = requests.post(url, json=params,headers=headers)
    data_json = r.json()
    data = data_json["datas"]
    if not data:
        return pd.DataFrame()
    temp_df = pd.DataFrame(data)
    temp_df.columns = [
        "代码",
        "名称",
        "昨收",
        "今开",
        "收盘金额",
        "抢筹幅度",
        "抢筹委托金额",
        "抢筹成交金额",
        "最新价",
        "_",
    ]

    temp_df["昨收"] = temp_df["昨收"]/10000
    temp_df["今开"] = temp_df["今开"] / 10000
    temp_df["抢筹幅度"] = round(temp_df["抢筹幅度"] * 100, 2)
    temp_df["最新价"] = round(temp_df["最新价"], 2)
    temp_df["涨跌幅"] = round((temp_df["最新价"] / temp_df["昨收"]-1) * 100, 2)
    temp_df["抢筹占比"] = round((temp_df["抢筹成交金额"] / temp_df["收盘金额"]) * 100, 2)

    temp_df = temp_df[
        [
            "代码",
            "名称",
            "最新价",
            "涨跌幅",
            "昨收",
            "今开",
            "收盘金额",
            "抢筹幅度",
            "抢筹委托金额",
            "抢筹成交金额",
            "抢筹占比",
        ]
    ]

    return temp_df

def stock_large_cap_filter(
    sort_field: str = "CHANGE_RATE",
    sort_direction: int = -1,
    min_market_cap: float = 50000000000,
    fetch_all: bool = True,
    sleep_time: float = 1.0,
    debug: bool = False
) -> pd.DataFrame:
    """
    东方财富网选股器-大市值股票筛选（聚合所有分页数据）
    https://data.eastmoney.com/dataapi/xuangu/list
    :param sort_field: 排序字段，默认CHANGE_RATE（涨跌幅），可选：NEW_PRICE、VOLUME_RATIO、TURNOVERRATE等
    :param sort_direction: 排序方向，-1降序，1升序
    :param min_market_cap: 最小市值筛选，默认500亿（50000000000）
    :param fetch_all: 是否获取所有分页数据，默认True
    :param sleep_time: 每次请求间隔时间(秒)，默认1.0秒，防止IP被封
    :param debug: 是否开启调试模式打印日志，默认False
    :return: 股票列表数据（所有分页聚合）
    :rtype: pandas.DataFrame
    """
    url = "https://data.eastmoney.com/dataapi/xuangu/list"

    # 构建返回字段
    fields = [
        "SECUCODE",
        "SECURITY_CODE",
        "SECURITY_NAME_ABBR",
        "NEW_PRICE",
        "CHANGE_RATE",
        "VOLUME_RATIO",
        "HIGH_PRICE",
        "LOW_PRICE",
        "PRE_CLOSE_PRICE",
        "VOLUME",
        "DEAL_AMOUNT",
        "TURNOVERRATE",
        "TOTAL_MARKET_CAP"
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://data.eastmoney.com/xuangu/",
        "Origin": "https://data.eastmoney.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }

    page_size = 50

    try:
        # 第一步：先请求第一页，获取总数信息
        first_params = {
            "st": sort_field,
            "sr": sort_direction,
            "ps": page_size,
            "p": 1,
            "sty": ",".join(fields),
            "filter": f"(TOTAL_MARKET_CAP>{min_market_cap})",
            "source": "SELECT_SECURITIES",
            "client": "WEB"
        }

        # 构建完整的URL
        full_url = f"{url}?{urlencode(first_params)}"
        if debug:
            print("正在获取第 1 页数据，并获取总数信息...")
            print(f"请求URL: {full_url}")
            print(f"请求Headers: {headers}")

        response = requests.get(full_url, headers=headers)

        if debug:
            print(f"响应状态码: {response.status_code}")
            print(f"响应Headers: {dict(response.headers)}")
            print(f"响应内容长度: {len(response.text)}")
            print(f"响应内容前500字符: {response.text[:500]}")

        response.raise_for_status()
        data_json = response.json()

        if debug:
            print(f"JSON结构: {list(data_json.keys())}")
            if 'result' in data_json:
                print(f"result结构: {list(data_json['result'].keys())}")

        # 检查是否有数据 - 正确的路径是 result.data
        if not data_json.get("result") or not data_json["result"].get("data"):
            if debug:
                print("未获取到任何数据")
            return pd.DataFrame()

        result = data_json["result"]

        # 获取总记录数 - 从result.count获取
        total_count = result.get("count", 0)
        if total_count == 0:
            if debug:
                print("总记录数为0")
            return pd.DataFrame()

        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size  # 向上取整

        if debug:
            print(f"总记录数: {total_count}")
            print(f"每页数量: {page_size}")
            print(f"总页数: {total_pages}")
            print(f"当前页: {result.get('currentpage', 1)}")
            print(f"是否有下一页: {result.get('nextpage', False)}")

        # 保存第一页数据
        all_data = result["data"]
        if debug:
            print(f"第 1 页获取成功，本页 {len(all_data)} 条")

        # 如果只需要第一页，直接返回
        if not fetch_all:
            if debug:
                print("只获取第一页数据")
        else:
            # 第二步：循环获取剩余页数据（从第2页到total_pages）
            for page in range(2, total_pages + 1):
                # 添加延时，防止频繁请求被封IP
                time.sleep(sleep_time)

                params = {
                    "st": sort_field,
                    "sr": sort_direction,
                    "ps": page_size,
                    "p": page,
                    "sty": ",".join(fields),
                    "filter": f"(TOTAL_MARKET_CAP>{min_market_cap})",
                    "source": "SELECT_SECURITIES",
                    "client": "WEB"
                }

                # 构建完整的URL
                page_url = f"{url}?{urlencode(params)}"
                if debug:
                    print(f"正在获取第 {page}/{total_pages} 页数据...")

                response = requests.get(page_url, headers=headers)

                if debug:
                    print(f"响应状态码: {response.status_code}")

                response.raise_for_status()
                data_json = response.json()

                # 解析 result.data
                if not data_json.get("result") or not data_json["result"].get("data"):
                    if debug:
                        print(f"第 {page} 页无数据，停止获取")
                    break

                page_result = data_json["result"]
                current_page_data = page_result["data"]
                all_data.extend(current_page_data)

                if debug:
                    print(f"第 {page} 页获取成功，本页 {len(current_page_data)} 条，累计 {len(all_data)} 条")

        # 转换为DataFrame
        temp_df = pd.DataFrame(all_data)

        # 重命名列
        column_mapping = {
            "SECUCODE": "证券代码",
            "SECURITY_CODE": "代码",
            "SECURITY_NAME_ABBR": "名称",
            "NEW_PRICE": "最新价",
            "CHANGE_RATE": "涨跌幅",
            "VOLUME_RATIO": "量比",
            "HIGH_PRICE": "最高价",
            "LOW_PRICE": "最低价",
            "PRE_CLOSE_PRICE": "昨收",
            "VOLUME": "成交量",
            "DEAL_AMOUNT": "成交额",
            "TURNOVERRATE": "换手率",
            "TOTAL_MARKET_CAP": "总市值"
        }

        temp_df.rename(columns=column_mapping, inplace=True)

        # 数据类型转换和格式化
        if not temp_df.empty:
            # 涨跌幅保留2位小数
            if "涨跌幅" in temp_df.columns:
                temp_df["涨跌幅"] = round(temp_df["涨跌幅"], 2)

            # 换手率保留2位小数
            if "换手率" in temp_df.columns:
                temp_df["换手率"] = round(temp_df["换手率"], 2)

            # 总市值转换为亿元
            if "总市值" in temp_df.columns:
                temp_df["总市值(亿)"] = round(temp_df["总市值"] / 100000000, 2)
                temp_df.drop("总市值", axis=1, inplace=True)

            # 成交额转换为万元
            if "成交额" in temp_df.columns:
                temp_df["成交额(万)"] = round(temp_df["成交额"] / 10000, 2)
                temp_df.drop("成交额", axis=1, inplace=True)

        # 重置索引
        temp_df.reset_index(drop=True, inplace=True)

        if debug:
            print(f"\n数据获取完成！总计 {len(temp_df)} 条记录")
        return temp_df

    except requests.RequestException as e:
        if debug:
            print(f"请求失败: {e}")
            print(f"异常类型: {type(e).__name__}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"错误响应状态码: {e.response.status_code}")
                print(f"错误响应内容: {e.response.text[:500]}")
        return pd.DataFrame()
    except Exception as e:
        if debug:
            print(f"数据处理失败: {e}")
            print(f"异常类型: {type(e).__name__}")
            import traceback
            traceback.print_exc()
        return pd.DataFrame()


if __name__ == "__main__":
    fund_chip_race_open_df = stock_chip_race_open()
    print(fund_chip_race_open_df)

    fund_chip_race_end_df = stock_chip_race_end()
    print(fund_chip_race_end_df)

    # 测试大市值股票筛选
    large_cap_df = stock_large_cap_filter(page=1, page_size=20)
    print("\n大市值股票筛选:")
    print(large_cap_df)
