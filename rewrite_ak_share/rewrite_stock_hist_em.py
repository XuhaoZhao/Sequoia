import pandas as pd
import requests

from akshare.utils.func import fetch_paginated_data

def stock_zh_a_hist(
    symbol: str = "000001",
    period: str = "daily",
    start_date: str = "19700101",
    end_date: str = "20500101",
    adjust: str = "",
    timeout: float = None,
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :param timeout: choice of None or a positive float number
    :type timeout: float
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    market_code = 1 if symbol.startswith("6") else 0
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{market_code}.{symbol}",
        "beg": start_date,
        "end": end_date,
    }
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Cookie": "qgqp_b_id=7799243f024f28375608b6baa2f685d3; st_nvi=sZ5wJjS0mb72RO4Ra3AA455db; nid=050c9a3903a345a659110a21bcb0bf24; nid_create_time=1757815890156; gvi=MCMzBpm5igSdeEdZraAHdb896; gvi_create_time=1757815890156; mtp=1; ct=AsitzKyhX9Y8ey3_om_-rOsbU_JVt5WMKKveYHmgvsk_Cdh4EBKmtQwcBB_JdvKuvcuNmY9JP-GhhGJ2wQ-U3Hky8BK0b5P37K5Azb3OUUp5kaWmUR-g6vnvTv7I8mxg3E9Es-ipGVWY7MDIuAyR0nQ7hhOTTn7jUVcA_dBNK8I; ut=FobyicMgeV7bfas_M05TDKSTU0yNvHLUVqpeFxDIFBZv6ZmcjRbomYslivTeqOJZRooNBknCy9vQsSZmuAZQtm7ngm62q8CfLbPih3iq5v_4ihmyv9E2AI91CZb9APnHRmUwafeg84cf2cv2di7YcTWZftBUA3GBsHgrm0-DA_8q7SfizBtGG2ciL3VVjg_shDmhEFLalOJJ94VbldmI-0uMqElnNIGWyKXEnHxuXN3CEj5nzKDQm1nN-ToMqJXPoO9ieHKrCIgofVzpeq_tiA; pi=6624017083067872%3Bq6624017083067872%3B%E8%82%A1%E5%8F%8B02533W269O%3B9Hbn7c57cB0yKAmu6Luaw8n%2BEhO87uCi8vXeQKNLh6a%2BZR7otocYVWhQHL0JpK%2FAN7vbHXsAkTjqxt6PvhzceudzS1upAx6%2B47IwlwZ4oQGSjqHWoL7KG%2FBYMmnVlt3pojPt9JEzGN%2BJpHMVShmhNvFvBaKCTIy4gBOhWRgJ6tYVlplVKR1jyavNPvculMJu9B9Wq43D%3BzTZSMcG%2F6NyhdRwELh8SmCTUdic3x%2FV053nFKWL2S6eC4J9AFyJIbNb6vxawY2hOdukh9Tj6qhV3p7qWFrOG0ZYL%2FbpNkD9a4LD%2FfJ9EWjwj7glY6lybDFQOSD%2Fbsa%2FGwnMgaJfe0DkaEMnOHnSL%2BVk9yWbDMg%3D%3D; uidal=6624017083067872%e8%82%a1%e5%8f%8b02533W269O; sid=; vtpst=|; fullscreengg=1; fullscreengg2=1; st_si=33631707201462; st_pvi=93805152401706; st_sp=2025-09-14%2010%3A11%3A29; st_inirUrl=https%3A%2F%2Fdata.eastmoney.com%2F; st_sn=128; st_psi=20251108081010355-113200354966-8753069020; st_asi=20251108081010355-113200354966-8753069020-web.xgnhqdy.rk-1",
        "Host": "push2his.eastmoney.com",
        "Referer": "https://quote.eastmoney.com/concept/sh603777.html?from=classic",
        "Sec-Fetch-Dest": "script",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    data_json = r.json()
    if not (data_json["data"] and data_json["data"]["klines"]):
        return pd.DataFrame()
    temp_df = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    temp_df["股票代码"] = symbol
    temp_df.columns = [
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
        "股票代码",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], errors="coerce").dt.date
    temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
    temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
    temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
    temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
    temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
    temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
    temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
    temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
    temp_df = temp_df[
        [
            "日期",
            "股票代码",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ]
    return temp_df

def stock_zh_a_hist_min_em(
    symbol: str = "000001",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
    period: str = "5",
    adjust: str = "",
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日分时行情
    https://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param period: choice of {'1', '5', '15', '30', '60'}
    :type period: str
    :param adjust: choice of {'', 'qfq', 'hfq'}
    :type adjust: str
    :return: 每日分时行情
    :rtype: pandas.DataFrame
    """
    market_code = 1 if symbol.startswith("6") else 0
    adjust_map = {
        "": "0",
        "qfq": "1",
        "hfq": "2",
    }
    headers = {
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Cookie": "qgqp_b_id=7799243f024f28375608b6baa2f685d3; st_nvi=sZ5wJjS0mb72RO4Ra3AA455db; nid=050c9a3903a345a659110a21bcb0bf24; nid_create_time=1757815890156; gvi=MCMzBpm5igSdeEdZraAHdb896; gvi_create_time=1757815890156; mtp=1; ct=AsitzKyhX9Y8ey3_om_-rOsbU_JVt5WMKKveYHmgvsk_Cdh4EBKmtQwcBB_JdvKuvcuNmY9JP-GhhGJ2wQ-U3Hky8BK0b5P37K5Azb3OUUp5kaWmUR-g6vnvTv7I8mxg3E9Es-ipGVWY7MDIuAyR0nQ7hhOTTn7jUVcA_dBNK8I; ut=FobyicMgeV7bfas_M05TDKSTU0yNvHLUVqpeFxDIFBZv6ZmcjRbomYslivTeqOJZRooNBknCy9vQsSZmuAZQtm7ngm62q8CfLbPih3iq5v_4ihmyv9E2AI91CZb9APnHRmUwafeg84cf2cv2di7YcTWZftBUA3GBsHgrm0-DA_8q7SfizBtGG2ciL3VVjg_shDmhEFLalOJJ94VbldmI-0uMqElnNIGWyKXEnHxuXN3CEj5nzKDQm1nN-ToMqJXPoO9ieHKrCIgofVzpeq_tiA; pi=6624017083067872%3Bq6624017083067872%3B%E8%82%A1%E5%8F%8B02533W269O%3B9Hbn7c57cB0yKAmu6Luaw8n%2BEhO87uCi8vXeQKNLh6a%2BZR7otocYVWhQHL0JpK%2FAN7vbHXsAkTjqxt6PvhzceudzS1upAx6%2B47IwlwZ4oQGSjqHWoL7KG%2FBYMmnVlt3pojPt9JEzGN%2BJpHMVShmhNvFvBaKCTIy4gBOhWRgJ6tYVlplVKR1jyavNPvculMJu9B9Wq43D%3BzTZSMcG%2F6NyhdRwELh8SmCTUdic3x%2FV053nFKWL2S6eC4J9AFyJIbNb6vxawY2hOdukh9Tj6qhV3p7qWFrOG0ZYL%2FbpNkD9a4LD%2FfJ9EWjwj7glY6lybDFQOSD%2Fbsa%2FGwnMgaJfe0DkaEMnOHnSL%2BVk9yWbDMg%3D%3D; uidal=6624017083067872%e8%82%a1%e5%8f%8b02533W269O; sid=; vtpst=|; fullscreengg=1; fullscreengg2=1; st_si=33631707201462; st_pvi=93805152401706; st_sp=2025-09-14%2010%3A11%3A29; st_inirUrl=https%3A%2F%2Fdata.eastmoney.com%2F; st_sn=128; st_psi=20251108081010355-113200354966-8753069020; st_asi=20251108081010355-113200354966-8753069020-web.xgnhqdy.rk-1",
    "Host": "push2his.eastmoney.com",
    "Referer": "https://quote.eastmoney.com/concept/sh603777.html?from=classic",
    "Sec-Fetch-Dest": "script",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"macOS\"",
    }
    if period == "1":
        url = "https://push2his.eastmoney.com/api/qt/stock/trends2/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "ndays": "5",
            "iscr": "0",
            "secid": f"{market_code}.{symbol}",
        }

        r = requests.get(url,headers=headers, timeout=15, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["trends"]]
        )
        temp_df.columns = [
            "时间",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "均价",
        ]
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["均价"] = pd.to_numeric(temp_df["均价"], errors="coerce")
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        return temp_df
    else:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": period,
            "fqt": adjust_map[adjust],
            "secid": f"{market_code}.{symbol}",
            "beg": "0",
            "end": "20500000",
        }
        r = requests.get(url,headers=headers, timeout=15, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
        temp_df.columns = [
            "时间",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["时间"])
        temp_df = temp_df[start_date:end_date]
        temp_df.reset_index(drop=True, inplace=True)
        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"], errors="coerce")
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["时间"] = pd.to_datetime(temp_df["时间"]).astype(str)
        temp_df = temp_df[
            [
                "时间",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "换手率",
            ]
        ]
        return temp_df

