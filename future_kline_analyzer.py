"""
期货价格K线分析器
==================

功能概述：
    1. 获取活跃主力合约列表（基于保证金、手续费、成交额筛选）
    2. 从 futures_5m 分表获取历史5分钟K线数据（最新1000条）
    3. 从 sina_futures_realtime 表获取当日实时行情（2s间隔tick），聚合成5分钟K线
    4. 合并历史与实时K线数据，构成完整的5分钟K线序列
    5. 计算波动比率：最新K线实体 / 前20根K线平均实体，按比值排名
    6. 在每个5分钟时间点自动输出排名到控制台

使用方法：
    python future_kline_analyzer.py

注意事项：
    - 需要先运行 sina_futures_interceptor.py 以收集实时数据
    - 需要先运行 futures_5m_collector.py 以收集历史K线数据
    - 程序会持续运行，按 Ctrl+C 停止
    - 后续可自行修改参数（lookback、historical_limit、check_interval 等）
"""

import re
import time
import pandas as pd
from datetime import datetime
from db_manager import IndustryDataDB


class FuturesKlineAnalyzer:
    """
    期货K线波动比率分析器

    通过分析5分钟K线的实体大小（|收盘价 - 开盘价|），
    计算最新K线波动与前N根K线平均波动的比值，对合约进行实时排名。
    比值越大说明当前K线的波动相对于近期平均波动越剧烈。
    """

    def __init__(self, lookback=20, historical_limit=1000):
        """
        初始化分析器

        Args:
            lookback:          回看K线数量，用于计算平均波动，默认20
            historical_limit:  历史K线最大条数，默认1000
        """
        self.db = IndustryDataDB()
        self.lookback = lookback
        self.historical_limit = historical_limit
        self.historical_cache = {}      # 缓存历史K线: {contract_code: DataFrame}
        self.last_ranked_window = None  # 上次输出排名的5分钟窗口（防止重复输出）
        self.contracts = []             # 活跃主力合约列表

    # =========================================================================
    # 第一步：获取合约列表
    # =========================================================================

    def get_active_contracts(self, min_amount=20.0, max_margin=50000.0, max_fee=30.0):
        """
        获取活跃主力合约列表

        通过 db.get_active_main_contracts_simple 查询符合条件的主力合约。
        筛选条件：成交额 >= min_amount亿、保证金 < max_margin元、手续费 < max_fee元。

        Args:
            min_amount:  最小成交额（亿元），默认20亿
            max_margin:  最大一手保证金（元），默认5万
            max_fee:     最大交易一手费率（开仓+平仓，元），默认30元

        Returns:
            合约字典列表，格式: [{"contract_code": "ag2606", "contract_name": "ag2606"}, ...]
        """
        self.contracts = self.db.get_active_main_contracts_simple(
            min_amount=min_amount,
            max_margin=max_margin,
            max_fee=max_fee,
            use_volume_filter=True
        )
        return self.contracts

    # =========================================================================
    # 第二步：获取历史K线数据（从 futures_5m 分表）
    # =========================================================================

    def _get_futures_5m_tables(self):
        """
        从 table_info 元数据表中获取所有 futures_5m 分表名，按月份倒序排列

        futures_5m 表按月分表，命名格式为 futures_5m_YYYY_MM（如 futures_5m_2026_04）。

        Returns:
            分表名列表，如 ['futures_5m_2026_04', 'futures_5m_2026_03', ...]
        """
        with self.db.get_connection() as conn:
            try:
                sql = """
                    SELECT table_name FROM table_info
                    WHERE period = 'futures_5m'
                    ORDER BY year_month DESC
                """
                df = pd.read_sql_query(sql, conn)
                return df['table_name'].tolist()
            except Exception:
                return []

    def get_historical_5m_klines(self, contract_code):
        """
        从 futures_5m 分表中获取指定合约的最新N条5分钟K线数据

        查询策略：
            由于 futures_5m 是按月分表的，需要从多个表中聚合数据。
            从最近的月份表开始查询，逐步向前查找，直到收集到足够的数据。
            注意：futures_5m 表中合约代码大小写不统一（如 ni2605 和 MA2605），
            因此使用 UPPER() 进行大小写无关匹配。

        Args:
            contract_code: 合约代码，如 'ag2606'、'MA605'

        Returns:
            DataFrame，列: [datetime, open_price, high_price, low_price, close_price, volume, amount]
            按 datetime 升序排列
        """
        # 构建 LIKE 模式：MA605 -> MA%05，可同时匹配 MA605 和 MA2605
        like_pattern = self._build_contract_like_pattern(contract_code)

        tables = self._get_futures_5m_tables()
        if not tables:
            return pd.DataFrame()

        all_data = []
        with self.db.get_connection() as conn:
            for table_name in tables:
                try:
                    # LIKE 匹配合约前缀，同时排除带F后缀的记录
                    sql = f"""
                        SELECT datetime, open_price, high_price, low_price,
                               close_price, volume, amount
                        FROM {table_name}
                        WHERE UPPER(contract_code) LIKE UPPER(?)
                          AND UPPER(contract_code) NOT LIKE '%F'
                        ORDER BY datetime DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(sql, conn, params=[like_pattern, self.historical_limit])
                    if not df.empty:
                        all_data.append(df)
                        # 如果已收集足够数据，提前退出
                        total = sum(len(d) for d in all_data)
                        if total >= self.historical_limit:
                            break
                except Exception:
                    # 表可能不存在或结构异常，跳过
                    continue

        if not all_data:
            return pd.DataFrame()

        # 合并、去重（按 datetime）、排序、截取最新N条
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values('datetime')
        result = result.drop_duplicates(subset='datetime', keep='last')
        result = result.tail(self.historical_limit).reset_index(drop=True)
        return result

    def cache_historical_data(self):
        """
        批量缓存所有合约的历史K线数据

        在启动时调用一次，后续运行中不再重复查询历史数据。
        """
        for contract in self.contracts:
            code = contract['contract_code']
            klines = self.get_historical_5m_klines(code)
            self.historical_cache[code] = klines

    # =========================================================================
    # 第三步：聚合实时数据为5分钟K线（从 sina_futures_realtime）
    # =========================================================================

    def aggregate_realtime_to_5m(self, contract_code, exclude_current_window=True):
        """
        从 sina_futures_realtime 获取当日实时行情数据，聚合成5分钟K线

        本方法将实时 tick 数据聚合为5分钟OHLC K线。
        注意：实时表中的合约代码可能带后缀（如 L2606F、V2605F），
        需要用 LIKE 匹配来处理。

        聚合逻辑：
            - 将每条tick的时间戳 floor 到最近的5分钟整点（如 10:03 → 10:00, 10:07 → 10:05）
            - 按5分钟窗口分组
            - Open  = 窗口内第一条 tick 的 latest_price
            - High  = 窗口内所有 tick 的 latest_price 最大值
            - Low   = 窗口内所有 tick 的 latest_price 最小值
            - Close = 窗口内最后一条 tick 的 latest_price
            - Volume = 窗口内成交量增量（最后一条累计量 - 第一条累计量）

        Args:
            contract_code:          合约代码，如 'l2606'
            exclude_current_window: 是否排除正在形成的当前5分钟窗口，默认True

        Returns:
            DataFrame，列: [datetime, open_price, high_price, low_price, close_price, volume]
            按 datetime 升序排列
        """
        today = datetime.now().strftime('%Y-%m-%d')

        # 构建 LIKE 模式：MA605 -> MA%05，可同时匹配 MA605 和 MA2605
        like_pattern = self._build_contract_like_pattern(contract_code)

        # 查询当日实时数据
        # LIKE 匹配合约前缀，同时排除带F后缀的记录（如 PP2605F）
        with self.db.get_connection() as conn:
            sql = """
                SELECT * FROM sina_futures_realtime
                WHERE UPPER(contract_code) LIKE UPPER(?)
                  AND UPPER(contract_code) NOT LIKE '%F'
                  AND datetime >= ?
                ORDER BY datetime ASC, sequence ASC
            """
            df = pd.read_sql_query(sql, conn, params=[like_pattern, f"{today} 00:00:00"])

        if df.empty or 'latest_price' not in df.columns:
            return pd.DataFrame()

        # 过滤掉 latest_price 为空的数据
        df = df.dropna(subset=['latest_price'])
        if df.empty:
            return pd.DataFrame()

        # 计算时间窗口并聚合
        df['datetime_parsed'] = pd.to_datetime(df['datetime'])
        df['window'] = df['datetime_parsed'].dt.floor('5min')

        # 按5分钟窗口聚合
        grouped = df.groupby('window').agg(
            open_price=('latest_price', 'first'),
            high_price=('latest_price', 'max'),
            low_price=('latest_price', 'min'),
            close_price=('latest_price', 'last'),
            volume=('volume', self._calc_volume_delta)
        )

        # 用窗口起始时间作为K线时间
        result = grouped.copy()
        result['datetime'] = result.index.strftime('%Y-%m-%d %H:%M:%S')
        result = result.reset_index(drop=True)

        # 选取需要的列
        cols = ['datetime', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
        result = result[[c for c in cols if c in result.columns]]

        # 排除正在形成的当前K线（当前5分钟窗口的数据还不完整）
        if exclude_current_window:
            current_window_str = self._get_current_5min_window_str()
            result = result[result['datetime'] < current_window_str]

        return result

    @staticmethod
    def _calc_volume_delta(series):
        """
        计算窗口内成交量增量

        sina_futures_realtime 中的 volume 是累计成交量（整个交易日的累计），
        因此窗口内的实际成交量 = 窗口最后一条的累计量 - 窗口第一条的累计量。

        Args:
            series: 一个5分钟窗口内所有 tick 的 volume 值

        Returns:
            成交量增量（整数），如果窗口内数据不足2条则返回0
        """
        if len(series) < 2:
            return 0
        try:
            delta = int(series.iloc[-1]) - int(series.iloc[0])
            return max(delta, 0)  # 成交量不应为负数
        except (ValueError, TypeError):
            return 0

    # =========================================================================
    # 第四步：合并K线数据
    # =========================================================================

    def merge_klines(self, historical_klines, realtime_5m_klines):
        """
        合并历史K线和实时5分钟K线数据

        合并策略：
            - 将两组数据按 datetime 合并
            - 重复时间点保留最后一条（优先使用实时聚合数据，因为更精确）
            - 按 datetime 升序排列

        Args:
            historical_klines:  历史K线 DataFrame（来自 futures_5m 分表）
            realtime_5m_klines: 实时聚合K线 DataFrame（来自 sina_futures_realtime）

        Returns:
            合并后的 DataFrame
        """
        if historical_klines.empty and realtime_5m_klines.empty:
            return pd.DataFrame()
        if historical_klines.empty:
            return realtime_5m_klines
        if realtime_5m_klines.empty:
            return historical_klines

        # 合并并去重
        merged = pd.concat([historical_klines, realtime_5m_klines], ignore_index=True)
        merged = merged.sort_values('datetime')
        merged = merged.drop_duplicates(subset='datetime', keep='last')
        merged = merged.reset_index(drop=True)
        return merged

    # =========================================================================
    # 第五步：计算波动比率并排名
    # =========================================================================

    def calculate_kline_ratio(self, klines):
        """
        计算单合约的K线波动比率

        计算方法：
            1. 取最后 lookback + 1 根K线
            2. 前 lookback 根K线：计算每根K线的 |收盘价 - 开盘价|，取平均值 → avg_range
            3. 最新1根K线：计算 |收盘价 - 开盘价| → latest_range
            4. 比值 = latest_range / avg_range

        含义：
            比值 > 1 表示当前波动大于近期平均水平（波动放大）
            比值 < 1 表示当前波动小于近期平均水平（波动缩小）
            比值 = 0 表示当前K线是十字星（开盘价 ≈ 收盘价）

        Args:
            klines: K线数据 DataFrame，需包含 open_price, close_price 列

        Returns:
            float: 波动比率（保留4位小数），数据不足时返回 None
        """
        if klines is None or len(klines) < self.lookback + 1:
            return None

        recent = klines.tail(self.lookback + 1)

        # 前 lookback 根K线的平均实体大小
        prev_bars = recent.head(self.lookback).copy()
        prev_bars['bar_range'] = (prev_bars['close_price'] - prev_bars['open_price']).abs()
        avg_range = prev_bars['bar_range'].mean()

        if avg_range == 0:
            return None  # 避免除以零

        # 最新K线的实体大小
        latest_bar = recent.tail(1).iloc[0]
        latest_range = abs(latest_bar['close_price'] - latest_bar['open_price'])

        return round(latest_range / avg_range, 4)

    def analyze_and_rank(self, all_klines):
        """
        分析所有合约的K线波动比率并排名

        Args:
            all_klines: dict，{contract_code: merged_klines_dataframe}

        Returns:
            排名结果 DataFrame，包含以下列：
            - contract_code: 合约代码
            - ratio:         波动比率
            - latest_open:   最新K线开盘价
            - latest_close:  最新K线收盘价
            - latest_range:  最新K线实体大小
            - avg_range:     前20根K线平均实体大小
            - kline_time:    最新K线时间（HH:MM:SS）
            按 ratio 降序排列，index 为排名
        """
        results = []
        for contract_code, klines in all_klines.items():
            ratio = self.calculate_kline_ratio(klines)
            if ratio is not None:
                latest = klines.tail(1).iloc[0]
                latest_range = abs(latest['close_price'] - latest['open_price'])
                results.append({
                    'contract_code': contract_code,
                    'ratio': ratio,
                    'latest_open': round(latest['open_price'], 2),
                    'latest_close': round(latest['close_price'], 2),
                    'latest_range': round(latest_range, 2),
                    'avg_range': round(latest_range / ratio, 2) if ratio != 0 else 0,
                    'kline_time': str(latest['datetime'])[-8:] if len(str(latest['datetime'])) >= 8 else str(latest['datetime']),
                })

        if not results:
            return pd.DataFrame()

        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('ratio', ascending=False).reset_index(drop=True)
        result_df.index += 1  # 排名从1开始
        result_df.index.name = 'rank'
        return result_df

    # =========================================================================
    # 辅助方法
    # =========================================================================

    @staticmethod
    def _build_contract_like_pattern(contract_code):
        """
        构建合约代码的 LIKE 匹配模式，兼容不同表中的编码差异

        不同表中的合约编码格式可能不同：
        - futures_contracts:     MA605 (3位数字: 年位+月份)
        - futures_5m:            MA2605 (4位数字: 年份+月份)
        - sina_futures_realtime: MA605 (3位数字) 或 MA2605 (4位数字)

        对于3位数字代码如 MA605，提取品种前缀 MA 和月份 05，
        生成模式 MA%05 可同时匹配 MA605 和 MA2605。

        对于4位数字代码如 ag2606，直接使用 AG2605% 精确匹配。

        Args:
            contract_code: 合约代码，如 'MA605'、'ag2606'

        Returns:
            LIKE 模式字符串，如 'MA%05'、'AG2606%'
        """
        code = contract_code.strip().upper()
        match = re.match(r'^([A-Za-z]+)(\d+)$', code)
        if not match:
            return code + '%'

        variety = match.group(1)
        digits = match.group(2)

        if len(digits) == 3:
            # 3位数字: MA605 -> MA%05，同时匹配 MA605 和 MA2605
            month = digits[-2:]
            return f"{variety}%{month}"
        else:
            # 4位及以上数字: ag2606 -> AG2606%
            return code + '%'

    @staticmethod
    def _get_current_5min_window(dt=None):
        """
        获取当前时间所在的5分钟窗口的起始时间

        将时间向下取整到最近的5分钟整点。
        例如：10:03 → 10:00，10:07 → 10:05，10:10 → 10:10

        Args:
            dt: datetime 对象，默认为当前时间

        Returns:
            datetime 对象，表示5分钟窗口的起始时间
        """
        if dt is None:
            dt = datetime.now()
        return dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)

    @staticmethod
    def _get_current_5min_window_str(dt=None):
        """
        获取当前5分钟窗口的字符串表示

        Returns:
            str，格式 'YYYY-MM-DD HH:MM:SS'
        """
        window = FuturesKlineAnalyzer._get_current_5min_window(dt)
        return window.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def print_ranking(ranking_df, timestamp=None):
        """
        格式化打印排名结果到控制台

        Args:
            ranking_df: analyze_and_rank() 返回的排名结果 DataFrame
            timestamp:  时间戳字符串，默认使用当前时间
        """
        if ranking_df.empty:
            print("  数据不足，无法计算排名")
            return

        if timestamp is None:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        separator = "=" * 95
        print(f"\n{separator}")
        print(f"  Futures K-Line Volatility Ratio Ranking  |  {timestamp}")
        print(f"{separator}")
        print(ranking_df.to_string())
        print(f"{separator}")

    # =========================================================================
    # 主循环
    # =========================================================================

    def run(self, check_interval=5, data_wait_seconds=3, max_wait_retries=5):
        """
        主循环：持续监控并在5分钟时间点输出排名

        运行逻辑：
            1. [初始化] 获取合约列表，加载历史K线数据（只执行一次）
            2. [循环]   每隔 check_interval 秒检查一次
            3. 当检测到跨过5分钟边界时（在边界后的前几秒内触发一次）：
               a. 等待 data_wait_seconds 秒让实时数据写入数据库
               b. 聚合所有合约的实时数据为5分钟K线
               c. 与历史K线合并，构成完整序列
               d. 计算波动比率并排名
               e. 输出排名到控制台
            4. 如果最新K线数据未到达，最多重试 max_wait_retries 次（每次间隔2秒）

        Args:
            check_interval:    循环检查间隔（秒），默认5秒
                               实时数据每2秒更新一次，5秒间隔可保证不遗漏
            data_wait_seconds: 检测到5分钟边界后，等待数据到达的秒数，默认3秒
                               给 sina_futures_interceptor 留出写入时间
            max_wait_retries:  数据未到达时的最大重试次数，默认5次
        """
        print("=" * 95)
        print("  Futures K-Line Volatility Ratio Analyzer")
        print("=" * 95)

        # --- 初始化阶段 ---
        print("\n[Step 1/2] Fetching active main contracts...")
        contracts = self.get_active_contracts()
        if not contracts:
            print("  No eligible contracts found. Exiting.")
            return
        print(f"  Found {len(contracts)} active main contracts:")
        for c in contracts:
            print(f"    - {c['contract_code']}")

        print("\n[Step 2/2] Loading historical 5-min kline data...")
        self.cache_historical_data()
        for code, klines in self.historical_cache.items():
            print(f"  {code}: {len(klines)} bars loaded")

        print(f"\n{'=' * 95}")
        print("  Real-time monitoring started (Press Ctrl+C to stop)")
        print(f"  Check interval: {check_interval}s | Data wait: {data_wait_seconds}s | Lookback: {self.lookback}")
        print(f"{'=' * 95}\n")

        # --- 主循环 ---
        while True:
            try:
                now = datetime.now()
                current_window = self._get_current_5min_window(now)

                # 判断是否刚刚跨过5分钟边界
                # 条件：当前窗口与上次不同，且在边界后的前 (check_interval + data_wait_seconds + 额外缓冲) 秒内
                boundary_buffer = check_interval + data_wait_seconds + 5
                crossed_boundary = (
                    current_window != self.last_ranked_window
                    and now.second < boundary_buffer
                )

                if crossed_boundary:
                    # 等待实时数据写入数据库
                    time.sleep(data_wait_seconds)

                    # 聚合所有合约的实时数据并合并历史数据
                    all_klines = self._build_merged_klines()

                    # 检查是否有足够的数据（至少 lookback + 1 根K线）
                    has_enough = any(
                        len(klines) >= self.lookback + 1
                        for klines in all_klines.values()
                    )

                    # 如果数据不足，等待并重试
                    if not has_enough:
                        for retry in range(1, max_wait_retries + 1):
                            print(f"  [{now.strftime('%H:%M:%S')}] "
                                  f"Waiting for latest kline data... (retry {retry}/{max_wait_retries})")
                            time.sleep(2)
                            all_klines = self._build_merged_klines()
                            has_enough = any(
                                len(klines) >= self.lookback + 1
                                for klines in all_klines.values()
                            )
                            if has_enough:
                                break

                    # 输出排名
                    if has_enough:
                        ranking = self.analyze_and_rank(all_klines)
                        self.print_ranking(ranking)
                    else:
                        print(f"  [{now.strftime('%H:%M:%S')}] "
                              f"Insufficient data, skipping this window.")

                    # 无论成功与否，更新 last_ranked_window 避免重复触发
                    self.last_ranked_window = current_window

                time.sleep(check_interval)

            except KeyboardInterrupt:
                print("\n\nProgram stopped by user.")
                break
            except Exception as e:
                print(f"\n[Error] {e}")
                import traceback
                traceback.print_exc()
                time.sleep(check_interval)

    def _build_merged_klines(self):
        """
        构建所有合约的合并K线数据

        对每个合约：
            1. 聚合实时tick数据为5分钟K线（排除正在形成的当前窗口）
            2. 与缓存的历史K线合并
            3. 去重并排序

        Returns:
            dict: {contract_code: merged_klines_dataframe}
        """
        all_klines = {}
        for contract in self.contracts:
            code = contract['contract_code']
            historical = self.historical_cache.get(code, pd.DataFrame())
            realtime_5m = self.aggregate_realtime_to_5m(code, exclude_current_window=True)
            merged = self.merge_klines(historical, realtime_5m)
            all_klines[code] = merged
        return all_klines


# =============================================================================
# 程序入口
# =============================================================================

if __name__ == '__main__':
    analyzer = FuturesKlineAnalyzer(
        lookback=20,           # 回看20根K线计算平均波动
        historical_limit=1000  # 加载最新1000条历史K线
    )
    analyzer.run(
        check_interval=5,         # 每5秒检查一次是否跨过5分钟边界
        data_wait_seconds=3,      # 边界后等待3秒让数据到达
        max_wait_retries=5        # 数据未到最多重试5次（每次间隔2秒）
    )
