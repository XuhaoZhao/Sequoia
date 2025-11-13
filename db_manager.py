import sqlite3
import pandas as pd
import os
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import threading
from contextlib import contextmanager
from functools import lru_cache


class IndustryDataDB:
    """
    行业数据SQLite数据库管理器
    支持按月分表存储1分钟、5分钟、30分钟和日K线数据
    """
    
    def __init__(self, db_path: str = "industry_data.db"):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.db_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
        self._lock = threading.Lock()
        
        # 确保数据库目录存在
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
        
        # 初始化数据库
        self._init_database()
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 支持字典式访问
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_database(self):
        """初始化数据库，创建必要的表"""
        with self.get_connection() as conn:
            # 创建股票/板块信息表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_info (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    sector TEXT,
                    industry TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 创建ETF信息表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS etf_info (
                    etf_code TEXT PRIMARY KEY,
                    etf_type TEXT NOT NULL,
                    etf_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 创建表信息记录表（记录已创建的月度表）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_info (
                    table_name TEXT PRIMARY KEY,
                    period TEXT NOT NULL,
                    year_month TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 创建MACD数据表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS macd_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    time TEXT NOT NULL,
                    macd REAL NOT NULL,
                    signal REAL NOT NULL,
                    instrument_type TEXT,
                    signal_type TEXT,
                    notification_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(code, time, instrument_type, signal_type)
                )
            """)

            # 创建MACD数据表的索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_code ON macd_data(code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_time ON macd_data(time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_code_time ON macd_data(code, time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_instrument_type ON macd_data(instrument_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_signal_type ON macd_data(signal_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_macd_notification_sent ON macd_data(notification_sent)")

            # 创建股票日K结果分析数据表（精细版本）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_k_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    latest_price REAL NOT NULL,

                    -- 均线数据
                    ma5 REAL,
                    ma10 REAL,
                    ma20 REAL,
                    ma60 REAL,
                    ma5_slope REAL,
                    ma10_slope REAL,
                    ma20_slope REAL,
                    ma_arrangement TEXT,
                    ma_signal_strength INTEGER,
                    ma_price_position TEXT,

                    -- 布林带数据
                    bb_upper REAL,
                    bb_middle REAL,
                    bb_lower REAL,
                    bb_position REAL,
                    bb_is_oversold BOOLEAN,
                    distance_to_lower REAL,

                    -- 成交量数据
                    volume_ma5 REAL,
                    volume_percentile REAL,
                    volume_status TEXT,
                    volume_grade INTEGER,
                    volume_trend TEXT,
                    volume_change_rate REAL,
                    volume_z_score REAL,

                    -- ZigZag数据
                    zigzag_key_points INTEGER,
                    zigzag_recent_highs TEXT,
                    zigzag_recent_lows TEXT,

                    -- 分形数据
                    fractal_highs TEXT,
                    fractal_lows TEXT,

                    -- 斐波那契数据
                    fib_swing_high REAL,
                    fib_swing_low REAL,
                    fib_retracement_levels TEXT,
                    fib_current_levels TEXT,

                    -- 综合分析
                    comprehensive_rating TEXT,
                    investment_advice TEXT,
                    signal_count INTEGER,
                    crossover_signals TEXT,
                    turning_signals TEXT,

                    -- 元数据
                    instrument_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(code, analysis_date, instrument_type)
                )
            """)

            # 创建日K分析数据表的索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_code ON daily_k_analysis(code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_date ON daily_k_analysis(analysis_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_code_date ON daily_k_analysis(code, analysis_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_instrument_type ON daily_k_analysis(instrument_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_rating ON daily_k_analysis(comprehensive_rating)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_ma_arrangement ON daily_k_analysis(ma_arrangement)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_k_analysis_volume_status ON daily_k_analysis(volume_status)")

            conn.commit()
    
    def _get_table_name(self, period: str, year_month: str) -> str:
        """
        生成表名

        Args:
            period: 数据周期 ('1m', '5m', '30m' 或 '1d')
            year_month: 年月 (格式: YYYY-MM)

        Returns:
            表名
        """
        return f"kline_{period}_{year_month.replace('-', '_')}"
    
    def _create_kline_table(self, table_name: str, period: str, year_month: str):
        """
        创建K线数据表
        
        Args:
            table_name: 表名
            period: 数据周期
            year_month: 年月
        """
        with self.get_connection() as conn:
            # 创建K线数据表
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume INTEGER DEFAULT 0,
                    amount REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(code, datetime)
                )
            """)
            # 创建索引
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code ON {table_name}(code)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_datetime ON {table_name}(datetime)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code_datetime ON {table_name}(code, datetime)")
            # 记录表信息
            conn.execute("""
                INSERT OR REPLACE INTO table_info (table_name, period, year_month) 
                VALUES (?, ?, ?)
            """, (table_name, period, year_month))
            conn.commit()
    
    def ensure_table_exists(self, period: str, datetime_str: str) -> str:
        """
        确保指定时间的表存在

        Args:
            period: 数据周期 ('1m', '5m', '30m' 或 '1d')
            datetime_str: 时间字符串 (格式: YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD)

        Returns:
            表名
        """
        # 提取年月
        dt = datetime.strptime(datetime_str.split()[0], '%Y-%m-%d')
        year_month = dt.strftime('%Y-%m')

        table_name = self._get_table_name(period, year_month)

        # 检查表是否存在
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name=?
            """, (table_name,))

            if not cursor.fetchone():
                self._create_kline_table(table_name, period, year_month)
        return table_name
    
    def insert_kline_data(self, period: str, data: List[Dict]) -> int:
        """
        插入K线数据

        Args:
            period: 数据周期 ('1m', '5m', '30m' 或 '1d')
            data: 数据列表，每个元素包含: code, name, datetime, open, high, low, close, volume, amount

        Returns:
            成功插入的记录数
        """
        if not data:
            return 0
        
        inserted_count = 0
        
        # 按年月分组数据
        monthly_data = {}
        for record in data:
            dt = record['datetime']
            year_month = datetime.strptime(dt.split()[0], '%Y-%m-%d').strftime('%Y-%m')
            if year_month not in monthly_data:
                monthly_data[year_month] = []
            monthly_data[year_month].append(record)
        # 按月插入数据
        for year_month, records in monthly_data.items():
            table_name = self._get_table_name(period, year_month)
            self.ensure_table_exists(period, records[0]['datetime'])
            with self.get_connection() as conn:
                for record in records:
                    try:
                        conn.execute(f"""
                            INSERT OR REPLACE INTO {table_name} 
                            (code, name, datetime, open_price, high_price, low_price, close_price, volume, amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            record['code'],
                            record['name'],
                            record['datetime'],
                            record['open'],
                            record['high'],
                            record['low'],
                            record['close'],
                            record.get('volume', 0),
                            record.get('amount', 0)
                        ))
                        inserted_count += 1
                    except sqlite3.Error as e:
                        print(f"插入数据失败: {e}, 记录: {record}")
                        continue
                
                conn.commit()
        return inserted_count
    
    def query_kline_data(self, period: str, code: str = None, start_date: str = None,
                        end_date: str = None, limit: int = None) -> pd.DataFrame:
        """
        查询K线数据

        Args:
            period: 数据周期 ('1m', '5m', '30m' 或 '1d')
            code: 股票/板块代码，为None时查询所有
            start_date: 开始日期 (格式: YYYY-MM-DD)
            end_date: 结束日期 (格式: YYYY-MM-DD)
            limit: 限制返回记录数

        Returns:
            包含K线数据的DataFrame
        """
        # 确定需要查询的表
        tables_to_query = self._get_tables_for_date_range(period, start_date, end_date)
        
        if not tables_to_query:
            return pd.DataFrame()
        
        all_data = []
        
        with self.get_connection() as conn:
            for table_name in tables_to_query:
                # 构建查询SQL
                sql = f"SELECT * FROM {table_name} WHERE 1=1"
                params = []
                
                if code:
                    sql += " AND code = ?"
                    params.append(code)
                
                if start_date:
                    sql += " AND datetime >= ?"
                    params.append(f"{start_date} 00:00:00")
                
                if end_date:
                    sql += " AND datetime <= ?"
                    params.append(f"{end_date} 23:59:59")
                
                sql += " ORDER BY datetime"
                
                if limit and len(tables_to_query) == 1:  # 只有一个表时才应用limit
                    sql += f" LIMIT {limit}"
                
                try:
                    df = pd.read_sql_query(sql, conn, params=params)
                    if not df.empty:
                        all_data.append(df)
                except sqlite3.Error as e:
                    print(f"查询表 {table_name} 失败: {e}")
                    continue
        
        if not all_data:
            return pd.DataFrame()
        
        # 合并所有数据
        result_df = pd.concat(all_data, ignore_index=True)
        result_df = result_df.sort_values('datetime').reset_index(drop=True)
        
        # 应用limit（如果有多个表）
        if limit and len(result_df) > limit:
            result_df = result_df.tail(limit)
        
        return result_df
    
    def _get_tables_for_date_range(self, period: str, start_date: str = None, end_date: str = None) -> List[str]:
        """
        获取指定日期范围内的所有表名
        
        Args:
            period: 数据周期
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            表名列表
        """

        with self.get_connection() as conn:
            if start_date is None and end_date is None:
                # 获取所有相关表
                cursor = conn.execute("""
                    SELECT table_name FROM table_info 
                    WHERE period = ? 
                    ORDER BY year_month
                """, (period,))
            else:
                # 根据日期范围获取表
                tables = []
                cursor = conn.execute("""
                    SELECT table_name, year_month FROM table_info 
                    WHERE period = ? 
                    ORDER BY year_month
                """, (period,))
                
                for row in cursor.fetchall():
                    table_name, year_month = row['table_name'], row['year_month']
                    
                    # 检查年月是否在范围内
                    if self._is_month_in_range(year_month, start_date, end_date):
                        tables.append(table_name)
                
                return tables
            
            return [row['table_name'] for row in cursor.fetchall()]
    
    def _is_month_in_range(self, year_month: str, start_date: str = None, end_date: str = None) -> bool:
        """
        检查年月是否在指定日期范围内
        
        Args:
            year_month: 年月 (格式: YYYY-MM)
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            是否在范围内
        """
        if start_date is None and end_date is None:
            return True
        
        # 如果只有一个边界，使用简单比较
        if start_date is None:
            return year_month <= end_date[:7]
        if end_date is None:
            return year_month >= start_date[:7]
        
        # 提取开始和结束月份之间的所有月份
        start_month = start_date[:7]  # YYYY-MM
        end_month = end_date[:7]      # YYYY-MM
        
        # 生成月份列表
        from datetime import datetime
        start_dt = datetime.strptime(start_month, '%Y-%m')
        end_dt = datetime.strptime(end_month, '%Y-%m')
        
        months_in_range = []
        current = start_dt
        while current <= end_dt:
            months_in_range.append(current.strftime('%Y-%m'))
            # 下一个月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        return year_month in months_in_range
    
    def add_or_update_stock_info(self, code: str, name: str, sector: str = None, industry: str = None):
        """
        添加或更新股票/板块信息

        Args:
            code: 代码
            name: 名称
            sector: 行业分类
            industry: 细分行业
        """
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (code, name, sector, industry, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (code, name, sector, industry))
            conn.commit()

    def add_or_update_etf_info(self, etf_code: str, etf_type: str, etf_name: str):
        """
        添加或更新ETF信息
        根据etf_code判断是否存在，存在则更新，不存在则插入

        Args:
            etf_code: ETF代码
            etf_type: ETF类型
            etf_name: ETF名称
        """
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO etf_info (etf_code, etf_type, etf_name, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (etf_code, etf_type, etf_name))
            conn.commit()
    
    
    def get_stock_info(self, code: str = None) -> pd.DataFrame:
        """
        获取股票/板块信息
        
        Args:
            code: 代码，为None时返回所有
        
        Returns:
            股票信息DataFrame
        """
        with self.get_connection() as conn:
            if code:
                sql = "SELECT * FROM stock_info WHERE code = ?"
                return pd.read_sql_query(sql, conn, params=[code])
            else:
                sql = "SELECT * FROM stock_info ORDER BY code"
                return pd.read_sql_query(sql, conn)

    @lru_cache(maxsize=128)
    def get_etf_info(self, etf_code: str = None) -> Dict[str, str]:
        """
        获取ETF信息（带缓存）

        Args:
            etf_code: ETF代码，为None时返回所有

        Returns:
            ETF信息字典，格式为 {etf_code: etf_type}
        """
        with self.get_connection() as conn:
            if etf_code:
                sql = "SELECT * FROM etf_info WHERE etf_code = ?"
                df = pd.read_sql_query(sql, conn, params=[etf_code])
            else:
                sql = "SELECT * FROM etf_info ORDER BY etf_code"
                df = pd.read_sql_query(sql, conn)

            # 转换为字典格式 {etf_code: etf_type}
            result = {}
            for _, row in df.iterrows():
                result[row['etf_code']] = row['etf_type']

            return result

    def get_table_statistics(self) -> pd.DataFrame:
        """
        获取数据库表统计信息
        
        Returns:
            表统计信息DataFrame
        """
        stats = []
        
        with self.get_connection() as conn:
            # 获取所有K线表信息
            cursor = conn.execute("SELECT * FROM table_info ORDER BY period, year_month")
            
            for row in cursor.fetchall():
                table_name = row['table_name']
                
                # 获取表的记录数
                count_cursor = conn.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                count = count_cursor.fetchone()['count']
                
                # 获取时间范围
                time_cursor = conn.execute(f"""
                    SELECT MIN(datetime) as min_time, MAX(datetime) as max_time 
                    FROM {table_name}
                """)
                time_row = time_cursor.fetchone()
                
                stats.append({
                    'table_name': table_name,
                    'period': row['period'],
                    'year_month': row['year_month'],
                    'record_count': count,
                    'min_datetime': time_row['min_time'],
                    'max_datetime': time_row['max_time'],
                    'created_at': row['created_at']
                })
        
        return pd.DataFrame(stats)
    
    def cleanup_old_data(self, keep_months: int = 6):
        """
        清理旧数据（删除超过指定月数的数据表）
        
        Args:
            keep_months: 保留的月数
        """
        cutoff_date = datetime.now().replace(day=1) - pd.DateOffset(months=keep_months)
        cutoff_year_month = cutoff_date.strftime('%Y-%m')
        
        with self.get_connection() as conn:
            # 获取要删除的表
            cursor = conn.execute("""
                SELECT table_name FROM table_info 
                WHERE year_month < ? 
                ORDER BY year_month
            """, (cutoff_year_month,))
            
            tables_to_drop = [row['table_name'] for row in cursor.fetchall()]
            
            # 删除表
            for table_name in tables_to_drop:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute("DELETE FROM table_info WHERE table_name = ?", (table_name,))
                    print(f"已删除表: {table_name}")
                except sqlite3.Error as e:
                    print(f"删除表 {table_name} 失败: {e}")
            
            conn.commit()
            
        print(f"清理完成，删除了 {len(tables_to_drop)} 个表")

    def get_today_30m_data(self, code: str) -> pd.DataFrame:
        """
        根据给定的code，读取当天1分钟数据，然后转换成30分钟数据

        Args:
            code: 股票/板块代码

        Returns:
            转换后的30分钟K线数据DataFrame
        """
        from datetime import date

        # 获取今天的日期
        today = date.today().strftime('%Y-%m-%d')

        # 读取当天1分钟数据
        df_1m = self.query_kline_data(
            period='1m',
            code=code,
            start_date=today,
            end_date=today
        )

        if df_1m.empty:
            print(f"未找到代码 {code} 当天的1分钟数据")
            return pd.DataFrame()

        # 确保datetime列是datetime类型
        df_1m['datetime'] = pd.to_datetime(df_1m['datetime'])
        df_1m = df_1m.sort_values('datetime').reset_index(drop=True)

        # 转换为30分钟数据
        df_30m = self._convert_to_30m(df_1m)

        return df_30m

    def _convert_to_30m(self, df_1m: pd.DataFrame) -> pd.DataFrame:
        """
        将1分钟K线数据转换为30分钟K线数据
        返回与数据库查询结果一致的列名和数据结构

        Args:
            df_1m: 1分钟K线数据DataFrame

        Returns:
            30分钟K线数据DataFrame，列名与数据库表结构一致
        """
        if df_1m.empty:
            return pd.DataFrame()

        # 按30分钟间隔分组（使用ceil符合股票市场K线标准）
        df_1m['time_group'] = df_1m['datetime'].dt.ceil('30min')

        # 分组聚合
        result = []

        for _, group in df_1m.groupby('time_group'):
            if group.empty:
                continue

            # 从该时间段的1分钟数据中取第一个记录的ID作为30分钟数据的ID
            # 这样保持了与原始数据的关联性
            first_record_id = group['id'].iloc[0] if 'id' in group.columns else None

            # 获取30分钟K线的OHLCV数据，使用与数据库一致的列名
            kline_30m = {
                'id': first_record_id,  # 使用原始1分钟数据的第一个ID
                'code': group['code'].iloc[0],
                'name': group['name'].iloc[0],
                'datetime': group['time_group'].iloc[0].strftime('%Y-%m-%d %H:%M:%S'),
                'open_price': group['open_price'].iloc[0],  # 使用与数据库一致的列名
                'high_price': group['high_price'].max(),   # 使用与数据库一致的列名
                'low_price': group['low_price'].min(),     # 使用与数据库一致的列名
                'close_price': group['close_price'].iloc[-1],  # 使用与数据库一致的列名
                'volume': group['volume'].iloc[-1] - group['volume'].iloc[0],  # 成交量差值（增量）
                'amount': group['amount'].iloc[-1] - group['amount'].iloc[0],   # 成交额差值（增量）
                'created_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')  # 添加创建时间字段
            }
            result.append(kline_30m)

        # 转换为DataFrame
        df_30m = pd.DataFrame(result)

        if not df_30m.empty:
            # 按时间排序
            df_30m['datetime'] = pd.to_datetime(df_30m['datetime'])
            df_30m = df_30m.sort_values('datetime').reset_index(drop=True)
            # 格式化datetime列为字符串
            df_30m['datetime'] = df_30m['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            # 确保created_at也是字符串格式
            df_30m['created_at'] = pd.to_datetime(df_30m['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

            # 重新排列列的顺序，使其与数据库查询结果一致
            desired_columns = [
                'id', 'code', 'name', 'datetime',
                'open_price', 'high_price', 'low_price', 'close_price',
                'volume', 'amount', 'created_at'
            ]
            df_30m = df_30m[desired_columns]

        return df_30m

    def insert_macd_data(self, data: List[Dict], instrument_type: str = None, signal_type: str = None) -> int:
        """
        插入MACD数据

        Args:
            data: MACD数据列表，每个元素包含: code, name, time, macd, signal
            instrument_type: 产品类型 (stock, etf, index等)
            signal_type: 信号类型 (金叉, 死叉, 底部收敛等)

        Returns:
            成功插入的记录数
        """
        if not data:
            return 0

        inserted_count = 0
        # 获取当前本地时间
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with self.get_connection() as conn:
            for record in data:
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO macd_data
                        (code, name, time, macd, signal, instrument_type, signal_type, notification_sent, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record['code'],
                        record['name'],
                        record['time'],
                        record['macd'],
                        record['signal'],
                        instrument_type,
                        signal_type,
                        0,  # 默认未发送通知
                        current_time  # 显式插入当前本地时间
                    ))
                    inserted_count += 1
                except sqlite3.Error as e:
                    print(f"插入MACD数据失败: {e}, 记录: {record}")
                    continue

            conn.commit()

        return inserted_count

    def query_macd_data(self, code: str = None, start_time: str = None,
                       end_time: str = None, instrument_type: str = None,
                       signal_type: str = None, limit: int = None) -> pd.DataFrame:
        """
        查询MACD数据

        Args:
            code: 产品代码，为None时查询所有
            start_time: 开始时间 (格式: YYYY-MM-DD HH:MM:SS)
            end_time: 结束时间 (格式: YYYY-MM-DD HH:MM:SS)
            instrument_type: 产品类型 (stock, etf, index等)
            signal_type: 信号类型 (金叉, 死叉, 底部收敛等)
            limit: 限制返回记录数

        Returns:
            包含MACD数据的DataFrame
        """
        with self.get_connection() as conn:
            sql = "SELECT * FROM macd_data WHERE 1=1"
            params = []

            if code:
                sql += " AND code = ?"
                params.append(code)

            if start_time:
                sql += " AND time >= ?"
                params.append(start_time)

            if end_time:
                sql += " AND time <= ?"
                params.append(end_time)

            if instrument_type:
                sql += " AND instrument_type = ?"
                params.append(instrument_type)

            if signal_type:
                sql += " AND signal_type = ?"
                params.append(signal_type)

            sql += " ORDER BY time DESC"

            if limit:
                sql += f" LIMIT {limit}"

            try:
                df = pd.read_sql_query(sql, conn, params=params)
                return df
            except sqlite3.Error as e:
                print(f"查询MACD数据失败: {e}")
                return pd.DataFrame()

    def update_notification_status(self, code: str, time: str, instrument_type: str = None,
                                  signal_type: str = None, sent: bool = True) -> bool:
        """
        更新MACD数据的通知状态

        Args:
            code: 产品代码
            time: 时间
            instrument_type: 产品类型
            signal_type: 信号类型
            sent: 是否已发送通知

        Returns:
            是否更新成功
        """
        with self.get_connection() as conn:
            try:
                sent_value = 1 if sent else 0
                if instrument_type and signal_type:
                    conn.execute("""
                        UPDATE macd_data
                        SET notification_sent = ?
                        WHERE code = ? AND time = ? AND instrument_type = ? AND signal_type = ?
                    """, (sent_value, code, time, instrument_type, signal_type))
                else:
                    conn.execute("""
                        UPDATE macd_data
                        SET notification_sent = ?
                        WHERE code = ? AND time = ?
                    """, (sent_value, code, time))

                conn.commit()
                return conn.total_changes > 0
            except sqlite3.Error as e:
                print(f"更新通知状态失败: {e}")
                return False

    def get_unnotified_data(self, instrument_type: str = None, signal_type: str = None, limit: int = None) -> pd.DataFrame:
        """
        获取未发送通知的MACD数据

        Args:
            instrument_type: 产品类型
            signal_type: 信号类型
            limit: 限制返回记录数

        Returns:
            未发送通知的MACD数据DataFrame
        """
        with self.get_connection() as conn:
            sql = "SELECT * FROM macd_data WHERE notification_sent = 0"
            params = []

            if instrument_type:
                sql += " AND instrument_type = ?"
                params.append(instrument_type)

            if signal_type:
                sql += " AND signal_type = ?"
                params.append(signal_type)

            sql += " ORDER BY time ASC"

            if limit:
                sql += f" LIMIT {limit}"

            try:
                df = pd.read_sql_query(sql, conn, params=params)
                return df
            except sqlite3.Error as e:
                print(f"查询未通知数据失败: {e}")
                return pd.DataFrame()

    def clear_all_macd_data(self) -> int:
        """
        清空所有MACD数据

        Returns:
            成功删除的记录数
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM macd_data")
                deleted_count = cursor.rowcount
                conn.commit()

                print(f"成功清空所有MACD数据，共删除 {deleted_count} 条记录")
                return deleted_count

            except sqlite3.Error as e:
                print(f"清空所有MACD数据失败: {e}")
                return 0

    def insert_daily_k_analysis(self, analysis_data: Dict, instrument_type: str = None) -> bool:
        """
        插入日K分析结果数据

        Args:
            analysis_data: 分析结果字典，包含所有技术指标数据
            instrument_type: 产品类型

        Returns:
            是否插入成功
        """
        try:
            import json

            # 从分析结果中提取关键数据
            basic_info = analysis_data.get("板块名称", "")
            analysis_date = analysis_data.get("最新日期", "")
            latest_price = analysis_data.get("最新收盘价", 0)

            # 提取均线分析数据
            ma_analysis = analysis_data.get("均线分析", {})
            ma_data = ma_analysis.get("均线数值", {})

            # 提取转折点分析中的导数（斜率）数据
            turning_points = analysis_data.get("转折点分析", {})
            derivative_analysis = turning_points.get("导数分析", {})

            # 提取布林带数据
            bb_analysis = analysis_data.get("布林带分析", {})

            # 提取成交量数据
            volume_analysis = analysis_data.get("成交量分析", {})

            # 提取ZigZag数据
            zigzag_analysis = analysis_data.get("ZigZag分析", {})

            # 提取分形数据
            fractal_analysis = analysis_data.get("分形分析", {})

            # 提取斐波那契数据
            fib_analysis = analysis_data.get("斐波那契分析", {})

            # 提取综合分析数据
            signals = analysis_data.get("综合分析信号", [])

            with self.get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO daily_k_analysis
                    (code, name, analysis_date, latest_price,
                     ma5, ma10, ma20, ma60, ma5_slope, ma10_slope, ma20_slope,
                     ma_arrangement, ma_signal_strength, ma_price_position,
                     bb_upper, bb_middle, bb_lower, bb_position, bb_is_oversold, distance_to_lower,
                     volume_ma5, volume_percentile, volume_status, volume_grade, volume_trend,
                     volume_change_rate, volume_z_score,
                     zigzag_key_points, zigzag_recent_highs, zigzag_recent_lows,
                     fractal_highs, fractal_lows,
                     fib_swing_high, fib_swing_low, fib_retracement_levels, fib_current_levels,
                     comprehensive_rating, investment_advice, signal_count, crossover_signals, turning_signals,
                     instrument_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    basic_info,  # code
                    basic_info,  # name (暂时使用相同的值)
                    analysis_date,
                    latest_price,

                    # 均线数据
                    ma_data.get("MA5"),
                    ma_data.get("MA10"),
                    ma_data.get("MA20"),
                    ma_data.get("MA60"),
                    derivative_analysis.get("MA5"),
                    derivative_analysis.get("MA10"),
                    derivative_analysis.get("MA20"),
                    ma_analysis.get("排列状态"),
                    ma_analysis.get("信号强度"),
                    ma_analysis.get("价格位置"),

                    # 布林带数据
                    bb_analysis.get("上轨"),
                    bb_analysis.get("中轨"),
                    bb_analysis.get("下轨"),
                    bb_analysis.get("布林带位置"),
                    bb_analysis.get("是否超跌"),
                    bb_analysis.get("距离下轨百分比"),

                    # 成交量数据
                    volume_analysis.get("当前5日成交量均线"),
                    volume_analysis.get("成交量百分位"),
                    volume_analysis.get("成交量状态"),
                    volume_analysis.get("成交量等级"),
                    volume_analysis.get("成交量趋势"),
                    volume_analysis.get("成交量变化率"),
                    volume_analysis.get("Z分数"),

                    # ZigZag数据
                    zigzag_analysis.get("关键点数量"),
                    json.dumps(zigzag_analysis.get("最近高点", []), ensure_ascii=False),
                    json.dumps(zigzag_analysis.get("最近低点", []), ensure_ascii=False),

                    # 分形数据
                    json.dumps(fractal_analysis.get("分形高点", []), ensure_ascii=False),
                    json.dumps(fractal_analysis.get("分形低点", []), ensure_ascii=False),

                    # 斐波那契数据
                    fib_analysis.get("摆动高点"),
                    fib_analysis.get("摆动低点"),
                    json.dumps(fib_analysis.get("斐波那契回撤位", {}), ensure_ascii=False),
                    json.dumps(fib_analysis.get("当前位置接近的回撤位", []), ensure_ascii=False),

                    # 综合分析
                    analysis_data.get("综合评级"),
                    analysis_data.get("投资建议"),
                    len(signals),
                    json.dumps(analysis_data.get("均线交叉信号", []), ensure_ascii=False),
                    json.dumps(turning_points.get("转折信号", []), ensure_ascii=False),

                    instrument_type
                ))

                conn.commit()
                return True

        except Exception as e:
            print(f"插入日K分析数据失败: {e}")
            return False

    def query_daily_k_analysis(self, code: str = None, start_date: str = None,
                              end_date: str = None, instrument_type: str = None,
                              rating: str = None, limit: int = None) -> pd.DataFrame:
        """
        查询日K分析结果数据

        Args:
            code: 产品代码，为None时查询所有
            start_date: 开始日期 (格式: YYYY-MM-DD)
            end_date: 结束日期 (格式: YYYY-MM-DD)
            instrument_type: 产品类型
            rating: 综合评级 (强烈超跌、可能超跌、观望、正常)
            limit: 限制返回记录数

        Returns:
            包含分析结果的DataFrame
        """
        with self.get_connection() as conn:
            sql = "SELECT * FROM daily_k_analysis WHERE 1=1"
            params = []

            if code:
                sql += " AND code = ?"
                params.append(code)

            if start_date:
                sql += " AND analysis_date >= ?"
                params.append(start_date)

            if end_date:
                sql += " AND analysis_date <= ?"
                params.append(end_date)

            if instrument_type:
                sql += " AND instrument_type = ?"
                params.append(instrument_type)

            if rating:
                sql += " AND comprehensive_rating = ?"
                params.append(rating)

            sql += " ORDER BY analysis_date DESC, code"

            if limit:
                sql += f" LIMIT {limit}"

            try:
                df = pd.read_sql_query(sql, conn, params=params)
                return df
            except sqlite3.Error as e:
                print(f"查询日K分析数据失败: {e}")
                return pd.DataFrame()

    def get_latest_analysis_by_code(self, code: str, instrument_type: str = None) -> Dict:
        """
        根据代码获取最新的分析结果

        Args:
            code: 产品代码
            instrument_type: 产品类型

        Returns:
            最新分析结果字典
        """
        with self.get_connection() as conn:
            sql = """
                SELECT * FROM daily_k_analysis
                WHERE code = ?
            """
            params = [code]

            if instrument_type:
                sql += " AND instrument_type = ?"
                params.append(instrument_type)

            sql += " ORDER BY analysis_date DESC LIMIT 1"

            try:
                cursor = conn.execute(sql, params)
                row = cursor.fetchone()

                if row:
                    # 转换为字典格式
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, row))
                else:
                    return {}

            except sqlite3.Error as e:
                print(f"获取最新分析结果失败: {e}")
                return {}

    def get_analysis_statistics(self, analysis_date: str = None,
                               instrument_type: str = None) -> Dict:
        """
        获取分析结果统计信息

        Args:
            analysis_date: 分析日期，为None时使用最新日期
            instrument_type: 产品类型

        Returns:
            统计信息字典
        """
        with self.get_connection() as conn:
            try:
                if analysis_date is None:
                    # 获取最新分析日期
                    cursor = conn.execute("SELECT MAX(analysis_date) FROM daily_k_analysis")
                    latest_date = cursor.fetchone()[0]
                    if not latest_date:
                        return {"error": "没有找到分析数据"}
                    analysis_date = latest_date

                sql = """
                    SELECT
                        COUNT(*) as total_count,
                        COUNT(CASE WHEN comprehensive_rating = '强烈超跌' THEN 1 END) as strong_oversold,
                        COUNT(CASE WHEN comprehensive_rating = '可能超跌' THEN 1 END) as possible_oversold,
                        COUNT(CASE WHEN comprehensive_rating = '观望' THEN 1 END) as观望,
                        COUNT(CASE WHEN comprehensive_rating = '正常' THEN 1 END) as normal,
                        AVG(ma5_slope) as avg_ma5_slope,
                        AVG(volume_change_rate) as avg_volume_change,
                        AVG(bb_position) as avg_bb_position
                    FROM daily_k_analysis
                    WHERE analysis_date = ?
                """
                params = [analysis_date]

                if instrument_type:
                    sql += " AND instrument_type = ?"
                    params.append(instrument_type)

                cursor = conn.execute(sql, params)
                stats = cursor.fetchone()

                return {
                    "analysis_date": analysis_date,
                    "instrument_type": instrument_type,
                    "total_count": stats[0],
                    "strong_oversold": stats[1],
                    "possible_oversold": stats[2],
                    "观望": stats[3],
                    "normal": stats[4],
                    "avg_ma5_slope": round(stats[5], 4) if stats[5] else 0,
                    "avg_volume_change": round(stats[6], 2) if stats[6] else 0,
                    "avg_bb_position": round(stats[7], 2) if stats[7] else 0
                }

            except sqlite3.Error as e:
                return {"error": f"获取统计信息失败: {e}"}

