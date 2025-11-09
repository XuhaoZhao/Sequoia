# 股票MACD金叉死叉分析系统

这是一个完整的股票日K MACD分析系统，用于分析股票MACD指标的金叉和死叉信号，并统计金叉死叉之间的时间间隔。

## 功能特性

- 📈 **MACD计算**: 自动计算MACD指标（DIF线、DEA线、MACD柱状图）
- 🎯 **信号识别**: 智能识别MACD金叉和死叉信号
- 📊 **统计分析**: 分析金叉死叉间隔的统计特性
- 📈 **数据可视化**: 生成多维度分析图表
- 💾 **数据导出**: 将分析结果导出为CSV文件

## 文件结构

```
Sequoia/
├── data/
│   └── stock_data_2025-11-07.csv    # 股票列表数据
├── stock_macd_analysis.py           # 主要分析脚本
├── run_macd_analysis.py             # 简化运行脚本
├── test_db_connection.py            # 数据库连接测试脚本
├── db_manager.py                    # 数据库管理器（现有）
├── README_MACD_Analysis.md          # 使用说明（本文件）
└── industry_data.db                 # 行业数据SQLite数据库（现有）
```

## 安装依赖

确保安装了以下Python包：

```bash
pip install pandas numpy matplotlib seaborn
```

## 使用方法

### 方法1: 快速开始（推荐）

```bash
python run_macd_analysis.py
```

这个脚本会：
1. 检查必要文件是否存在
2. 如果数据库不存在，自动创建测试数据库
3. 提供交互式菜单选择分析规模
4. 运行分析并生成图表

### 方法2: 测试数据库连接

在运行分析之前，建议先测试数据库连接：

```bash
python test_db_connection.py
```

这个脚本会检查：
- CSV文件是否存在和可读
- 数据库连接是否正常
- 股票信息和K线数据是否存在
- 各种周期的数据可用性

### 方法3: 手动步骤

```bash
python stock_macd_analysis.py
```

## 输出文件

运行分析后会生成以下文件：

1. **macd_intervals_analysis.csv**: 详细的间隔分析数据
   - 包含每只股票的金叉死叉间隔天数
   - 价格变化统计
   - 股票基本信息

2. **分析图表**: 多个PNG文件
   - `macd_golden_to_death_analysis.png`: 金叉到死叉专项分析
   - `macd_death_to_golden_analysis.png`: 死叉到金叉专项分析
   - `macd_comparison_analysis.png`: 两者对比分析
   - `macd_stock_distribution_analysis.png`: 股票分布分析

## 分析指标说明

### MACD参数
- **快线周期**: 12日EMA
- **慢线周期**: 26日EMA
- **信号线周期**: 9日EMA

### 金叉死叉定义
- **金叉**: MACD柱状图从负值变为正值（DIF上穿DEA）
- **死叉**: MACD柱状图从正值变为负值（DIF下穿DEA）

### 统计指标
对于每种间隔类型（金叉到死叉、死叉到金叉）：
- **样本数量**: 该类型间隔的总数量
- **平均天数**: 间隔天数的算术平均值
- **中位数天数**: 间隔天数的中位数
- **最短/最长天数**: 间隔天数的极值
- **标准差**: 间隔天数的离散程度
- **平均价格变化**: 间隔期间的平均价格涨跌幅

## 自定义配置

### 修改MACD参数

在`stock_macd_analysis.py`中，可以修改以下参数：

```python
# 修改MACD计算参数
df = self.calculate_macd(df,
                        fast_period=12,    # 快线周期
                        slow_period=26,    # 慢线周期
                        signal_period=9)   # 信号线周期
```

### 修改数据库连接

```python
# 修改数据库路径
analyzer = StockMACDAnalysis(db_path='your_database.db')

# 修改CSV文件路径
analyzer.run_analysis(csv_path='your_stock_list.csv')
```

## 数据库结构

系统使用现有的 `industry_data.db` SQLite数据库，包含以下表：

### 股票信息表 (`stock_info`)
```sql
CREATE TABLE stock_info (
    code TEXT PRIMARY KEY,      -- 股票代码
    name TEXT NOT NULL,         -- 股票名称
    sector TEXT,               -- 行业分类
    industry TEXT,             -- 细分行业
    created_at TIMESTAMP,      -- 创建时间
    updated_at TIMESTAMP       -- 更新时间
)
```

### K线数据表 (按月分表)
表名格式: `kline_{period}_{year_month}`
```sql
CREATE TABLE kline_1d_2025_11 (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,            -- 股票代码
    name TEXT NOT NULL,            -- 股票名称
    datetime TEXT NOT NULL,        -- 日期时间 (YYYY-MM-DD HH:MM:SS)
    open_price REAL NOT NULL,      -- 开盘价
    high_price REAL NOT NULL,      -- 最高价
    low_price REAL NOT NULL,       -- 最低价
    close_price REAL NOT NULL,     -- 收盘价
    volume INTEGER DEFAULT 0,      -- 成交量
    amount REAL DEFAULT 0,         -- 成交额
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### MACD数据表 (`macd_data`)
```sql
CREATE TABLE macd_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,            -- 产品代码
    name TEXT NOT NULL,            -- 产品名称
    time TEXT NOT NULL,            -- 时间
    macd REAL NOT NULL,            -- MACD值
    signal REAL NOT NULL,          -- 信号线值
    instrument_type TEXT,          -- 产品类型
    signal_type TEXT,              -- 信号类型
    notification_sent INTEGER DEFAULT 0,  -- 通知状态
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## 示例输出

```
=== 股票MACD金叉死叉分析系统 ===

开始股票MACD分析...
成功加载 30 只股票
正在分析股票: 603019 - 中科曙光
正在分析股票: 002493 - 荣盛石化
...

=== 汇总统计 ===
共分析 30 只股票
总共发现 156 个间隔
包含间隔数据的股票数量: 28

=== 按间隔类型统计 ===

金叉到死叉:
  样本数: 78
  平均天数: 25.3
  中位数天数: 23.0
  最短天数: 5
  最长天数: 67
  标准差: 12.1
  涉及股票数: 25
  平均间隔最长的股票: 中科曙光 (35.2天)
  平均间隔最短的股票: 荣盛石化 (18.7天)
```

## 注意事项

1. **数据质量**: 确保日K数据的完整性和准确性
2. **计算周期**: MACD计算需要足够的历史数据（建议至少100个交易日）
3. **交易时间**: 脚本会自动过滤周末数据，但需注意节假日处理
4. **内存使用**: 分析大量股票时可能需要较大内存，建议分批处理

## 故障排除

### 常见问题

1. **找不到CSV文件**
   - 确保`data/stock_data_2025-11-07.csv`文件存在
   - 检查文件路径是否正确

2. **数据库连接失败**
   - 检查SQLite数据库文件权限
   - 运行`create_test_database.py`重新创建数据库

3. **matplotlib显示问题**
   - 在某些环境下可能需要设置显示后端
   - 图表仍会保存为PNG文件

4. **内存不足**
   - 减少`max_stocks`参数
   - 或者分批运行分析

## 扩展功能

可以基于现有代码扩展以下功能：
- 添加其他技术指标（RSI、KDJ等）
- 实现实时数据更新
- 添加回测功能
- 生成分析报告PDF
- 集成到Web应用中

## 许可证

本项目仅供学习和研究使用。