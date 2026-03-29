# 新浪财经期货实时数据拦截器

## 快速开始

### 安装依赖

```bash
# 1. 安装Python依赖包
pip install -r requirements.txt

# 2. 安装Playwright浏览器驱动 ⚠️ 必须执行
playwright install chromium
```

**详细安装说明请查看**: [SINA_FUTURES_INSTALL.md](SINA_FUTURES_INSTALL.md)

### 运行程序

```bash
# 持续监听模式（手动停止）
python3 sina_futures_interceptor.py

# 测试运行（15秒自动停止）
python3 test_sina_continuous.py
```

## 功能特点

✓ **持续监听** - 不丢失任何一次数据更新
✓ **实时保存** - 每次拦截立即追加到CSV文件
✓ **高频捕获** - 捕获页面所有自动请求（约2次/秒）
✓ **自定义品种** - 自由选择要监听的期货代码
✓ **躲避反爬** - 使用Playwright模拟真实浏览器行为

## 运行方式

### 1. 直接运行（持续监听模式）

```bash
python3 sina_futures_interceptor.py
```

- 浏览器窗口会显示
- 持续监听直到按 `Ctrl+C` 停止
- 数据实时保存到 `data/sina_futures_realtime_YYYYMMDD_HHMMSS.csv`

### 2. 测试运行（15秒自动停止）

```bash
python3 test_sina_continuous.py
```

## 自定义配置

编辑 `sina_futures_interceptor.py` 的 `main()` 函数：

```python
# 自定义要监听的期货代码
custom_symbols = [
    'nf_V2605',   # 聚氯乙烯
    'nf_LC2703',  # 碳酸锂
    'nf_TA2605',  # PTA
    'nf_MA2605',  # 甲醇
    'nf_AG2606',  # 白银
    # 添加更多...
]

# 创建拦截器
interceptor = SinaFuturesInterceptor(
    headless=False,     # True=无头模式（后台运行），False=显示浏览器
    custom_symbols=custom_symbols,
    continuous=True     # True=持续监听，False=定时监听
)
```

## 常见期货代码

- **商品期货**：
  - `nf_V2605` - 聚氯乙烯
  - `nf_LC2703` - 碳酸锂
  - `nf_TA2605` - PTA
  - `nf_MA2605` - 甲醇
  - `nf_AG2606` - 白银
  - `nf_AU2606` - 黄金
  - `nf_CU2606` - 铜

代码格式：`nf_` + 期货品种代码（如V、LC、TA等）+ 交割月份（如2605表示2026年5月）

## 数据字段说明

生成的CSV文件包含以下字段：

| 字段 | 说明 |
|------|------|
| 期货代码 | 期货合约代码 |
| 名称 | 期货品种名称 |
| 合约 | 合约编号 |
| 最新价 | 最新成交价 |
| 昨收 | 昨日收盘价 |
| 今开 | 今日开盘价 |
| 最高 | 今日最高价 |
| 最低 | 今日最低价 |
| 买一 | 买一价 |
| 卖一 | 卖一价 |
| 成交量 | 成交量 |
| 持仓量 | 持仓量 |
| 时间 | 数据更新时间（精确到秒） |
| 序号 | 拦截批次序号 |

## 数据特点

1. **高频实时** - 页面约每0.5秒发起一次请求，拦截器捕获所有请求
2. **时间序列** - 每条数据都带有精确时间戳，适合时间序列分析
3. **完整记录** - 保存所有拦截批次，不丢失任何更新
4. **多品种** - 一次可监听多个期货品种

## 运行统计

运行时会显示：
- 每次拦截的批次和数据量
- 每10次显示详细数据
- 每60次显示运行统计（频率、时长）
- 最终统计总拦截次数和平均频率

示例输出：
```
✓ 第 30 次拦截 - 2 条数据
📊 运行统计: 已拦截 60 次 | 运行时长: 30秒 | 频率: 2.00次/秒

📊 最终统计:
  总拦截次数: 90
  运行时长: 45秒 (0.8分钟)
  平均频率: 2.00次/秒
  数据文件: data/sina_futures_realtime_20260328_092313.csv
```

## 注意事项

1. **网络稳定** - 确保网络连接稳定，否则可能丢失数据
2. **磁盘空间** - 长时间运行会产生大量数据，注意磁盘空间
3. **交易时间** - 建议在期货交易时段运行，数据更新更频繁
4. **手动停止** - 按 `Ctrl+C` 优雅停止，数据会自动保存

## 数据分析建议

可以使用Python pandas进行分析：

```python
import pandas as pd

# 读取数据
df = pd.read_csv('data/sina_futures_realtime_20260328_092313.csv')

# 分析特定期货
v2605 = df[df['期货代码'] == 'V2605']

# 按时间排序
v2605 = v2605.sort_values('时间')

# 查看价格变化
print(v2605[['时间', '最新价', '成交量']].head())
```

## 技术实现

- **Playwright** - 浏览器自动化
- **请求拦截** - 修改API请求参数
- **响应解析** - 解析新浪返回的JavaScript格式
- **异步处理** - asyncio实现高效监听

## 反爬应对

1. 使用真实Chrome浏览器
2. 自动修改请求参数
3. 模拟正常用户访问
4. 保持默认请求头

## 文件说明

- `sina_futures_interceptor.py` - 主程序
- `test_sina_continuous.py` - 测试程序（15秒自动停止）
- `data/` - 数据保存目录
