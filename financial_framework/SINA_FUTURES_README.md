# 新浪期货数据采集器

## 概述

本项目包含两个数据采集器，用于从新浪财经获取期货数据：

1. **sina_futures_api_interceptor.py** - 实时期货数据拦截器
2. **sina_futures_kline_collector.py** - K线数据采集器

---

## 1. 实时期货数据拦截器

### 功能
- 拦截新浪期货页面的实时行情数据
- 支持多个期货合约的监听
- 实时打印拦截到的数据
- 支持数据导出到CSV文件

### 目标API
- 实时行情: `http://hq.sinajs.cn/list=期货代码`
- 页面示例: https://finance.sina.com.cn/futures/quotes/TA2605.shtml

### 数据字段
| 字段 | 说明 |
|------|------|
| symbol | 期货代码 |
| name | 品种名称 |
| date | 日期 |
| time | 时间 (HHMMSS) |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 最新价 |
| volume | 成交量 |
| open_interest | 持仓量 |
| change | 涨跌额 |
| change_percent | 涨跌幅(%) |
| bid1 | 买一价 |
| ask1 | 卖一价 |

### 使用方法

#### 基本使用
```python
from financial_framework.sina_futures_api_interceptor import SinaFuturesAPIInterceptor

# 初始化拦截器
interceptor = SinaFuturesAPIInterceptor(
    headless=False,           # 是否无头模式
    log_full_data=False,      # 是否记录完整数据
    log_summary_only=True     # 是否只显示摘要
)

# 开始拦截
interceptor.start_interception(
    symbol="TA2605",      # 期货代码
    duration=60,          # 监听时长(秒)
    check_interval=1      # 检查间隔(秒)
)

# 保存数据
interceptor.save_to_csv("data/sina_futures_data.csv")

# 关闭浏览器
interceptor.close()
```

#### 直接运行
```bash
cd financial_framework
python sina_futures_api_interceptor.py
```

---

## 2. K线数据采集器

### 功能
- 采集多种时间周期的K线数据
- 支持多种K线类型（1分、5分、15分、30分、60分、日K、周K、月K）
- 自动解析并保存到CSV文件

### 目标API
```
https://stock2.finance.sina.com.cn/futures/api/jsonp.php/.../InnerFuturesNewService.getFewMinLine?symbol=期货代码&type=类型
```

### K线类型
| type值 | 说明 |
|--------|------|
| 0 | 1分钟K线 |
| 1 | 5分钟K线 |
| 2 | 15分钟K线 |
| 3 | 30分钟K线 |
| 4 | 60分钟K线 |
| 5 | 日K线 |
| 6 | 周K线 |
| 7 | 月K线 |

### 数据字段
| 字段 | 说明 |
|------|------|
| datetime | 日期时间 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 收盘价 |
| volume | 成交量 |
| open_interest | 持仓量 |

### 使用方法

#### 基本使用
```python
from financial_framework.sina_futures_kline_collector import SinaFuturesKlineCollector

# 初始化采集器
collector = SinaFuturesKlineCollector(log_full_data=False)

# 获取K线数据
kline_data = collector.fetch_kline_data(
    symbol="TA2605",      # 期货代码
    kline_type=1          # K线类型 (0-7)
)

# 保存到CSV
collector.save_to_csv("data/futures_kline_5min.csv")
```

#### 直接运行
```bash
cd financial_framework
python sina_futures_kline_collector.py
```

---

## 输出文件

### 实时数据CSV
- 路径: `data/sina_futures_data.csv`
- 包含所有拦截到的实时行情数据

### K线数据CSV
- 路径: `data/futures_kline_5min.csv`
- 包含指定时间周期的K线数据

---

## 注意事项

1. **日志编码**: 已修复Windows GBK编码问题，所有日志正常显示
2. **JSON解析**: 已修复多行JSON数组解析问题，支持大量数据
3. **浏览器依赖**: 实时拦截器需要Selenium/Chrome Driver
4. **数据更新**: 实时数据会持续更新，建议根据需要设置监听时长

---

## 技术细节

### JSONP响应格式
新浪使用JSONP回调函数返回数据，格式如下：

**实时行情:**
```javascript
var hq_str_nf_TA2605="PTA2605,225959,6790.000,6858.000,...";
```

**K线数据:**
```javascript
/*<script>location.href='//sina.com';</script>*/
var _TA2605_5_1773934511848=([{"d":"2026-02-27 10:05:00","o":"5220.000",...}]);
```

解析器会自动提取并解析这些数据。

---

## 更新日志

### 2026-03-19
- 修复K线数据JSON解析问题
- 修复Windows控制台编码问题
- 优化日志输出格式
- 支持多种K线时间周期
