# 东方财富选股API拦截器 - 日志配置说明

## 概述

`EastmoneySearchCodeInterceptor` 提供了灵活的日志配置选项，可以根据需要控制日志的详细程度和输出量。

## 配置参数

### 1. `log_full_data` (默认: `False`)

控制是否记录完整的请求和响应数据到数据日志文件。

- **`False` (推荐)**:
  - ✅ 只记录关键摘要信息（URL、状态码、数据统计等）
  - ✅ 大幅减少日志文件大小
  - ✅ 提高性能
  - ❌ 不保存完整的请求头、请求体、响应体

- **`True`**:
  - ✅ 记录完整的请求和响应数据
  - ✅ 便于调试和问题排查
  - ❌ 日志文件可能非常大
  - ❌ 可能影响性能

### 2. `log_summary_only` (默认: `False`)

控制控制台输出的详细程度。

- **`False`** (详细模式):
  - 显示完整的日志信息
  - 包含请求参数、分页信息、数据统计等
  - 适合调试和开发

- **`True` (推荐)** (摘要模式):
  - 只显示关键统计信息
  - 简洁的单行输出
  - 适合生产环境和长时间运行

## 使用示例

### 示例 1: 生产环境（推荐配置）

```python
# 简洁输出，节省空间
interceptor = EastmoneySearchCodeInterceptor(
    headless=False,
    log_full_data=False,      # 不记录完整数据
    log_summary_only=True     # 只显示摘要
)
```

**输出示例：**
```
✓ 拦截响应 #1 | 状态:200 | 大小:15234字符
📊 第一页: 总数123 | 返回50条
📄 分页: 3页 × 50条/页
🔄 请求第2/3页...
  ✓ 第2页成功 (14567字符)
    返回 50 条数据
🔄 请求第3/3页...
  ✓ 第3页成功 (7890字符)
    返回 23 条数据
前5条数据预览:
  1. {"code": "000001", "name": "平安银行", "price": 12.34}
  2. {"code": "000002", "name": "万科A", "price": 15.67}
  ... 还有 121 条数据
```

### 示例 2: 调试模式

```python
# 详细输出，完整数据
interceptor = EastmoneySearchCodeInterceptor(
    headless=False,
    log_full_data=True,       # 记录完整数据
    log_summary_only=False    # 显示详细日志
)
```

**输出示例：**
```
================================================================================
✓ 拦截到第 1 个有效响应
URL: https://np-tjxg-g.eastmoney.com/api/smart-tag/stock/v3/pw/search-code
状态码: 200
响应大小: 15234 字符
✓ 响应数据为JSON格式
  总记录数: 123
  本次返回: 50 条
============================================================
关键请求参数:
  keyWord: 白马股
  pageSize: 50
  pageNo: 1
============================================================
分页信息: 第 1 页, 每页 50 条
✓ API数据已记录到日志
================================================================================
```

### 示例 3: 平衡模式

```python
# 详细输出，但不保存完整数据
interceptor = EastmoneySearchCodeInterceptor(
    headless=False,
    log_full_data=False,      # 不记录完整数据（节省空间）
    log_summary_only=False    # 显示详细日志（便于监控）
)
```

## 日志文件说明

### 控制台日志 (`self.logger`)
- 显示实时进度和关键信息
- 受 `log_summary_only` 参数控制

### 数据日志文件 (`self.data_logger`)
- 保存所有拦截的API数据
- 受 `log_full_data` 参数控制
- 文件路径: `logs/data/*.log`

## 数据记录内容对比

### `log_full_data=False` (默认)

记录的 API 信息包含：
```json
{
  "url": "...",
  "status": 200,
  "method": "POST",
  "intercept_time": "2025-10-17 10:30:45",
  "request_params": {
    "keyWord": "白马股",
    "pageSize": 50,
    "pageNo": 1
  },
  "response_summary": {
    "total": 123,
    "data_count": 50
  }
}
```

### `log_full_data=True`

额外包含：
```json
{
  ..., // 上面所有字段
  "request_headers": {...},
  "request_post_data": "...",
  "request_json": {...},
  "response_body": "...",
  "response_json": {...}
}
```

## 性能和存储对比

| 配置 | 日志文件大小 | 内存占用 | 适用场景 |
|------|-------------|---------|---------|
| `log_full_data=False` + `log_summary_only=True` | 很小 (~MB) | 低 | 生产环境，长期运行 |
| `log_full_data=False` + `log_summary_only=False` | 小 (~MB) | 中 | 开发环境，监控 |
| `log_full_data=True` + `log_summary_only=False` | 很大 (~GB) | 高 | 调试，问题排查 |

## 最佳实践

1. **生产环境**: 使用 `log_full_data=False, log_summary_only=True`
2. **开发调试**: 使用 `log_full_data=True, log_summary_only=False`
3. **定期清理**: 定期清理 `logs/` 目录中的旧日志文件
4. **监控磁盘**: 如果启用 `log_full_data=True`，注意监控磁盘空间

## 关键数据保留

无论配置如何，以下关键数据**始终会被记录**：

- ✅ 请求参数（keyWord, pageSize, pageNo）
- ✅ 响应统计（total, data_count）
- ✅ 分页信息
- ✅ 时间戳
- ✅ 状态码

这确保了即使在最小日志配置下，仍能获取所有关键业务数据。
