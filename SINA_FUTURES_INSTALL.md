# 新浪期货拦截器 - 安装指南

## 安装步骤

### 1. 安装Python依赖包

```bash
pip install -r requirements.txt
```

这会安装包括 `playwright==1.58.0` 在内的所有Python包。

### 2. 安装Playwright浏览器驱动 ⚠️ 重要

**必须单独运行此命令**，下载Chromium浏览器：

```bash
playwright install chromium
```

如果需要安装所有支持的浏览器：
```bash
playwright install
```

### 3. 验证安装

运行测试脚本验证安装是否成功：

```bash
python3 test_sina_continuous.py
```

## requirements.txt 中Playwright相关依赖

```
playwright==1.58.0
```

这个包提供了Playwright的Python接口，但浏览器驱动需要单独安装。

## 常见问题

### Q: 为什么要运行两次安装？

A:
- `pip install` 安装的是Python代码库
- `playwright install` 下载的是实际的浏览器程序（Chromium, ~170MB）

### Q: 可以使用系统已安装的Chrome吗？

A: 不建议。Playwright需要使用它自己下载的特定版本浏览器以确保功能正常。

### Q: 浏览器驱动安装在哪里？

A: 不同系统位置不同：
- macOS: `~/Library/Caches/ms-playwright/`
- Linux: `~/.cache/ms-playwright/`
- Windows: `%USERPROFILE%\AppData\Local\ms-playwright\`

### Q: 如何卸载浏览器驱动？

```bash
playwright uninstall chromium
# 或卸载所有
playwright uninstall
```

## 完整的首次运行流程

```bash
# 1. 克隆或下载项目后
cd /path/to/Sequoia

# 2. 安装Python依赖
pip install -r requirements.txt

# 3. 安装Playwright浏览器（必须！）
playwright install chromium

# 4. 测试运行
python3 test_sina_continuous.py

# 5. 正式运行
python3 sina_futures_interceptor.py
```

## Docker 部署

如果使用Docker，需要在Dockerfile中添加：

```dockerfile
# 安装Python依赖
RUN pip install -r requirements.txt

# 安装Playwright浏览器
RUN playwright install chromium

# 或者安装所有浏览器（更大）
# RUN playwright install --with-deps chromium
```

## 系统依赖

某些Linux系统可能需要额外的系统库：

```bash
# Ubuntu/Debian
sudo apt-get install -y \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2
```

macOS和Windows通常不需要额外安装系统依赖。
