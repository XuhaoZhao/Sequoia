"""
macOS打包脚本 - Build for macOS
使用PyInstaller将Python应用程序打包为macOS应用
"""
import os
import sys
import shutil
import subprocess
from pathlib import Path

def check_dependencies():
    """检查依赖包是否安装"""
    required_packages = [
        'pyinstaller',
        'matplotlib',
        'pandas',
        'numpy',
        'akshare',
        'talib',
        'schedule'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"缺少以下依赖包: {', '.join(missing_packages)}")
        print("请使用以下命令安装:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def create_spec_file():
    """创建PyInstaller规格文件"""
    spec_content = '''# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from pathlib import Path

# 获取项目根目录
project_root = Path(__file__).parent

# 添加数据文件
datas = []

# 添加隐藏导入
hiddenimports = [
    'matplotlib.backends.backend_tkagg',
    'matplotlib.figure',
    'matplotlib.pyplot',
    'matplotlib.dates',
    'pandas',
    'numpy',
    'akshare',
    'talib',
    'schedule',
    'threading',
    'queue',
    'tkinter',
    'tkinter.ttk',
    'tkinter.messagebox',
    'tkinter.scrolledtext',
    'PIL',
    'PIL.Image',
    'PIL.ImageTk'
]

# 分析
a = Analysis(
    ['industry_analysis_gui.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pytest', 'test'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

# 打包
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# 创建可执行文件
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='IndustryAnalysis',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # 设置为False以隐藏控制台窗口
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# 创建应用程序包
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='IndustryAnalysis'
)

# 创建macOS应用程序包
app = BUNDLE(
    coll,
    name='板块分析系统.app',
    icon=None,  # 可以添加图标文件路径
    bundle_identifier='com.industry.analysis',
    info_plist={
        'NSHighResolutionCapable': 'True',
        'CFBundleDisplayName': '板块分析系统',
        'CFBundleName': '板块分析系统',
        'CFBundleVersion': '1.0.0',
        'CFBundleShortVersionString': '1.0.0',
        'NSAppleEventsUsageDescription': '用于系统通知和事件处理',
        'NSMicrophoneUsageDescription': '应用程序不需要麦克风权限',
        'NSCameraUsageDescription': '应用程序不需要摄像头权限',
        'NSLocationUsageDescription': '应用程序不需要位置权限'
    }
)
'''
    
    with open('industry_analysis_macos.spec', 'w', encoding='utf-8') as f:
        f.write(spec_content)
    
    print("已创建macOS打包规格文件: industry_analysis_macos.spec")

def build_app():
    """构建macOS应用程序"""
    print("开始构建macOS应用程序...")
    
    # 清理之前的构建
    if os.path.exists('build'):
        shutil.rmtree('build')
        print("清理build目录")
    
    if os.path.exists('dist'):
        shutil.rmtree('dist')
        print("清理dist目录")
    
    # 使用PyInstaller构建
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--clean',
        '--noconfirm',
        'industry_analysis_macos.spec'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("构建成功！")
        print("应用程序位置: dist/板块分析系统.app")
        return True
    else:
        print("构建失败！")
        print("错误信息:")
        print(result.stderr)
        return False

def create_dmg():
    """创建DMG安装包（可选）"""
    print("创建DMG安装包...")
    
    dmg_name = "IndustryAnalysis_macOS"
    app_path = "dist/板块分析系统.app"
    
    if not os.path.exists(app_path):
        print("未找到应用程序，请先构建应用")
        return False
    
    # 创建临时目录
    temp_dir = "dmg_temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    # 复制应用到临时目录
    shutil.copytree(app_path, f"{temp_dir}/板块分析系统.app")
    
    # 创建符号链接到Applications
    os.symlink("/Applications", f"{temp_dir}/Applications")
    
    # 使用hdiutil创建DMG
    cmd = [
        'hdiutil', 'create',
        '-volname', '板块分析系统',
        '-srcfolder', temp_dir,
        '-ov',
        '-format', 'UDZO',
        f"{dmg_name}.dmg"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # 清理临时目录
    shutil.rmtree(temp_dir)
    
    if result.returncode == 0:
        print(f"DMG创建成功: {dmg_name}.dmg")
        return True
    else:
        print("DMG创建失败:")
        print(result.stderr)
        return False

def main():
    """主函数"""
    print("=== 板块分析系统 macOS 打包工具 ===")
    print()
    
    # 检查依赖
    if not check_dependencies():
        return False
    
    # 检查PyInstaller
    try:
        import PyInstaller
        print(f"PyInstaller版本: {PyInstaller.__version__}")
    except ImportError:
        print("PyInstaller未安装，请运行: pip install pyinstaller")
        return False
    
    # 检查主程序文件
    if not os.path.exists('industry_analysis_gui.py'):
        print("未找到主程序文件 industry_analysis_gui.py")
        return False
    
    if not os.path.exists('industry_analysis.py'):
        print("未找到核心模块文件 industry_analysis.py")
        return False
    
    # 创建规格文件
    create_spec_file()
    
    # 构建应用
    if not build_app():
        return False
    
    # 询问是否创建DMG
    try:
        create_dmg_choice = input("是否创建DMG安装包? (y/N): ").strip().lower()
        if create_dmg_choice in ['y', 'yes']:
            create_dmg()
    except KeyboardInterrupt:
        print("\n用户取消操作")
    
    print("\n构建完成！")
    print("使用说明:")
    print("1. 应用程序: dist/板块分析系统.app")
    print("2. 双击运行或拖拽到Applications文件夹")
    print("3. 首次运行可能需要在系统偏好设置中允许运行")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)