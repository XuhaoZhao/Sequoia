"""
Windows打包脚本 - Build for Windows
使用PyInstaller将Python应用程序打包为Windows可执行文件
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
    'PIL.ImageTk',
    'win32api',
    'win32gui',
    'win32con'
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

# 创建单文件可执行文件
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='板块分析系统',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # 设置为False以隐藏控制台窗口
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version='version_info.txt',  # 版本信息文件
    icon='icon.ico'  # 图标文件（如果有的话）
)
'''
    
    with open('industry_analysis_windows.spec', 'w', encoding='utf-8') as f:
        f.write(spec_content)
    
    print("已创建Windows打包规格文件: industry_analysis_windows.spec")

def create_version_info():
    """创建版本信息文件"""
    version_info = '''# UTF-8
# 版本信息文件，用于Windows可执行文件

VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=(1, 0, 0, 0),
    prodvers=(1, 0, 0, 0),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo(
      [
      StringTable(
        u'040904B0',
        [StringStruct(u'CompanyName', u'Industry Analysis'),
        StringStruct(u'FileDescription', u'板块分析系统'),
        StringStruct(u'FileVersion', u'1.0.0.0'),
        StringStruct(u'InternalName', u'IndustryAnalysis'),
        StringStruct(u'LegalCopyright', u'Copyright © 2024'),
        StringStruct(u'OriginalFilename', u'板块分析系统.exe'),
        StringStruct(u'ProductName', u'板块分析系统'),
        StringStruct(u'ProductVersion', u'1.0.0.0')])
      ]), 
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)
'''
    
    with open('version_info.txt', 'w', encoding='utf-8') as f:
        f.write(version_info)
    
    print("已创建版本信息文件: version_info.txt")

def create_installer_script():
    """创建NSIS安装程序脚本（可选）"""
    nsis_script = '''# 板块分析系统安装程序
# 使用NSIS编译器生成安装程序

!define APP_NAME "板块分析系统"
!define APP_VERSION "1.0.0"
!define APP_PUBLISHER "Industry Analysis"
!define APP_EXE "板块分析系统.exe"
!define APP_DIR "IndustryAnalysis"

# 包含现代UI
!include "MUI2.nsh"

# 应用程序信息
Name "${APP_NAME}"
OutFile "${APP_NAME}_Setup.exe"
InstallDir "$PROGRAMFILES\\${APP_DIR}"
InstallDirRegKey HKCU "Software\\${APP_DIR}" ""
RequestExecutionLevel admin

# 界面设置
!define MUI_ABORTWARNING
!define MUI_ICON "icon.ico"
!define MUI_UNICON "icon.ico"

# 页面
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "LICENSE.txt"
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_WELCOME
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

# 语言
!insertmacro MUI_LANGUAGE "SimpChinese"

# 安装部分
Section "MainSection" SEC01
  SetOutPath "$INSTDIR"
  SetOverwrite ifnewer
  File "dist\\${APP_EXE}"
  
  # 创建开始菜单快捷方式
  CreateDirectory "$SMPROGRAMS\\${APP_NAME}"
  CreateShortCut "$SMPROGRAMS\\${APP_NAME}\\${APP_NAME}.lnk" "$INSTDIR\\${APP_EXE}"
  CreateShortCut "$SMPROGRAMS\\${APP_NAME}\\卸载.lnk" "$INSTDIR\\uninstall.exe"
  
  # 创建桌面快捷方式
  CreateShortCut "$DESKTOP\\${APP_NAME}.lnk" "$INSTDIR\\${APP_EXE}"
  
  # 写入注册表
  WriteRegStr HKCU "Software\\${APP_DIR}" "" $INSTDIR
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${APP_DIR}" "DisplayName" "${APP_NAME}"
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${APP_DIR}" "UninstallString" "$INSTDIR\\uninstall.exe"
  WriteRegDWORD HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${APP_DIR}" "NoModify" 1
  WriteRegDWORD HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${APP_DIR}" "NoRepair" 1
  WriteUninstaller "$INSTDIR\\uninstall.exe"
SectionEnd

# 卸载部分
Section "Uninstall"
  Delete "$INSTDIR\\${APP_EXE}"
  Delete "$INSTDIR\\uninstall.exe"
  
  Delete "$SMPROGRAMS\\${APP_NAME}\\${APP_NAME}.lnk"
  Delete "$SMPROGRAMS\\${APP_NAME}\\卸载.lnk"
  RMDir "$SMPROGRAMS\\${APP_NAME}"
  
  Delete "$DESKTOP\\${APP_NAME}.lnk"
  
  DeleteRegKey HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\${APP_DIR}"
  DeleteRegKey /ifempty HKCU "Software\\${APP_DIR}"
  
  RMDir "$INSTDIR"
SectionEnd
'''
    
    with open('installer.nsi', 'w', encoding='utf-8') as f:
        f.write(nsis_script)
    
    print("已创建NSIS安装程序脚本: installer.nsi")

def build_app():
    """构建Windows可执行文件"""
    print("开始构建Windows可执行文件...")
    
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
        'industry_analysis_windows.spec'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
    
    if result.returncode == 0:
        print("构建成功！")
        exe_path = "dist/板块分析系统.exe"
        if os.path.exists(exe_path):
            print(f"可执行文件位置: {exe_path}")
            print(f"文件大小: {os.path.getsize(exe_path) / (1024*1024):.1f} MB")
        return True
    else:
        print("构建失败！")
        print("错误信息:")
        print(result.stderr)
        return False

def create_portable_version():
    """创建便携版"""
    print("创建便携版...")
    
    exe_path = "dist/板块分析系统.exe"
    if not os.path.exists(exe_path):
        print("未找到可执行文件，请先构建应用")
        return False
    
    portable_dir = "IndustryAnalysis_Portable"
    if os.path.exists(portable_dir):
        shutil.rmtree(portable_dir)
    
    os.makedirs(portable_dir)
    
    # 复制主程序
    shutil.copy2(exe_path, portable_dir)
    
    # 创建说明文件
    readme_content = '''板块分析系统 - 便携版
========================

使用说明：
1. 直接双击"板块分析系统.exe"运行程序
2. 程序会在当前目录下创建"industry_data"文件夹存储数据
3. 数据和配置文件都保存在程序目录下，可以整个文件夹拷贝到其他电脑使用

功能介绍：
- 实时数据收集：自动获取股票板块数据
- 技术分析：MACD指标分析和信号检测
- 可视化图表：价格走势和技术指标图表
- 自动监控：定时数据收集和分析

注意事项：
- 首次运行可能需要较长时间初始化
- 确保网络连接正常以获取实时数据
- Windows系统可能提示安全警告，选择"仍要运行"即可

版本：v1.0.0
'''
    
    with open(f"{portable_dir}/使用说明.txt", 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"便携版创建成功: {portable_dir}/")
    return True

def create_batch_files():
    """创建批处理文件"""
    
    # 运行程序的批处理文件
    run_bat = '''@echo off
title 板块分析系统
echo 正在启动板块分析系统...
echo.
"板块分析系统.exe"
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo 程序异常退出，错误代码: %ERRORLEVEL%
    pause
)
'''
    
    with open('IndustryAnalysis_Portable/运行程序.bat', 'w', encoding='gbk') as f:
        f.write(run_bat)
    
    print("已创建运行批处理文件")

def main():
    """主函数"""
    print("=== 板块分析系统 Windows 打包工具 ===")
    print()
    
    # 检查操作系统
    if sys.platform != 'win32':
        print("警告：当前不在Windows系统上，生成的可执行文件可能无法在Windows上正常运行")
        print("建议在Windows系统上进行打包")
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
    
    # 创建相关文件
    create_spec_file()
    create_version_info()
    create_installer_script()
    
    # 构建应用
    if not build_app():
        return False
    
    # 创建便携版
    create_portable_version()
    create_batch_files()
    
    print("\\n构建完成！")
    print("生成的文件:")
    print("1. 单文件可执行程序: dist/板块分析系统.exe")
    print("2. 便携版: IndustryAnalysis_Portable/")
    print("3. NSIS安装程序脚本: installer.nsi")
    print()
    print("使用说明:")
    print("- 直接运行: 双击 dist/板块分析系统.exe")
    print("- 便携版: 使用 IndustryAnalysis_Portable/ 整个文件夹")
    print("- 安装程序: 使用NSIS编译 installer.nsi 生成安装包")
    print()
    print("注意：首次运行可能需要Windows防火墙和杀毒软件允许")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)