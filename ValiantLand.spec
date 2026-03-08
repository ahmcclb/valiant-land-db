# -*- mode: python ; coding: utf-8 -*-

import sys
import os
from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT

block_cipher = None

# Determine base path
base_path = os.path.abspath('.')

a = Analysis(
    ['desktop_app.py'],
    pathex=[base_path],
    binaries=[],
    datas=[
        # Templates folder
        ('templates', 'templates'),
        # Static assets (CSS, JS, images)
        ('static', 'static'),
        # Python modules that might be imported dynamically
        ('database_pg.py', '.'),
        ('sync_service.py', '.'),
        ('app.py', '.'),
    ],
    hiddenimports=[
        'flask',
		'jinja2',              
        'werkzeug',            
        'itsdangerous',        
        'click',               
        'markupsafe',          
        'uuid',     
        'waitress',
        'psycopg2',
        'psycopg2.extensions',
        'psycopg2.extras',
        'supabase',
        'python-docx',
        'docx',
        'docx.oxml',
        'docx.oxml.ns',
        'openpyxl',
        'openpyxl.cell._writer',
        'openpyxl.styles',
        'requests',
        'urllib3',
        'charset_normalizer',
        'idna',
        'certifi',
        'dotenv',
        'postgrest',
        'gotrue',
        'realtime',
        'storage3',
        'supabase.lib.client_options',
        # PostgreSQL specific
        'psycopg2._ipaddress',
        'psycopg2._json',
        'psycopg2._range',
        'psycopg2.errorcodes',
        'psycopg2.extensions',
        'psycopg2.extras',
        'psycopg2.pool',
        'psycopg2.sql',
        'psycopg2.tz',
        # Windows specific
        'webview',
        'webview.http',
        'webview.menu',
        'webview.screen',
        'webview.window',
        'clr',  # Pythonnet for WebView2
		'sync_service',  # <-- ADD THIS LINE
		'database_pg',   # <-- ADD THIS LINE (for safety)
		'app',    		 # <-- ADD THIS LINE (for safety)
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
	excludes=[
		# Only exclude heavy third-party GUI/ML libraries
		'matplotlib',
		'numpy',
		'pandas',
		'scipy',
		'tkinter',
		'PyQt5',
		'PyQt6',
		'PySide2',
		'PySide6',
	],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ValiantLand',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,  # Compress executable
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # NO CONSOLE WINDOW - this is key
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='static/favicon.ico' if os.path.exists('static/favicon.ico') else None,
    # Windows specific options
    manifest='''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
    <assemblyIdentity version="1.0.0.0" name="ValiantLand"/>
    <description>Valiant Land Real Estate Database</description>
    <dependency>
        <dependentAssembly>
            <assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls" 
                version="6.0.0.0" processorArchitecture="*" publicKeyToken="6595b64144ccf1df" language="*"/>
        </dependentAssembly>
    </dependency>
    <application xmlns="urn:schemas-microsoft-com:asm.v3">
        <windowsSettings>
            <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
            <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">permonitorv2,permonitor,system</dpiAwareness>
        </windowsSettings>
    </application>
    <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1">
        <application>
            <!-- Windows 10/11 -->
            <supportedOS Id="{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}"/>
        </application>
    </compatibility>
    </assembly>''',
)