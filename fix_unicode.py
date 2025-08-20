#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unicode文字エラー修正用スクリプト
絵文字を通常文字に置換してcp932環境での実行エラーを解決
"""

import os
import re

def fix_unicode_in_file(file_path):
    """ファイル内のUnicode文字を通常文字に置換"""
    if not os.path.exists(file_path):
        print(f"[WARNING] File not found: {file_path}")
        return False
    
    try:
        # ファイル読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Unicode文字置換マップ
        unicode_replacements = {
            '🗃️': '[DB]',
            '📁': '[FOLDER]', 
            '📊': '[STATS]',
            '🔍': '[SEARCH]',
            '📋': '[DATA]',
            '🚀': '[EXEC]',
            '✅': '[OK]',
            '❌': '[ERROR]',
            '⚠️': '[WARNING]',
            '💾': '[SAVE]',
            '🔧': '[TOOLS]',
            '📈': '[CHART]',
            '🧹': '[CLEAN]',
            '🗑️': '[DELETE]',
            '📝': '[NOTE]',
            '🏗️': '[BUILD]',
            '💻': '[SQL]',
            '📄': '[FILE]',
            '🎯': '[TARGET]',
            '🔄': '[REFRESH]',
            '🚧': '[WIP]',
            '📌': '[PIN]',
            '💡': '[IDEA]',
        }
        
        # 置換実行
        original_content = content
        for emoji, replacement in unicode_replacements.items():
            content = content.replace(emoji, replacement)
        
        # 変更があった場合のみ書き込み
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[INFO] Fixed Unicode characters in: {file_path}")
            return True
        else:
            print(f"[INFO] No Unicode characters found in: {file_path}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Failed to fix {file_path}: {str(e)}")
        return False

def main():
    """メイン処理"""
    print("=" * 60)
    print("[INFO] Unicode Character Fix Tool for SQLite GUI Manager")
    print("=" * 60)
    
    # 修正対象ファイル
    target_files = [
        'cli_test.py',
        'universal_excel_to_sqlite.py', 
        'sqlite_cli.py',
        'SQLite_GUI_Manager.py'
    ]
    
    fixed_count = 0
    for file_path in target_files:
        if fix_unicode_in_file(file_path):
            fixed_count += 1
    
    print("=" * 60)
    print(f"[RESULT] Fixed {fixed_count} files out of {len(target_files)}")
    print("[NEXT] You can now run:")
    print("  python cli_test.py")
    print("  python universal_excel_to_sqlite.py")
    print("=" * 60)

if __name__ == "__main__":
    main()