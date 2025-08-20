import sqlite3
import pandas as pd
import os
import numpy as np
import json
import re
from pathlib import Path

def load_excel_config():
    """Excel設定ファイルを読み込み"""
    config_path = Path("excel_config.json")
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        print("[WARNING] excel_config.json が見つかりません。デフォルト設定を使用します。")
        return {
            "files": {},
            "default_settings": {
                "header_row": 3,
                "data_start_row": 4,
                "auto_detect_types": True
            },
            "data_cleanup": {
                "remove_comma": True,
                "remove_decimal_zero": True,
                "convert_text_to_integer": True,
                "avoid_real_type": True
            }
        }

def clean_numeric_data(value):
    """数値データのクリーニング（カンマ除去、.0除去）"""
    if pd.isna(value) or value == '':
        return value
    
    value_str = str(value).strip()
    
    # カンマ除去
    value_str = value_str.replace(',', '')
    
    # .0で終わる場合は除去
    if value_str.endswith('.0'):
        value_str = value_str[:-2]
    
    # 数値に変換できるかチェック
    try:
        if '.' in value_str:
            return float(value_str)
        else:
            return int(value_str)
    except:
        return value

def detect_data_types(df, file_config=None):
    """データ型を自動判定（設定ファイル優先、integer_fieldsは自動判定しない）"""
    sqlite_types = {}
    force_text_fields = []
    integer_fields = []
    date_fields = []
    real_to_text_fields = file_config.get('real_to_text_fields', []) if file_config else []

    # 設定ファイルから指定されたフィールドを取得
    if file_config:
        force_text_fields = file_config.get('text_fields', [])
        integer_fields = file_config.get('integer_fields', [])
        date_fields = file_config.get('date_fields', [])

    for col in df.columns:
        # 設定ファイルで指定されている場合は優先
        if col in real_to_text_fields:
            sqlite_types[col] = "REAL"
        elif col in force_text_fields:
            sqlite_types[col] = "TEXT"
        elif col in integer_fields:
            sqlite_types[col] = "INTEGER"
        elif col in date_fields:
            sqlite_types[col] = "TIMESTAMP"
        elif "決定項目" in str(col) or "決定事項" in str(col):
            sqlite_types[col] = "TEXT"
        else:
            # データ内容で判定（自動判定は数値のみ）
            sample_data = df[col].dropna().head(10)
            if len(sample_data) == 0:
                sqlite_types[col] = "TEXT"
            elif pd.api.types.is_integer_dtype(df[col]):
                sqlite_types[col] = "INTEGER"
            elif pd.api.types.is_float_dtype(df[col]):
                sqlite_types[col] = "REAL"
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                sqlite_types[col] = "TIMESTAMP"
            else:
                sqlite_types[col] = "TEXT"
    return sqlite_types, force_text_fields, integer_fields, date_fields

def clean_dataframe_with_config(df, file_config, cleanup_config):
    """データフレームのクリーニング（設定ファイル対応）"""
    # 空の列を削除
    df = df.dropna(axis=1, how='all')

    # 設定ファイルから指定されたフィールドを取得
    comma_cleanup_fields = file_config.get('comma_cleanup_fields', [])
    real_to_text_fields = file_config.get('real_to_text_fields', [])

    # データクリーニング処理
    for col in df.columns:
        # 基本処理：空値を文字列に変換
        df[col] = df[col].fillna('')

        # カンマ・小数点クリーニング
        if col in comma_cleanup_fields and cleanup_config.get('remove_comma', True):
            df[col] = df[col].apply(clean_numeric_data)

        # REAL→REAL変換（float化）
        if col in real_to_text_fields:
            df[col] = df[col].apply(lambda x: float(str(x).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '')) if str(x).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '').replace('.', '').replace('-', '').isdigit() and x != '' else None)

        # その他の列は文字列に変換
        if col not in comma_cleanup_fields and col not in real_to_text_fields:
            df[col] = df[col].astype(str)

    return df

def clean_dataframe(df):
    """データフレームのクリーニング（後方互換性）"""
    return clean_dataframe_with_config(df, {}, {})

def process_koutei_file(df):
    """工程ファイルの特別処理"""
    # 固定フィールド名リスト（koutei_to_sqlite.pyと同じ）
    fixed_columns = [
        "優先順", "所要区分", "主要品目", "主要品目1", "出庫数量", "MRP", "主要MRP", "子指図", 
        "子品目コード", "子品目テキスト", "所要日", "必要数", "計画数", "備考", "進捗", "完成期限",
        "C", "A", "C,A以外", "SMT計画", "SMT実績", "自挿計画", "自挿実績", "組立計画", 
        "組立実績", "完成_予定", "完成_実績", "完成数", "欠品品目数", "1120_欠品", "1120_未出庫",
        "1210_欠品", "1210_未出庫", "1121_欠品", "1121_未出庫", "1122_欠品", "1122_未出庫",
        "部材_供給_期限", "部材_供給日", "完成_予定日", "完成日", "外注", "MRP1", "小型", 
        "SSL_内", "SSL_外", "SSS", "富士電工", "開発・品証", "大型", "件名", "memo1", "memo2",
        "機械係_入力用", "入力禁止", "伝票_NO", "標準原価"
    ]
    
    # データフレームの列名を固定フィールド名に変更
    if len(df.columns) >= len(fixed_columns):
        df.columns = fixed_columns[:len(df.columns)]
    else:
        # 列数が足りない場合は追加
        missing_cols = len(fixed_columns) - len(df.columns)
        for i in range(missing_cols):
            df[f'Unnamed_{len(df.columns)}'] = ''
        df.columns = fixed_columns
    
    # 強制的にTEXT型にするフィールド
    force_text_fields = ["優先順", "所要区分", "主要品目", "主要品目1", "MRP", "主要MRP", "子指図", 
                        "子品目コード", "子品目テキスト", "備考", "進捗", "C", "A", "C,A以外", 
                        "外注", "MRP1", "小型", "SSL_内", "SSL_外", "SSS", "富士電工", "開発・品証", 
                        "大型", "件名", "memo1", "memo2", "機械係_入力用", "入力禁止", "伝票_NO"]
    
    # 日付フィールドの候補
    date_fields = ["所要日", "完成期限", "自挿実績", "完成_予定", "完成_実績", "部材_供給_期限", 
                  "部材_供給日", "完成_予定日", "完成日"]
    
    # 数値フィールド（INTEGER型に強制）
    integer_fields = ["出庫数量", "必要数", "計画数", "SMT計画", "SMT実績", "自挿計画", "組立計画", 
                    "組立実績", "完成数", "欠品品目数", "1120_欠品", "1120_未出庫", "1210_欠品", 
                    "1210_未出庫", "1121_欠品", "1121_未出庫", "1122_欠品", "1122_未出庫", "標準原価"]
    
    # データ型の前処理
    for col in df.columns:
        if col in force_text_fields:
            df[col] = df[col].fillna('')
            df[col] = df[col].apply(lambda x: str(int(float(x))) if str(x).replace('.', '').replace('-', '').isdigit() and x != '' else str(x))
        elif col in integer_fields:
            # 数値フィールド：INTEGER型に強制変換
            df[col] = df[col].fillna(0)
            df[col] = df[col].apply(lambda x: int(float(x)) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else 0)
        elif col in date_fields:
            # 日付フィールド：Timestamp型を文字列に変換
            df[col] = df[col].fillna('')
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else '')
        else:
            if pd.api.types.is_numeric_dtype(df[col]):
                if df[col].notna().all() and (df[col] % 1 == 0).all():
                    df[col] = df[col].astype('Int64')
            else:
                # その他の列も文字列に変換
                df[col] = df[col].fillna('').astype(str)
    
    # 日付列をTIMESTAMP形式に変換（SQLite保存用）
    for col in date_fields:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        elif col in df.columns:
            # 日付フィールドがdatetime型でない場合も文字列として保持
            df[col] = df[col].fillna('').astype(str)
    
    # SQLite用の型判定（REALは一切使わない）
    sqlite_types = {}
    for col in df.columns:
        if col in force_text_fields:
            sqlite_types[col] = "TEXT"
        elif col in integer_fields:
            sqlite_types[col] = "INTEGER"  # 数値フィールドはINTEGER型
        elif col in date_fields:
            sqlite_types[col] = "TIMESTAMP"  # 日付フィールドはTIMESTAMP型
        elif pd.api.types.is_integer_dtype(df[col]):
            sqlite_types[col] = "INTEGER"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            sqlite_types[col] = "TIMESTAMP"  # 日付はTIMESTAMP型として保存
        else:
            sqlite_types[col] = "TEXT"
    
    return df, sqlite_types

def detect_header_row(excel_path):
    """ヘッダー行を自動判定"""
    try:
        # 最初の10行を読み込んでヘッダー行を判定
        df_sample = pd.read_excel(excel_path, header=None, nrows=10)
        
        # 各行をチェックして、データらしい行（数値や日付が多い行）を除外
        for row_idx in range(min(10, len(df_sample))):
            row_data = df_sample.iloc[row_idx]
            
            # 空のセルが多い行はスキップ
            if row_data.isna().sum() > len(row_data) * 0.7:
                continue
            
            # 数値や日付が少なく、文字列が多い行をヘッダー候補とする
            text_count = 0
            numeric_count = 0
            
            for cell in row_data:
                if pd.isna(cell):
                    continue
                cell_str = str(cell).strip()
                if cell_str == '':
                    continue
                
                # 数値かどうか判定
                try:
                    float(cell_str)
                except:
                    pass
            
            # 空のセルが多い行はスキップ
            if row_data.isna().sum() > len(row_data) * 0.7:
                continue
            
            # 数値や日付が少なく、文字列が多い行をヘッダー候補とする
            text_count = 0
            numeric_count = 0
            
            for cell in row_data:
                if pd.isna(cell):
                    continue
                cell_str = str(cell).strip()
                if cell_str == '':
                    continue
                
                # 数値かどうか判定
                try:
                    float(cell_str)
                    numeric_count += 1
                except:
                    text_count += 1
            
            # 文字列が多く、数値が少ない行をヘッダー行とする
            if text_count > numeric_count and text_count > 2:
                return row_idx
        
        # デフォルトは3行目（0ベースで2）
        return 2
        
    except Exception as e:
        print(f"[WARNING] ヘッダー行自動判定エラー: {str(e)}")
        return 2

def convert_excel_to_sqlite(excel_path, db_path, table_name=None, header_row=None, data_start_row=None):
    """ExcelファイルをSQLiteに変換（設定ファイル対応）"""
    try:
        print(f"[FOLDER] 処理中: {excel_path}")
        
        # 設定ファイル読み込み
        config = load_excel_config()
        file_name = Path(excel_path).name
        file_config = config.get('files', {}).get(file_name, {})
        
        # ファイル名から工程ファイルかどうか判定
        file_stem = Path(excel_path).stem.lower()
        is_koutei_file = "工程" in file_stem or "koutei" in file_stem
        
        # ヘッダー行の決定（設定ファイル優先）
        if header_row is None:
            if 'header_row' in file_config:
                header_row = file_config['header_row']
                print(f"[SEARCH] 設定ファイル指定ヘッダー行: {header_row + 1}行目")
            else:
                # 自動判定
                detected_header = detect_header_row(excel_path)
                print(f"[SEARCH] 自動判定ヘッダー行: {detected_header + 1}行目")
                header_row = detected_header
        else:
            print(f"[SEARCH] 指定ヘッダー行: {header_row + 1}行目")
        
        # データ開始行の設定
        if data_start_row is None:
            data_start_row = file_config.get('data_start_row', header_row + 1)
        
        # Excel読み込み
        df = pd.read_excel(excel_path, header=header_row)
        
        # データ開始行の調整
        if data_start_row > header_row + 1:
            df = df.iloc[data_start_row - header_row - 1:]
            df = df.copy()  # SettingWithCopyWarning回避
        
        # データクリーニング（設定ファイル対応）
        df = clean_dataframe_with_config(df, file_config, config.get('data_cleanup', {}))
        
        # 工程ファイルの場合は特別処理
        if is_koutei_file:
            print("[TOOLS] 工程ファイル検出 - 特別処理を適用")
            df, sqlite_types = process_koutei_file(df)
            # 工程ファイル用の変数を設定
            force_text_fields = []
            integer_fields = []
            date_fields = []
        else:
            # データ型判定（設定ファイル対応）
            sqlite_types, force_text_fields, integer_fields, date_fields = detect_data_types(df, file_config)
        
        # データ型の前処理（設定ファイル対応強化）
        real_to_text_fields = file_config.get('real_to_text_fields', [])
        
        def to_float_or_none(val):
            try:
                # クリーニング: カンマ・全角スペース・％等除去
                s = str(val).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '')
                # 空欄や記号のみはNone
                if s == '' or s in ['-', '--', '―', '－', '–', '—', '−', 'null', 'None']:
                    return None
                return float(s)
            except:
                return None

        def to_int_or_none(val):
            try:
                s = str(val).replace('.', '').replace('-', '')
                if s == '' or not s.isdigit():
                    return None
                return int(float(val))
            except:
                return None

        for col in df.columns:
            if col in integer_fields:
                df[col] = df[col].apply(to_int_or_none)
            elif col in date_fields:
                df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x != '' else '')
            elif col in real_to_text_fields:
                df[col] = df[col].apply(to_float_or_none)
                sqlite_types[col] = "REAL"
            elif col in force_text_fields:
                df[col] = df[col].apply(lambda x: str(x) if x != '' else '')
                sqlite_types[col] = "TEXT"
            else:
                df[col] = df[col].apply(lambda x: str(x) if x != '' else '')
        
        # SQLiteに接続
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # テーブル名の決定
        if table_name is None:
            table_name = Path(excel_path).stem.lower()
        
        # テーブル作成
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
        column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
        create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
        print(f"[TOOLS] テーブル作成SQL: {create_sql}")
        cursor.execute(create_sql)
        
        # データ挿入（型変換強化）
        columns = list(df.columns)
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join([f'"{col}"' for col in columns])
        insert_sql = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
        
        for index, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if col in real_to_text_fields:
                    if value is None:
                        values.append(None)
                    else:
                        try:
                            values.append(float(value))
                        except:
                            values.append(None)
                elif col in integer_fields:
                    if value is None:
                        values.append(None)
                    else:
                        try:
                            values.append(int(float(value)))
                        except:
                            values.append(None)
                else:
                    values.append(str(value))
            cursor.execute(insert_sql, values)
        
        # 接続終了
        conn.commit()
        
        # テーブル作成確認
        cursor.execute(f"PRAGMA table_info('{table_name}')")
        table_info = cursor.fetchall()
        print(f"[DATA] テーブル構造確認: {len(table_info)}カラム")
        
        conn.close()
        
        print(f"[OK] 成功: {table_name} ({len(df)}行)")
        return True
        
    except Exception as e:
        print(f"[ERROR] エラー: {excel_path} - {str(e)}")
        import traceback
        print(f"[SEARCH] エラー詳細: {traceback.format_exc()}")
        return False

def batch_convert_excel_files(excel_dir, db_path):
    """複数のExcelファイルを一括変換"""
    excel_dir = Path(excel_dir)
    excel_files = list(excel_dir.glob("*.xlsx")) + list(excel_dir.glob("*.xls"))
    
    # 一時ファイル（~$で始まるファイル）を除外
    excel_files = [f for f in excel_files if not f.name.startswith('~$')]
    
    print(f"[STATS] 処理対象: {len(excel_files)}ファイル")
    
    success_count = 0
    error_count = 0
    success_files = []
    error_files = []
    
    for excel_file in excel_files:
        try:
            # ファイル名からテーブル名を生成
            table_name = excel_file.stem.lower()
            
            print(f"\n[SEARCH] 処理中: {excel_file.name}")
            
            # 変換実行
            if convert_excel_to_sqlite(str(excel_file), db_path, table_name):
                success_count += 1
                success_files.append(excel_file.name)
                print(f"[OK] 成功: {excel_file.name}")
            else:
                error_count += 1
                error_files.append(excel_file.name)
                print(f"[ERROR] 失敗: {excel_file.name}")
                
        except Exception as e:
            print(f"[ERROR] 致命的エラー: {excel_file.name} - {str(e)}")
            error_count += 1
            error_files.append(excel_file.name)
    
    print(f"\n[CHART] 処理結果:")
    print(f"  成功: {success_count}ファイル")
    if success_files:
        print(f"  成功ファイル: {', '.join(success_files)}")
    print(f"  失敗: {error_count}ファイル")
    if error_files:
        print(f"  失敗ファイル: {', '.join(error_files)}")
    print(f"  データベース: {db_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Excel→SQLite変換ツール")
    parser.add_argument("input", help="入力Excelファイル")
    parser.add_argument("db", help="出力SQLite DBファイル")
    parser.add_argument("--sheet", help="処理対象のシート名（省略時は全シート）", default=None)
    parser.add_argument("--header", type=int, help="ヘッダー行番号（デフォルトは自動判定）", default=None)
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[ERROR] 入力ファイルが見つかりません: {args.input}")
    else:
        # 単一ファイル変換
        convert_excel_to_sqlite(args.input, args.db, header_row=args.header)