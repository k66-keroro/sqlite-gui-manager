import sqlite3
import pandas as pd
import os
import numpy as np
import json
import re
import csv
from pathlib import Path

def load_csv_txt_config():
    """独自config読込"""
    config_path = Path(__file__).parent / "csv_txt_config.json"
    if not config_path.exists():
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def detect_data_types(df, file_config):
    """データ型を推測し、SQLiteの型を返す"""
    # (既存のコードと同じ)
    integer_fields = file_config.get("integer_fields", [])
    real_to_text_fields = file_config.get("real_to_text_fields", [])
    force_text_fields = file_config.get("force_text_fields", [])
    date_fields = file_config.get("date_fields", [])
    text_fields = file_config.get("text_fields", [])
    comma_cleanup_fields = file_config.get("comma_cleanup_fields", [])
    sqlite_types = {}
    for col in df.columns:
        if col in integer_fields:
            sqlite_types[col] = "INTEGER"
        elif col in real_to_text_fields:
            sqlite_types[col] = "REAL"
        elif col in date_fields:
            sqlite_types[col] = "TIMESTAMP"
        elif col in force_text_fields or col in text_fields or col in comma_cleanup_fields:
            sqlite_types[col] = "TEXT"
        else:
            sample = df[col].dropna().astype(str)
            if len(sample) == 0:
                sqlite_types[col] = "TEXT"
                continue
            int_count = 0
            float_count = 0
            for v in sample:
                try:
                    if '.' in v:
                        float(v)
                        float_count += 1
                    else:
                        int(float(v))
                        int_count += 1
                except:
                    pass
            total = len(sample)
            if int_count / total > 0.8:
                sqlite_types[col] = "INTEGER"
            elif (int_count + float_count) / total > 0.8:
                sqlite_types[col] = "REAL"
            else:
                sqlite_types[col] = "TEXT"
    return sqlite_types, force_text_fields + text_fields + comma_cleanup_fields, integer_fields, date_fields

def clean_dataframe_with_config(df, file_config):
    """設定に基づいてDataFrameをクリーンアップ"""
    comma_cleanup_fields = file_config.get("comma_cleanup_fields", [])
    for col in comma_cleanup_fields:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = df[col].str.replace(r"\.0$", "", regex=True)
    
    integer_fields = file_config.get("integer_fields", [])
    date_fields = file_config.get("date_fields", [])
    real_to_text_fields = file_config.get("real_to_text_fields", [])
    force_text_fields = file_config.get("force_text_fields", [])

    def to_float_or_none(val):
        try:
            s = str(val).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '')
            if s == '' or s in ['-', '--', '―', '－', '–', '—', '−', 'null', 'None']: return None
            return float(s)
        except: return None

    def to_int_or_none(val):
        try:
            s = str(val).replace('.0', '').replace('-', '')
            if s == '' or not s.isdigit(): return None
            return int(s)
        except: return None

    def to_date_or_none(val):
        try:
            s = str(val).strip()
            if s == '' or s.lower() in ['nan', 'none', 'null', '-']: return None
            if re.match(r'^\d{8}$', s): return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            if re.match(r'^\d{4}-\d{2}-\d{2}$', s): return s
            if re.match(r'^\d{4}/\d{2}/\d{2}$', s): return s.replace('/', '-')
            d = pd.to_datetime(s, errors='coerce')
            return None if pd.isna(d) else d.strftime('%Y-%m-%d')
        except: return None

    for col in df.columns:
        if col in integer_fields:
            df[col] = df[col].apply(to_int_or_none)
        elif col in date_fields:
            df[col] = df[col].apply(to_date_or_none)
        elif col in real_to_text_fields:
            df[col] = df[col].apply(to_float_or_none)
        elif col in force_text_fields:
            df[col] = df[col].astype(str)

    return df

def process_and_insert_data(conn, file_path, config):
    """DataFrameを処理し、SQLiteに挿入する共通関数"""
    file_name = file_path.name
    file_config = config.get('files', {}).get(file_name, {})
    
    ext = file_path.suffix.lower()
    default_delimiter = ',' if ext == '.csv' else '\t'
    
    engine = file_config.get('engine', 'c')

    read_csv_params = {
        'delimiter': file_config.get('delimiter', default_delimiter),
        'encoding': file_config.get('encoding', 'utf-8'),
        'header': file_config.get('header_row', 0),
        'on_bad_lines': 'warn',
        'engine': engine
    }

    if engine != 'python':
        read_csv_params['low_memory'] = False

    if 'quoting' in file_config:
        read_csv_params['quoting'] = file_config['quoting'] 
    if 'escapechar' in file_config:
        read_csv_params['escapechar'] = file_config['escapechar']

    table_name = file_config.get('table_name', file_path.stem.lower())

    try:
        df = pd.read_csv(file_path, **read_csv_params)
    except UnicodeDecodeError:
        print(f"[WARNING] {file_name}: {read_csv_params['encoding']}での読み込みに失敗。cp932で再試行します。")
        read_csv_params['encoding'] = 'cp932'
        try:
            df = pd.read_csv(file_path, **read_csv_params)
        except Exception as e:
            print(f"[ERROR] {file_name}: cp932でも読み込みに失敗しました: {e}")
            return
    except Exception as e:
        print(f"[ERROR] {file_name}: 読み込み中に予期せぬエラー: {e}")
        return

    df = clean_dataframe_with_config(df.copy(), file_config)
    sqlite_types, _, _, _ = detect_data_types(df, file_config)

    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False, dtype=sqlite_types)
        print(f"[OK] 成功: {table_name} ({len(df)}行)")
    except Exception as e:
        print(f"[ERROR] {file_name} -> {table_name} のDB書き込み中にエラー: {e}")

def batch_convert_csv_txt_files(target_dir, db_path):
    """指定されたディレクトリ内のCSV/TXT/TSVファイルを一括でSQLiteに変換する"""
    target_path = Path(target_dir)
    files_to_process = list(target_path.glob("*.csv")) + list(target_path.glob("*.txt")) + list(target_path.glob("*.tsv"))
    
    if not files_to_process:
        print("[INFO] 処理対象のファイルが見つかりません。")
        return
        
    print(f"[INFO] {len(files_to_process)}個のファイルが一括処理の対象です。")
    
    config = load_csv_txt_config()
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        for file_path in files_to_process:
            print(f"--- 処理開始: {file_path.name} ---")
            process_and_insert_data(conn, file_path, config)
        
        print("\n[INFO] 全ての処理が完了しました。データベース接続をコミット・クローズします。")
        conn.commit()

    except sqlite3.Error as e:
        print(f"[FATAL] SQLiteデータベースエラーが発生しました: {e}")
    finally:
        if conn:
            conn.close()
            print("[INFO] データベース接続をクローズしました。")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CSV/TXT/TSVファイルまたはディレクトリをSQLiteに変換します。")
    parser.add_argument("input", help="入力ファイルまたはディレクトリのパス")
    parser.add_argument("db", help="出力SQLite DBファイル")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] 入力ファイルまたはディレクトリが見つかりません: {args.input}")
    else:
        db_path = args.db
        if input_path.is_dir():
            batch_convert_csv_txt_files(str(input_path), db_path)
        else:
            config = load_csv_txt_config()
            conn = None
            try:
                conn = sqlite3.connect(db_path)
                print(f"--- 処理開始: {input_path.name} ---")
                process_and_insert_data(conn, input_path, config)
                conn.commit()
                print("[INFO] 処理が完了しました。")
            except sqlite3.Error as e:
                print(f"[FATAL] SQLiteデータベースエラー: {e}")
            finally:
                if conn:
                    conn.close()
