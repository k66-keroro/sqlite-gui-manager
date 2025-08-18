import sqlite3
import pandas as pd
import os
import numpy as np
import json
import re
from pathlib import Path


# 独自config読込
def load_csv_txt_config():
    config_path = Path(__file__).parent / "csv_txt_config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
def detect_data_types(df, file_config):
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
            # データ内容で判定（8割以上が整数ならINTEGER、8割以上が小数ならREAL、そうでなければTEXT）
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
    # force_text_fieldsにtext_fieldsとcomma_cleanup_fieldsも加える（date_fieldsは除外）
    return sqlite_types, force_text_fields + text_fields + comma_cleanup_fields, integer_fields, date_fields

def clean_dataframe_with_config(df, file_config, cleanup_config):
    # カンマ除去・.0除去など
    comma_cleanup_fields = file_config.get("comma_cleanup_fields", [])
    for col in comma_cleanup_fields:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = df[col].str.replace(r"\.0$", "", regex=True)
    return df.copy()

def convert_csv_to_sqlite(csv_path, db_path, table_name=None, header_row=0, delimiter=','):
    """CSVファイルをSQLiteに変換（設定ファイル対応）"""
    print(f"[FOLDER] 処理中: {csv_path}")
    config = load_csv_txt_config()
    file_name = Path(csv_path).name
    file_config = config.get('files', {}).get(file_name, {})
    encoding = file_config.get('encoding', 'utf-8')
    # delimiter優先順位: 引数 > config > デフォルト
    delimiter_in_config = file_config.get('delimiter')
    use_delimiter = delimiter if delimiter is not None else (delimiter_in_config if delimiter_in_config is not None else ',')
    # CSV読み込み
    df = pd.read_csv(csv_path, header=header_row, delimiter=use_delimiter, encoding=encoding, low_memory=False)
    df = df.copy()
    # データクリーニング
    df = clean_dataframe_with_config(df, file_config, config.get('data_cleanup', {}))
    # 型判定
    sqlite_types, force_text_fields, integer_fields, date_fields = detect_data_types(df, file_config)
    real_to_text_fields = file_config.get('real_to_text_fields', [])
    def to_float_or_none(val):
        try:
            s = str(val).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '')
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
    def to_date_or_none(val):
        # 文字列→YYYY-MM-DD or NULL
        try:
            s = str(val).strip()
            if s == '' or s.lower() in ['nan', 'none', 'null', '-']:
                return None
            # 8桁数字→YYYY-MM-DD
            if re.match(r'^\d{8}$', s):
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            # 10桁（YYYY-MM-DD）
            if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
                return s
            # 10桁（YYYY/MM/DD）
            if re.match(r'^\d{4}/\d{2}/\d{2}$', s):
                return s.replace('/', '-')
            # それ以外はpandasでパース
            d = pd.to_datetime(s, errors='coerce')
            if pd.isna(d):
                return None
            return d.strftime('%Y-%m-%d')
        except:
            return None
    for col in df.columns:
        if col in integer_fields:
            df[col] = df[col].apply(to_int_or_none)
        elif col in date_fields:
            df[col] = df[col].apply(to_date_or_none)
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
    if table_name is None:
        table_name = Path(csv_path).stem.lower()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
    create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
    print(f"[TOOLS] テーブル作成SQL: {create_sql}")
    cursor.execute(create_sql)
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
    conn.commit()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    table_info = cursor.fetchall()
    print(f"[DATA] テーブル構造確認: {len(table_info)}カラム")
    conn.close()
    print(f"[OK] 成功: {table_name} ({len(df)}行)")
    return True

def convert_txt_to_sqlite(txt_path, db_path, table_name=None, encoding='utf-8', delimiter='\t', header_row=0):
    """TXTファイル（区切りテキスト）をSQLiteに変換（設定ファイル対応）"""
    print(f"[FOLDER] 処理中: {txt_path}")
    config = load_csv_txt_config()
    file_name = Path(txt_path).name
    file_config = config.get('files', {}).get(file_name, {})
    encoding = file_config.get('encoding', encoding)
    delimiter_in_config = file_config.get('delimiter')
    use_delimiter = delimiter if delimiter is not None else (delimiter_in_config if delimiter_in_config is not None else '\t')
    df = pd.read_csv(txt_path, header=header_row, delimiter=use_delimiter, encoding=encoding, low_memory=False)
    df = df.copy()
    df = clean_dataframe_with_config(df, file_config, config.get('data_cleanup', {}))
    sqlite_types, force_text_fields, integer_fields, date_fields = detect_data_types(df, file_config)
    real_to_text_fields = file_config.get('real_to_text_fields', [])
    def to_float_or_none(val):
        try:
            s = str(val).replace(',', '').replace(' ', '').replace('　', '').replace('%', '').replace('％', '')
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
    def to_date_or_none(val):
        try:
            s = str(val).strip()
            if s == '' or s.lower() in ['nan', 'none', 'null', '-']:
                return None
            if re.match(r'^\d{8}$', s):
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
            if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
                return s
            if re.match(r'^\d{4}/\d{2}/\d{2}$', s):
                return s.replace('/', '-')
            d = pd.to_datetime(s, errors='coerce')
            if pd.isna(d):
                return None
            return d.strftime('%Y-%m-%d')
        except:
            return None
    for col in df.columns:
        if col in integer_fields:
            df[col] = df[col].apply(to_int_or_none)
        elif col in date_fields:
            df[col] = df[col].apply(to_date_or_none)
        elif col in real_to_text_fields:
            df[col] = df[col].apply(to_float_or_none)
            sqlite_types[col] = "REAL"
        elif col in force_text_fields:
            df[col] = df[col].apply(lambda x: str(x) if x != '' else '')
            sqlite_types[col] = "TEXT"
        else:
            df[col] = df[col].apply(lambda x: str(x) if x != '' else '')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if table_name is None:
        table_name = Path(txt_path).stem.lower()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
    create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
    print(f"[TOOLS] テーブル作成SQL: {create_sql}")
    cursor.execute(create_sql)
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
    conn.commit()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    table_info = cursor.fetchall()
    print(f"[DATA] テーブル構造確認: {len(table_info)}カラム")
    conn.close()
    print(f"[OK] 成功: {table_name} ({len(df)}行)")
    return True
  
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CSV/TXT→SQLite変換ツール")
    parser.add_argument("input", nargs="?", help="入力ファイル（CSV/TXT）")
    parser.add_argument("db", help="出力SQLite DBファイル")
    parser.add_argument("--table", help="テーブル名（省略時はファイル名）", default=None)
    parser.add_argument("--header", type=int, help="ヘッダー行番号（デフォルト0）", default=0)
    parser.add_argument("--delimiter", help="区切り文字（CSVはデフォルト','、TXTはデフォルト'\\t'）", default=None)
    parser.add_argument("--encoding", help="TXTファイルの文字コード（デフォルトutf-8）", default="utf-8")
    parser.add_argument("--batch", help="フォルダ内の全CSV/TXT/TSVファイルを一括変換", action="store_true")
    args = parser.parse_args()

    # バッチ処理は一時的に無効化（検証・開発時は個別ファイルで実行推奨）
    # if args.batch:
    #     # inputはフォルダパス
    #     folder = args.input if args.input else "./"
    #     folder_path = Path(folder)
    #     files = list(folder_path.glob("*.csv")) + list(folder_path.glob("*.txt")) + list(folder_path.glob("*.tsv"))
    #     print(f"[BATCH] {len(files)}ファイル処理開始: {folder_path}")
    #     config = load_csv_txt_config()
    #     for f in files:
    #         ext = f.suffix.lower()
    #         table_name = f.stem.lower()
    #         file_name = f.name
    #         file_config = config.get('files', {}).get(file_name, {})
    #         delimiter_in_config = file_config.get('delimiter')
    #         try:
    #             if ext == ".csv":
    #                 # 優先順位: args.delimiter > config > デフォルト
    #                 delimiter = args.delimiter if args.delimiter is not None else (delimiter_in_config if delimiter_in_config is not None else ",")
    #                 convert_csv_to_sqlite(str(f), args.db, table_name, args.header, delimiter)
    #             elif ext in [".txt", ".tsv"]:
    #                 delimiter = args.delimiter if args.delimiter is not None else (delimiter_in_config if delimiter_in_config is not None else "\t")
    #                 convert_txt_to_sqlite(str(f), args.db, table_name, args.encoding, delimiter, args.header)
    #         except Exception as e:
    #             print(f"[ERROR] {f}: {e}")
    #     print(f"[BATCH] 完了: {len(files)}ファイル")
    #
    # 個別ファイル検証時は--batch無しで実行してください

    if not args.input:
        print("[ERROR] inputファイルまたは--batchフォルダを指定してください")
    else:
        ext = Path(args.input).suffix.lower()
        config = load_csv_txt_config()
        file_name = Path(args.input).name
        file_config = config.get('files', {}).get(file_name, {})
        delimiter_in_config = file_config.get('delimiter')
        if ext == ".csv":
            delimiter = args.delimiter if args.delimiter else (delimiter_in_config if delimiter_in_config is not None else ",")
            convert_csv_to_sqlite(args.input, args.db, args.table, args.header, delimiter)
        elif ext in [".txt", ".tsv"]:
            delimiter = args.delimiter if args.delimiter else (delimiter_in_config if delimiter_in_config is not None else "\t")
            convert_txt_to_sqlite(args.input, args.db, args.table, args.encoding, delimiter, args.header)
        else:
            print(f"[ERROR] 未対応の拡張子: {ext}")
