import sqlite3
import pandas as pd
import os
import re

# ファイルパス指定
excel_path = r"C:\Users\sem3171\sqlite-gui-manager\仕掛明細(WBS集約).xlsx"
db_path = os.path.join(os.path.dirname(excel_path), "test.db")
sheet_name = 0

# Excel読み込み（4行目からフィールド名、5行目からデータ）
df = pd.read_excel(excel_path, sheet_name=sheet_name, header=3)

# A列のフィールド名無し → 'key' として追加
df.insert(0, 'key', df.iloc[:, 0])
df = df.drop(df.columns[1], axis=1)

def normalize_age(age_str):
    match = re.match(r'(\d+)年(\d+)ケ月', str(age_str))
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return f"{year}年{month:02d}ケ月"
    return age_str

# 仕掛年齢の正規化
if "仕掛年齢" in df.columns:
    df["仕掛年齢"] = df["仕掛年齢"].apply(normalize_age)

# 強制的にTEXT型にするフィールド（コード系など）
force_text_fields = ["key", "工事番号", "工事コード", "作業コード"]  # 実際のフィールド名に合わせる

# 日付フィールドの候補
date_fields = ["受注日", "着工日", "完成予定日", "登録日"]  # 実際のフィールド名に合わせる

# データ型の前処理
for col in df.columns:
    if col in force_text_fields:
        # コード系：NaNを空文字に置換後、数値は整数部分のみを文字列化
        df[col] = df[col].fillna('')
        df[col] = df[col].apply(lambda x: str(int(float(x))) if str(x).replace('.', '').replace('-', '').isdigit() and x != '' else str(x))
    
    elif col in date_fields:
        # 日付系：datetime型に変換を試行
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except:
            pass
    
    else:
        # その他の数値フィールド：REAL型を避けるため整数変換を試行
        if pd.api.types.is_numeric_dtype(df[col]):
            # 小数点以下が全て0なら整数に変換
            if df[col].notna().all() and (df[col] % 1 == 0).all():
                df[col] = df[col].astype('Int64')  # nullable integer

# SQLite用の型判定（REALは一切使わない）
sqlite_types = {}
for col in df.columns:
    if col in force_text_fields:
        sqlite_types[col] = "TEXT"
    elif col in date_fields:
        sqlite_types[col] = "TEXT"
    elif pd.api.types.is_integer_dtype(df[col]):
        sqlite_types[col] = "INTEGER"
    elif pd.api.types.is_datetime64_any_dtype(df[col]):
        sqlite_types[col] = "TEXT"
    else:
        sqlite_types[col] = "TEXT"

# 日付列を文字列に変換（SQLite保存用）
for col in date_fields:
    if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.strftime('%Y-%m-%d').fillna('')

# SQLiteに接続
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# テーブル作成（明示的な型指定）
table_name = "shikake_meisai"
cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
print(f"テーブル作成SQL: {create_sql}")
cursor.execute(create_sql)

# データを明示的に挿入
columns = list(df.columns)
placeholders = ", ".join(["?" for _ in columns])
column_names = ", ".join([f'"{col}"' for col in columns])
insert_sql = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'

for index, row in df.iterrows():
    values = [row[col] for col in columns]
    cursor.execute(insert_sql, values)

# 型確認用クエリ
cursor.execute(f"PRAGMA table_info({table_name})")
table_info = cursor.fetchall()
print("\n📋 テーブル構造:")
for info in table_info:
    print(f"  {info[1]}: {info[2]}")

# 接続終了
conn.commit()
conn.close()

print(f"\n✅ Excelファイルを SQLite に格納しました: {db_path}")
print("🔍 REAL型は使用せず、INTEGER/TEXTのみで構成されています")