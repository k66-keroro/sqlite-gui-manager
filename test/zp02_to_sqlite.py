import sqlite3
import pandas as pd
import os
import numpy as np

# ファイルパス指定
txt_path = r"C:\Users\sem3171\sqlite-gui-manager\zp02.txt"
db_path = os.path.join(os.path.dirname(txt_path), "test.db")

# データ読み込み
df = pd.read_csv(txt_path, delimiter="\t", encoding="cp932")

# フィールド分類（画像の分類に基づく）
# a: integer型（数値）
integer_fields = ["台数", "完成残数", "完成数", "受注伝票番号", "受注伝票明細"]  # 実際のフィールド名に合わせて調整

# x: code型（TEXT）
code_fields = ["製造区分", "保管場所", "立会予定日"]  # 実際のフィールド名に合わせて調整

# 日付フィールドの候補（実際のカラム名に合わせて調整してください）
date_fields = ["登録日","出図予定日","出図実績日","製造着手日","先手配実績","入検予定日","出庫日付","要求日","回答日","計画終了","計画開始","発行日","CRTD日付","REL日付","PCNF日付","CNF日付","DLV日付","TECO日付","実績開始日","実績終了日"
]  # 実際のフィールド名に合わせる

# データ型の前処理
for col in df.columns:
    if col in integer_fields:
        # 整数フィールド（a）: 数値として処理し、.0を除去
        df[col] = df[col].fillna(0)
        df[col] = df[col].apply(lambda x: int(float(x)) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else 0)
    
    elif col in code_fields:
        # コードフィールド（x）: 文字列として処理（.0を除去）
        df[col] = df[col].fillna('')
        df[col] = df[col].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() and x != '' else str(x))
    
    elif col in date_fields:
        # 日付系：pandasのdatetime型に変換を試行
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except:
            pass  # 変換できない場合はそのまま
    
    else:
        # その他の数値フィールド：REAL型を避けるため整数変換を試行
        if pd.api.types.is_numeric_dtype(df[col]):
            # 小数点以下が全て0なら整数に変換
            if df[col].notna().all() and (df[col] % 1 == 0).all():
                df[col] = df[col].astype('Int64')  # nullable integer

# SQLite用の型判定（REALは一切使わない）
sqlite_types = {}
for col in df.columns:
    if col in integer_fields:
        sqlite_types[col] = "INTEGER"  # 整数フィールド（a）
    elif col in code_fields:
        sqlite_types[col] = "TEXT"     # コードフィールド（x）
    elif col in date_fields:
        sqlite_types[col] = "TIMESTAMP"  # 日付フィールドはTIMESTAMP型
    elif pd.api.types.is_integer_dtype(df[col]):
        sqlite_types[col] = "INTEGER"
    elif pd.api.types.is_datetime64_any_dtype(df[col]):
        sqlite_types[col] = "TIMESTAMP"  # 日付はTIMESTAMP型として保存
    else:
        sqlite_types[col] = "TEXT"

# 日付列をTIMESTAMP形式に変換（SQLite保存用）
for col in date_fields:
    if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    elif col in df.columns:
        # 日付フィールドがdatetime型でない場合も文字列として保持
        df[col] = df[col].fillna('').astype(str)

# SQLiteに接続
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# テーブル作成（明示的な型指定）
table_name = "zp02"
cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
print(f"テーブル作成SQL: {create_sql}")
cursor.execute(create_sql)

# データを明示的に挿入（to_sqlではなく手動でINSERT）
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

print(f"\n✅ zp02.txt を SQLite に格納しました: {db_path}")
print("🔍 REAL型は使用せず、INTEGER/TEXTのみで構成されています")