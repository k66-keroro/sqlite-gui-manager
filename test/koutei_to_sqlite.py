import sqlite3
import pandas as pd
import os

# ファイルパス指定
excel_path = r"C:\Users\sem3171\sqlite-gui-manager\工程.xlsx"
db_path = os.path.join(os.path.dirname(excel_path), "test.db")
sheet_name = 0

# Excel読み込み（4行目からフィールド名、5行目からデータ）
df = pd.read_excel(excel_path, sheet_name=sheet_name, header=3)

# 固定フィールド名リスト（セル統合問題を解決）
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

# SQLiteに接続
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# テーブル作成
table_name = "koutei"
cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in sqlite_types.items()])
create_sql = f'CREATE TABLE "{table_name}" ({column_defs});'
print(f"テーブル作成SQL: {create_sql}")
cursor.execute(create_sql)

# データを挿入
columns = list(df.columns)
placeholders = ", ".join(["?" for _ in columns])
column_names = ", ".join([f'"{col}"' for col in columns])
insert_sql = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'

for index, row in df.iterrows():
    values = []
    for col in columns:
        val = row[col]
        if pd.isna(val):
            values.append('')
        elif isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
            values.append(str(val))
        elif isinstance(val, (int, float)):
            values.append(int(val) if val == int(val) else str(val))
        else:
            values.append(str(val))
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

print(f"\n✅ 工程.xlsxファイルを SQLite に格納しました: {db_path}")
print("🔍 REAL型は使用せず、INTEGER/TEXTのみで構成されています") 