import sqlite3
import pandas as pd
import os

# データベースパス
db_path = r"C:\Users\sem3171\sqlite-gui-manager\test.db"

def validate_database():
    """データベースの構造とデータを検証"""
    
    if not os.path.exists(db_path):
        print("[ERROR] データベースファイルが見つかりません:", db_path)
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("[DB]  SQLite データベース検証")
    print("=" * 60)
    
    # 1. テーブル一覧取得
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"\n[STATS] テーブル数: {len(tables)}")
    print(f"テーブル名: {', '.join(tables)}")
    
    # 2. 各テーブルの詳細確認
    for table in tables:
        print(f"\n" + "="*40)
        print(f"[DATA] テーブル: {table}")
        print("="*40)
        
        # テーブル構造
        cursor.execute(f"PRAGMA table_info({table})")
        columns_info = cursor.fetchall()
        print(f"\n[BUILD]  カラム構造 ({len(columns_info)}列):")
        for col in columns_info:
            print(f"  {col[1]:20} | {col[2]:10} | NULL: {not col[3]} | デフォルト: {col[4]}")
        
        # レコード数
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        record_count = cursor.fetchone()[0]
        print(f"\n[CHART] レコード数: {record_count:,}")
        
        # サンプルデータ（先頭5件）
        if record_count > 0:
            cursor.execute(f"SELECT * FROM {table} LIMIT 5")
            sample_data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            print(f"\n[SEARCH] サンプルデータ (先頭5件):")
            df_sample = pd.DataFrame(sample_data, columns=column_names)
            print(df_sample.to_string(index=False, max_cols=10, max_colwidth=15))
            
            # NULL値チェック
            cursor.execute(f"""
                SELECT {', '.join([f"SUM(CASE WHEN {col[1]} IS NULL THEN 1 ELSE 0 END) as {col[1]}_null" 
                                 for col in columns_info[:5]])}  
                FROM {table}
            """)
            null_counts = cursor.fetchone()
            if any(count > 0 for count in null_counts):
                print(f"\n[WARNING]  NULL値の存在:")
                for i, col in enumerate(columns_info[:5]):
                    if null_counts[i] > 0:
                        print(f"  {col[1]}: {null_counts[i]}件")
    
    # 3. テーブル間の関連性チェック
    print(f"\n" + "="*60)
    print("🔗 テーブル間関連性チェック")
    print("="*60)
    
    if 'zp02' in tables and 'shikake_meisai' in tables:
        # 共通キーの確認例
        try:
            cursor.execute("""
                SELECT z.受注伝票番号, COUNT(*) as count
                FROM zp02 z
                LEFT JOIN shikake_meisai s ON z.受注伝票番号 = s.key
                GROUP BY z.受注伝票番号
                LIMIT 5
            """)
            join_result = cursor.fetchall()
            print("\n[SEARCH] zp02 ⟷ shikake_meisai 結合テスト:")
            for row in join_result:
                print(f"  受注伝票番号: {row[0]} → 関連件数: {row[1]}")
        except Exception as e:
            print(f"[ERROR] JOIN テストエラー: {e}")
    
    # 4. パフォーマンステスト
    print(f"\n" + "="*60)
    print("⚡ パフォーマンステスト")
    print("="*60)
    
    import time
    for table in tables:
        start_time = time.time()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        cursor.fetchone()
        end_time = time.time()
        print(f"  {table:20} COUNT(*): {(end_time - start_time)*1000:.2f}ms")
    
    conn.close()
    print(f"\n[OK] 検証完了！")

def interactive_query():
    """対話的SQLクエリ実行"""
    conn = sqlite3.connect(db_path)
    
    print("\n" + "="*60)
    print("[SQL] 対話的SQLクエリモード")
    print("[NOTE] 複数行入力: 空行で実行 | 'exit' で終了")
    print("[TOOLS] 便利コマンド:")
    print("   'tables' - テーブル一覧")
    print("   'desc テーブル名' - カラム構造表示")
    print("   'clear' - バッファクリア")
    print("="*60)
    
    query_buffer = []
    
    while True:
        if not query_buffer:
            prompt = "SQL> "
        else:
            prompt = "...> "
            
        line = input(prompt).strip()
        
        # 終了コマンド
        if line.lower() in ['exit', 'quit', 'q']:
            break
        
        # 特別コマンド処理
        if line.lower().startswith('desc ') or line.lower().startswith('\\d '):
            # テーブル構造表示コマンド
            table_name = line.split()[1] if len(line.split()) > 1 else ''
            if table_name:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = cursor.fetchall()
                    print(f"\n[DATA] {table_name} のカラム構造:")
                    for i, col in enumerate(columns, 1):
                        print(f"  {i:2d}. {col[1]:25} ({col[2]})")
                except Exception as e:
                    print(f"[ERROR] エラー: {e}")
        # クリアコマンド
        if line.lower() == 'clear':
            query_buffer = []
            print("[DELETE]  バッファをクリアしました")
            continue
            
        # テーブル一覧表示コマンド
        if line.lower() in ['tables', '\\dt', 'show tables']:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [row[0] for row in cursor.fetchall()]
                print(f"\n[STATS] テーブル一覧 ({len(tables)}個):")
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table}")
            except Exception as e:
                print(f"[ERROR] エラー: {e}")
            continue
        
        # 空行で実行
        if not line and query_buffer:
            query = '\n'.join(query_buffer)
            print(f"\n[EXEC] 実行中のSQL:")
            print("-" * 40)
            print(query)
            print("-" * 40)
            
            try:
                df = pd.read_sql_query(query, conn)
                print(f"\n[STATS] 結果 ({len(df)}件):")
                if len(df) > 0:
                    print(df.to_string(index=False, max_cols=15, max_colwidth=20))
                else:
                    print("結果なし")
            except Exception as e:
                print(f"[ERROR] エラー: {e}")
            
            query_buffer = []
            continue
        
        # セミコロンで終わる場合は即座に実行
        if line.endswith(';'):
            query_buffer.append(line)
            query = '\n'.join(query_buffer)
            print(f"\n[EXEC] 実行中のSQL:")
            print("-" * 40)
            print(query)
            print("-" * 40)
            
            try:
                df = pd.read_sql_query(query, conn)
                print(f"\n[STATS] 結果 ({len(df)}件):")
                if len(df) > 0:
                    print(df.to_string(index=False, max_cols=15, max_colwidth=20))
                else:
                    print("結果なし")
            except Exception as e:
                print(f"[ERROR] エラー: {e}")
            
            query_buffer = []
            continue
        
        # 通常の行追加
        if line:
            query_buffer.append(line)
    
    conn.close()

def show_table_columns():
    """全テーブルのカラム名を一覧表示"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    print("\n" + "="*60)
    print("[DATA] 全テーブルのカラム名一覧")
    print("="*60)
    
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        
        print(f"\n🗂️  {table}:")
        for i, col in enumerate(columns, 1):
            print(f"  {i:2d}. {col[1]:25} ({col[2]})")
    
    conn.close()

if __name__ == "__main__":
    validate_database()
    show_table_columns()
    
    # 対話モードも実行するか確認
    response = input("\n対話的SQLクエリモードを開始しますか？ (y/n): ")
    if response.lower() in ['y', 'yes']:
        interactive_query()