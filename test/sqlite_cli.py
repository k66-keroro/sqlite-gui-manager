#!/usr/bin/env python3
"""
Enhanced SQLite CLI Manager
データベース設計ガイドラインに基づく高機能版
"""

import sqlite3
import sys
import os
from typing import Optional, List, Tuple, Dict
import time

class SQLiteManager:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        
    def connect(self, db_path: str) -> bool:
        """データベースに接続"""
        try:
            self.db_path = db_path
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            
            # パフォーマンス最適化のPRAGMA設定
            self.optimize_database()
            
            print(f"[OK] データベースに接続しました: {db_path}")
            return True
        except sqlite3.Error as e:
            print(f"[ERROR] データベース接続エラー: {e}")
            return False
    
    def optimize_database(self):
        """データベースのパフォーマンス最適化設定"""
        if not self.cursor:
            return
            
        try:
            # WALモード: 書き込みパフォーマンス向上
            self.cursor.execute("PRAGMA journal_mode = WAL")
            
            # 同期設定: 安全性とパフォーマンスのバランス
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            
            # キャッシュサイズ: 10MB (デフォルトより大きく)
            self.cursor.execute("PRAGMA cache_size = -10000")
            
            # 外部キー制約を有効化
            self.cursor.execute("PRAGMA foreign_keys = ON")
            
            print("[TOOLS] データベース最適化設定を適用しました")
        except sqlite3.Error as e:
            print(f"[WARNING] 最適化設定エラー: {e}")
    
    def create_database(self, db_path: str) -> bool:
        """新しいデータベースを作成"""
        try:
            if os.path.exists(db_path):
                overwrite = input(f"ファイル '{db_path}' は既に存在します。上書きしますか？ (y/N): ")
                if overwrite.lower() != 'y':
                    return False
                os.remove(db_path)
            
            return self.connect(db_path)
        except Exception as e:
            print(f"[ERROR] データベース作成エラー: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """テーブル一覧を取得"""
        if not self.cursor:
            return []
        
        try:
            self.cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            return [row[0] for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"[ERROR] テーブル一覧取得エラー: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> List[Tuple]:
        """テーブル構造を取得"""
        if not self.cursor:
            return []
        
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"[ERROR] テーブル構造取得エラー: {e}")
            return []
    
    def get_table_indexes(self, table_name: str) -> List[Tuple]:
        """テーブルのインデックス一覧を取得"""
        if not self.cursor:
            return []
        
        try:
            self.cursor.execute("""
                SELECT name, sql FROM sqlite_master 
                WHERE type='index' AND tbl_name=? AND name NOT LIKE 'sqlite_%'
            """, (table_name,))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"[ERROR] インデックス情報取得エラー: {e}")
            return []
    
    def analyze_table_performance(self, table_name: str) -> Dict:
        """テーブルのパフォーマンス分析"""
        if not self.cursor:
            return {}
        
        try:
            # レコード数
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
            
            # テーブルサイズ（ページ数）
            self.cursor.execute("""
                SELECT COUNT(*) FROM pragma_table_info(?) 
            """, (table_name,))
            column_count = self.cursor.fetchone()[0]
            
            # 統計情報更新
            self.cursor.execute(f"ANALYZE {table_name}")
            
            return {
                'row_count': row_count,
                'column_count': column_count,
                'has_indexes': len(self.get_table_indexes(table_name)) > 0
            }
        except sqlite3.Error as e:
            print(f"[ERROR] パフォーマンス分析エラー: {e}")
            return {}
    
    def create_index(self, table_name: str, column_names: List[str], index_name: Optional[str] = None) -> bool:
        """インデックスを作成"""
        if not self.cursor:
            return False
        
        try:
            if not index_name:
                column_str = "_".join(column_names)
                index_name = f"idx_{table_name}_{column_str}"
            
            columns = ", ".join(column_names)
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})"
            
            self.cursor.execute(sql)
            self.conn.commit()
            print(f"[OK] インデックスを作成しました: {index_name}")
            return True
        except sqlite3.Error as e:
            print(f"[ERROR] インデックス作成エラー: {e}")
            return False
    
    def vacuum_database(self) -> bool:
        """データベースを最適化（VACUUM実行）"""
        if not self.conn:
            return False
        
        try:
            print("[REFRESH] データベースを最適化中...")
            start_time = time.time()
            
            self.conn.execute("VACUUM")
            
            end_time = time.time()
            print(f"[OK] データベース最適化完了 ({end_time - start_time:.2f}秒)")
            return True
        except sqlite3.Error as e:
            print(f"[ERROR] VACUUM実行エラー: {e}")
            return False
    
    def get_database_info(self) -> Dict:
        """データベース情報を取得"""
        if not self.cursor:
            return {}
        
        try:
            info = {}
            
            # ファイルサイズ
            if self.db_path and os.path.exists(self.db_path):
                info['file_size'] = os.path.getsize(self.db_path)
            
            # PRAGMA情報
            pragma_queries = [
                ("journal_mode", "PRAGMA journal_mode"),
                ("synchronous", "PRAGMA synchronous"),
                ("cache_size", "PRAGMA cache_size"),
                ("page_size", "PRAGMA page_size"),
                ("page_count", "PRAGMA page_count"),
                ("foreign_keys", "PRAGMA foreign_keys")
            ]
            
            for key, query in pragma_queries:
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                info[key] = result[0] if result else None
            
            return info
        except sqlite3.Error as e:
            print(f"[ERROR] データベース情報取得エラー: {e}")
            return {}
    
    def backup_database(self, backup_path: str) -> bool:
        """データベースをバックアップ"""
        if not self.conn:
            return False
        
        try:
            print(f"[REFRESH] バックアップ作成中: {backup_path}")
            
            # バックアップ先データベースを作成
            backup_conn = sqlite3.connect(backup_path)
            
            # データベース全体をバックアップ
            self.conn.backup(backup_conn)
            backup_conn.close()
            
            print(f"[OK] バックアップ完了: {backup_path}")
            return True
        except sqlite3.Error as e:
            print(f"[ERROR] バックアップエラー: {e}")
            return False
    
    def execute_query(self, query: str) -> Tuple[List, Optional[str]]:
        """SQLクエリを実行"""
        if not self.cursor:
            return [], "データベースに接続されていません"
        
        try:
            start_time = time.time()
            self.cursor.execute(query)
            end_time = time.time()
            
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
                self.conn.commit()
                affected = self.cursor.rowcount
                return [], f"[OK] クエリ実行完了 ({affected}行影響, {end_time - start_time:.3f}秒)"
            else:
                results = self.cursor.fetchall()
                return results, f"[OK] {len(results)}行取得 ({end_time - start_time:.3f}秒)"
                
        except sqlite3.Error as e:
            return [], f"[ERROR] SQL実行エラー: {e}"
    
    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            print("🔐 データベース接続を閉じました")

class EnhancedSQLiteCLI:
    def __init__(self):
        self.manager = SQLiteManager()
        self.history: List[str] = []
    
    def display_menu(self):
        """メインメニューを表示"""
        print("\n" + "="*50)
        print("[DB]  Enhanced SQLite Database Manager")
        print("="*50)
        print("1.  📂 データベースを開く")
        print("2.  ➕ 新しいデータベースを作成")
        print("3.  [DATA] テーブル一覧を表示")
        print("4.  [SEARCH] テーブル構造を表示")
        print("5.  [STATS] データを表示")
        print("6.  [SQL] SQLクエリを実行")
        print("7.  [CHART] テーブルパフォーマンス分析")
        print("8.  🗂️  インデックスを作成")
        print("9.  [TOOLS] データベース最適化(VACUUM)")
        print("10. [SAVE] データベースバックアップ")
        print("11. ℹ️  データベース情報を表示")
        print("12. 📜 クエリ履歴を表示")
        print("13. 🚪 終了")
        print("="*50)
    
    def run(self):
        """メインループ"""
        print("[EXEC] Enhanced SQLite Database Manager を開始します")
        
        while True:
            self.display_menu()
            choice = input("\n選択してください (1-13): ").strip()
            
            if choice == '1':
                self.open_database()
            elif choice == '2':
                self.create_database()
            elif choice == '3':
                self.show_tables()
            elif choice == '4':
                self.show_table_structure()
            elif choice == '5':
                self.show_data()
            elif choice == '6':
                self.execute_query()
            elif choice == '7':
                self.analyze_performance()
            elif choice == '8':
                self.create_index()
            elif choice == '9':
                self.vacuum_database()
            elif choice == '10':
                self.backup_database()
            elif choice == '11':
                self.show_database_info()
            elif choice == '12':
                self.show_query_history()
            elif choice == '13':
                self.exit_program()
                break
            else:
                print("[ERROR] 無効な選択です。1-13の数字を入力してください。")
    
    def open_database(self):
        """データベースを開く"""
        db_path = input("データベースファイルのパスを入力してください: ").strip()
        if db_path:
            self.manager.connect(db_path)
    
    def create_database(self):
        """新しいデータベースを作成"""
        db_path = input("作成するデータベースファイル名を入力してください: ").strip()
        if db_path:
            self.manager.create_database(db_path)
    
    def show_tables(self):
        """テーブル一覧を表示"""
        tables = self.manager.get_tables()
        if tables:
            print(f"\n[DATA] テーブル一覧 ({len(tables)}個):")
            for i, table in enumerate(tables, 1):
                print(f"  {i:2d}. {table}")
        else:
            print("[DATA] テーブルがありません")
    
    def show_table_structure(self):
        """テーブル構造を表示"""
        tables = self.manager.get_tables()
        if not tables:
            print("[DATA] テーブルがありません")
            return
        
        print("\n[DATA] 利用可能なテーブル:")
        for i, table in enumerate(tables, 1):
            print(f"  {i:2d}. {table}")
        
        table_name = input("\nテーブル名を入力してください: ").strip()
        if table_name in tables:
            info = self.manager.get_table_info(table_name)
            indexes = self.manager.get_table_indexes(table_name)
            
            print(f"\n[SEARCH] テーブル '{table_name}' の構造:")
            print("-" * 80)
            print(f"{'列名':<20} {'データ型':<15} {'NULL許可':<10} {'デフォルト':<15} {'主キー'}")
            print("-" * 80)
            
            for column in info:
                cid, name, data_type, not_null, default_value, pk = column
                null_ok = "No" if not_null else "Yes"
                pk_status = "Yes" if pk else "No"
                default_str = str(default_value) if default_value is not None else ""
                print(f"{name:<20} {data_type:<15} {null_ok:<10} {default_str:<15} {pk_status}")
            
            if indexes:
                print(f"\n🗂️ インデックス ({len(indexes)}個):")
                for idx_name, idx_sql in indexes:
                    print(f"  • {idx_name}")
            else:
                print("\n🗂️ インデックス: なし")
    
    def show_data(self):
        """データを表示"""
        tables = self.manager.get_tables()
        if not tables:
            print("[DATA] テーブルがありません")
            return
        
        print("\n[DATA] 利用可能なテーブル:")
        for i, table in enumerate(tables, 1):
            print(f"  {i:2d}. {table}")
        
        table_name = input("\nテーブル名を入力してください: ").strip()
        if table_name not in tables:
            print("[ERROR] 指定されたテーブルが見つかりません")
            return
        
        limit = input("表示件数を入力してください（Enter=全件）: ").strip()
        
        query = f"SELECT * FROM {table_name}"
        if limit.isdigit():
            query += f" LIMIT {limit}"
        
        results, message = self.manager.execute_query(query)
        print(f"\n{message}")
        
        if results:
            # カラム名を取得
            columns = [desc[1] for desc in self.manager.get_table_info(table_name)]
            
            print("\n[STATS] データ:")
            print("-" * 100)
            print(" | ".join(f"{col:<15}" for col in columns))
            print("-" * 100)
            
            for row in results:
                print(" | ".join(f"{str(val):<15}" for val in row))
    
    def execute_query(self):
        """SQLクエリを実行"""
        print("\n[SQL] SQLクエリを入力してください（'exit'で終了）:")
        
        while True:
            query = input("SQL> ").strip()
            
            if query.lower() == 'exit':
                break
            
            if not query:
                continue
            
            # 履歴に追加
            self.history.append(query)
            
            results, message = self.manager.execute_query(query)
            print(message)
            
            if results:
                print("\n[STATS] 結果:")
                for row in results:
                    print(row)
    
    def analyze_performance(self):
        """テーブルパフォーマンス分析"""
        tables = self.manager.get_tables()
        if not tables:
            print("[DATA] テーブルがありません")
            return
        
        print("\n[DATA] 利用可能なテーブル:")
        for i, table in enumerate(tables, 1):
            print(f"  {i:2d}. {table}")
        
        table_name = input("\nテーブル名を入力してください: ").strip()
        if table_name not in tables:
            print("[ERROR] 指定されたテーブルが見つかりません")
            return
        
        analysis = self.manager.analyze_table_performance(table_name)
        
        print(f"\n[CHART] テーブル '{table_name}' のパフォーマンス分析:")
        print("-" * 40)
        print(f"レコード数: {analysis.get('row_count', 'N/A'):,}")
        print(f"カラム数: {analysis.get('column_count', 'N/A')}")
        print(f"インデックス: {'あり' if analysis.get('has_indexes') else 'なし'}")
        
        if not analysis.get('has_indexes') and analysis.get('row_count', 0) > 1000:
            print("\n[IDEA] 推奨: レコード数が多いため、インデックスの作成を検討してください")
    
    def create_index(self):
        """インデックスを作成"""
        tables = self.manager.get_tables()
        if not tables:
            print("[DATA] テーブルがありません")
            return
        
        print("\n[DATA] 利用可能なテーブル:")
        for i, table in enumerate(tables, 1):
            print(f"  {i:2d}. {table}")
        
        table_name = input("\nテーブル名を入力してください: ").strip()
        if table_name not in tables:
            print("[ERROR] 指定されたテーブルが見つかりません")
            return
        
        # テーブル構造を表示
        info = self.manager.get_table_info(table_name)
        print(f"\n[SEARCH] テーブル '{table_name}' のカラム:")
        for i, column in enumerate(info, 1):
            print(f"  {i:2d}. {column[1]} ({column[2]})")
        
        columns_input = input("\nインデックスを作成するカラム名を入力してください（複数の場合はカンマ区切り）: ").strip()
        columns = [col.strip() for col in columns_input.split(',')]
        
        index_name = input("インデックス名を入力してください（空白=自動生成）: ").strip()
        if not index_name:
            index_name = None
        
        self.manager.create_index(table_name, columns, index_name)
    
    def vacuum_database(self):
        """データベース最適化"""
        confirm = input("[WARNING] データベースを最適化しますか？ (y/N): ").strip()
        if confirm.lower() == 'y':
            self.manager.vacuum_database()
    
    def backup_database(self):
        """データベースバックアップ"""
        if not self.manager.db_path:
            print("[ERROR] データベースが接続されていません")
            return
        
        backup_path = input("バックアップファイル名を入力してください: ").strip()
        if backup_path:
            self.manager.backup_database(backup_path)
    
    def show_database_info(self):
        """データベース情報を表示"""
        info = self.manager.get_database_info()
        
        print("\nℹ️ データベース情報:")
        print("-" * 40)
        
        if 'file_size' in info:
            size_mb = info['file_size'] / (1024 * 1024)
            print(f"ファイルサイズ: {size_mb:.2f} MB")
        
        for key, value in info.items():
            if key != 'file_size':
                print(f"{key}: {value}")
    
    def show_query_history(self):
        """クエリ履歴を表示"""
        if not self.history:
            print("📜 クエリ履歴はありません")
            return
        
        print(f"\n📜 クエリ履歴 ({len(self.history)}件):")
        print("-" * 60)
        for i, query in enumerate(self.history, 1):
            print(f"{i:2d}. {query}")
    
    def exit_program(self):
        """プログラム終了"""
        self.manager.close()
        print("👋 プログラムを終了します")

if __name__ == "__main__":
    cli = EnhancedSQLiteCLI()
    
    # コマンドライン引数でファイル指定
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
        cli.manager.connect(db_path)
    
    try:
        cli.run()
    except KeyboardInterrupt:
        print("\n\n👋 プログラムを終了します")
        cli.manager.close()