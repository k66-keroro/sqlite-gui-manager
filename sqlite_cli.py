#!/usr/bin/env python3
"""
SQLite CLI Manager
A command-line tool for SQLite database management
"""

import argparse
import sqlite3
import sys
import os
import csv
import json
from pathlib import Path
from datetime import datetime

class SQLiteManager:
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """データベースに接続"""
        if not self.db_path:
            raise ValueError("データベースパスが指定されていません")
        
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # 辞書形式でアクセス可能
            return True
        except sqlite3.Error as e:
            print(f"データベース接続エラー: {e}")
            return False
    
    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
    
    def create_database(self, db_path):
        """新しいデータベースを作成"""
        try:
            conn = sqlite3.connect(db_path)
            conn.close()
            print(f"データベース '{db_path}' を作成しました")
            return True
        except sqlite3.Error as e:
            print(f"データベース作成エラー: {e}")
            return False
    
    def list_tables(self):
        """テーブル一覧を表示"""
        if not self.connect():
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            if tables:
                print("テーブル一覧:")
                for table in tables:
                    print(f"  - {table[0]}")
            else:
                print("テーブルが見つかりません")
            
            return True
        except sqlite3.Error as e:
            print(f"テーブル一覧取得エラー: {e}")
            return False
        finally:
            self.close()
    
    def describe_table(self, table_name):
        """テーブル構造を表示"""
        if not self.connect():
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            if columns:
                print(f"テーブル '{table_name}' の構造:")
                print("ID | 列名        | データ型    | NULL許可 | デフォルト値 | 主キー")
                print("-" * 70)
                for col in columns:
                    print(f"{col[0]:2} | {col[1]:10} | {col[2]:10} | {col[3]:8} | {col[4] or 'NULL':11} | {col[5]}")
            else:
                print(f"テーブル '{table_name}' が見つかりません")
            
            return True
        except sqlite3.Error as e:
            print(f"テーブル構造取得エラー: {e}")
            return False
        finally:
            self.close()
    
    def execute_query(self, query):
        """クエリを実行"""
        if not self.connect():
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                if results:
                    # 列名を取得
                    columns = [description[0] for description in cursor.description]
                    print(" | ".join(columns))
                    print("-" * (len(" | ".join(columns))))
                    
                    for row in results:
                        print(" | ".join(str(value) for value in row))
                else:
                    print("結果がありません")
            else:
                self.conn.commit()
                print(f"クエリを実行しました。影響行数: {cursor.rowcount}")
            
            return True
        except sqlite3.Error as e:
            print(f"クエリ実行エラー: {e}")
            return False
        finally:
            self.close()
    
    def export_table_csv(self, table_name, output_file):
        """テーブルデータをCSVでエクスポート"""
        if not self.connect():
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            
            if results:
                columns = [description[0] for description in cursor.description]
                
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    for row in results:
                        writer.writerow(row)
                
                print(f"テーブル '{table_name}' を '{output_file}' にエクスポートしました")
            else:
                print(f"テーブル '{table_name}' にデータがありません")
            
            return True
        except sqlite3.Error as e:
            print(f"エクスポートエラー: {e}")
            return False
        except IOError as e:
            print(f"ファイル書き込みエラー: {e}")
            return False
        finally:
            self.close()
    
    def backup_database(self, backup_path):
        """データベースをバックアップ"""
        if not self.connect():
            return False
        
        try:
            backup = sqlite3.connect(backup_path)
            self.conn.backup(backup)
            backup.close()
            print(f"データベースを '{backup_path}' にバックアップしました")
            return True
        except sqlite3.Error as e:
            print(f"バックアップエラー: {e}")
            return False
        finally:
            self.close()
    
    def get_database_stats(self):
        """データベース統計情報を表示"""
        if not self.connect():
            return False
        
        try:
            cursor = self.conn.cursor()
            
            # データベースサイズ
            db_size = os.path.getsize(self.db_path)
            print(f"データベースファイルサイズ: {db_size:,} bytes")
            
            # テーブル数
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            print(f"テーブル数: {table_count}")
            
            # 各テーブルのレコード数
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            print("\nテーブル別レコード数:")
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor.fetchone()[0]
                print(f"  {table[0]}: {count:,} レコード")
            
            return True
        except sqlite3.Error as e:
            print(f"統計情報取得エラー: {e}")
            return False
        finally:
            self.close()
    
    def vacuum_database(self):
        """データベースを最適化（VACUUM）"""
        if not self.connect():
            return False
        
        try:
            self.conn.execute("VACUUM")
            print("データベースの最適化が完了しました")
            return True
        except sqlite3.Error as e:
            print(f"最適化エラー: {e}")
            return False
        finally:
            self.close()

def main():
    parser = argparse.ArgumentParser(description='SQLite CLI Manager')
    parser.add_argument('command', help='実行するコマンド')
    parser.add_argument('database', nargs='?', help='データベースファイルパス')
    parser.add_argument('target', nargs='?', help='対象（テーブル名、クエリなど）')
    parser.add_argument('output', nargs='?', help='出力ファイル（エクスポート時）')
    
    args = parser.parse_args()
    
    manager = SQLiteManager(args.database)
    
    if args.command == 'create':
        if not args.database:
            print("エラー: データベースパスを指定してください")
            sys.exit(1)
        manager.create_database(args.database)
    
    elif args.command == 'list-tables':
        if not args.database:
            print("エラー: データベースパスを指定してください")
            sys.exit(1)
        manager.list_tables()
    
    elif args.command == 'describe':
        if not args.database or not args.target:
            print("エラー: データベースパスとテーブル名を指定してください")
            sys.exit(1)
        manager.describe_table(args.target)
    
    elif args.command == 'query':
        if not args.database or not args.target:
            print("エラー: データベースパスとクエリを指定してください")
            sys.exit(1)
        manager.execute_query(args.target)
    
    elif args.command == 'export':
        if not args.database or not args.target or not args.output:
            print("エラー: データベースパス、テーブル名、出力ファイルを指定してください")
            sys.exit(1)
        manager.export_table_csv(args.target, args.output)
    
    elif args.command == 'backup':
        if not args.database or not args.target:
            print("エラー: データベースパスとバックアップファイルパスを指定してください")
            sys.exit(1)
        manager.backup_database(args.target)
    
    elif args.command == 'stats':
        if not args.database:
            print("エラー: データベースパスを指定してください")
            sys.exit(1)
        manager.get_database_stats()
    
    elif args.command == 'vacuum':
        if not args.database:
            print("エラー: データベースパスを指定してください")
            sys.exit(1)
        manager.vacuum_database()
    
    else:
        print(f"エラー: 未知のコマンド '{args.command}'")
        print("\n使用可能なコマンド:")
        print("  create <db_path>                    - 新しいデータベースを作成")
        print("  list-tables <db_path>               - テーブル一覧を表示")
        print("  describe <db_path> <table_name>     - テーブル構造を表示")
        print("  query <db_path> '<sql_query>'       - SQLクエリを実行")
        print("  export <db_path> <table> <csv_file> - テーブルをCSVエクスポート")
        print("  backup <db_path> <backup_path>      - データベースをバックアップ")
        print("  stats <db_path>                     - データベース統計情報を表示")
        print("  vacuum <db_path>                    - データベースを最適化")
        sys.exit(1)

if __name__ == "__main__":
    main()