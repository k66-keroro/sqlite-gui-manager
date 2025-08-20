import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import pandas as pd
import os
import json
import subprocess
from datetime import datetime

class SQLiteGUIManager:
    """SQLite GUI Manager メインクラス"""
    
    def __init__(self, root):
        """初期化"""
        self.root = root
        self.root.title("[DB] SQLite GUI Manager")
        self.root.geometry("1200x800")
        self.root.configure(bg='#f0f0f0')
        
        # スタイル設定
        style = ttk.Style()
        style.configure("Danger.TButton", foreground="red", font=('Helvetica', 8, 'bold'))
        
        # データベース接続設定
        self.config_path = os.path.join(os.path.dirname(__file__), '.sqlite_gui_manager_config.json')
        self.db_path = self.load_last_db_path() or r"C:\Users\sem3171\sqlite-gui-manager\test.db"
        self.conn = None
        self.tables = []
        self.current_results = pd.DataFrame()
        self.predefined_queries = {}
        self.clicked_column_id = None
        
        # UI構築
        self.setup_ui()
        self.connect_database()
    
    def load_last_db_path(self):
        """前回使用したDBパスを設定ファイルから取得"""
        try:
            import json
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    conf = json.load(f)
                return conf.get('last_db_path')
        except Exception as e:
            print(f"[ERROR] load_last_db_path: {e}")
        return None

    def save_last_db_path(self, db_path):
        """DBパスを設定ファイルに保存"""
        try:
            import json
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump({'last_db_path': db_path}, f)
        except Exception as e:
            print(f"[ERROR] save_last_db_path: {e}")
    
    def setup_ui(self):
        """UI構築"""
        # メニューバー
        self.create_menu()
        
        # メインフレーム
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # グリッド設定
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(0, weight=1)
        
        # 左パネル（テーブル選択・操作）
        self.setup_left_panel(main_frame)
        
        # 右パネル（データ表示・SQL実行）
        self.setup_right_panel(main_frame)
        
        # 下部ステータスバー
        self.setup_status_bar()
    
    def setup_left_panel(self, parent):
        """左パネル構築"""
        left_frame = ttk.LabelFrame(parent, text="[STATS] テーブル操作", padding="10")
        left_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 10))
        
        # テーブル選択
        ttk.Label(left_frame, text="テーブル選択:").grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        self.table_var = tk.StringVar()
        self.table_combo = ttk.Combobox(left_frame, textvariable=self.table_var, 
                                      state="readonly", width=20)
        self.table_combo.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        self.table_combo.bind('<<ComboboxSelected>>', self.on_table_selected)
        
        # ボタンエリア
        button_frame = ttk.Frame(left_frame)
        button_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Button(button_frame, text="[SEARCH] 全件表示", 
                  command=self.show_all_data).pack(fill=tk.X, pady=2)
        ttk.Button(button_frame, text="[STATS] 件数確認", 
                  command=self.show_record_count).pack(fill=tk.X, pady=2)
        ttk.Button(button_frame, text="[BUILD] 構造表示", 
                  command=self.show_table_structure).pack(fill=tk.X, pady=2)
        
        # 危険な操作用のセパレータとボタン
        ttk.Separator(button_frame, orient='horizontal').pack(fill=tk.X, pady=10)
        
        delete_button = ttk.Button(button_frame, text="[DELETE] テーブル削除", 
                                  command=self.delete_table, style="Danger.TButton")
        delete_button.pack(fill=tk.X, pady=2)

        reimport_button = ttk.Button(button_frame, text="[IMPORT] データ再インポート",
                                   command=self.reimport_table)
        reimport_button.pack(fill=tk.X, pady=2)
        
        # 検索エリア
        self.setup_search_area(left_frame)

    def delete_table(self):
        """選択中のテーブルを削除する"""
        table_to_delete = self.table_var.get()
        if not table_to_delete:
            messagebox.showwarning("テーブル削除", "削除するテーブルが選択されていません。")
            return

        # 警告メッセージを表示
        if not messagebox.askyesno("テーブル削除の確認", 
                                   f"本当にテーブル '{table_to_delete}' を削除しますか？\nこの操作は元に戻せません。",
                                   icon='warning'):
            return

        try:
            cursor = self.conn.cursor()
            cursor.execute(f'DROP TABLE "{table_to_delete}"')
            self.conn.commit()
            
            messagebox.showinfo("成功", f"テーブル '{table_to_delete}' を削除しました。")
            
            # テーブルリストを更新
            self.connect_database()
            
        except sqlite3.Error as e:
            messagebox.showerror("エラー", f"テーブル '{table_to_delete}' の削除中にエラーが発生しました。\n{e}")

    def reimport_table(self):
        """選択中のテーブルのデータを再インポートする"""
        table_to_reimport = self.table_var.get()
        if not table_to_reimport:
            messagebox.showwarning("再インポート", "再インポートするテーブルが選択されていません。")
            return

        # インポート設定を検索
        config = self.find_import_config(table_to_reimport)
        if not config:
            messagebox.showerror("エラー", f"テーブル '{table_to_reimport}' のインポート設定が見つかりません。\n`csv_txt_config.json` または `excel_config.json` を確認してください。")
            return

        # 再インポートの確認
        if not messagebox.askyesno("再インポートの確認",
                                   f"テーブル '{table_to_reimport}' を再インポートしますか？\n既存のデータは削除されます。",
                                   icon='warning'):
            return

        # テーブルを削除
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS "{table_to_reimport}"')
            self.conn.commit()
        except sqlite3.Error as e:
            messagebox.showerror("エラー", f"テーブル削除中にエラーが発生しました。\n{e}")
            return

        # インポート処理を実行
        self.run_importer_subprocess(config)

        # UIを更新
        self.connect_database()
        messagebox.showinfo("成功", f"テーブル '{table_to_reimport}' の再インポートが完了しました。")

    def find_import_config(self, table_name):
        """指定されたテーブル名に対応するインポート設定を検索する"""
        base_dir = os.path.dirname(__file__)
        csv_txt_config_path = os.path.join(base_dir, 'csv_txt_config.json')
        excel_config_path = os.path.join(base_dir, 'excel_config.json')

        # CSV/TXT設定ファイルの確認
        if os.path.exists(csv_txt_config_path):
            with open(csv_txt_config_path, 'r', encoding='utf-8') as f:
                csv_txt_configs = json.load(f)
            for config in csv_txt_configs:
                if config.get('table_name') == table_name:
                    config['type'] = 'csv_txt' # インポートタイプを特定するために追加
                    return config

        # Excel設定ファイルの確認
        if os.path.exists(excel_config_path):
            with open(excel_config_path, 'r', encoding='utf-8') as f:
                excel_configs = json.load(f)
            for config in excel_configs:
                file_path = config.get('file_path')
                if file_path:
                    # テーブル名は「ファイル名_シート名」なので、前方一致で判定
                    base_filename = os.path.splitext(os.path.basename(file_path))[0]
                    if table_name.startswith(base_filename):
                        config['type'] = 'excel' # インポートタイプを特定するために追加
                        return config

        return None

    def run_importer_subprocess(self, config):
        """インポータースクリプトをサブプロセスで実行する"""
        base_dir = os.path.dirname(__file__)
        importer_type = config.get('type')
        file_path = config.get('file_path')

        if importer_type == 'csv_txt':
            script_name = 'universal_csv_txt_to_sqlite.py'
            # CSV/TXTの場合はバッチ処理ではないので、ファイルパスを直接指定
            command = ['python', os.path.join(base_dir, script_name), file_path, self.db_path]
        elif importer_type == 'excel':
            script_name = 'universal_excel_to_sqlite.py'
            command = ['python', os.path.join(base_dir, script_name), file_path, self.db_path]
        else:
            messagebox.showerror("エラー", "不明なインポートタイプです。")
            return

        try:
            # サブプロセス実行
            progress_dialog = self.show_progress_dialog("再インポート実行中...")
            result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8', check=True)
            progress_dialog.destroy()

            # 成功した場合の処理
            if result.returncode == 0:
                self.status_var.set(f"[SUCCESS] テーブル '{config.get('table_name', os.path.basename(file_path))}' の再インポートが完了しました。")
            else:
                # 標準エラーに何かあれば表示
                error_message = result.stderr.strip()
                messagebox.showerror("インポートエラー", f"インポート処理中にエラーが発生しました。\n\n{error_message}")

        except FileNotFoundError:
            progress_dialog.destroy()
            messagebox.showerror("エラー", f"スクリプト '{script_name}' が見つかりません。")
        except subprocess.CalledProcessError as e:
            progress_dialog.destroy()
            # CalledProcessErrorから標準エラーを取得
            error_message = e.stderr.strip()
            messagebox.showerror("インポートエラー", f"インポート処理が失敗しました (終了コード: {e.returncode})。\n\n{error_message}")
        except Exception as e:
            progress_dialog.destroy()
            messagebox.showerror("予期せぬエラー", f"インポート中に予期せぬエラーが発生しました。\n{e}")

    def show_progress_dialog(self, title):
        """進捗ダイアログを表示する"""
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.geometry("300x100")
        dialog.transient(self.root) # 親ウィンドウの上に表示
        dialog.grab_set() # モーダルにする
        dialog.resizable(False, False)

        ttk.Label(dialog, text="処理を実行しています。しばらくお待ちください...", padding=20).pack(expand=True)
        progress = ttk.Progressbar(dialog, mode='indeterminate')
        progress.pack(fill=tk.X, padx=20, pady=10)
        progress.start(10)
        
        self.root.update_idletasks()
        return dialog

    def show_tree_menu(self, event):
        """Treeviewの右クリックメニューを表示する"""
        # クリックされた場所のアイテムを特定
        region = self.tree.identify_region(event.x, event.y)
        if region != "heading":
            return # ヘッダー以外は無視

        column_id = self.tree.identify_column(event.x)
        self.clicked_column_id = int(column_id.replace('#', '')) - 1

        # メニューを表示
        self.tree_menu.post(event.x_root, event.y_root)

    def check_column_data_types(self):
        """選択された列のデータ型をチェックして統計情報を表示する"""
        if self.clicked_column_id is None:
            return

        table_name = self.table_var.get()
        column_name = self.tree['columns'][self.clicked_column_id]

        try:
            # 列の全データを取得
            cur = self.conn.cursor()
            cur.execute(f'SELECT "{column_name}" FROM "{table_name}"')
            all_data = [row[0] for row in cur.fetchall()]

            # データ型を判定・集計
            type_counts = {'整数': 0, '浮動小数点数': 0, '日付': 0, '文字列': 0, '空値': 0}
            for value in all_data:
                if value is None or value == '':
                    type_counts['空値'] += 1
                elif self.is_integer(value):
                    type_counts['整数'] += 1
                elif self.is_float(value):
                    type_counts['浮動小数点数'] += 1
                elif self.is_date(value):
                    type_counts['日付'] += 1
                else:
                    type_counts['文字列'] += 1
            
            # 結果を表示
            total = len(all_data)
            result_message = f"テーブル: {table_name}\n列: {column_name}\n\nデータ型分布 (総件数: {total}):\n"
            for data_type, count in type_counts.items():
                if count > 0:
                    percentage = (count / total) * 100
                    result_message += f"- {data_type}: {count}件 ({percentage:.2f}%)\n"

            messagebox.showinfo("データ型チェック結果", result_message)

        except Exception as e:
            messagebox.showerror("エラー", f"データ型チェック中にエラーが発生しました。\n{e}")

    def is_integer(self, value):
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def is_float(self, value):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def is_date(self, value):
        # よく使われる日付フォーマットを試す
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S']
        if not isinstance(value, str):
            return False
        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False

    def check_for_missing_data(self):
        """格納漏れチェックを実行する"""
        dialog = MissingDataCheckDialog(self.root, self.tables, self.table_var.get(), self.conn)
        if dialog.result:
            self.run_missing_data_check(dialog.result)

    def run_missing_data_check(self, config):
        """ダイアログからの情報をもとに、格納漏れチェックを実行し、結果をCSVに出力する"""
        source_file = config['source_file']
        table_name = config['table_name']
        source_key = config['source_key']
        db_key = config['db_key']

        progress_dialog = self.show_progress_dialog("格納漏れチェック実行中...")

        try:
            # 1. 元ファイルを読み込み
            if source_file.endswith('.csv'):
                source_df = pd.read_csv(source_file, dtype={source_key: str})
            else:
                source_df = pd.read_excel(source_file, dtype={source_key: str})
            
            # 2. DBデータを読み込み
            db_query = f'SELECT "{db_key}" FROM "{table_name}"'
            db_df = pd.read_sql_query(db_query, self.conn, dtype={db_key: str})

            # 3. マージして差分を検出
            source_df[source_key] = source_df[source_key].astype(str)
            db_df[db_key] = db_df[db_key].astype(str)
            merged_df = pd.merge(source_df, db_df, left_on=source_key, right_on=db_key, how='left', indicator=True)
            
            missing_data = merged_df[merged_df['_merge'] == 'left_only']
            missing_count = len(missing_data)

            progress_dialog.destroy()

            # 4. 結果をCSVに出力
            if missing_count > 0:
                save_path = filedialog.asksaveasfilename(
                    title="格納漏れデータを保存",
                    defaultextension=".csv",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*m")])
                
                if save_path:
                    result_df = missing_data.drop(columns=['_merge', db_key])
                    result_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                    messagebox.showinfo("チェック完了", 
                                        f"{missing_count}件の格納漏れデータが見つかりました。\n\n結果を以下に保存しました:\n{save_path}")
                else:
                    messagebox.showinfo("チェック完了", f"{missing_count}件の格納漏れデータが見つかりましたが、保存はキャンセルされました。")
            else:
                messagebox.showinfo("チェック完了", "格納漏れデータは見つかりませんでした。")

        except Exception as e:
            if 'progress_dialog' in locals() and progress_dialog.winfo_exists():
                progress_dialog.destroy()
            messagebox.showerror("エラー", f"格納漏れチェック中にエラーが発生しました。\n{e}")
    
    def setup_search_area(self, parent):
        """検索エリア構築"""
        search_frame = ttk.LabelFrame(parent, text="[SEARCH] 検索", padding="5")
        search_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        ttk.Label(search_frame, text="カラム:").grid(row=0, column=0, sticky=tk.W)
        self.search_column_var = tk.StringVar()
        self.search_column_combo = ttk.Combobox(search_frame, textvariable=self.search_column_var, 
                                               state="readonly", width=15)
        self.search_column_combo.grid(row=0, column=1, pady=2)
        
        ttk.Label(search_frame, text="値:").grid(row=1, column=0, sticky=tk.W)
        self.search_value_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_value_var, width=18)
        search_entry.grid(row=1, column=1, pady=2)
        search_entry.bind('<Return>', lambda e: self.search_data())
        
        # 検索タイプ選択
        ttk.Label(search_frame, text="タイプ:").grid(row=2, column=0, sticky=tk.W)
        self.search_type_var = tk.StringVar(value="部分一致")
        search_type_combo = ttk.Combobox(search_frame, textvariable=self.search_type_var, 
                                       values=["部分一致", "前方一致", "後方一致", "完全一致"], 
                                       state="readonly", width=12)
        search_type_combo.grid(row=2, column=1, pady=2)
        
        ttk.Button(search_frame, text="[SEARCH] 検索", 
                  command=self.search_data).grid(row=3, column=0, columnspan=2, pady=5)

    def setup_right_panel(self, parent):
        """右パネル構築"""
        right_frame = ttk.Frame(parent)
        right_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)
        
        # 上部：SQLエディタ
        sql_frame = ttk.LabelFrame(right_frame, text="[SQL] SQLエディタ", padding="5")
        sql_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        sql_frame.columnconfigure(0, weight=1)
        
        # SQL入力エリア
        self.sql_text = tk.Text(sql_frame, height=6, wrap=tk.WORD)
        sql_scroll = ttk.Scrollbar(sql_frame, orient=tk.VERTICAL, command=self.sql_text.yview)
        self.sql_text.configure(yscrollcommand=sql_scroll.set)
        self.sql_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        sql_scroll.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # SQL実行ボタン
        sql_button_frame = ttk.Frame(sql_frame)
        sql_button_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0))
        
        ttk.Button(sql_button_frame, text="[EXEC] SQL実行", 
                  command=self.execute_sql).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(sql_button_frame, text="[CLEAR] クリア", 
                  command=self.clear_sql_text).pack(side=tk.LEFT, padx=(0, 5))
        
        # 下部：データ表示エリア
        data_frame = ttk.LabelFrame(right_frame, text="[DATA] データ表示", padding="5")
        data_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        data_frame.columnconfigure(0, weight=1)
        data_frame.rowconfigure(0, weight=1)
        
        # Treeview（データ表示）
        self.tree = ttk.Treeview(data_frame)
        tree_scroll_v = ttk.Scrollbar(data_frame, orient=tk.VERTICAL, command=self.tree.yview)
        tree_scroll_h = ttk.Scrollbar(data_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_scroll_v.set, xscrollcommand=tree_scroll_h.set)
        
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        tree_scroll_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        tree_scroll_h.grid(row=1, column=0, sticky=(tk.W, tk.E))

        # 右クリックメニューの設定
        self.tree_menu = tk.Menu(self.tree, tearoff=0)
        self.tree_menu.add_command(label="[VALIDATE] 選択列のデータ型チェック", command=self.check_column_data_types)
        self.tree.bind("<Button-3>", self.show_tree_menu)
    
    def setup_status_bar(self):
        """ステータスバー構築"""
        status_frame = ttk.Frame(self.root)
        status_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), padx=10, pady=(5, 10))
        status_frame.columnconfigure(1, weight=1)
        
        # DB情報
        self.db_info_var = tk.StringVar()
        ttk.Label(status_frame, textvariable=self.db_info_var, 
                 relief=tk.SUNKEN, anchor=tk.W).grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))
        
        # ステータス情報
        self.status_var = tk.StringVar(value="[STATUS] 準備完了")
        ttk.Label(status_frame, textvariable=self.status_var, 
                 relief=tk.SUNKEN, anchor=tk.W).grid(row=0, column=1, sticky=(tk.W, tk.E))
    
    def create_menu(self):
        """メニューバー作成"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # ファイルメニュー
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="[FILE] ファイル", menu=file_menu)
        file_menu.add_command(label="[DB] データベース選択", command=self.select_database)
        file_menu.add_command(label="[DB] 新規データベース", command=self.create_new_database)
        file_menu.add_separator()
        file_menu.add_command(label="[EXPORT] CSVエクスポート", command=self.export_sql_results)
        file_menu.add_separator()
        file_menu.add_command(label="[EXIT] 終了", command=self.root.quit)
        
        # データメニュー
        data_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="[DATA] データ", menu=data_menu)
        data_menu.add_command(label="[RELOAD] DB再読込", command=self.connect_database)
        data_menu.add_command(label="[STATS] DB統計情報", command=self.show_db_stats)
        data_menu.add_separator()
        data_menu.add_command(label="[VALIDATE] 格納漏れチェック", command=self.check_for_missing_data)
        data_menu.add_separator()
        data_menu.add_command(label="[VACUUM] DB最適化", command=self.vacuum_database)
        
        # SQLメニュー
        sql_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="[SQL] SQL", menu=sql_menu)
        sql_menu.add_command(label="[SAMPLE] サンプルSQL", command=self.load_sample_sql)
        sql_menu.add_command(label="[FORMAT] SQL整形", command=self.format_sql_text)

    def connect_database(self):
        """データベース接続"""
        try:
            if self.conn:
                self.conn.close()
            
            self.conn = sqlite3.connect(self.db_path)
            # パフォーマンス設定
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA cache_size=10000;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            
            # テーブル一覧取得
            cur = self.conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            self.tables = [row[0] for row in cur.fetchall()]
            
            # コンボボックス更新
            if hasattr(self, 'table_combo'):
                self.table_combo['values'] = self.tables
                if self.tables:
                    self.table_combo.set(self.tables[0])
                    self.on_table_selected()
            
            # ステータス更新
            if hasattr(self, 'db_info_var'):
                db_name = os.path.basename(self.db_path)
                self.db_info_var.set(f"[FOLDER] DB: {db_name}")
            
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[STATUS] DB接続完了 - {len(self.tables)}テーブル")
            
            return True
        except Exception as e:
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] DB接続失敗: {e}")
            messagebox.showerror("DB接続エラー", f"データベースに接続できません:\n{e}")
            return False
    
    def show_record_count(self):
        """選択中のテーブルのレコード数を表示"""
        try:
            table = self.table_var.get()
            if not table or not self.conn:
                return
            
            cur = self.conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM [{table}];")
            count = cur.fetchone()[0]
            
            messagebox.showinfo("レコード数", f"テーブル: {table}\nレコード数: {count:,}件")
            
            if hasattr(self, 'status_var'):
                self.status_var.set(f"レコード数取得完了: {count:,}件")
                
        except Exception as e:
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] レコード数取得: {e}")
            messagebox.showerror("エラー", f"レコード数取得エラー:\n{e}")

    def show_all_data(self):
        """選択中のテーブルの全データを表示"""
        try:
            table = self.table_var.get()
            if not table or not self.conn:
                return

            # データ取得
            cur = self.conn.cursor()
            cur.execute(f"SELECT * FROM [{table}] LIMIT 1000;")  # 最大1000件に制限
            rows = cur.fetchall()

            # カラム名取得
            cur.execute(f"PRAGMA table_info([{table}]);")
            columns = [row[1] for row in cur.fetchall()]

            self.display_data(columns, rows)
            
            if hasattr(self, 'status_var'):
                self.status_var.set(f"データ取得完了: {len(rows)}件")

        except Exception as e:
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] データ取得: {e}")
            messagebox.showerror("エラー", f"データ取得エラー:\n{e}")

    def display_data(self, columns, rows):
        """データをTreeviewに表示"""
        try:
            # Treeviewをクリア
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # カラム設定
            self.tree['columns'] = columns
            self.tree['show'] = 'headings'
            
            for col in columns:
                self.tree.heading(col, text=col)
                self.tree.column(col, width=100, minwidth=50)
            
            # データ追加
            for row in rows:
                # None値を空文字に変換
                display_row = [str(val) if val is not None else '' for val in row]
                self.tree.insert('', 'end', values=display_row)
            
        except Exception as e:
            messagebox.showerror("エラー", f"データ表示エラー:\n{e}")

    def on_table_selected(self, event=None):
        """テーブル選択時の処理"""
        try:
            table = self.table_var.get()
            if not table or not self.conn:
                return
            
            # テーブルのカラム情報を取得
            cur = self.conn.cursor()
            cur.execute(f"PRAGMA table_info([{table}]);")
            columns = [row[1] for row in cur.fetchall()]
            
            # 検索用コンボボックスを更新
            if hasattr(self, 'search_column_combo'):
                self.search_column_combo['values'] = columns
                if columns:
                    self.search_column_combo.set(columns[0])
                    
        except Exception as e:
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] テーブル情報取得: {e}")

    def show_table_structure(self):
        """テーブル構造を表示"""
        try:
            table = self.table_var.get()
            if not table or not self.conn:
                messagebox.showwarning("構造表示", "テーブルが選択されていません。")
                return
            
            cur = self.conn.cursor()
            cur.execute(f"PRAGMA table_info([{table}]);")
            columns = cur.fetchall()
            
            # 構造情報を整形
            structure_info = []
            structure_info.append(f"テーブル: {table}")
            structure_info.append("-" * 50)
            structure_info.append(f"{ 'No.':<4} {'カラム名':<20} {'型':<15} {'NULL':<5} {'デフォルト'}")
            structure_info.append("-" * 50)
            
            for col in columns:
                cid, name, col_type, notnull, default, pk = col
                null_str = "NO" if notnull else "YES"
                default_str = str(default) if default is not None else ""
                pk_str = " (PK)" if pk else ""
                structure_info.append(f"{cid:<4} {name:<20} {col_type:<15} {null_str:<5} {default_str}{pk_str}")
            
            messagebox.showinfo("テーブル構造", "\n".join(structure_info))
            
        except Exception as e:
            messagebox.showerror("構造表示エラー", f"テーブル構造取得エラー:\n{e}")

    def search_data(self):
        """データ検索機能（完全実装）"""
        try:
            if not self.conn:
                messagebox.showwarning("検索", "データベースに接続されていません。")
                return
            
            table = self.table_var.get()
            if not table:
                messagebox.showwarning("検索", "テーブルが選択されていません。")
                return
            
            search_column = self.search_column_var.get()
            search_value = self.search_value_var.get().strip()
            search_type = self.search_type_var.get()
            
            if not search_value:
                messagebox.showwarning("検索", "検索値を入力してください。")
                return
            
            # SQL文を構築
            if search_type == "完全一致":
                sql = f"SELECT * FROM [{table}] WHERE [{search_column}] = ? LIMIT 1000;"
                params = (search_value,)
            elif search_type == "部分一致":
                sql = f"SELECT * FROM [{table}] WHERE [{search_column}] LIKE ? LIMIT 1000;"
                params = (f"%{search_value}%",)
            elif search_type == "前方一致":
                sql = f"SELECT * FROM [{table}] WHERE [{search_column}] LIKE ? LIMIT 1000;"
                params = (f"{search_value}%",)
            elif search_type == "後方一致":
                sql = f"SELECT * FROM [{table}] WHERE [{search_column}] LIKE ? LIMIT 1000;"
                params = (f"%{search_value}",)
            else:  # 空値検索
                sql = f"SELECT * FROM [{table}] WHERE [{search_column}] IS NULL OR [{search_column}] = '' LIMIT 1000;"
                params = ()
            
            # クエリ実行
            cur = self.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            # カラム名取得
            cur.execute(f"PRAGMA table_info([{table}]);")
            columns = [row[1] for row in cur.fetchall()]
            
            # 結果を表示
            self.display_data(columns, rows)
            
            # ステータス更新
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[SEARCH] 検索完了: {len(rows)}件 / 検索条件: {search_column} {search_type} '{search_value}'")
            
            # 検索結果がない場合の通知
            if len(rows) == 0:
                messagebox.showinfo("検索結果", 
                    f"検索条件に一致するデータが見つかりませんでした。\n\n" 
                    f"テーブル: {table}\n" 
                    f"検索条件: {search_column} {search_type} '{search_value}'")
                
        except Exception as e:
            error_msg = f"検索エラー: {e}"
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] {error_msg}")
            messagebox.showerror("検索エラー", error_msg)
    
    def execute_sql(self):
        """カスタムSQL実行機能"""
        try:
            if not self.conn:
                messagebox.showwarning("SQL実行", "データベースに接続されていません。")
                return
            
            # SQL文を取得
            sql_text = self.sql_text.get(1.0, tk.END).strip()
            if not sql_text:
                messagebox.showwarning("SQL実行", "SQL文を入力してください。")
                return
            
            # 複数のSQL文を分割（セミコロンで区切る）
            sql_statements = [stmt.strip() for stmt in sql_text.split(';') if stmt.strip()]
            
            if not sql_statements:
                messagebox.showwarning("SQL実行", "有効なSQL文が見つかりません。")
                return
            
            # SQLの種別判定（SELECT系かそれ以外か）
            first_statement = sql_statements[0].upper().strip()
            is_select = first_statement.startswith('SELECT') or first_statement.startswith('WITH')
            
            cur = self.conn.cursor()
            results = []
            
            if is_select and len(sql_statements) == 1:
                # SELECT系の場合（単一文）
                cur.execute(sql_statements[0])
                rows = cur.fetchall()
                
                # カラム名を取得
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    
                    # SQL結果用のTreeviewに表示
                    self.display_sql_results(columns, rows)
                    
                    result_msg = f"[SQL] SELECT実行完了: {len(rows)}件取得"
                    if hasattr(self, 'status_var'):
                        self.status_var.set(result_msg)
                        
                    # 件数が多い場合の警告
                    if len(rows) >= 1000:
                        messagebox.showinfo("SQL実行結果", 
                            f"結果: {len(rows)}件のデータを取得しました。\n"
                            "※ パフォーマンスの観点から、大量データの場合はLIMIT句の使用を推奨します。")
                else:
                    messagebox.showinfo("SQL実行結果", "SQLは正常に実行されましたが、結果がありません。")
                    
            else:
                # INSERT/UPDATE/DELETE系、または複数文の場合
                affected_total = 0
                for i, stmt in enumerate(sql_statements):
                    try:
                        cur.execute(stmt)
                        affected_total += cur.rowcount
                        results.append(f"文 {i+1}: OK (影響行数: {cur.rowcount})")
                    except Exception as stmt_error:
                        results.append(f"文 {i+1}: ERROR - {stmt_error}")
                        raise stmt_error
                
                self.conn.commit()
                
                result_msg = f"[SQL] 実行完了: {len(sql_statements)}文実行, 総影響行数: {affected_total}"
                if hasattr(self, 'status_var'):
                    self.status_var.set(result_msg)
                
                # 実行結果詳細を表示
                detail_msg = f"SQL実行が完了しました。\n\n実行文数: {len(sql_statements)}\n総影響行数: {affected_total}\n\n詳細:\n" + "\n".join(results)
                messagebox.showinfo("SQL実行結果", detail_msg)
                
                # データが変更された場合、テーブル一覧を更新
                if any(stmt.upper().strip().startswith(('CREATE', 'DROP', 'ALTER')) for stmt in sql_statements):
                    self.connect_database()  # テーブル一覧を再読込
                
        except Exception as e:
            self.conn.rollback()  # エラー時はロールバック
            error_msg = f"SQL実行エラー: {e}"
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] {error_msg}")
            messagebox.showerror("SQL実行エラー", 
                f"SQL実行中にエラーが発生しました:\n\n{e}\n\n" 
                "※ トランザクションはロールバックされました。")
    
    def display_sql_results(self, columns, rows):
        """SQL実行結果をTreeviewに表示"""
        self.display_data(columns, rows)
    
    def clear_sql_text(self):
        """SQL入力エリアをクリア"""
        self.sql_text.delete(1.0, tk.END)
        if hasattr(self, 'status_var'):
            self.status_var.set("[INFO] SQLテキストクリア完了")
    
    def load_sample_sql(self):
        """サンプルSQL文をロード"""
        if not self.tables:
            messagebox.showwarning("サンプルSQL", "テーブルがありません。データベースを選択してください。")
            return
        
        table = self.table_var.get() if self.table_var.get() else self.tables[0]
        
        sample_queries = [
            f"-- 基本的なデータ取得\nSELECT * FROM [{table}] LIMIT 10;",
            f"-- 件数確認\nSELECT COUNT(*) as 件数 FROM [{table}];",
            f"-- 重複チェック（最初のカラムで）\nSELECT [カラム名], COUNT(*) as 件数 \nFROM [{table}] \nGROUP BY [カラム名] \nHAVING COUNT(*) > 1;",
            f"-- テーブル一覧\nSELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
            f"-- テーブル構造確認\nPRAGMA table_info([{table}]);",
        ]
        
        # サンプルクエリ選択ダイアログ
        from tkinter import simpledialog
        
        # f-stringを避けるために、リストを事前に作成
        query_prompts = []
        for i, q in enumerate(sample_queries):
            prompt_line = f"{i+1}. {q.split('--')[1].split(chr(10))[0].strip()}"
            query_prompts.append(prompt_line)
        
        prompt_text = "使用したいサンプルSQLを選択してください:\n\n" + "\n".join(query_prompts) + f"\n\n選択 (1-{len(sample_queries)}):"
        
        choice = simpledialog.askinteger("サンプルSQL選択", 
            prompt_text,
            minvalue=1, maxvalue=len(sample_queries))
        
        if choice:
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(tk.END, sample_queries[choice-1])
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[INFO] サンプルSQL {choice} をロードしました")
    
    def export_sql_results(self):
        """SQL実行結果をCSVエクスポート"""
        try:
            # 結果が存在するかチェック
            if not self.sql_result_tree.get_children():
                messagebox.showwarning("エクスポート", "エクスポートする結果がありません。")
                return
            
            # ファイル保存ダイアログ
            file_path = filedialog.asksaveasfilename(
                title="SQL結果をエクスポート",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            
            if not file_path:
                return
            
            # CSVファイルに書き出し
            import csv
            with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                
                # ヘッダー行を書き出し
                columns = self.sql_result_tree['columns']
                writer.writerow(columns)
                
                # データ行を書き出し
                for item in self.sql_result_tree.get_children():
                    values = self.sql_result_tree.item(item)['values']
                    writer.writerow(values)
            
            messagebox.showinfo("エクスポート完了", 
                f"SQL結果を以下のファイルにエクスポートしました:\n{file_path}")
            
            if hasattr(self, 'status_var'):
                self.status_var.set("[EXPORT] SQL結果エクスポート完了")
                
        except Exception as e:
            error_msg = f"エクスポートエラー: {e}"
            if hasattr(self, 'status_var'):
                self.status_var.set(f"[ERROR] {error_msg}")
            messagebox.showerror("エクスポートエラー", error_msg)
    
    def format_sql_text(self):
        """SQL文の簡易フォーマット"""
        try:
            sql_content = self.sql_text.get(1.0, tk.END).strip()
            if not sql_content:
                return
            
            # 簡易的なSQL整形（基本的なキーワードのみ）
            keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 
                       'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
                       'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN']
            
            formatted_sql = sql_content
            for keyword in keywords:
                # 大文字小文字を区別せずに置換
                import re
                pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
                formatted_sql = re.sub(pattern, keyword, formatted_sql, flags=re.IGNORECASE)
            
            self.sql_text.delete(1.0, tk.END)
            self.sql_text.insert(tk.END, formatted_sql)
            
            if hasattr(self, 'status_var'):
                self.status_var.set("[INFO] SQL文フォーマット完了")
                
        except Exception as e:
            messagebox.showerror("フォーマットエラー", f"SQL整形エラー:\n{e}")

    # --- ここに未定義だったメソッドを追加 ---
    def create_new_database(self):
        path = filedialog.asksaveasfilename(defaultextension=".db", filetypes=[("SQLite DB", "*.db")])
        if path:
            self.db_path = path
            self.save_last_db_path(path)
            self.connect_database()

    def select_database(self):
        path = filedialog.askopenfilename(filetypes=[("SQLite DB", "*.db;*.sqlite;*.sqlite3"), ("All files", "*.*")])
        if path:
            self.db_path = path
            self.save_last_db_path(path)
            self.connect_database()

    def show_db_stats(self):
        # 実装は省略
        messagebox.showinfo("DB Stats", "This feature is not fully implemented yet.")

    def vacuum_database(self):
        try:
            self.conn.execute("VACUUM")
            messagebox.showinfo("Vacuum", "Database vacuumed successfully.")
        except Exception as e:
            messagebox.showerror("Vacuum Error", str(e))

    def drop_all_tables(self):
        # 実装は省略
        messagebox.showinfo("Drop All Tables", "This feature is not fully implemented yet.")

    def generate_sample_queries(self):
        self.load_sample_sql()

    def show_predefined_queries(self):
        # 実装は省略
        messagebox.showinfo("Predefined Queries", "This feature is not fully implemented yet.")

    def show_import_dialog(self):
        # 実装は省略
        messagebox.showinfo("Import", "This feature is not fully implemented yet.")
        
    def run_single_import(self):
        # 実装は省略
        messagebox.showinfo("Import", "This feature is not fully implemented yet.")

    def advanced_search(self):
        # 実装は省略
        messagebox.showinfo("Advanced Search", "This feature is not fully implemented yet.")

if __name__ == "__main__":
    root = tk.Tk()
    app = SQLiteGUIManager(root)
    root.mainloop()

class MissingDataCheckDialog(tk.Toplevel):
    """格納漏れチェック用の設定を入力するダイアログ"""
    def __init__(self, parent, tables, default_table, conn):
        super().__init__(parent)
        self.transient(parent)
        self.title("格納漏れチェック設定")
        self.parent = parent
        self.conn = conn
        self.result = None

        # 変数
        self.source_file_path = tk.StringVar()
        self.table_name = tk.StringVar(value=default_table)
        self.source_key_column = tk.StringVar()
        self.db_key_column = tk.StringVar()

        body = ttk.Frame(self, padding=20)
        self.initial_focus = self.body(body, tables)
        body.pack()

        self.buttonbox()

        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.cancel)
        self.geometry(f"+{ (parent.winfo_rootx() + 50)}+{(parent.winfo_rooty() + 50)}")
        self.initial_focus.focus_set()
        self.wait_window(self)

    def body(self, master, tables):
        # 1. 元ファイル選択
        ttk.Label(master, text="元ファイル (CSV/Excel):").grid(row=0, column=0, sticky=tk.W, pady=2)
        ttk.Entry(master, textvariable=self.source_file_path, width=50).grid(row=0, column=1, pady=2)
        ttk.Button(master, text="参照...", command=self.select_source_file).grid(row=0, column=2, padx=5)

        # 2. 比較対象テーブル
        ttk.Label(master, text="比較対象テーブル:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.table_combo = ttk.Combobox(master, textvariable=self.table_name, values=tables, state='readonly', width=47)
        self.table_combo.grid(row=1, column=1, pady=2)
        self.table_combo.bind('<<ComboboxSelected>>', self.update_db_columns)

        # 3. 元ファイルのキー列
        ttk.Label(master, text="元ファイルのキー列:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.source_col_combo = ttk.Combobox(master, textvariable=self.source_key_column, state='readonly', width=47)
        self.source_col_combo.grid(row=2, column=1, pady=2)

        # 4. DBテーブルのキー列
        ttk.Label(master, text="DBテーブルのキー列:").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.db_col_combo = ttk.Combobox(master, textvariable=self.db_key_column, state='readonly', width=47)
        self.db_col_combo.grid(row=3, column=1, pady=2)

        self.update_db_columns() # 初期表示
        return self.source_col_combo

    def buttonbox(self):
        box = ttk.Frame(self)
        ttk.Button(box, text="実行", command=self.apply, default=tk.ACTIVE).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(box, text="キャンセル", command=self.cancel).pack(side=tk.LEFT, padx=5, pady=5)
        self.bind("<Return>", self.apply)
        self.bind("<Escape>", self.cancel)
        box.pack()

    def select_source_file(self):
        path = filedialog.askopenfilename(filetypes=[
            ("CSV/Excel files", "*.csv *.xlsx *.xls"),
            ("All files", "*.*")])
        if path:
            self.source_file_path.set(path)
            self.update_source_columns()

    def update_source_columns(self):
        path = self.source_file_path.get()
        if not path:
            return
        try:
            if path.endswith('.csv'):
                df = pd.read_csv(path, nrows=0) # ヘッダーのみ読み込み
            else:
                df = pd.read_excel(path, nrows=0)
            self.source_col_combo['values'] = list(df.columns)
            if df.columns.any():
                self.source_key_column.set(df.columns[0])
        except Exception as e:
            messagebox.showerror("ファイル読み込みエラー", f"ファイルの列情報を読み込めませんでした。\n{e}", parent=self)

    def update_db_columns(self, event=None):
        table = self.table_name.get()
        if not table or not self.conn:
            return
        try:
            cur = self.conn.cursor()
            cur.execute(f'PRAGMA table_info("{table}")')
            columns = [row[1] for row in cur.fetchall()]
            self.db_col_combo['values'] = columns
            if columns:
                self.db_key_column.set(columns[0])
        except Exception as e:
            messagebox.showerror("DBエラー", f"テーブルの列情報を読み込めませんでした。\n{e}", parent=self)

    def apply(self, event=None):
        source_file = self.source_file_path.get()
        table = self.table_name.get()
        source_key = self.source_key_column.get()
        db_key = self.db_key_column.get()

        if not all([source_file, table, source_key, db_key]):
            messagebox.showwarning("入力エラー", "すべての項目を入力してください。", parent=self)
            return

        self.result = {
            'source_file': source_file,
            'table_name': table,
            'source_key': source_key,
            'db_key': db_key
        }
        self.cancel()

    def cancel(self, event=None):
        self.parent.focus_set()
        self.destroy()
