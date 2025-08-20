# SQLite GUI Manager - 推奨コマンド集

## プロジェクト実行コマンド

### CLI実行
```cmd
# 基本起動（データベース選択画面）
python sqlite_cli.py

# データベースファイル指定起動
python sqlite_cli.py path\to\database.db
python sqlite_cli.py test.db
python sqlite_cli.py universal_test.db
```

### GUI実行（開発中）
```cmd
python SQLite_GUI_Manager.py
```

### データインポート
```cmd
# Excelファイルインポート
python excel_to_sqlite.py

# 汎用Excelインポート
python universal_excel_to_sqlite.py

# 工程データインポート
python koutei_to_sqlite.py

# テキストデータインポート
python zp02_to_sqlite.py
```

### テスト・検証
```cmd
# データベース検証
python cli_test.py

# テストデータベース確認
python -c "import sqlite3; conn=sqlite3.connect('test.db'); print([x[0] for x in conn.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall()])"
```

## 開発・デバッグコマンド

### Pythonインタラクティブ実行
```cmd
# Python対話モード起動
python

# データベース内容確認（対話モード内）
import sqlite3
conn = sqlite3.connect('test.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print(cursor.fetchall())
```

### ファイル操作（Windows）
```cmd
# ディレクトリ内容確認
dir
dir *.py
dir *.db

# ファイル内容確認
type README.md
type claude.md

# ファイル検索
findstr "class" *.py
findstr "def" sqlite_cli.py
```

### Git操作
```cmd
# 状態確認
git status

# 変更をステージング
git add .
git add filename.py

# コミット
git commit -m "コミットメッセージ"

# プッシュ
git push origin main
```

## プロジェクト管理コマンド

### 依存関係管理（将来用）
```cmd
# requirements.txt生成（将来実装予定）
pip freeze > requirements.txt

# 依存関係インストール（将来実装予定）
pip install -r requirements.txt
```

### プロジェクト構造確認
```cmd
# ツリー表示（PowerShell）
tree /f

# ファイル数確認
dir *.py | find /c ".py"
```