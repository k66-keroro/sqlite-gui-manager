# SQLite GUI Manager - プロジェクト構造

## ディレクトリ構造
```
sqlite-gui-manager/
├── .obsidian/                    # Obsidian設定（ドキュメント管理）
├── .serena/                      # Serena MCP設定
├── テキスト/                     # テストデータ・サンプルファイル
├── README.md                     # プロジェクト説明
├── claude.md                     # 開発ログ・作業ガイド
├── .mcp.json                     # MCP（Model Context Protocol）設定
├── excel_config.json             # Excelインポート設定
├── 基本的なテーブル確認.md       # データベース設計メモ
├── 工程.xlsx                     # サンプルExcelファイル
├── 仕掛明細(WBS集約).xlsx        # サンプルExcelファイル
└── (各種Pythonファイル)
```

## 主要Pythonファイル

### メインアプリケーション
- **`sqlite_cli.py`**: CLI機能のメインアプリケーション
  - SQLiteManager クラス
  - EnhancedSQLiteCLI クラス
  - インタラクティブメニューシステム

- **`SQLite_GUI_Manager.py`**: GUI機能（開発中）
  - SQLiteGUIManager クラス
  - tkinter使用予定

### データインポート機能
- **`excel_to_sqlite.py`**: Excelファイルインポート
- **`universal_excel_to_sqlite.py`**: 汎用Excelインポート
- **`koutei_to_sqlite.py`**: 工程データ専用インポート
- **`zp02_to_sqlite.py`**: テキストデータインポート

### テスト・検証
- **`cli_test.py`**: データベース検証スクリプト

## データベースファイル
- **`test.db`**: テスト用データベース
- **`universal_test.db`**: 汎用テスト用データベース

## 設定ファイル

### `excel_config.json`
Excelファイルインポート時の詳細設定
- ヘッダー行指定
- データ開始行指定
- フィールド型指定（integer, date, text）
- データクリーンアップ設定

### `.mcp.json`
Model Context Protocol設定（Serena連携用）

## テストデータ
`テキスト/` ディレクトリ内に製造業務関連のサンプルデータ
- 工程実績データ
- 生産計画データ
- 部材データ
- 各種マスタデータ