# SQLite GUI Manager - コードスタイル・規約

## コーディングスタイル
- **PEP 8準拠**: Pythonの標準コーディング規約に従う
- **型ヒント使用**: typing モジュールを活用した型注釈
- **ドキュメント文字列**: 関数・クラスに適切な説明を記載

## 命名規約
- **クラス名**: PascalCase（例: `SQLiteManager`, `EnhancedSQLiteCLI`）
- **関数・変数名**: snake_case（例: `db_path`, `connect_database`）
- **定数**: UPPER_SNAKE_CASE（例: `DEFAULT_CACHE_SIZE`）

## ファイル構成
- **メインファイル**: 機能別に分離
  - `sqlite_cli.py`: CLI機能
  - `SQLite_GUI_Manager.py`: GUI機能
  - `excel_to_sqlite.py`: データインポート機能
- **設定ファイル**: JSON形式
  - `excel_config.json`: Excelインポート設定

## エラーハンドリング
- **例外処理**: 適切なtry-catch文の使用
- **ユーザーフレンドリーなメッセージ**: ❌、✅ 等の絵文字使用
- **詳細ログ**: デバッグ用の詳細な情報出力

## クラス設計
- **単一責任原則**: 各クラスは明確な責任を持つ
- **モジュラー設計**: 機能別の明確な分離
- **型注釈**: Optional, List, Tuple, Dict等の適切な使用

## データベース操作
- **PRAGMA設定**: パフォーマンス最適化
  - WALモード使用
  - キャッシュサイズ設定
  - 外部キー制約有効化
- **接続管理**: 適切な接続・切断処理