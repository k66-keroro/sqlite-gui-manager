# sqlite-gui-manager
SQLite database management GUI tool with Python tkinter
# SQLite GUI Manager

## プロジェクト概要

SQLiteデータベースを効率的に管理・操作するためのPythonツールです。CLI（コマンドライン）とGUI（グラフィカルユーザーインターフェース）の両方をサポートし、データベース管理作業を自動化・効率化します。

## 主要機能

### CLI機能（現在実装済み）

- **データベース接続・作成**: SQLiteファイルの新規作成・既存ファイル接続
- **テーブル管理**: テーブル一覧表示・構造確認
- **データ操作**: データ表示（全件・件数指定）
- **SQL実行**: カスタムSQLクエリの実行
- **インタラクティブメニュー**: 直感的なメニューシステム

### GUI機能（開発予定）

- **データベースブラウザ**: ツリー形式でのDB構造表示
- **SQLエディタ**: シンタックスハイライト付きエディタ
- **データビューア**: テーブルデータのGUI表示・編集
- **クエリビルダー**: ビジュアルクエリ作成機能
- **データインポート/エクスポート**: CSV、Excel対応

## 技術スタック

- **言語**: Python 3.8+
- **データベース**: SQLite3
- **CLI**: 標準ライブラリ（sqlite3、os、sys）
- **GUI（予定）**: tkinter または PyQt5
- **データ処理**: pandas（予定）
- **ファイル処理**: CSV、Excel対応（予定）

## ファイル構造

```
sqlite-gui-manager/
├── README.md                    # このファイル
├── claude.md                    # 開発ログ・設計メモ
├── sqlite_cli.py                # CLIメインアプリケーション
├── requirements.txt             # 依存関係（予定）
├── src/                         # ソースコード（予定）
│   ├── cli/
│   │   └── sqlite_cli.py
│   ├── gui/
│   │   ├── main_window.py
│   │   ├── database_browser.py
│   │   └── sql_editor.py
│   └── core/
│       ├── database_manager.py
│       └── query_executor.py
├── tests/                       # テストコード（予定）
├── docs/                        # ドキュメント（予定）
└── examples/                    # サンプルDB・スクリプト（予定）
```

## インストール・使用方法

### 前提条件

- Python 3.8以上
- SQLite3（Pythonに標準搭載）

### CLIツールの使用

#### 1. 基本的な起動

```bash
python sqlite_cli.py
```

#### 2. データベースファイル指定起動

```bash
python sqlite_cli.py path/to/your/database.db
```

#### 3. 主要な操作フロー

1. **データベース接続**: 既存DBを開くか新規作成
2. **テーブル確認**: テーブル一覧・構造の確認
3. **データ操作**: データ表示・SQL実行
4. **終了**: 安全な接続終了

### メニュー機能

```
=== SQLite Database Manager ===
1. テーブル一覧を表示
2. テーブル構造を表示
3. データを表示
4. SQLクエリを実行
5. データベース情報を表示
6. 終了
```

## 開発ロードマップ

### Phase 1: CLI機能強化 ✅ 部分完了

- [x] 基本CLI機能実装
- [x] インタラクティブメニュー
- [ ] エラーハンドリング改善
- [ ] SQL履歴機能
- [ ] データエクスポート機能（CSV、JSON）
- [ ] バックアップ・リストア機能
- [ ] 設定ファイル対応

### Phase 2: GUI開発

- [ ] tkinter/PyQt5でのGUI実装
- [ ] データベースブラウザ
- [ ] SQLエディタ機能
- [ ] テーブルデータのGUI表示・編集
- [ ] ビジュアルクエリビルダー
- [ ] データインポート/エクスポートGUI

### Phase 3: 高度な機能

- [ ] 複数DB同時管理
- [ ] データベース比較機能
- [ ] パフォーマンス分析
- [ ] プラグインシステム
- [ ] データ可視化機能

## 主要コンポーネント詳細

### 1. データベース管理機能

- SQLiteファイルの作成・接続・切断
- テーブル・インデックス・ビューの管理
- データベース整合性チェック

### 2. クエリ実行エンジン

- SQL文の実行・結果表示
- クエリ履歴管理
- エラーハンドリング・詳細ログ

### 3. データインポート/エクスポート

- CSV、TSV、Excelファイル対応
- エンコーディング自動検出
- データプレビュー機能
- バッチ処理対応

### 4. ユーザーインターフェース

- 直感的なCLIメニュー
- GUI版でのドラッグ&ドロップ対応
- キーボードショートカット

## 開発ガイドライン

### コード品質

- PEP 8準拠のコーディングスタイル
- 適切な例外処理とログ出力
- ユニットテストの実装
- ドキュメント文字列の充実

### アーキテクチャ原則

- **モジュラー設計**: 機能別の明確な分離
- **単一責任原則**: 各クラス・関数の責任を明確化
- **拡張性**: 新機能追加の容易さ
- **保守性**: 可読性・理解しやすさの重視

### エラーハンドリング

- ユーザーフレンドリーなエラーメッセージ
- 詳細なログ出力によるデバッグ支援
- グレースフルな例外処理

## 貢献・開発参加

### 開発環境のセットアップ

```bash
# リポジトリのクローン
git clone https://github.com/k66-keroro/sqlite-gui-manager.git
cd sqlite-gui-manager

# 依存関係のインストール（将来）
pip install -r requirements.txt

# 開発用実行
python sqlite_cli.py
```

### 開発フロー

1. **Issue作成**: 新機能・バグ報告
2. **ブランチ作成**: feature/機能名 または fix/修正内容
3. **実装・テスト**: 機能実装とテストコード作成
4. **プルリクエスト**: レビュー・マージ

## ライセンス

MIT License - 詳細は`LICENSE`ファイルを参照

## 更新履歴

### v0.1.0 (2025/08/01)

- ✅ 初期リリース
- ✅ 基本CLI機能実装
- ✅ インタラクティブメニューシステム
- ✅ SQLite基本操作（接続・テーブル表示・データ表示・SQL実行）

## サポート・連絡先

- **Issues**: [GitHub Issues](https://github.com/k66-keroro/sqlite-gui-manager/issues)
- **Wiki**: [Project Wiki](https://github.com/k66-keroro/sqlite-gui-manager/wiki)
- **開発ログ**: `claude.md`ファイルを参照

## 参考資料

- [SQLite公式ドキュメント](https://www.sqlite.org/docs.html)
- [Python sqlite3モジュール](https://docs.python.org/3/library/sqlite3.html)
- [tkinter公式ドキュメント](https://docs.python.org/3/library/tkinter.html)