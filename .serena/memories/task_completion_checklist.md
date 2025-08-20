# SQLite GUI Manager - タスク完了時の推奨手順

## 開発タスク完了時のチェックリスト

### 1. コード品質確認
- [ ] **PEP 8準拠**: コーディング規約に従っているか
- [ ] **型ヒント**: 適切な型注釈が付いているか
- [ ] **ドキュメント**: 関数・クラスに説明が記載されているか
- [ ] **エラーハンドリング**: 例外処理が適切に実装されているか

### 2. 機能テスト
- [ ] **CLI機能**: 各メニュー項目が正常動作するか
```cmd
python sqlite_cli.py
```
- [ ] **GUI機能**: 画面表示・操作が正常に動作するか（該当時）
```cmd
python SQLite_GUI_Manager.py
```
- [ ] **インポート機能**: データ取り込みが正常に完了するか
```cmd
python cli_test.py
```

### 3. ドキュメント更新
- [ ] **README.md**: 変更内容を反映
- [ ] **claude.md**: 作業ログ・現在の状況を更新
- [ ] **設定ファイル**: 必要に応じてexcel_config.json等を更新

### 4. バージョン管理
```cmd
# 変更確認
git status
git diff

# ステージング
git add .

# コミット（適切なメッセージで）
git commit -m "機能名: 変更内容の簡潔な説明"

# プッシュ
git push origin main
```

### 5. 次回作業の準備
- [ ] **claude.md更新**: 次に進めるべきタスクを明記
- [ ] **作業ファイル整理**: 不要な一時ファイルの削除
- [ ] **データベースバックアップ**: 重要なデータベースファイルの保存

## 推奨commit メッセージ形式
```
分類: 変更内容の要約

詳細説明（必要に応じて）
```

### commit分類例：
- `feat:` 新機能追加
- `fix:` バグ修正  
- `docs:` ドキュメント更新
- `refactor:` コードリファクタリング
- `test:` テスト追加・修正
- `config:` 設定ファイル変更

## トラブル発生時の対処
### データベース関連
```cmd
# データベース整合性チェック
python -c "import sqlite3; conn=sqlite3.connect('test.db'); conn.execute('PRAGMA integrity_check')"

# テーブル一覧確認
python cli_test.py
```

### Python実行エラー
```cmd
# 依存関係確認
python -c "import sqlite3, pandas, os, sys; print('All imports OK')"

# Python version確認
python --version
```