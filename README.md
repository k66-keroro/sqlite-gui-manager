# Universal SQLite Importer & GUI Viewer

## 1. プロジェクト概要

このプロジェクトは、**40ファイル以上に及ぶ大量のTXT, CSV, Excelファイルを、簡単かつ正確にSQLiteデータベースへ一括でインポートする**ことを主な目的としたツール群です。

インポート処理の自動化・効率化に加え、インポートされたデータが正しく格納されたかをスムーズに検証するためのGUIビューア `SQLite_GUI_Manager` を提供します。

**主な目的:**
- **データ集約:** 散在するフラットファイルを、単一のSQLiteデータベースに統合します。
- **データ検証:** GUIツールを使って、インポート結果を直感的に確認、検索、検証します。

## 2. 主要機能

### ① データインポート機能 (コア機能)

- **`universal_csv_txt_to_sqlite.py`**
  - CSV, TXT, TSVファイルをSQLiteにインポートします。
  - ファイル名や設定ファイル(`csv_txt_config.json`)に基づき、区切り文字やエンコーディングを自動的に判別します。
  - `--batch` オプションにより、指定フォルダ内の全ファイルを一括で処理できます。
- **`universal_excel_to_sqlite.py`**
  - Excelファイル（.xlsx, .xls）の各シートを個別のテーブルとしてSQLiteにインポートします。
  - 設定ファイル(`excel_config.json`)に基づき、ヘッダー行の指定などが可能です。

### ② データ検証GUIツール (`SQLite_GUI_Manager.py`)

インポート結果の検証作業を効率化するためのツールです。

- **データベースブラウザ:** インポートしたDBのテーブル一覧を直感的に表示します。
- **データビューア:** テーブルデータをGUIで表示し、ソートや検索が可能です。
- **データ検証機能:**
    - 日付や数字のみのコードなど、意図したデータ型で格納されているかを確認・修正。
    - 元ファイルとの突合による、格納漏れチェック機能 (結果はCSVに出力)。
- **SQL実行機能:** 検証のために、カスタムSQLクエリを実行して結果を確認できます。
- **テーブル操作:**
    - 個別のテーブル削除と再格納。
    - テーブルの全データ削除とVACUUMによるDB最適化。

## 3. 技術スタック

- **言語**: Python 3.8+
- **GUI**: tkinter
- **データ処理**: pandas

## 4. ファイル構成

```
sqlite-gui-manager/
├── README.md                       # このファイル
├── universal_csv_txt_to_sqlite.py  # CSV/TXTインポートツール
├── universal_excel_to_sqlite.py    # Excelインポートツール
├── SQLite_GUI_Manager.py           # データ検証用GUIツール
├── csv_txt_config.json             # CSV/TXTインポート設定
└── excel_config.json               # Excelインポート設定
```

## 5. 使用方法

### Step 1: データのインポート

ターミナル（コマンドプロンプトやPowerShell）で、以下のスクリプトを実行します。

**例1: 指定フォルダ内の全CSV/TXTを一括インポート**
```bash
python universal_csv_txt_to_sqlite.py [テキストファイルのあるフォルダパス] [DBファイル名] --batch
```

**例2: 個別のExcelファイルをインポート**
```bash
python universal_excel_to_sqlite.py [Excelファイルのパス] [DBファイル名]
```

### Step 2: インポート結果の検証

インポートが完了したら、GUIツールを起動して結果を確認します。

```bash
python SQLite_GUI_Manager.py
```
GUIが起動したら、「ファイル」メニューからインポート時に作成・更新したDBファイルを選択してください。

## 6. 開発ロードマップ

- [x] コア機能：CSV/TXT/Excelのインポートスクリプト作成
- [x] コア機能：インポート結果を検証するための基本GUIビューア作成
- [ ] GUIの検証機能強化（データ型チェック、格納漏れチェックなど）
- [ ] インポート設定ファイル(`config.json`)の機能拡充
- [ ] エラーハンドリングとログ出力の改善
