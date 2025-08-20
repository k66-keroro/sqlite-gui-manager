import os
import chardet

def detect_file_encoding(file_path):
    """
    ファイルのエンコードを検出する関数

    Args:
        file_path (str): 判定するファイルのパス

    Returns:
        str: 検出されたエンコード名
    """
    with open(file_path, 'rb') as f:
        # ファイルの先頭から一定量読み込む
        # chardetはファイル全体を読み込まなくても判定可能
        result = chardet.detect(f.read(100000))
        return result['encoding']

# フォルダ内のtxtとcsvファイルのエンコードを調べるメイン関数
def main():
    folder_path = r'C:\Users\sem3171\sqlite-gui-manager\テキスト'
    
    if not os.path.isdir(folder_path):
        print(f"エラー: 指定されたフォルダ '{folder_path}' が見つかりません。")
        return

    print(f"フォルダ '{folder_path}' 内のファイルのエンコードを調べます。\n")
    
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        
        if os.path.isfile(file_path) and (filename.endswith('.txt') or filename.endswith('.csv')):
            try:
                # 最初にCP932としてデコードを試みる
                with open(file_path, 'r', encoding='cp932') as f:
                    f.read() # ファイル全体を読み込み、デコードに成功するか確認
                    print(f"ファイル: {filename} -> エンコード: CP932 (正常に読み込み可能)")
            except UnicodeDecodeError:
                # CP932で失敗した場合、chardetで再度エンコードを推定
                try:
                    encoding = detect_file_encoding(file_path)
                    # 推定されたエンコードで再度デコードを試みる
                    with open(file_path, 'r', encoding=encoding) as f:
                        f.read()
                        print(f"ファイル: {filename} -> エンコード: {encoding} (chardetによる推定)")
                except Exception as e:
                    print(f"ファイル: {filename} -> エラー: {e}")

if __name__ == "__main__":
    main()