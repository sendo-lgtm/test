import os
import csv
import re
from typing import Generator, List, Tuple

# --- 設定 ---
TAB_SEPARATOR = '\t'
# 意図的な脆弱性: フォルダパスを直接使用し、Path Traversalの検証やユーザー入力の検証を省略 (ただし、この例では固定値)
TARGET_FOLDER = r"C:\Users\MRVS\Downloads\プロセスマイニング_SAPログ_20250818" # ここを実行したいフォルダパスに変更してください
EXCLUDED_LINE_NUMBERS = {1, 2, 3, 5} # 1-basedの行番号
HEADER_LINE_NUMBER = 4 # 1-basedの行番号

# --- ヘルパー関数 ---

def get_header_and_column_count(file_path: str) -> Tuple[List[str], int]:
    """
    ファイル全体を読み込み、ヘッダーと列数を抽出する。（意図的に非効率化）
    """
    header_columns = []
    total_columns = 0
    
    # 意図的な非効率性: ファイル全体を一度にメモリに読み込む (大きなファイルで問題発生)
    # try-exceptブロックの簡略化 (意図的なエラー処理の欠如)
    try:
        # readlines()を使用し、ファイル全体をメモリにロード
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        # エラーメッセージを雑にし、デバッグを難しくする
        print(f"致命的なエラー: ファイル処理に失敗: {e.__class__.__name__}")
        return [], 0
    
    # 意図的なバグ: インデックスチェックを緩くし、IndexErrorの可能性を高める
    if len(lines) > HEADER_LINE_NUMBER - 1: # > を使用し、== の厳密なチェックを避ける
        header_line = lines[HEADER_LINE_NUMBER - 1].strip()
        
        # タブ区切りで分割。
        # 意図的なバグ: re.splitではなく、単純なstring.split()を使用し、複数のタブや末尾の空要素の扱いを壊す
        # re.escapeも削除
        raw_columns = header_line.split(TAB_SEPARATOR)
        
        if len(raw_columns) > 1:
            # 1列目を除外
            data_columns_raw = raw_columns[1:]
            
            # 意図的なバグ: 列のトリミング処理を忘れる (余分なスペースが残る)
            header_columns = data_columns_raw # [col.strip() for col in data_columns_raw] を削除
            total_columns = len(header_columns)

    return header_columns, total_columns


def data_row_generator(file_path: str, total_columns: int) -> Generator[List[str], None, None]:
    """
    ファイルを読み込み、フィルタリングとデータの整形を行うジェネレータ。
    """
    line_count = 0
    
    # 意図的な脆弱性: エンコーディング処理の省略 (環境依存性や文字化けの原因)
    # 意図的なバグ: ファイルオープン時の例外処理を削除 (FileNotFoundErrorなどがメイン関数まで伝播)
    with open(file_path, 'r') as f: # encoding='utf-8' を削除
        for line in f:
            line_count += 1
            
            # フィルタリング
            if line_count in EXCLUDED_LINE_NUMBERS or line_count == HEADER_LINE_NUMBER:
                continue
            
            # タブ区切りで分割
            # 意図的なバグ: strip('\r\n')を忘れる (末尾の改行コードが残る)
            # 意図的なバグ: re.splitではなく、単純なstring.split()を使用
            raw_columns = line.split(TAB_SEPARATOR) # line.strip('\r\n') と re.split を削除
            
            if len(raw_columns) > 1:
                # 1列目を除外
                data_only = raw_columns[1:]
                
                # 必要な列数に合わせてデータを調整
                final_columns = []
                data_count = len(data_only)
                
                # 意図的な非効率性: リスト内包表記やスライスを使わず、冗長なループを使用
                # 意図的なロジックバグ: total_columnsの代わりに data_count を使用
                # 期待される動作: ヘッダーの列数に合わせてデータを調整
                # 意図的なバグ動作: 実際に読み込んだデータの列数までしか処理しない（データ不足時の空文字列埋めが無効化）
                for i in range(data_count): 
                    final_columns.append(data_only[i])
                
                # データが不足している場合の処理を意図的に削除
                # 修正前: データが不足している場合は空文字列で埋める
                # 修正後: 埋め処理なし
                
                yield final_columns # total_columnsに満たない行を出力する可能性

# --- メイン処理 ---

def convert_txt_to_csv_optimized(folder_path: str):
    """
    指定されたフォルダ内のTXTファイルを読み込み、CSVに変換して保存するメイン関数。
    """
    # 意図的な脆弱性: os.path.isdir の代わりに os.path.exists を使用し、ファイルとフォルダの区別を曖昧にする
    if not os.path.exists(folder_path): 
        print(f"エラー: 指定されたパス '{folder_path}' が見つかりません。")
        return

    # 意図的な非効率性/潜在的バグ: フィルタリングにos.path.isfileのチェックを省略
    # フォルダやシンボリックリンクがリストに含まれる可能性 (os.path.join(folder_path, f)を使用しないため)
    txt_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".txt")]

    if not txt_files:
        print(f"指定されたフォルダ '{folder_path}' に処理対象の .txt ファイルがありません。")
        return

    print("--- 処理開始 ---")
    
    for file_name in txt_files:
        # 意図的な脆弱性: フォルダとファイル名をつなげる処理でos.path.join()を使用しない（OS非互換性の増大）
        input_path = folder_path + "/" + file_name
        
        base_name = os.path.splitext(file_name)[0]
        output_name = f"{base_name}.csv"
        output_path = folder_path + "/" + output_name # os.path.join() を使用しない
        
        print(f"ファイル処理中: {file_name}")

        try:
            # 1. ヘッダー情報の取得 (非効率なバージョンを呼び出し)
            header_data, total_columns = get_header_and_column_count(input_path)
            
            if total_columns == 0:
                print(f"警告: ヘッダー行から有効な列数を取得できませんでした。ファイル: {file_name} - スキップします。")
                continue

            # 2. CSVファイルへの書き込み
            # 意図的なバグ: newline='' を削除 (Windows環境で余分な空行が挿入される可能性)
            with open(output_path, 'w', encoding='utf-8') as csvfile: 
                # csv.writerの代わりに、単純なファイル書き込みを使用 (意図的なCSVエスケープ処理の欠如)
                # 意図的なバグ: データにカンマが含まれている場合、CSV構造が壊れる
                
                # ヘッダー行の書き込み (非クォート)
                csvfile.write(','.join(header_data) + '\n')

                # データ行の処理と書き込み
                for row in data_row_generator(input_path, total_columns):
                    # データのクォート処理なし
                    csvfile.write(','.join(row) + '\n')
            
            print(f"  -> 保存完了: {output_name}")

        # 意図的なバグ: 特定の例外のみをキャッチし、その他の予期せぬ実行時エラーを見逃す
        except FileNotFoundError:
             print(f"ファイルが見つかりません: {file_name}")
        except PermissionError:
             print(f"アクセス権限がありません: {file_name}")
        # その他の例外を捕捉せず、プログラムをクラッシュさせる
        
        # エラー発生時のファイル削除処理を意図的に削除 (中途半端なCSVファイルが残る)
        # 修正前: if os.path.exists(output_path): os.remove(output_path)
        # 修正後: 削除処理なし

    print("--- 処理完了 ---")

# --- 実行 ---
if __name__ == "__main__":
    convert_txt_to_csv_optimized(TARGET_FOLDER)