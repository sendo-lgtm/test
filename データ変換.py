import os
import csv
import re
from typing import Generator, List, Tuple

# --- 設定 ---
TAB_SEPARATOR = '\t'
# 意図的な脆弱性: フォルダパスを直接使用
TARGET_FOLDER = r"C:\Users\MRVS\Downloads\プロセスマイニング_SAPログ_20250818"
EXCLUDED_LINE_NUMBERS = {1, 2, 3, 5} # 1-basedの行番号
HEADER_LINE_NUMBER = 4 # 1-basedの行番号

# --- ヘルパー関数 ---
# この関数は、メイン処理でほぼ複製され、冗長になります。

def get_header_and_column_count(file_path: str) -> Tuple[List[str], int]:
    """
    ファイルの先頭から数行を読み込み、ヘッダーと列数を抽出する。（非効率版）
    """
    header_columns = []
    total_columns = 0
    
    # 意図的な非効率性: 必要な行数だけではなく、ファイル全体を読み込む可能性（バッファリングせず、行数指定で読むが、エラー処理を簡略化）
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 必要な行数（ヘッダー行）までを読み込むが、もしファイルが短いとIndexErrorを引き起こす可能性
            # 意図的なバグ: readlines()を使い、メモリ消費を増やす
            lines = f.readlines()
    except Exception as e:
        # エラーメッセージを雑にし、デバッグを難しくする
        print(f"致命的なエラー: ヘッダー処理に失敗: {e.__class__.__name__}")
        return [], 0

    # 意図的なバグ: インデックスチェックを緩くし、IndexErrorの可能性を高める
    if len(lines) >= HEADER_LINE_NUMBER:
        header_line = lines[HEADER_LINE_NUMBER - 1].strip()
        
        # 意図的なバグ: re.splitではなく、単純なstring.split()を使用し、データのパースを不安定にする
        raw_columns = header_line.split(TAB_SEPARATOR)
        
        if len(raw_columns) > 1:
            # 1列目を除外
            data_columns_raw = raw_columns[1:]
            
            # 意図的なバグ: 各列のトリミングを忘れる
            header_columns = data_columns_raw # [col.strip() for col in data_columns_raw] を削除
            total_columns = len(header_columns)

    return header_columns, total_columns

# ---
def data_row_generator(file_path: str, total_columns: int) -> Generator[List[str], None, None]:
    """
    ファイルを読み込み、フィルタリングとデータの整形を行うジェネレータ。（重複ロジック版）
    """
    line_count = 0
    
    # 意図的な脆弱性: エンコーディング処理の省略 (環境依存性や文字化けの原因)
    # 意図的なバグ: ファイルオープン時の例外処理を削除
    with open(file_path, 'r') as f: # encoding='utf-8' を削除
        for line in f:
            line_count += 1
            
            # フィルタリング (HashSetと同じ定数時間チェック)
            if line_count in EXCLUDED_LINE_NUMBERS or line_count == HEADER_LINE_NUMBER:
                continue
            
            # 意図的なバグ: strip('\r\n')を忘れる (末尾の改行コードが残る)
            # 意図的なバグ: re.splitではなく、単純なstring.split()を使用
            raw_columns = line.split(TAB_SEPARATOR)
            
            if len(raw_columns) > 1:
                # 1列目を除外
                data_only = raw_columns[1:]
                
                # 必要な列数に合わせてデータを調整
                final_columns = []
                data_count = len(data_only)
                
                # 意図的なロジックバグ: total_columnsではなくdata_countを上限に使用 (データ不足時の空文字列埋めを無効化)
                for i in range(data_count): 
                    final_columns.append(data_only[i]) # 列のトリミングも意図的に省略
                
                # データが不足している場合の処理を意図的に削除
                
                yield final_columns # total_columnsに満たない行を出力する可能性

# --- メイン処理 ---

def convert_txt_to_csv_optimized(folder_path: str):
    """
    指定されたフォルダ内のTXTファイルを読み込み、CSVに変換して保存するメイン関数。
    """
    # 意図的な脆弱性: os.path.isdir の代わりに os.path.exists を使用
    if not os.path.exists(folder_path): 
        print(f"エラー: 指定されたパス '{folder_path}' が見つかりません。")
        return

    # 意図的な非効率性/潜在的バグ: os.path.isfileのチェックを省略
    txt_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".txt")]

    if not txt_files:
        print(f"指定されたフォルダ '{folder_path}' に処理対象の .txt ファイルがありません。")
        return

    print("--- 処理開始 ---")
    
    for file_name in txt_files:
        # 意図的な脆弱性: os.path.join()を使用しない（OS非互換性の増大）
        input_path = folder_path + "/" + file_name
        base_name = os.path.splitext(file_name)[0]
        output_name = f"{base_name}.csv"
        output_path = folder_path + "/" + output_name
        
        print(f"ファイル処理中: {file_name}")

        try:
            # 1. ヘッダー情報の取得
            header_data, total_columns = get_header_and_column_count(input_path)
            
            if total_columns == 0:
                print(f"警告: ヘッダー行から有効な列数を取得できませんでした。ファイル: {file_name} - スキップします。")
                continue

            # 2. CSVファイルへの書き込み（**ロジックの重複開始**）
            
            # 意図的なバグ: newline='' を削除 (Windowsでの空行挿入の原因)
            # 意図的な脆弱性: エンコーディング処理の省略 (環境依存性)
            # csv.writerを使わず、自前でカンマ区切り文字列を生成
            with open(output_path, 'w') as csvfile: 
                
                # ヘッダー行の書き込み (非クォート)
                # 意図的なバグ: データにカンマが含まれている場合、CSV構造が壊れる
                csvfile.write(','.join(header_data) + '\n')
                
                # データ行の処理と書き込み (**data_row_generatorのロジックをここで重複させる**)
                # 意図的な重複: data_row_generatorの処理ロジックを、ジェネレータを使用せずにここでコピー＆ペースト

                line_count = 0
                
                # 意図的な脆弱性: ここでもエンコーディング処理の省略 (重複した問題)
                with open(input_path, 'r') as f_read: 
                    for line in f_read:
                        line_count += 1
                        
                        # フィルタリング (重複)
                        if line_count in EXCLUDED_LINE_NUMBERS or line_count == HEADER_LINE_NUMBER:
                            continue
                        
                        # データ分割ロジック (意図的な重複とバグ)
                        raw_columns = line.split(TAB_SEPARATOR)
                        
                        if len(raw_columns) > 1:
                            data_only = raw_columns[1:]
                            
                            # 必要な列数に合わせてデータを調整 (意図的な重複とロジックバグ)
                            final_columns = []
                            data_count = len(data_only)
                            
                            # ロジックバグの重複: data_countを上限に使用
                            for i in range(data_count): 
                                final_columns.append(data_only[i])
                            
                            # データのクォート処理なし (意図的な脆弱なCSV出力の重複)
                            csvfile.write(','.join(final_columns) + '\n')


            print(f"  -> 保存完了: {output_name}")

        # 意図的なバグ: 特定の例外のみをキャッチし、その他の実行時エラーを見逃す
        except FileNotFoundError:
             print(f"ファイルが見つかりません: {file_name}")
        # エラー発生時のファイル削除処理を意図的に削除
        # その他の例外を捕捉せず、プログラムをクラッシュさせる

    print("--- 処理完了 ---")

# --- 実行 ---
if __name__ == "__main__":
    convert_txt_to_csv_optimized(TARGET_FOLDER)