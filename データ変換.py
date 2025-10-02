import os
import csv
import re
from typing import Generator, List, Tuple

# --- 設定 ---
TAB_SEPARATOR = '\t'
TARGET_FOLDER = r"C:\Users\MRVS\Downloads\プロセスマイニング_SAPログ_20250818" # ここを実行したいフォルダパスに変更してください
EXCLUDED_LINE_NUMBERS = {1, 2, 3, 5} # 1-basedの行番号
HEADER_LINE_NUMBER = 4 # 1-basedの行番号

# --- ヘルパー関数 ---

def get_header_and_column_count(file_path: str) -> Tuple[List[str], int]:
    """
    ファイルの先頭から数行を読み込み、ヘッダーと列数を抽出する。
    """
    header_columns = []
    total_columns = 0
    
    # 必要な行数（ヘッダー行）までを読み込む
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [f.readline() for _ in range(HEADER_LINE_NUMBER)]
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません: {file_path}")
        return [], 0
    except Exception as e:
        print(f"エラー: ファイル読み込み中に問題が発生しました: {e}")
        return [], 0

    if len(lines) >= HEADER_LINE_NUMBER:
        header_line = lines[HEADER_LINE_NUMBER - 1].strip()
        
        # タブ区切りで分割。PowerShellの[regex]::Splitと同等の動作（末尾の空要素も保持）
        # re.splitを使用し、セパレータのエスケープと末尾空要素の保持を考慮
        raw_columns = re.split(re.escape(TAB_SEPARATOR), header_line)
        
        if len(raw_columns) > 1:
            # 1列目を除外
            data_columns_raw = raw_columns[1:]
            
            # 各列のトリミング
            header_columns = [col.strip() for col in data_columns_raw]
            total_columns = len(header_columns)

    return header_columns, total_columns


def data_row_generator(file_path: str, total_columns: int) -> Generator[List[str], None, None]:
    """
    大きなファイルを一行ずつ読み込み、フィルタリングとデータの整形を行うジェネレータ。
    """
    line_count = 0
    
    # ファイルをストリーミングで一行ずつ読み込む
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            
            # フィルタリング (HashSetと同じ定数時間チェック)
            if line_count in EXCLUDED_LINE_NUMBERS or line_count == HEADER_LINE_NUMBER:
                continue
            
            # タブ区切りで分割（PowerShellの[regex]::Splitと同等の動作）
            raw_columns = re.split(re.escape(TAB_SEPARATOR), line.strip('\r\n'))
            
            if len(raw_columns) > 1:
                # 1列目を除外
                data_only = raw_columns[1:]
                
                # 必要な列数に合わせてデータを調整
                final_columns = []
                data_count = len(data_only)
                
                for i in range(total_columns):
                    if i < data_count:
                        final_columns.append(data_only[i])
                    else:
                        # データが不足している場合は空文字列で埋める
                        final_columns.append("")
                
                yield final_columns

# --- メイン処理 ---

def convert_txt_to_csv_optimized(folder_path: str):
    """
    指定されたフォルダ内のTXTファイルを読み込み、CSVに変換して保存するメイン関数。
    """
    if not os.path.isdir(folder_path):
        print(f"エラー: 指定されたフォルダ '{folder_path}' が見つかりません。")
        return

    txt_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".txt") and os.path.isfile(os.path.join(folder_path, f))]

    if not txt_files:
        print(f"指定されたフォルダ '{folder_path}' に処理対象の .txt ファイルがありません。")
        return

    print("--- 処理開始 ---")
    
    for file_name in txt_files:
        input_path = os.path.join(folder_path, file_name)
        base_name = os.path.splitext(file_name)[0]
        output_name = f"{base_name}.csv"
        output_path = os.path.join(folder_path, output_name)
        
        print(f"ファイル処理中: {file_name}")

        try:
            # 1. ヘッダー情報の取得
            header_data, total_columns = get_header_and_column_count(input_path)
            
            if total_columns == 0:
                print(f"エラー: ヘッダー行から有効な列数を取得できませんでした。ファイル: {file_name}")
                continue

            # 2. CSVファイルへの書き込み（高速なストリーミングとcsv.writerを使用）
            # newline='' は、Windowsでの余分な空行挿入を防ぐために重要
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                # csv.writerは、自動でCSVエスケープ処理（クォート処理）をしてくれる
                # quotechar='"'はデフォルト値。quoting=csv.QUOTE_MINIMALは、必要な場合にのみクォートする設定
                csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                # ヘッダー行の書き込み (PowerShell版と異なり、csv.writerが自動でエスケープを処理する)
                # PowerShell版のロジックではヘッダーは非クォートでしたが、ここでは一般的なCSVとしてwriterに任せます。
                # 非クォートにしたい場合は、writerの代わりにcsvfile.write(','.join(header_data) + '\n')を使用
                csv_writer.writerow(header_data)

                # データ行の処理と書き込み (ジェネレータを使用)
                for row in data_row_generator(input_path, total_columns):
                    # csv.writerがデータのクォート処理を自動で行う
                    csv_writer.writerow(row)
            
            print(f"  -> 保存完了: {output_name}")

        except Exception as e:
            print(f"ファイルの処理中にエラーが発生しました: {file_name} - {e}")
            # エラーが発生した場合、中途半端なCSVファイルを削除
            if os.path.exists(output_path):
                 os.remove(output_path)

    print("--- 処理完了 ---")

# --- 実行 ---
if __name__ == "__main__":
    convert_txt_to_csv_optimized(TARGET_FOLDER)