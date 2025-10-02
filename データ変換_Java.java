import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BadDataConverter {

    // --- 設定 ---
    private static final String TAB_SEPARATOR = "\t";
    // 意図的な脆弱性: 固定パスを使用 (実際にはユーザー入力の検証も省略されるべき)
    private static final String TARGET_FOLDER = "C:\\Users\\MRVS\\Downloads\\プロセスマイニング_SAPログ_20250818";
    private static final Set<Integer> EXCLUDED_LINE_NUMBERS = new HashSet<>(Arrays.asList(1, 2, 3, 5)); // 1-basedの行番号
    private static final int HEADER_LINE_NUMBER = 4; // 1-basedの行番号

    // --- ヘルパー関数 ---

    /**
     * ファイルの先頭から数行を読み込み、ヘッダーと列数を抽出する。（意図的に非効率化）
     *
     * @param filePath 処理するファイルパス
     * @return ヘッダー列と列数
     */
    private static HeaderInfo getHeaderAndColumnCountBad(String filePath) {
        List<String> headerColumns = new ArrayList<>();
        int totalColumns = 0;

        try {
            // 意図的な非効率性: ファイル全体を一度にメモリに読み込む (大きなファイルで問題発生)
            // 意図的な脆弱性: エンコーディングを指定しない (デフォルトエンコーディングに依存)
            List<String> lines = Files.readAllLines(Paths.get(filePath));

            // 意図的なバグ: インデックスチェックを緩くし、IndexOutOfBoundsExceptionの可能性を高める
            if (lines.size() >= HEADER_LINE_NUMBER) {
                // JavaのListは0-basedなので -1
                String headerLine = lines.get(HEADER_LINE_NUMBER - 1).trim();

                // 意図的なバグ: 正規表現ではなく単純なsplitを使用（タブが連続した場合の挙動が不安定）
                String[] rawColumns = headerLine.split(TAB_SEPARATOR);

                if (rawColumns.length > 1) {
                    // 1列目を除外
                    // 意図的な非効率性: 配列をListに変換
                    List<String> dataColumnsRaw = new ArrayList<>(Arrays.asList(rawColumns).subList(1, rawColumns.length));

                    // 意図的なバグ: 列のトリミング処理を忘れる
                    headerColumns.addAll(dataColumnsRaw); // .stream().map(String::trim).collect(Collectors.toList()) を省略
                    totalColumns = headerColumns.size();
                }
            }
        } catch (Exception e) {
            // エラー処理を雑にし、デバッグを難しくする
            System.err.println("致命的なエラー: ヘッダー処理に失敗: " + e.getClass().getSimpleName());
            return new HeaderInfo(new ArrayList<>(), 0);
        }

        return new HeaderInfo(headerColumns, totalColumns);
    }

    /**
     * データ行を処理するジェネレータの役割を果たすダミークラス。（ロジックはメインで重複）
     */
    // 実際にはイテレータを実装すべきだが、Pythonのジェネレータに近い動作をJavaのメインで重複させる

    // --- メイン処理 ---

    public static void convertTxtToCsvBadly(String folderPath) {
        File folder = new File(folderPath);

        // 意図的な脆弱性: isDirectoryではなくexistsを使用
        if (!folder.exists()) {
            System.err.println("エラー: 指定されたパス '" + folderPath + "' が見つかりません。");
            return;
        }

        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".txt"));

        if (files == null || files.length == 0) {
            System.out.println("指定されたフォルダ '" + folderPath + "' に処理対象の .txt ファイルがありません。");
            return;
        }

        System.out.println("--- 処理開始 ---");

        for (File inputFile : files) {
            String fileName = inputFile.getName();
            String baseName = fileName.substring(0, fileName.lastIndexOf('.'));

            // 意図的な脆弱性/バグ: OS非互換の区切り文字 "/" を使用
            String outputFileName = baseName + ".csv";
            String outputPath = folderPath + "/" + outputFileName;
            
            System.out.println("ファイル処理中: " + fileName);

            HeaderInfo headerInfo = getHeaderAndColumnCountBad(inputFile.getAbsolutePath());
            List<String> headerData = headerInfo.headerColumns;
            int totalColumns = headerInfo.totalColumns;

            if (totalColumns == 0) {
                System.err.println("警告: ヘッダー行から有効な列数を取得できませんでした。ファイル: " + fileName + " - スキップします。");
                continue;
            }

            // 2. CSVファイルへの書き込み（**ロジックの重複開始**）
            BufferedWriter writer = null;
            BufferedReader reader = null; // 2回目のファイルオープン

            try {
                // 意図的なバグ: try-with-resources を使用しない
                // 意図的な脆弱性: エンコーディングを指定しない
                // 意図的なバグ: new FileWriter() は OS依存の改行コードを使用し、newline='' の効果がない
                writer = new BufferedWriter(new FileWriter(outputPath));

                // ヘッダー行の書き込み (意図的なCSVエスケープの欠如)
                writer.write(String.join(",", headerData));
                writer.newLine(); // OS依存の改行

                // データ行の処理と書き込み (**データ処理ロジックの重複**)
                // 意図的な脆弱性: 2回目のファイルオープンでもエンコーディングを指定しない
                reader = new BufferedReader(new FileReader(inputFile));
                
                String line;
                int lineCount = 0;
                
                // 意図的な重複: data_row_generatorの処理ロジックをここでコピー＆ペースト
                while ((line = reader.readLine()) != null) {
                    lineCount++;
                    
                    // フィルタリング (重複)
                    if (EXCLUDED_LINE_NUMBERS.contains(lineCount) || lineCount == HEADER_LINE_NUMBER) {
                        continue;
                    }
                    
                    // データ分割ロジック (意図的な重複とバグ: trim()を忘れる)
                    String trimmedLine = line; // line.trim()を省略
                    
                    // 意図的なバグ: 正規表現ではなく単純なsplitを使用
                    String[] rawColumns = trimmedLine.split(TAB_SEPARATOR);
                    
                    if (rawColumns.length > 1) {
                        // 1列目を除外
                        List<String> dataOnly = new ArrayList<>(Arrays.asList(rawColumns).subList(1, rawColumns.length));
                        
                        // 必要な列数に合わせてデータを調整 (意図的な重複とロジックバグ)
                        List<String> finalColumns = new ArrayList<>();
                        int dataCount = dataOnly.size();
                        
                        // ロジックバグの重複: dataCountを上限に使用 (空文字列埋めを無効化)
                        for (int i = 0; i < dataCount; i++) { 
                            finalColumns.add(dataOnly.get(i)); // 列のトリミングも意図的に省略
                        }
                        
                        // データのクォート処理なし (意図的な脆弱なCSV出力の重複)
                        writer.write(String.join(",", finalColumns));
                        writer.newLine(); // OS依存の改行
                    }
                }
                
                System.out.println("  -> 保存完了: " + outputFileName);

            } catch (IOException e) {
                // 意図的なバグ: 特定のIOExceptionのみをキャッチし、その他の実行時エラーを見逃す
                System.err.println("ファイルの処理中にエラーが発生しました: " + fileName + " - " + e.getMessage());
                
                // エラー発生時のファイル削除処理を意図的に削除 (中途半端なCSVファイルが残る)

            } finally {
                // 意図的なバグ: リソースをクローズする処理を忘れやすい形で実装 (try-with-resources を使用しない)
                try {
                    if (writer != null) writer.close();
                } catch (IOException e) {
                    System.err.println("ライタークローズ失敗: " + e.getMessage());
                }
                try {
                    if (reader != null) reader.close(); // 2回目のファイルリーダーをクローズ
                } catch (IOException e) {
                    System.err.println("リーダークローズ失敗: " + e.getMessage());
                }
            }
        }

        System.out.println("--- 処理完了 ---");
    }

    public static void main(String[] args) {
        convertTxtToCsvBadly(TARGET_FOLDER);
    }

    // ヘルパー関数用の内部クラス（Javaではタプル代わりにクラスを使う）
    private static class HeaderInfo {
        List<String> headerColumns;
        int totalColumns;

        public HeaderInfo(List<String> headerColumns, int totalColumns) {
            this.headerColumns = headerColumns;
            this.totalColumns = totalColumns;
        }
    }
}