function Convert-TxtToCsvWithFilteringOptimized {
    param(
        [Parameter(Mandatory=$true)]
        [string]$FolderPath
    )

    if (-not (Test-Path -Path $FolderPath -PathType Container)) {
        Write-Error "指定されたフォルダ '$FolderPath' が見つかりません。"
        return
    }

    $txtFiles = Get-ChildItem -Path $FolderPath -Filter "*.txt" -File

    if ($txtFiles.Count -eq 0) {
        Write-Host "指定されたフォルダ '$FolderPath' に処理対象の .txt ファイルがありません。"
        return
    }

    Write-Host "--- 処理開始 ---"
    
    foreach ($file in $txtFiles) {
        Write-Host "ファイル処理中: $($file.Name)"
        $separator = "`t" # タブ区切り
        # 空の要素を保持するオプションを指定
        $splitOptions = [System.StringSplitOptions]::None 
        $excludedLineNumbers = @(1, 2, 3, 5) # 1-based: 1, 2, 3, 5行目を削除
        $headerLineNumber = 4             # 1-based: 4行目をヘッダとして使用
        $totalColumns = 0                 # 最終的なヘッダーの総列数を保持する変数
        
        try {
            # 1. 最初の数行を個別に読み込み、ヘッダー行を抽出するために保持
            $initialContent = Get-Content -Path $file.FullName -TotalCount 5
            
            # 2. ヘッダ行の作成 (4行目を使用し、1列目を削除)
            $csvHeader = ""
            if ($initialContent.Count -ge $headerLineNumber) {
                # 4行目 (インデックスは3) を .Split() メソッドで分割し、空のセルを保持する
                $headerColumns = $initialContent[$headerLineNumber - 1].Split([char[]]"$separator", $splitOptions)
                
                if ($headerColumns.Count -gt 1) {
                    # 1列目 (インデックス0) 以外の列を取得
                    $dataColumns = $headerColumns[1..($headerColumns.Count - 1)]
                    
                    # **ヘッダーの総列数を記録 (これはデータ部分の列数)**
                    $totalColumns = $dataColumns.Count
                    
                    # ヘッダーのクォート処理 (カンマを含む場合)
                    $quotedHeaderColumns = $dataColumns | ForEach-Object {
                        # 🌟 修正箇所: カンマのチェックを "*,*" に変更 🌟
                        if ($_ -like "*,*") {
                            # リテラルのダブルクォーテーションを "" でエスケープ 
                            $escapedValue = $_ -replace '"', '""'
                            # ダブルクォート文字列で全体を囲む
                            "`"$escapedValue`""
                        } else {
                            $_
                        }
                    }
                    
                    # コンマ区切りで結合
                    $csvHeader = $quotedHeaderColumns -join ","
                }
            }
            
            # ヘッダー列数が取得できなかった場合はエラー
            if ($totalColumns -eq 0) {
                 Write-Error "ヘッダー行から有効な列数を取得できませんでした。ファイル: $($file.Name)"
                 continue
            }

            $newFileName = "$($file.BaseName).csv"
            $newFilePath = Join-Path -Path $FolderPath -ChildPath $newFileName

            # 3. CSVファイル内容の生成とパイプラインによる保存
            
            # ヘッダー行を出力ストリームの最初に置く
            $outputContent = @()
            if ($csvHeader -ne "") {
                $outputContent += $csvHeader
            }
            
            # データ行の処理をパイプラインで実行 (メモリ効率が高い)
            $lineCount = 0
            
            # Get-Content をストリームとして使用し、各行を処理
            $processedData = Get-Content -Path $file.FullName | ForEach-Object {
                $lineCount++
                
                # 削除対象の行 (1, 2, 3, 5行目) またはヘッダ行 (4行目) の場合はスキップ
                if ($excludedLineNumbers -contains $lineCount -or $lineCount -eq $headerLineNumber) {
                    return # スキップ
                }
                
                # データ行を .Split() メソッドで分割し、空のセルを保持する
                $columns = $_.Split([char[]]"$separator", $splitOptions)
                
                if ($columns.Count -gt 1) {
                    # 1列目 (インデックス0) を除外した、実際のデータ部分の列を取得
                    $dataOnly = $columns[1..($columns.Count - 1)]

                    # **データの列数がヘッダー列数より少ない場合に、空文字列で埋める処理**
                    if ($dataOnly.Count -lt $totalColumns) {
                        # 必要な空文字列の数を計算
                        $paddingCount = $totalColumns - $dataOnly.Count
                        
                        # 必要な数の空文字列を格納した配列を作成
                        $padding = 1..$paddingCount | ForEach-Object { "" } 
                        
                        # 既存のデータ配列に追加
                        $dataOnly += $padding
                    }
                    
                    # 結合時にヘッダーの列数より多くならないように、必要な数だけ切り出す
                    $finalColumns = $dataOnly[0..($totalColumns - 1)]

                    # カンマを含む要素にダブルクォーテーションを付与する処理
                    $quotedColumns = $finalColumns | ForEach-Object {
                        # 🌟 修正箇所: カンマのチェックを "*,*" に変更 🌟
                        if ($_ -like "*,*") {
                            # リテラルのダブルクォーテーションを "" でエスケープ 
                            $escapedValue = $_ -replace '"', '""'
                            # ダブルクォート文字列で全体を囲む
                            "`"$escapedValue`""
                        } else {
                            # カンマが含まれない場合、そのまま
                            $_
                        }
                    }
                    
                    # パイプラインで結果を出力（自動的に次の処理へ渡される）
                    $quotedColumns -join ","
                }
            }
            
            # ヘッダー行と処理されたデータ行を結合してファイルに出力
            $outputContent + $processedData | Out-File -FilePath $newFilePath -Encoding UTF8
            
            Write-Host "  -> 保存完了: $($newFileName)"
        }
        catch {
            Write-Error "ファイルの処理中にエラーが発生しました: $($file.Name) - $($_.Exception.Message)"
        }
    }
    
    Write-Host "--- 処理完了 ---"
} 

# --- 実行例 ---
# 処理したいファイルがあるフォルダのパスを指定してください。
$targetFolder = "C:\Users\MRVS\Downloads\プロセスマイニング_SAPログ_20250818"

Convert-TxtToCsvWithFilteringOptimized -FolderPath $targetFolder