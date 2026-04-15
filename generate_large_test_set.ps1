param(
    [int]$NumFiles = 1000,
    [int]$FileSize = 50KB,
    [string]$OutputDir = ".\large_test_data"
)

Write-Host "Generating large test dataset..." -ForegroundColor Cyan
Write-Host "Number of files: $NumFiles" -ForegroundColor Yellow
Write-Host "File size: $([Math]::Round($FileSize/1KB, 2)) KB" -ForegroundColor Yellow
Write-Host ""

if (Test-Path $OutputDir) {
    Write-Host "Removing existing test directory..." -ForegroundColor Yellow
    Remove-Item $OutputDir -Recurse -Force
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# Generate base content
$baseContent = [string]::Empty
for ($i = 0; $i -lt 100; $i++) {
    $baseContent += "This is test content line $i with some data patterns for file hashing.`n"
}
$baseContent = $baseContent * 20  # Repeat to reach target size

Write-Host "Creating duplicate sets..." -ForegroundColor Yellow

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

# Create 10 groups of $NumFiles/10 identical files
$groupSize = [Math]::Max(10, $NumFiles / 10)
$NumGroups = [Math]::Ceiling($NumFiles / $groupSize)

for ($group = 0; $group -lt $NumGroups; $group++) {
    $filesInGroup = [Math]::Min($groupSize, $NumFiles - ($group * $groupSize))
    
    for ($i = 0; $i -lt $filesInGroup; $i++) {
        $fileNum = $group * $groupSize + $i
        $fileName = "file_$($group.ToString('D3'))_$($i.ToString('D3')).dat"
        $filePath = Join-Path $OutputDir $fileName
        
        # Create content variations for same group (identical files)
        $content = $baseContent + "`n[GROUP $group MEMBER $i]`n"
        
        $content | Out-File -FilePath $filePath -Encoding ASCII -NoNewline
        
        if (($fileNum + 1) % 100 -eq 0) {
            Write-Host "  Created $($fileNum + 1)/$NumFiles files" -ForegroundColor Gray
        }
    }
}

$stopwatch.Stop()

Write-Host "Test data generation completed in $($stopwatch.ElapsedMilliseconds) ms" -ForegroundColor Green
Write-Host ""

# Show statistics
$files = Get-ChildItem -Path $OutputDir -File
$totalSize = ($files | Measure-Object -Property Length -Sum).Sum
$avgSize = $totalSize / $files.Count

Write-Host "Dataset Statistics:" -ForegroundColor Cyan
Write-Host "  Total files: $($files.Count)" -ForegroundColor Yellow
Write-Host "  Total size: $([Math]::Round($totalSize/1MB, 2)) MB" -ForegroundColor Yellow
Write-Host "  Average file size: $([Math]::Round($avgSize/1KB, 2)) KB" -ForegroundColor Yellow
Write-Host ""
Write-Host "Dataset prepared at: $OutputDir" -ForegroundColor Green
