# Create large test dataset with duplicates
$testDir = 'd:\proj\ddfgo\test_duplicates_large'
if (-not (Test-Path $testDir)) { mkdir $testDir | Out-Null }

cd $testDir

# Create large file content (10MB each)
$largeContent = ('X' * (10 * 1024 * 1024))

"Original large file 1" | Out-File -FilePath 'large_1.txt' -Encoding UTF8

# Write actual large content
[System.IO.File]::WriteAllText((Join-Path $testDir 'large_original_1.bin'), $largeContent)

# Create duplicates
for ($i = 1; $i -le 5; $i++) {
    Copy-Item 'large_original_1.bin' "large_duplicate_1_$i.bin"
}

# Create another large file group
[System.IO.File]::WriteAllText((Join-Path $testDir 'large_original_2.bin'), ('Y' * (5 * 1024 * 1024)))
for ($i = 1; $i -le 3; $i++) {
    Copy-Item 'large_original_2.bin' "large_duplicate_2_$i.bin"
}

# Check file count and size
$files = Get-ChildItem
$totalSize = ($files | Measure-Object -Sum Length).Sum
$fileCount = $files.Count

Write-Host "Files created: $fileCount"
Write-Host "Total size: $($totalSize / 1MB) MB"
Write-Host "Location: $testDir"
