# Create test dataset with duplicates
$testDir = 'd:\proj\ddfgo\test_duplicates'
if (-not (Test-Path $testDir)) { mkdir $testDir | Out-Null }

cd $testDir

# Create original files
"Original file 1 - Lorem ipsum dolor sit amet consectetur adipiscing elit" | Out-File -FilePath 'original_1.txt'
"Original file 2 - Different content with unique data for testing" | Out-File -FilePath 'original_2.txt'
"Original file 3 - Another test file for duplication testing" | Out-File -FilePath 'original_3.txt'

# Create duplicates for each file
for ($i = 1; $i -le 10; $i++) {
    Copy-Item 'original_1.txt' "duplicate_1_$i.txt"
}

for ($i = 1; $i -le 8; $i++) {
    Copy-Item 'original_2.txt' "duplicate_2_$i.txt"
}

for ($i = 1; $i -le 5; $i++) {
    Copy-Item 'original_3.txt' "duplicate_3_$i.txt"
}

# Check file count
$fileCount = (Get-ChildItem | Measure-Object).Count
Write-Host "Test files created: $fileCount"
Write-Host "Location: $testDir"
