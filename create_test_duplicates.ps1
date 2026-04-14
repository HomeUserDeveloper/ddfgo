# Создаем тестовый набор с дубликатами
$testDir = 'd:\proj\ddfgo\test_duplicates'
if (-not (Test-Path $testDir)) { mkdir $testDir | Out-Null }

cd $testDir

# Создаем несколько оригинальных файлов
"Original file 1 - Lorem ipsum dolor sit amet consectetur adipiscing elit" | Out-File -FilePath 'original_1.txt'
"Original file 2 - Different content with unique data" | Out-File -FilePath 'original_2.txt'
"Original file 3 - Another test file for duplication" | Out-File -FilePath 'original_3.txt'

# Создаем дубликаты для каждого файла
for ($i = 1; $i -le 10; $i++) {
    Copy-Item 'original_1.txt' "duplicate_1_$i.txt"
}

for ($i = 1; $i -le 8; $i++) {
    Copy-Item 'original_2.txt' "duplicate_2_$i.txt"
}

for ($i = 1; $i -le 5; $i++) {
    Copy-Item 'original_3.txt' "duplicate_3_$i.txt"
}

# Проверяем количество файлов
$fileCount = (Get-ChildItem | Measure-Object).Count
Write-Host "Создано файлов: $fileCount"
Write-Host "Тестовые данные готовы в: $testDir"
