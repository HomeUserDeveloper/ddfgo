# Build script for ddfgo
# Simple build with UTF-8 output encoding

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$versionFile = "version.txt"
$exeName = "ddfgo.exe"
$mainFile = "ddfgo.go"

Write-Host ""
Write-Host "====== BUILD SYSTEM dlya ddfgo ======"
Write-Host ""

# Check if version file exists
if (-not (Test-Path $versionFile)) {
    Write-Host "Sozdayu fail versii..."
    "000.000.000.0001" | Out-File -Encoding UTF8 -FilePath $versionFile
}

# Read current version
$currentVersion = (Get-Content $versionFile -Encoding UTF8).Trim()
Write-Host "Tekushaya versiya: $currentVersion"

# Check if Go is installed
$goPath = Get-Command go -ErrorAction SilentlyContinue
if (-not $goPath) {
    Write-Host "Oshibka: Go ne ustanovlen ili ne v PATH"
    Read-Host "Nazhimte Enter"
    exit 1
}

Write-Host ""
Write-Host "====== KOMPILYATSIYA ======"
Write-Host ""

$newVersion = $currentVersion
Write-Host "Novaya versiya: $newVersion"
Write-Host ""

Write-Host "Kompilyatsiya ddfgo versiya $newVersion..."

# Compile with version embedded
& go build -ldflags "-X main.Version=$newVersion" -o $exeName $mainFile

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Oshibka: Kompilyatsiya ne udalas!"
    Read-Host "Nazhimte Enter"
    exit 1
}

Write-Host ""
Write-Host "====== KOMPILYATSIYA USPESHNA ======"
Write-Host ""

$fileInfo = Get-Item $exeName
Write-Host "Fail: $exeName"
Write-Host "Versiya: $newVersion"
Write-Host "Vremya: $($fileInfo.LastWriteTime)"
Write-Host ""
Write-Host "Informatsiya o faile:"
Write-Host " Razmer: $($fileInfo.Length) bayt"
Write-Host " Put: $($fileInfo.FullName)"
Write-Host ""
Write-Host "Versiya vstroenA v ispolnjaemyj fail. Proverte:"
Write-Host "  $exeName -version"
Write-Host ""

Read-Host "Nazhimte Enter dlya vykhoda"
