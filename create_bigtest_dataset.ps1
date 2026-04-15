param(
    [string]$TestDir  = "d:\ddfgo_largetest",
    [switch]$SkipClean
)

$ErrorActionPreference = 'Stop'
$sw = [System.Diagnostics.Stopwatch]::StartNew()

function Log {
    param([string]$Msg, [string]$Color = 'Cyan')
    $t = [math]::Round($sw.Elapsed.TotalSeconds, 1)
    Write-Host "[+${t}s] $Msg" -ForegroundColor $Color
}

# Pre-flight
$drive = (Split-Path -Qualifier $TestDir).TrimEnd(':')
$freeGB = [math]::Round((Get-PSDrive $drive).Free / 1GB, 1)
Log "Free on ${drive}: $freeGB GB" 'Green'
if ($freeGB -lt 160) { Write-Error "Need 160 GB free. Aborting."; exit 1 }

if (-not $SkipClean -and (Test-Path $TestDir)) {
    Log "Removing $TestDir ..." 'Yellow'
    Remove-Item $TestDir -Recurse -Force
}
$null = New-Item $TestDir -ItemType Directory -Force
Log "TestDir: $TestDir"

# Write one file of $Size bytes, unique per $GroupId
function Write-Original {
    param([string]$Path, [long]$Size, [int]$Gid)
    $dir = Split-Path $Path -Parent
    if (-not (Test-Path $dir)) { $null = New-Item $dir -ItemType Directory -Force }

    $hdrText = ("GRP{0:D6}DDFGO_PERF_TEST_" -f $Gid).PadRight(512, 'X').Substring(0, 512)
    $header  = [System.Text.Encoding]::ASCII.GetBytes($hdrText)

    $blkSz = 4 * 1024 * 1024
    $fill  = New-Object byte[] $blkSz
    $seed  = [byte](($Gid * 37) % 251)
    for ($i = 0; $i -lt $blkSz; $i++) { $fill[$i] = [byte](($seed + $i) % 256) }

    $fs = [System.IO.File]::OpenWrite($Path)
    $fs.Write($header, 0, $header.Length)
    $rem = $Size - $header.Length
    while ($rem -gt 0) {
        $n = [Math]::Min($rem, [long]$blkSz)
        $fs.Write($fill, 0, [int]$n)
        $rem -= $n
    }
    $fs.Close()
}

function Write-Copy {
    param([string]$Src, [string]$Dst)
    $dir = Split-Path $Dst -Parent
    if (-not (Test-Path $dir)) { $null = New-Item $dir -ItemType Directory -Force }
    Copy-Item -LiteralPath $Src -Destination $Dst
}

# Dataset spec
# Total ~142 GB:
#  01: 100 x 50KB  x 6 =   30 MB
#  02:  50 x 200KB x 5 =   50 MB
#  03:  30 x 10MB  x 4 =  1.2 GB
#  04:  20 x 50MB  x 4 =    4 GB
#  05:   8 x 500MB x 4 =   16 GB
#  06:   4 x 1GB   x 3 =   12 GB
#  07:   8 x 2GB   x 4 =   64 GB
#  08:   3 x 5GB   x 3 =   45 GB
#  Total                = ~142.3 GB

$specs = @(
    [pscustomobject]@{ Dir="01_small_50KB";  Size=[long](50*1KB);   Orig=100; CN=5 }
    [pscustomobject]@{ Dir="02_small_200KB"; Size=[long](200*1KB);  Orig=50;  CN=4 }
    [pscustomobject]@{ Dir="03_medium_10MB"; Size=[long](10*1MB);   Orig=30;  CN=3 }
    [pscustomobject]@{ Dir="04_medium_50MB"; Size=[long](50*1MB);   Orig=20;  CN=3 }
    [pscustomobject]@{ Dir="05_large_500MB"; Size=[long](500*1MB);  Orig=8;   CN=3 }
    [pscustomobject]@{ Dir="06_large_1GB";   Size=[long](1024*1MB); Orig=4;   CN=2 }
    [pscustomobject]@{ Dir="07_xlarge_2GB";  Size=[long](2048*1MB); Orig=8;   CN=3 }
    [pscustomobject]@{ Dir="08_xlarge_5GB";  Size=[long](5120*1MB); Orig=3;   CN=2 }
)

$totalFiles = ($specs | ForEach-Object { $_.Orig * (1 + $_.CN) } | Measure-Object -Sum).Sum
$totalBytes = ($specs | ForEach-Object { $_.Size * $_.Orig * (1 + $_.CN) } | Measure-Object -Sum).Sum
$totalGB    = [math]::Round($totalBytes / 1GB, 1)
$expDupes   = ($specs | ForEach-Object { $_.Orig * $_.CN } | Measure-Object -Sum).Sum

Log "Plan: $totalFiles files, $totalGB GB, expected duplicates: $expDupes"
Log "ETA on SSD ~400 MB/s: approx $([math]::Round($totalGB * 1024 / 400 / 60, 0)) min"

$donBytes = [long]0
$donFiles = 0
$gid      = 0

foreach ($s in $specs) {
    $grpGB = [math]::Round($s.Size * $s.Orig * (1 + $s.CN) / 1GB, 2)
    Log "Group [$($s.Dir)]  $([math]::Round($s.Size/1MB,1)) MB/file  $($s.Orig) orig x $($s.CN) copies = $grpGB GB" 'Yellow'

    $sub = Join-Path $TestDir $s.Dir

    for ($o = 0; $o -lt $s.Orig; $o++) {
        $gid++
        $origPath = Join-Path $sub ("orig_{0:D5}_{1:D4}.dat" -f $gid, $o)

        $skip = $false
        if ($SkipClean -and (Test-Path $origPath)) {
            if ((Get-Item $origPath).Length -eq $s.Size) { $skip = $true }
        }
        if (-not $skip) { Write-Original -Path $origPath -Size $s.Size -Gid $gid }
        $donBytes += $s.Size
        $donFiles++

        for ($c = 1; $c -le $s.CN; $c++) {
            $cpPath = Join-Path $sub ("copy_{0:D5}_{1}_{2:D4}.dat" -f $gid, $c, $o)
            $skipC  = $false
            if ($SkipClean -and (Test-Path $cpPath)) {
                if ((Get-Item $cpPath).Length -eq $s.Size) { $skipC = $true }
            }
            if (-not $skipC) { Write-Copy -Src $origPath -Dst $cpPath }
            $donBytes += $s.Size
            $donFiles++
        }

        $pct  = [math]::Round($donBytes * 100.0 / $totalBytes, 1)
        $gbD  = [math]::Round($donBytes  / 1GB, 2)
        $mbps = if ($sw.Elapsed.TotalSeconds -gt 1) {
            [math]::Round($donBytes / 1MB / $sw.Elapsed.TotalSeconds)
        } else { 0 }
        $eta = if ($mbps -gt 0) {
            [math]::Round(($totalBytes - $donBytes) / 1MB / $mbps / 60, 1)
        } else { '?' }
        Write-Host ("  {0}/{1} files  {2:F2}/{3} GB  {4}%  {5} MB/s  ETA {6} min" -f `
            $donFiles, $totalFiles, $gbD, $totalGB, $pct, $mbps, $eta) -ForegroundColor Gray
    }
    Log "  Done: [$($s.Dir)]" 'Green'
}

$sw.Stop()
$stat = Get-ChildItem $TestDir -Recurse -File | Measure-Object -Property Length -Sum
Log ""
Log "=== GENERATION COMPLETE ===" 'Green'
Log "Files: $($stat.Count)   Size: $([math]::Round($stat.Sum/1GB,1)) GB   Time: $([math]::Round($sw.Elapsed.TotalMinutes,1)) min" 'Green'
Log ""
Log "Run test:" 'Cyan'
Log "  cd d:\proj\ddfgo-test" 'White'
Log "  .\ddfgo.exe -dir '$TestDir' -test -clean -all -cpuprofile cpu_large.prof -memprofile mem_large.prof" 'White'
Log ""
Log "Cleanup:" 'Cyan'
Log "  Remove-Item '$TestDir' -Recurse -Force" 'White'
