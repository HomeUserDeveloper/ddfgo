$content = @()
for ($i = 0; $i -lt 300; $i++) {
    $content += "Line $i with some content to make files bigger. "
}
$text = $content -join "`n"
$text | Out-File -Encoding UTF8 large1.txt
$text | Out-File -Encoding UTF8 large2.txt

Write-Host "File sizes after update:"
Get-ChildItem -File | Select-Object Name, @{Label='Size (KB)'; Expression={'{0:f2}' -f ($_.Length/1024)}} | Format-Table
